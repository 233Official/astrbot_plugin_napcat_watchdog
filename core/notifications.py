"""Transition notification delivery, HTML cards, and pending catch-up state.

The notification layer deliberately knows nothing about WebSocket details.  It
accepts only committed state-machine transitions and the small amount of
display data needed by the notification card.  This keeps connection reasons,
tokens, and addresses out of both messages and persisted pending records.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import hashlib
import inspect
import json
import logging
import math
import os
import time
from collections import OrderedDict
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from .duration import format_natural_duration
from .profile import data_uri_is_valid, sanitize_nickname
from .state_machine import QQStatus, TransitionKind
from .subscriptions import SubscriptionRecord, SubscriptionStore

logger = logging.getLogger(__name__)

_SCHEMA_VERSION = 2
_MAX_ATTEMPTS = 3
_MAX_DEDUPE_IDS = 512
_RENDER_OPTIONS = {
    "full_page": True,
    "type": "jpeg",
    "quality": 90,
    "viewport_width": 812,
    "viewport_height": 320,
    "device_scale_factor_level": "high",
    "wait_until": "domcontentloaded",
}


async def render_html_image(
    renderer: Callable[..., Any] | None,
    template_path: Path,
    data: dict[str, Any],
    *,
    options: Mapping[str, Any] | None = None,
    diagnostic_label: str = "NapCat HTML rendering",
) -> Any | None:
    """Render a local HTML template and return only validated image data."""
    if not callable(renderer):
        return None
    try:
        template = template_path.read_text(encoding="utf-8")
        result = renderer(
            template,
            data,
            return_url=False,
            options=dict(options or _RENDER_OPTIONS),
        )
        if inspect.isawaitable(result):
            result = await result
        if _valid_image_result(result):
            return result
        logger.warning("%s returned invalid image data", diagnostic_label)
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("%s failed", diagnostic_label)
    return None


class NotificationPersistenceError(Exception):
    """Raised when pending notification state is corrupt or cannot be saved."""


@dataclass
class PendingNotification:
    """Minimal durable summary for one event and one exact UMO target."""

    event_id: str
    self_id: int
    event_type: str
    event_time: float
    duration_seconds: float | None
    umo: str
    owner_id: str
    attempts: int = 0
    # ``None`` is retained only for migrated v1 records whose event ID used
    # the old timestamp-based format.
    episode_id: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "self_id": self.self_id,
            "event_type": self.event_type,
            "event_time": self.event_time,
            "duration_seconds": self.duration_seconds,
            "umo": self.umo,
            "owner_id": self.owner_id,
            "attempts": self.attempts,
            "episode_id": self.episode_id,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> PendingNotification:
        return cls(
            event_id=data["event_id"],
            self_id=data["self_id"],
            event_type=data["event_type"],
            event_time=data["event_time"],
            duration_seconds=data.get("duration_seconds"),
            umo=data["umo"],
            owner_id=data["owner_id"],
            attempts=data.get("attempts", 0),
            episode_id=data.get("episode_id"),
        )


class PendingNotificationStore:
    """Atomic JSON store for pending notifications."""

    _ENTRY_KEYS = frozenset(
        {
            "event_id",
            "self_id",
            "event_type",
            "event_time",
            "duration_seconds",
            "umo",
            "owner_id",
            "attempts",
            "episode_id",
        }
    )

    def __init__(self, path: Path) -> None:
        self.path = path
        self._items: dict[str, PendingNotification] = {}
        # Ordered maps preserve deterministic oldest-first eviction while
        # remaining compatible with Python 3.10.
        self._seen: OrderedDict[str, None] = OrderedDict()
        self._completed: OrderedDict[str, None] = OrderedDict()
        self._needs_migration = False

    @property
    def count(self) -> int:
        return len(self._items)

    def all(self) -> list[PendingNotification]:
        return list(self._items.values())

    def get(self, notification_id: str) -> PendingNotification | None:
        return self._items.get(notification_id)

    def is_seen(self, event_id: str) -> bool:
        return event_id in self._seen

    def mark_seen(self, event_id: str) -> None:
        self._seen[event_id] = None
        self._seen.move_to_end(event_id)
        self._prune_dedupe()

    def discard_seen(self, event_id: str) -> None:
        self._seen.pop(event_id, None)

    def is_completed(self, event_id: str) -> bool:
        return event_id in self._completed

    def mark_completed(self, event_id: str) -> None:
        self._completed[event_id] = None
        self._completed.move_to_end(event_id)
        self._prune_dedupe()

    @property
    def needs_migration(self) -> bool:
        return self._needs_migration

    def upsert(self, item: PendingNotification) -> None:
        self._validate_item(item)
        self._items[item.event_id] = item
        self._prune_dedupe()

    def remove(self, notification_id: str) -> bool:
        removed = self._items.pop(notification_id, None) is not None
        self._prune_dedupe()
        return removed

    def drop_for_umo(self, umo: str) -> int:
        removed = [key for key, item in self._items.items() if item.umo == umo]
        for key in removed:
            del self._items[key]
        self._prune_dedupe()
        return len(removed)

    def load(self) -> None:
        if not self.path.is_file():
            return
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise NotificationPersistenceError(
                f"Corrupt pending notification file: {type(exc).__name__}"
            ) from exc

        if not isinstance(raw, dict):
            raise NotificationPersistenceError("Invalid pending notification envelope")
        schema_version = raw.get("_schema_version")
        if schema_version not in {1, _SCHEMA_VERSION}:
            raise NotificationPersistenceError(
                "Unsupported pending notification schema"
            )
        pending = raw.get("pending")
        if not isinstance(pending, dict):
            raise NotificationPersistenceError("Invalid pending notification entries")

        if schema_version == 1:
            if set(raw) != {"_schema_version", "pending"}:
                raise NotificationPersistenceError(
                    "Invalid legacy pending notification envelope"
                )
            entry_keys = self._ENTRY_KEYS - {"episode_id"}
            self._needs_migration = True
            seen: OrderedDict[str, None] = OrderedDict()
            completed: OrderedDict[str, None] = OrderedDict()
        else:
            if set(raw) == {"_schema_version", "pending"}:
                # Accept the intermediate v2 draft that added the episode
                # field before durable seen/completed arrays were introduced.
                seen = OrderedDict()
                completed = OrderedDict()
                self._needs_migration = True
            elif set(raw) == {
                "_schema_version",
                "pending",
                "seen",
                "completed",
            }:
                seen = _load_event_id_set(raw.get("seen"), "seen")
                completed = _load_event_id_set(raw.get("completed"), "completed")
                self._needs_migration = False
            else:
                raise NotificationPersistenceError(
                    "Invalid pending notification envelope"
                )
            entry_keys = self._ENTRY_KEYS

        loaded: dict[str, PendingNotification] = {}
        for key, value in pending.items():
            if not isinstance(key, str) or not isinstance(value, dict):
                raise NotificationPersistenceError(
                    "Invalid pending notification record"
                )
            value_keys = set(value)
            legacy_entry_keys = self._ENTRY_KEYS - {"episode_id"}
            if value_keys != entry_keys and not (
                schema_version == 2 and value_keys == legacy_entry_keys
            ):
                raise NotificationPersistenceError(
                    "Unexpected pending notification fields"
                )
            if value_keys == legacy_entry_keys:
                self._needs_migration = True
            try:
                item = PendingNotification.from_dict(value)
                self._validate_item(item)
            except (KeyError, TypeError, ValueError) as exc:
                raise NotificationPersistenceError(
                    "Invalid pending notification record values"
                ) from exc
            if item.event_id != key:
                raise NotificationPersistenceError("Pending notification key mismatch")
            loaded[key] = item
        self._items = loaded
        self._seen = seen
        self._completed = completed
        before = (len(self._seen), len(self._completed))
        self._prune_dedupe()
        if before != (len(self._seen), len(self._completed)):
            self._needs_migration = True

    def save(self) -> None:
        self._prune_dedupe()
        data = {
            "_schema_version": _SCHEMA_VERSION,
            "pending": {key: item.to_dict() for key, item in self._items.items()},
            "seen": list(self._seen),
            "completed": list(self._completed),
        }
        tmp_path = self.path.with_name(self.path.name + ".tmp")
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            fd = os.open(
                str(tmp_path),
                os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_SYNC,
                0o600,
            )
            try:
                os.write(
                    fd,
                    json.dumps(data, indent=2, sort_keys=True).encode("utf-8"),
                )
                os.fsync(fd)
            finally:
                os.close(fd)
            os.replace(str(tmp_path), str(self.path))
        except OSError as exc:
            try:
                tmp_path.unlink(missing_ok=True)
            except OSError:
                pass
            raise NotificationPersistenceError(
                f"Failed to persist pending notifications: {type(exc).__name__}"
            ) from exc

    @classmethod
    def _validate_item(cls, item: PendingNotification) -> None:
        if not item.event_id or not isinstance(item.event_id, str):
            raise ValueError("event_id must be non-empty")
        if isinstance(item.self_id, bool) or not isinstance(item.self_id, int):
            raise ValueError("self_id must be an integer")
        if item.self_id <= 0:
            raise ValueError("self_id must be positive")
        if item.event_type not in {"offline", "recovered"}:
            raise ValueError("invalid event_type")
        if isinstance(item.event_time, bool) or not isinstance(
            item.event_time, (int, float)
        ):
            raise ValueError("event_time must be numeric")
        if item.duration_seconds is not None and (
            isinstance(item.duration_seconds, bool)
            or not isinstance(item.duration_seconds, (int, float))
        ):
            raise ValueError("duration_seconds must be numeric or null")
        if not _is_valid_umo(item.umo) or not isinstance(item.owner_id, str):
            raise ValueError("invalid target context")
        if not item.owner_id:
            raise ValueError("owner_id must be non-empty")
        if isinstance(item.attempts, bool) or not isinstance(item.attempts, int):
            raise ValueError("attempts must be an integer")
        if not 0 <= item.attempts <= _MAX_ATTEMPTS:
            raise ValueError("attempts out of range")
        if item.episode_id is not None and (
            isinstance(item.episode_id, bool)
            or not isinstance(item.episode_id, int)
            or item.episode_id < 0
        ):
            raise ValueError("episode_id must be a non-negative integer or null")

    def _prune_dedupe(self) -> None:
        """Retain a bounded, deterministic recent-ID window.

        Current pending records are always protected: their base event IDs
        remain in ``seen`` and their target IDs remain in ``completed`` if a
        crash left both records present.
        """
        protected_seen = {
            item.event_id.rsplit(":", 1)[0]
            for item in self._items.values()
            if ":" in item.event_id
        }
        protected_completed = {item.event_id for item in self._items.values()}
        _evict_oldest(self._seen, protected_seen)
        _evict_oldest(self._completed, protected_completed)


# Short alias for callers that prefer the domain term.
PendingStore = PendingNotificationStore


class NotificationManager:
    """Deliver committed state transitions to current subscription UMOs."""

    def __init__(
        self,
        context: Any,
        subscription_store: SubscriptionStore,
        pending_path: Path,
        *,
        html_render: Callable[..., Any] | None = None,
        template_path: Path | None = None,
        retry_delay: float = 0.0,
        max_attempts: int = _MAX_ATTEMPTS,
        wall_clock: Callable[[], float] = time.time,
        profile_provider: Callable[[int], Any] | None = None,
        instance_label: str | None = None,
    ) -> None:
        self.context = context
        self.subscription_store = subscription_store
        self.pending = PendingNotificationStore(pending_path)
        self.html_render = html_render
        self.template_path = template_path or (
            Path(__file__).resolve().parents[1] / "templates" / "notification_card.html"
        )
        self.retry_delay = max(0.0, min(float(retry_delay), 2.0))
        self.max_attempts = max(1, min(int(max_attempts), _MAX_ATTEMPTS))
        self.wall_clock = wall_clock
        self.profile_provider = profile_provider
        self.instance_label = instance_label

    @property
    def pending_count(self) -> int:
        return self.pending.count

    def load(self) -> None:
        self.pending.load()
        if self.pending.needs_migration:
            # Rewrite v1 pending deliveries immediately so a restart during
            # the next delivery cycle cannot lose the migrated record.
            self.pending.save()

    def persist(self) -> None:
        self.pending.save()

    async def handle_transition(
        self,
        self_id: int,
        new_status: QQStatus,
        kind: TransitionKind,
        *,
        event_time: float | None = None,
        offline_since: float | None = None,
        online_since: float | None = None,
        episode_id: int | None = None,
        offline_episode_id: int | None = None,
    ) -> None:
        """Handle exactly one committed notification transition.

        Pending/offline signals never call this method in the state machine;
        the kind check is repeated here as a fail-closed boundary.
        """
        event_type = _event_type(kind)
        if event_type is None:
            return
        if event_type == "offline" and new_status != QQStatus.OFFLINE:
            return
        if event_type == "recovered" and new_status != QQStatus.ONLINE:
            return
        if isinstance(self_id, bool) or not isinstance(self_id, int) or self_id <= 0:
            return

        event_ts = _safe_timestamp(event_time, self.wall_clock())
        duration = None
        if event_type == "offline":
            duration = _duration_since(event_ts, online_since)
        elif event_type == "recovered":
            duration = _duration_since(event_ts, offline_since)
        if episode_id is None:
            episode_id = offline_episode_id
        base_id = _event_key(self_id, event_type, event_ts, episode_id)
        if self.pending.is_seen(base_id):
            return
        subscriptions = [
            record
            for record in self.subscription_store.all()
            if _is_valid_umo(record.umo)
        ]
        self.pending.mark_seen(base_id)
        self._safe_persist()
        if not subscriptions:
            return

        image_result = await self._render_card(
            self_id=self_id,
            event_type=event_type,
            event_time=event_ts,
            duration_seconds=duration,
        )

        for record in subscriptions:
            pending = self._find_for_target(base_id, record.umo)
            if pending is None:
                pending = PendingNotification(
                    event_id=_target_event_key(base_id, record.umo),
                    self_id=self_id,
                    event_type=event_type,
                    event_time=event_ts,
                    duration_seconds=duration,
                    umo=record.umo,
                    owner_id=record.owner_id,
                    episode_id=episode_id,
                )
            else:
                # The current subscription owner controls an offline mention.
                pending.owner_id = record.owner_id
            try:
                await self._deliver(pending, record, image_result)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("NapCat notification target delivery failed")

    async def catch_up(self) -> None:
        """Resume valid pending entries, dropping removed subscriptions."""
        for pending in list(self.pending.all()):
            record = self.subscription_store.get(pending.umo)
            if record is None or not _is_valid_umo(record.umo):
                self.pending.remove(pending.event_id)
                self._safe_persist()
                continue
            if pending.attempts >= self.max_attempts:
                # A capped run must remain catch-up eligible after restart;
                # the cap applies to one delivery session, while the durable
                # record is precisely what lets a later plugin start resume.
                pending.attempts = 0
                self._safe_persist()
            pending.owner_id = record.owner_id
            image_result = await self._render_card(
                self_id=pending.self_id,
                event_type=pending.event_type,
                event_time=pending.event_time,
                duration_seconds=pending.duration_seconds,
            )
            try:
                await self._deliver(pending, record, image_result)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("NapCat pending target delivery failed")

    def drop_for_umo(self, umo: str) -> None:
        """Discard pending entries for a subscription that was removed."""
        if self.pending.drop_for_umo(umo):
            self._safe_persist()

    async def _deliver(
        self,
        pending: PendingNotification,
        record: SubscriptionRecord,
        image_result: Any,
    ) -> None:
        if not _is_valid_umo(record.umo):
            return
        if self.pending.is_completed(pending.event_id):
            self.pending.remove(pending.event_id)
            self._safe_persist()
            return
        self.pending.upsert(pending)
        self._safe_persist()

        while pending.attempts < self.max_attempts:
            pending.attempts += 1
            try:
                sent = await self._send_once(
                    record.umo,
                    pending,
                    image_result,
                    record.owner_id,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("NapCat notification delivery callback failed")
                sent = False

            if sent:
                self.pending.remove(pending.event_id)
                self.pending.mark_completed(pending.event_id)
                self._safe_persist()
                return

            self.pending.upsert(pending)
            self._safe_persist()
            if pending.attempts < self.max_attempts and self.retry_delay:
                await asyncio.sleep(self.retry_delay)

        logger.error(
            "NapCat notification delivery exhausted retries event_type=%s attempts=%s",
            pending.event_type,
            pending.attempts,
        )

    async def _send_once(
        self,
        umo: str,
        pending: PendingNotification,
        image_result: Any,
        owner_id: str,
    ) -> bool:
        """Send one image or safe text message using the exact subscription UMO."""
        if image_result is not None:
            chain = _build_image_chain(
                image_result,
                owner_id if pending.event_type == "offline" else None,
            )
        else:
            chain = _build_text_chain(
                _plain_text(
                    pending,
                    include_owner=pending.event_type == "offline",
                    nickname=self._profile_values(pending.self_id)[0],
                ),
                owner_id if pending.event_type == "offline" else None,
            )
        result = await self.context.send_message(umo, chain)
        return bool(result)

    async def _render_card(
        self,
        *,
        self_id: int,
        event_type: str,
        event_time: float,
        duration_seconds: float | None,
    ) -> Any | None:
        nickname, avatar_data_uri = self._profile_values(self_id)
        data = _template_data(
            self_id=self_id,
            event_type=event_type,
            event_time=event_time,
            duration_seconds=duration_seconds,
            nickname=nickname,
            avatar_data_uri=avatar_data_uri,
            instance_label=self.instance_label,
        )
        return await render_html_image(
            self.html_render,
            self.template_path,
            data,
            diagnostic_label="NapCat notification card rendering",
        )

    def _find_for_target(self, base_id: str, umo: str) -> PendingNotification | None:
        target_id = _target_event_key(base_id, umo)
        return self.pending.get(target_id)

    def _profile_values(self, self_id: int) -> tuple[str | None, str | None]:
        provider = self.profile_provider
        if not callable(provider):
            return None, None
        try:
            profile = provider(self_id)
        except Exception:
            return None, None
        if profile is None:
            return None, None
        if isinstance(profile, dict):
            nickname = profile.get("nickname")
            avatar = profile.get("avatar_data_uri")
        else:
            nickname = getattr(profile, "nickname", None)
            avatar = getattr(profile, "avatar_data_uri", None)
        return sanitize_nickname(nickname), _safe_avatar_data_uri(avatar)

    def _safe_persist(self) -> None:
        try:
            self.pending.save()
        except NotificationPersistenceError:
            logger.exception("NapCat pending notification persistence failed")


# Compatibility alias for callers using service terminology.
NotificationService = NotificationManager


def _event_type(kind: TransitionKind) -> str | None:
    if kind == TransitionKind.OFFLINE_TIMEOUT:
        return "offline"
    if kind in {TransitionKind.FIRST_ONLINE, TransitionKind.RECOVERED}:
        # First online is intentionally rendered with the recovered/online
        # variant: it has the same no-owner message rule.
        return "recovered"
    return None


def _event_key(
    self_id: int,
    event_type: str,
    event_time: float,
    episode_id: int | None = None,
) -> str:
    """Build a stable event identity.

    New state-machine events use the durable episode sequence.  The timestamp
    fallback keeps direct callers and migrated v1 pending records compatible.
    """
    if episode_id is not None:
        return f"{self_id}:{event_type}:{episode_id}"
    return f"{self_id}:{event_type}:{event_time:.6f}"


def _target_event_key(base_id: str, umo: str) -> str:
    digest = hashlib.sha256(umo.encode("utf-8")).hexdigest()[:16]
    return f"{base_id}:{digest}"


def _load_event_id_set(value: Any, field_name: str) -> OrderedDict[str, None]:
    if not isinstance(value, list) or any(
        not isinstance(item, str) or not item for item in value
    ):
        raise NotificationPersistenceError(f"Invalid pending notification {field_name}")
    if len(set(value)) != len(value):
        raise NotificationPersistenceError(
            f"Duplicate pending notification {field_name} entries"
        )
    return OrderedDict((item, None) for item in value)


def _evict_oldest(values: OrderedDict[str, None], protected: set[str]) -> None:
    while len(values) > _MAX_DEDUPE_IDS:
        candidate: str | None = None
        for event_id in values:
            if event_id not in protected:
                candidate = event_id
                break
        if candidate is None:
            # This can only happen if a malformed/oversized pending set
            # protects every retained ID.  Preservation is safer than
            # deleting IDs still needed for catch-up.
            break
        values.pop(candidate, None)


def _safe_timestamp(value: float | None, fallback: float) -> float:
    try:
        candidate = float(value if value is not None else fallback)
    except (TypeError, ValueError, OverflowError):
        candidate = float(fallback)
    if not _is_finite_nonnegative(candidate):
        try:
            candidate = float(fallback)
        except (TypeError, ValueError, OverflowError):
            candidate = 0.0
    return candidate if _is_finite_nonnegative(candidate) else 0.0


def _is_finite_nonnegative(value: Any) -> bool:
    if isinstance(value, bool):
        return False
    try:
        return math.isfinite(float(value)) and float(value) >= 0
    except (TypeError, ValueError, OverflowError):
        return False


def _duration_since(event_time: Any, since: Any) -> float | None:
    """Return a valid elapsed duration, never fabricating an unavailable one."""
    if not _is_finite_nonnegative(event_time) or not _is_finite_nonnegative(since):
        return None
    duration = float(event_time) - float(since)
    if not math.isfinite(duration) or duration < 0:
        return None
    return duration


def _is_valid_umo(umo: str) -> bool:
    if not isinstance(umo, str):
        return False
    value = umo.strip()
    if not value or value != umo or len(value) > 512:
        return False
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        return False
    # UMO must retain platform scope and a group message type.  Never
    # reconstruct a target from a group/session suffix.
    if value.count(":") != 2:
        return False
    platform_id, message_type, session_id = value.split(":", 2)
    return bool(platform_id and message_type == "GroupMessage" and session_id)


def _mask_qq(self_id: int) -> str:
    raw = str(self_id)
    if len(raw) <= 4:
        return "*" * len(raw)
    return f"{raw[:2]}****{raw[-2:]}"


def _format_time(value: float) -> str:
    try:
        return datetime.fromtimestamp(value).strftime("%Y-%m-%d %H:%M:%S")
    except (OverflowError, OSError, ValueError):
        return "时间未知"


def _format_duration(seconds: float | None) -> str:
    if not _is_finite_nonnegative(seconds):
        return "未记录"
    return format_natural_duration(seconds)


def _template_data(
    *,
    self_id: int,
    event_type: str,
    event_time: float,
    duration_seconds: float | None,
    nickname: str | None = None,
    avatar_data_uri: str | None = None,
    instance_label: str | None = None,
) -> dict[str, Any]:
    recovered = event_type == "recovered"
    data: dict[str, Any] = {
        "event_type": event_type,
        "service_name": "NapCat",
        "nickname": sanitize_nickname(nickname),
        "avatar_data_uri": _safe_avatar_data_uri(avatar_data_uri),
        "instance_label": sanitize_nickname(instance_label),
        "masked_qq": _mask_qq(self_id),
        "title": "账号已恢复在线" if recovered else "账号连接中断",
        "subtitle": (
            "连接已经恢复，监测继续运行。"
            if recovered
            else "暂时无法保持在线，正在持续监测。"
        ),
        "event_time": _format_time(event_time),
        "safe_reason_label": "已恢复在线" if recovered else "确认离线",
        "source_label": "NapCat Watchdog",
    }
    if _is_finite_nonnegative(duration_seconds):
        data["duration_text"] = _format_duration(duration_seconds)
    return data


def _plain_text(
    pending: PendingNotification,
    *,
    include_owner: bool,
    nickname: str | None = None,
) -> str:
    status = "NapCat 掉线" if pending.event_type == "offline" else "NapCat 上线"
    text = (
        f"{'⚠️' if include_owner else '✅'} {status}\n"
        f"QQ：{_mask_qq(pending.self_id)}\n"
        f"时间：{_format_time(pending.event_time)}"
    )
    safe_nickname = sanitize_nickname(nickname)
    if safe_nickname:
        text += f"\n账号：{safe_nickname}"
    if _is_finite_nonnegative(pending.duration_seconds):
        duration_label = (
            "离线时长" if pending.event_type == "recovered" else "本次在线时长"
        )
        text += f"\n{duration_label}：{_format_duration(pending.duration_seconds)}"
    if include_owner:
        text += "\n请订阅管理员关注。"
    return text


def _safe_avatar_data_uri(value: Any) -> str | None:
    if not data_uri_is_valid(value):
        return None
    return value


def _build_image_chain(image_result: Any, owner_id: str | None = None) -> Any:
    from astrbot.api.event import MessageChain
    from astrbot.api.message_components import Image, Plain

    try:
        image = Image(file=image_result)
    except TypeError:
        if isinstance(image_result, bytes) and hasattr(Image, "fromBytes"):
            image = Image.fromBytes(image_result)
        else:
            raise
    components: list[Any] = [image]
    if owner_id:
        try:
            from astrbot.api.message_components import At

            components.append(At(qq=owner_id))
        except (ImportError, TypeError, AttributeError):
            components.append(Plain(f" @{owner_id}"))
    return _chain_with_components(MessageChain, components)


def _build_text_chain(text: str, owner_id: str | None) -> Any:
    from astrbot.api.event import MessageChain
    from astrbot.api.message_components import Plain

    components: list[Any] = [Plain(text)]
    if owner_id:
        try:
            from astrbot.api.message_components import At

            components.append(At(qq=owner_id))
        except (ImportError, TypeError, AttributeError):
            # AstrBot has At in supported runtimes.  Keep test/minimal stubs
            # useful without putting owner data into the image card.
            components.append(Plain(f" @{owner_id}"))
    return _chain_with_components(MessageChain, components)


def _chain_with_components(message_chain_type: Any, components: list[Any]) -> Any:
    try:
        chain = message_chain_type()
        chain.chain.extend(components)
    except (TypeError, AttributeError):
        chain = message_chain_type(chain=components)
    use_t2i = getattr(chain, "use_t2i", None)
    if callable(use_t2i):
        use_t2i(False)
    return chain


def _valid_image_result(value: Any) -> bool:
    if isinstance(value, bytes):
        return _valid_image_bytes(value)
    if not isinstance(value, str) or not value.strip():
        return False
    result = value.strip()
    if result.startswith("base64://"):
        try:
            return _valid_image_bytes(base64.b64decode(result[9:], validate=True))
        except (ValueError, binascii.Error):
            return False
    if result.startswith("data:image/") and ";base64," in result:
        try:
            return _valid_image_bytes(
                base64.b64decode(result.split(",", 1)[1], validate=True)
            )
        except (ValueError, binascii.Error):
            return False
    path = Path(result)
    if not path.is_file():
        return False
    try:
        return _valid_image_bytes(path.read_bytes())
    except OSError:
        return False


def _valid_image_bytes(value: bytes) -> bool:
    if len(value) < 12:
        return False
    return value.startswith(
        (
            b"\x89PNG\r\n\x1a\n",
            b"\xff\xd8\xff",
            b"GIF87a",
            b"GIF89a",
            b"BM",
        )
    ) or (value[:4] == b"RIFF" and value[8:12] == b"WEBP")

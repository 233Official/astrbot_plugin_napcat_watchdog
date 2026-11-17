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
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from .duration import format_natural_duration
from .profile import data_uri_is_valid, sanitize_nickname
from .state_machine import QQStatus, TransitionKind
from .subscriptions import SubscriptionRecord, SubscriptionStore

logger = logging.getLogger(__name__)

_SCHEMA_VERSION = 3
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
        self.delivery_targets: dict[str, float] = {}
        self._needs_migration = False
        self._legacy_backup: tuple[int, bytes] | None = None

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
        self.delivery_targets.pop(umo, None)
        self._prune_dedupe()
        return len(removed)

    def load(self) -> None:
        if not self.path.is_file():
            return
        try:
            content = self.path.read_bytes()
            raw = json.loads(content.decode("utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise NotificationPersistenceError(
                f"Corrupt pending notification file: {type(exc).__name__}"
            ) from exc

        if not isinstance(raw, dict):
            raise NotificationPersistenceError("Invalid pending notification envelope")
        schema_version = raw.get("_schema_version")
        if type(schema_version) is not int or schema_version not in {1, 2, 3}:
            raise NotificationPersistenceError(
                "Unsupported pending notification schema"
            )
        pending = raw.get("pending")
        if not isinstance(pending, dict):
            raise NotificationPersistenceError("Invalid pending notification entries")

        legacy_keys = {"_schema_version", "pending"}
        dedupe_keys = legacy_keys | {"seen", "completed"}
        needs_migration = schema_version != _SCHEMA_VERSION
        targets: dict[str, float] = {}
        if schema_version == 1:
            valid_envelope = set(raw) == legacy_keys
            seen: OrderedDict[str, None] = OrderedDict()
            completed: OrderedDict[str, None] = OrderedDict()
        elif schema_version == 2 and set(raw) == legacy_keys:
            # The early v2 draft did not yet persist the deduplication arrays.
            valid_envelope = True
            seen = OrderedDict()
            completed = OrderedDict()
        else:
            expected = dedupe_keys
            if schema_version == _SCHEMA_VERSION:
                expected = expected | {"delivery_targets"}
            valid_envelope = set(raw) == expected
            seen = _load_event_id_set(raw.get("seen"), "seen")
            completed = _load_event_id_set(raw.get("completed"), "completed")
        if not valid_envelope:
            raise NotificationPersistenceError("Invalid pending notification envelope")

        if schema_version == _SCHEMA_VERSION:
            raw_targets = raw["delivery_targets"]
            if not isinstance(raw_targets, dict):
                raise NotificationPersistenceError("Invalid notification target state")
            for umo, next_allowed in raw_targets.items():
                if (
                    not _is_valid_umo(umo)
                    or isinstance(next_allowed, bool)
                    or not isinstance(next_allowed, (int, float))
                    or not _is_finite_nonnegative(next_allowed)
                ):
                    raise NotificationPersistenceError(
                        "Invalid notification target allowance"
                    )
                targets[umo] = float(next_allowed)

        loaded: dict[str, PendingNotification] = {}
        legacy_entry_keys = self._ENTRY_KEYS - {"episode_id"}
        entry_keys = legacy_entry_keys if schema_version == 1 else self._ENTRY_KEYS
        for key, value in pending.items():
            if not isinstance(key, str) or not isinstance(value, dict):
                raise NotificationPersistenceError(
                    "Invalid pending notification record"
                )
            value_keys = set(value)
            if value_keys != entry_keys and not (
                schema_version == 2 and value_keys == legacy_entry_keys
            ):
                raise NotificationPersistenceError(
                    "Unexpected pending notification fields"
                )
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
        self.delivery_targets = targets
        before = (len(self._seen), len(self._completed))
        self._prune_dedupe()
        self._needs_migration = needs_migration or before != (
            len(self._seen),
            len(self._completed),
        )
        self._legacy_backup = (
            (schema_version, content) if schema_version < _SCHEMA_VERSION else None
        )

    def save(self) -> None:
        self._prune_dedupe()
        data = {
            "_schema_version": _SCHEMA_VERSION,
            "pending": {key: item.to_dict() for key, item in self._items.items()},
            "seen": list(self._seen),
            "completed": list(self._completed),
            "delivery_targets": dict(self.delivery_targets),
        }
        tmp_path = self.path.with_name(self.path.name + ".tmp")
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._backup_legacy()
            fd = os.open(
                str(tmp_path),
                os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_SYNC,
                0o600,
            )
            with os.fdopen(fd, "wb") as stream:
                stream.write(
                    json.dumps(data, indent=2, sort_keys=True, allow_nan=False).encode(
                        "utf-8"
                    )
                )
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(str(tmp_path), str(self.path))
            # A reservation is not durable until the renamed directory entry is.
            parent_fd = os.open(str(self.path.parent), os.O_RDONLY)
            try:
                os.fsync(parent_fd)
            finally:
                os.close(parent_fd)
        except (OSError, ValueError) as exc:
            try:
                tmp_path.unlink(missing_ok=True)
            except OSError:
                pass
            raise NotificationPersistenceError(
                f"Failed to persist pending notifications: {type(exc).__name__}"
            ) from exc
        self._needs_migration = False
        self._legacy_backup = None

    def _backup_legacy(self) -> None:
        """Preserve original legacy bytes before the first schema-v3 replacement."""
        if self._legacy_backup is None:
            return
        version, content = self._legacy_backup
        digest = hashlib.sha256(content).hexdigest()[:16]
        backup = self.path.with_name(f"{self.path.name}.v{version}.{digest}.bak")
        try:
            fd = os.open(backup, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            if backup.read_bytes() != content:
                raise ValueError("Legacy notification backup mismatch")
            return
        complete = False
        try:
            with os.fdopen(fd, "wb") as stream:
                stream.write(content)
                stream.flush()
                os.fsync(stream.fileno())
            complete = True
        finally:
            if not complete:
                backup.unlink(missing_ok=True)

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
        if not _is_finite_nonnegative(item.event_time):
            raise ValueError("event_time must be finite and non-negative")
        if item.duration_seconds is not None and not _is_finite_nonnegative(
            item.duration_seconds
        ):
            raise ValueError("duration_seconds must be finite and non-negative")
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
        min_send_interval: float = 30.0,
        failure_cooldown: float = 300.0,
        max_pending_age: float = 3600.0,
        max_attempts: int = _MAX_ATTEMPTS,
        wall_clock: Callable[[], float] = time.time,
        profile_provider: Callable[[int], Any] | None = None,
        profile_waiter: Callable[[int], Awaitable[Any]] | None = None,
        profile_wait_timeout: float = 3.0,
        instance_label: str | None = None,
        state_provider: Callable[[int], str | None] | None = None,
        target_guard: Callable[[str], Awaitable[bool]] | None = None,
    ) -> None:
        self.context = context
        self.subscription_store = subscription_store
        self.pending = PendingNotificationStore(pending_path)
        self.html_render = html_render
        self.template_path = template_path or (
            Path(__file__).resolve().parents[1] / "templates" / "notification_card.html"
        )
        self.min_send_interval = max(0.0, float(min_send_interval))
        self.failure_cooldown = max(self.min_send_interval, float(failure_cooldown))
        self.max_pending_age = max(0.0, float(max_pending_age))
        self.max_attempts = max(1, min(int(max_attempts), _MAX_ATTEMPTS))
        self.wall_clock = wall_clock
        self.profile_provider = profile_provider
        self.profile_waiter = profile_waiter
        self.profile_wait_timeout = max(0.0, min(float(profile_wait_timeout), 3.0))
        self.instance_label = instance_label
        self.state_provider = state_provider
        self.target_guard = target_guard
        self._closed = False
        self._workers: dict[str, asyncio.Task[None]] = {}
        self._render_tasks: dict[str, asyncio.Task[Any | None]] = {}
        self._render_users: dict[str, int] = {}
        self._owned_renders: set[asyncio.Task[Any | None]] = set()

    @property
    def pending_count(self) -> int:
        return self.pending.count

    def load(self) -> None:
        self.pending.load()
        if self.pending.needs_migration:
            # Preserve attempts and deduplication before any delivery is scheduled.
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
        """Durably enqueue one transition, awaiting only newly due workers."""
        if self._closed:
            return
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
        if episode_id is not None and (
            isinstance(episode_id, bool)
            or not isinstance(episode_id, int)
            or episode_id < 0
        ):
            return
        base_id = _event_key(self_id, event_type, event_ts, episode_id)
        if self.pending.is_seen(base_id):
            return
        self.pending.mark_seen(base_id)
        for record in self.subscription_store.all():
            if not _is_valid_umo(record.umo):
                continue
            target_id = _target_event_key(base_id, record.umo)
            # A migrated record may predate its durable seen marker. Never
            # replace it with a fresh attempt budget.
            if self.pending.get(target_id) is None:
                self.pending.upsert(
                    PendingNotification(
                        event_id=target_id,
                        self_id=self_id,
                        event_type=event_type,
                        event_time=event_ts,
                        duration_seconds=duration,
                        umo=record.umo,
                        owner_id=record.owner_id,
                        episode_id=episode_id,
                    )
                )
        self._normalize(_safe_timestamp(None, self.wall_clock()))
        if not self._safe_persist():
            return
        workers = self.dispatch_pending()
        if workers:
            await asyncio.gather(*workers)

    async def catch_up(self) -> None:
        """Normalize startup state without rendering, sending, or starting tasks."""
        if not self._closed and self._normalize(
            _safe_timestamp(None, self.wall_clock())
        ):
            self._safe_persist()

    def dispatch_pending(self) -> tuple[asyncio.Task[None], ...]:
        """Start at most one due worker per exact UMO, without waiting for it."""
        if self._closed:
            return ()
        now = _safe_timestamp(None, self.wall_clock())
        if self._normalize(now) and not self._safe_persist():
            return ()
        selected: dict[str, PendingNotification] = {}
        for pending in self.pending.all():
            if (
                pending.umo in self._workers
                or pending.attempts >= self.max_attempts
                or self.pending.delivery_targets.get(pending.umo, 0.0) > now
                or self._state_action(pending) != "deliver"
            ):
                continue
            previous = selected.get(pending.umo)
            if previous is None or (
                pending.event_time,
                pending.event_type == "recovered",
                pending.event_id,
            ) > (
                previous.event_time,
                previous.event_type == "recovered",
                previous.event_id,
            ):
                selected[pending.umo] = pending

        workers: list[asyncio.Task[None]] = []
        for umo, pending in selected.items():
            base_id = pending.event_id.rsplit(":", 1)[0]
            worker = asyncio.create_task(self._deliver(pending))
            self._workers[umo] = worker
            self._render_users[base_id] = self._render_users.get(base_id, 0) + 1
            worker.add_done_callback(
                lambda task, target=umo, event=base_id: self._worker_done(
                    target, event, task
                )
            )
            workers.append(worker)
        return tuple(workers)

    async def close(self) -> None:
        """Stop all owned work; shielded connection profile refreshes survive."""
        self._closed = True
        tasks = (*self._workers.values(), *self._owned_renders)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def drop_for_umo(self, umo: str) -> None:
        """Discard both queued summaries and pacing for an unsubscribed UMO."""
        had_target = umo in self.pending.delivery_targets
        if self.pending.drop_for_umo(umo) or had_target:
            self._safe_persist()

    @staticmethod
    def _is_newer(
        candidate: PendingNotification, previous: PendingNotification
    ) -> bool:
        if (
            candidate.episode_id is not None
            and previous.episode_id is not None
            and candidate.episode_id != previous.episode_id
        ):
            return candidate.episode_id > previous.episode_id
        return (
            candidate.event_time,
            candidate.event_type == "recovered",
            candidate.event_id,
        ) > (
            previous.event_time,
            previous.event_type == "recovered",
            previous.event_id,
        )

    def _normalize(self, now: float) -> bool:
        changed = False
        newest: dict[tuple[int, str], PendingNotification] = {}
        pending_items = self.pending.all()
        # Coalesce before filtering: discarding a newer obsolete/expired event
        # must never expose an older summary from the same account and target.
        for pending in pending_items:
            base_id = pending.event_id.rsplit(":", 1)[0]
            if not self.pending.is_seen(base_id):
                self.pending.mark_seen(base_id)
                changed = True
            key = (pending.self_id, pending.umo)
            previous = newest.get(key)
            if previous is None or self._is_newer(pending, previous):
                newest[key] = pending
        for pending in pending_items:
            record = self.subscription_store.get(pending.umo)
            if (
                newest[(pending.self_id, pending.umo)] is not pending
                or record is None
                or not _is_valid_umo(record.umo)
                or now - pending.event_time > self.max_pending_age
                or self.pending.is_completed(pending.event_id)
                or self._state_action(pending) == "drop"
            ):
                self.pending.remove(pending.event_id)
                changed = True
            elif pending.owner_id != record.owner_id:
                pending.owner_id = record.owner_id
                changed = True
        for umo, next_allowed in list(self.pending.delivery_targets.items()):
            record = self.subscription_store.get(umo)
            if record is None or not _is_valid_umo(record.umo) or next_allowed <= now:
                del self.pending.delivery_targets[umo]
                changed = True
        return changed

    def _state_action(self, pending: PendingNotification) -> str:
        if self.state_provider is None:
            return "deliver"
        try:
            status = self.state_provider(pending.self_id)
        except Exception:
            logger.debug("NapCat notification state lookup failed", exc_info=True)
            return "defer"
        if status is None:
            return "drop"
        if status not in {"online", "offline"}:
            return "defer"
        expected = "online" if pending.event_type == "recovered" else "offline"
        return "deliver" if status == expected else "drop"

    def _ready_record(
        self, pending: PendingNotification, now: float
    ) -> SubscriptionRecord | None:
        if self._closed or self.pending.get(pending.event_id) is not pending:
            return None
        record = self.subscription_store.get(pending.umo)
        state_action = self._state_action(pending)
        if (
            record is None
            or not _is_valid_umo(record.umo)
            or now - pending.event_time > self.max_pending_age
            or self.pending.is_completed(pending.event_id)
            or state_action == "drop"
        ):
            self.pending.remove(pending.event_id)
            self._safe_persist()
            return None
        if (
            state_action != "deliver"
            or pending.attempts >= self.max_attempts
            or self.pending.delivery_targets.get(pending.umo, 0.0) > now
        ):
            return None
        return record

    async def _target_allowed(self, umo: str) -> bool:
        if self.target_guard is None:
            return True
        try:
            allowed = bool(await self.target_guard(umo))
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.warning(
                "NapCat notification target policy check failed", exc_info=True
            )
            allowed = False
        if not allowed and not self._closed:
            record = self.subscription_store.get(umo)
            if record is not None and _is_valid_umo(record.umo):
                now = _safe_timestamp(None, self.wall_clock())
                self.pending.delivery_targets[umo] = max(
                    self.pending.delivery_targets.get(umo, 0.0),
                    now + self.failure_cooldown,
                )
                self._safe_persist()
        return allowed

    async def _shared_render(self, pending: PendingNotification) -> Any | None:
        base_id = pending.event_id.rsplit(":", 1)[0]
        task = self._render_tasks.get(base_id)
        if task is None:
            task = asyncio.create_task(
                self._render_card(
                    self_id=pending.self_id,
                    event_type=pending.event_type,
                    event_time=pending.event_time,
                    duration_seconds=pending.duration_seconds,
                )
            )
            self._render_tasks[base_id] = task
            self._owned_renders.add(task)
            task.add_done_callback(self._render_done)
        return await asyncio.shield(task)

    def _render_done(self, task: asyncio.Task[Any | None]) -> None:
        self._owned_renders.discard(task)
        if not task.cancelled():
            task.exception()

    def _worker_done(self, umo: str, base_id: str, task: asyncio.Task[None]) -> None:
        if self._workers.get(umo) is task:
            del self._workers[umo]
        remaining = self._render_users[base_id] - 1
        if remaining:
            self._render_users[base_id] = remaining
        else:
            del self._render_users[base_id]
            render = self._render_tasks.pop(base_id, None)
            if render is not None and not render.done():
                render.cancel()
        if not task.cancelled():
            task.exception()

    async def _deliver(self, pending: PendingNotification) -> None:
        try:
            if (
                self._ready_record(pending, _safe_timestamp(None, self.wall_clock()))
                is None
            ):
                return
            if not await self._target_allowed(pending.umo):
                return
            if (
                self._ready_record(pending, _safe_timestamp(None, self.wall_clock()))
                is None
            ):
                return
            image_result = await self._shared_render(pending)
            if (
                self._ready_record(pending, _safe_timestamp(None, self.wall_clock()))
                is None
            ):
                return
            if not await self._target_allowed(pending.umo):
                return
            now = _safe_timestamp(None, self.wall_clock())
            record = self._ready_record(pending, now)
            if record is None:
                return
            pending.owner_id = record.owner_id
            pending.attempts += 1
            self.pending.delivery_targets[pending.umo] = now + self.failure_cooldown
            # No send is permitted unless its spent attempt and conservative
            # unknown-outcome cooldown have reached durable storage first.
            if not self._safe_persist():
                return
            try:
                sent = await self._send_once(
                    record.umo, pending, image_result, record.owner_id
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("NapCat notification delivery callback failed")
                sent = False

            now = _safe_timestamp(None, self.wall_clock())
            record = self.subscription_store.get(pending.umo)
            if record is not None and _is_valid_umo(record.umo):
                delay = self.min_send_interval if sent else self.failure_cooldown
                self.pending.delivery_targets[pending.umo] = now + delay
            if sent:
                # A newer event may have superseded this one during the send.
                if self.pending.get(pending.event_id) is pending:
                    self.pending.remove(pending.event_id)
                self.pending.mark_completed(pending.event_id)
            self._safe_persist()
        except asyncio.CancelledError:
            # An unknown send outcome keeps the pre-send reservation intact.
            raise
        except Exception:
            logger.exception("NapCat notification target delivery failed")

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
        if (
            event_type == "recovered"
            and not nickname
            and not avatar_data_uri
            and callable(self.profile_waiter)
            and self.profile_wait_timeout > 0
        ):
            try:
                # The connection owns this refresh: neither a card deadline
                # nor cancellation of its delivery may cancel the lookup.
                await asyncio.wait_for(
                    asyncio.shield(self.profile_waiter(self_id)),
                    timeout=self.profile_wait_timeout,
                )
            except asyncio.TimeoutError:
                pass
            except Exception:
                logger.debug("OneBot notification profile wait failed", exc_info=True)
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

    def _safe_persist(self) -> bool:
        try:
            self.pending.save()
        except NotificationPersistenceError:
            logger.exception("NapCat pending notification persistence failed")
            return False
        return True


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
        "service_name": "OneBot",
        "nickname": sanitize_nickname(nickname),
        "avatar_data_uri": _safe_avatar_data_uri(avatar_data_uri),
        "instance_label": sanitize_nickname(instance_label),
        "masked_qq": _mask_qq(self_id),
        "title": "监控已恢复" if recovered else "监控失联",
        "subtitle": (
            "OneBot 监控连接已恢复，继续监测心跳。"
            if recovered
            else "OneBot 监控连接或心跳中断，不代表账号离线。"
        ),
        "event_time": _format_time(event_time),
        "safe_reason_label": "监控连接正常" if recovered else "监控连接或心跳超时",
        "source_label": "OneBot Watchdog",
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
    status = (
        "OneBot 监控失联" if pending.event_type == "offline" else "OneBot 监控已恢复"
    )
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
            "失联时长" if pending.event_type == "recovered" else "本次监控正常时长"
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

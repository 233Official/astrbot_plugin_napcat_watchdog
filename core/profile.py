"""Best-effort durable display profiles and validated QQ avatar enrichment."""

from __future__ import annotations

import asyncio
import base64
import binascii
import itertools
import json
import math
import os
import tempfile
import threading
import time
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import aiohttp

MAX_AVATAR_BYTES = 512 * 1024
AVATAR_READ_CHUNK_BYTES = 64 * 1024
MAX_PROFILE_BYTES = 1024 * 1024
MAX_RESTORED_PROFILES = 20
DEFAULT_NICKNAME_TTL = 24 * 60 * 60.0
DEFAULT_AVATAR_TTL = 24 * 60 * 60.0
DEFAULT_REFRESH_COOLDOWN = 5 * 60.0
DEFAULT_AVATAR_TIMEOUT = 2.0
QQ_AVATAR_URL = "https://q1.qlogo.cn/g?b=qq&nk={self_id}&s=640"


@dataclass(frozen=True)
class ProfileSnapshot:
    """Safe display-only profile data."""

    self_id: int
    nickname: str | None = None
    avatar_data_uri: str | None = None


@dataclass(frozen=True)
class _CachedProfile:
    snapshot: ProfileSnapshot
    nickname_updated_at: float | None = None
    avatar_updated_at: float | None = None


AvatarFetcher = Callable[[int], Awaitable[Any]]


class ProfileCache:
    """Keep successful profiles displayable while refreshing stale fields."""

    def __init__(
        self,
        *,
        storage_dir: Path | None = None,
        avatar_fetcher: AvatarFetcher | None = None,
        nickname_ttl: float = DEFAULT_NICKNAME_TTL,
        avatar_ttl: float = DEFAULT_AVATAR_TTL,
        avatar_timeout: float = DEFAULT_AVATAR_TIMEOUT,
        on_cache_error: Callable[[str], None] | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._storage_dir = storage_dir
        self._avatar_fetcher = avatar_fetcher
        self._nickname_ttl = max(1.0, float(nickname_ttl))
        self._avatar_ttl = max(1.0, float(avatar_ttl))
        self._avatar_timeout = max(0.05, min(float(avatar_timeout), 10.0))
        self._clock = clock
        self._on_cache_error = on_cache_error
        self._profiles: dict[int, _CachedProfile] = {}
        self._refresh_attempts: dict[int, float] = {}
        self._avatar_attempts: dict[int, float] = {}
        self._revisions: dict[int, int] = {}
        self._revision_counter = itertools.count(1)
        self._update_locks: dict[int, asyncio.Lock] = {}
        # A cancelled to_thread waiter does not stop its writer. Serialize
        # replacements and reject obsolete revisions inside the worker too.
        self._storage_lock = threading.Lock()

    def get(self, self_id: int) -> ProfileSnapshot | None:
        cached = self._profiles.get(self_id)
        return cached.snapshot if cached is not None else None

    def clear(self, self_id: int | None = None) -> None:
        """Forget memory only; durable successful profiles remain available."""
        if self_id is None:
            self._profiles.clear()
            self._refresh_attempts.clear()
            self._avatar_attempts.clear()
            self._revisions.clear()
            return
        self._profiles.pop(self_id, None)
        self._refresh_attempts.pop(self_id, None)
        self._avatar_attempts.pop(self_id, None)
        self._revisions.pop(self_id, None)

    def load(self, self_ids: Iterable[int]) -> None:
        """Restore only bounded, known accounts; call off the event loop."""
        if self._storage_dir is None:
            return
        for self_id in itertools.islice(self_ids, MAX_RESTORED_PROFILES):
            if not _valid_self_id(self_id) or self_id in self._profiles:
                continue
            try:
                with (self._storage_dir / f"{self_id}.json").open("rb") as source:
                    raw = source.read(MAX_PROFILE_BYTES + 1)
                if len(raw) > MAX_PROFILE_BYTES:
                    raise ValueError("oversized profile cache")
                cached = _validated_cached_profile(json.loads(raw), self_id)
                if cached is None:
                    raise ValueError("invalid profile cache")
            except FileNotFoundError:
                continue
            except (OSError, ValueError, UnicodeError, RecursionError):
                self._report_cache_error("read")
                continue
            self._profiles.setdefault(self_id, cached)

    def should_refresh(self, self_id: int) -> bool:
        """Claim a due lookup, suppressing repeated failed attempts for 5 min."""
        if not _valid_self_id(self_id):
            return False
        now = self._clock()
        cached = self._profiles.get(self_id)
        if cached is not None and (
            _is_fresh(cached.nickname_updated_at, now, self._nickname_ttl)
            and _is_fresh(cached.avatar_updated_at, now, self._avatar_ttl)
        ):
            return False
        if _is_fresh(
            self._refresh_attempts.get(self_id), now, DEFAULT_REFRESH_COOLDOWN
        ):
            return False
        self._refresh_attempts[self_id] = now
        return True

    async def update(self, self_id: int, nickname: str) -> ProfileSnapshot:
        """Publish nickname immediately, then independently refresh the avatar."""
        if not _valid_self_id(self_id):
            raise ValueError("self_id must be a positive integer")
        now = self._clock()
        previous = self._profiles.get(self_id)
        old_snapshot = (
            previous.snapshot if previous is not None else ProfileSnapshot(self_id)
        )
        safe_nickname = sanitize_nickname(nickname)
        profile = ProfileSnapshot(
            self_id=self_id,
            nickname=safe_nickname or old_snapshot.nickname,
            avatar_data_uri=old_snapshot.avatar_data_uri,
        )
        cached = _CachedProfile(
            profile,
            now
            if safe_nickname
            else (previous.nickname_updated_at if previous else None),
            previous.avatar_updated_at if previous else None,
        )
        revision = next(self._revision_counter)
        self._revisions[self_id] = revision
        self._profiles[self_id] = cached
        self._refresh_attempts[self_id] = now
        lock = self._update_locks.setdefault(self_id, asyncio.Lock())
        async with lock:
            if self._revisions.get(self_id) != revision:
                return self.get(self_id) or profile
            await self._persist(self_id, revision, cached)
            if self._revisions.get(self_id) != revision:
                return self.get(self_id) or profile
            now = self._clock()
            if _is_fresh(cached.avatar_updated_at, now, self._avatar_ttl) or _is_fresh(
                self._avatar_attempts.get(self_id), now, DEFAULT_REFRESH_COOLDOWN
            ):
                return profile
            self._avatar_attempts[self_id] = now
            try:
                avatar_data_uri = await self._get_avatar(self_id)
            except asyncio.CancelledError:
                self._avatar_attempts.pop(self_id, None)
                raise
            if self._revisions.get(self_id) != revision:
                self._avatar_attempts.pop(self_id, None)
                return self.get(self_id) or profile
            if avatar_data_uri is not None:
                profile = ProfileSnapshot(self_id, profile.nickname, avatar_data_uri)
                cached = _CachedProfile(
                    profile, cached.nickname_updated_at, self._clock()
                )
                self._profiles[self_id] = cached
                await self._persist(self_id, revision, cached)
            return profile

    async def _persist(
        self, self_id: int, revision: int, cached: _CachedProfile
    ) -> None:
        if self._storage_dir is not None and (
            cached.snapshot.nickname is not None
            or cached.snapshot.avatar_data_uri is not None
        ):
            await asyncio.to_thread(self._write_profile, self_id, revision, cached)

    def _write_profile(
        self, self_id: int, revision: int, cached: _CachedProfile
    ) -> None:
        if self._storage_dir is None:
            return
        temporary: str | None = None
        try:
            with self._storage_lock:
                if self._revisions.get(self_id) != revision:
                    return
                payload = {
                    "_schema_version": 1,
                    "self_id": self_id,
                    "nickname": cached.snapshot.nickname,
                    "avatar_data_uri": cached.snapshot.avatar_data_uri,
                    "nickname_updated_at": cached.nickname_updated_at,
                    "avatar_updated_at": cached.avatar_updated_at,
                }
                if _validated_cached_profile(payload, self_id) is None:
                    raise ValueError("invalid profile cache")
                raw = json.dumps(payload, ensure_ascii=False, allow_nan=False).encode(
                    "utf-8"
                )
                if len(raw) > MAX_PROFILE_BYTES:
                    raise ValueError("oversized profile cache")
                self._storage_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
                fd, temporary = tempfile.mkstemp(
                    prefix=f".{self_id}.", suffix=".tmp", dir=self._storage_dir
                )
                with os.fdopen(fd, "wb") as target:
                    os.fchmod(target.fileno(), 0o600)
                    target.write(raw)
                    target.flush()
                    os.fsync(target.fileno())
                if self._revisions.get(self_id) != revision:
                    return
                os.replace(temporary, self._storage_dir / f"{self_id}.json")
                temporary = None
                try:
                    parent_fd = os.open(self._storage_dir, os.O_RDONLY)
                    try:
                        os.fsync(parent_fd)
                    finally:
                        os.close(parent_fd)
                except OSError:
                    pass
        except (OSError, ValueError):
            self._report_cache_error("write")
        finally:
            if temporary is not None:
                try:
                    os.unlink(temporary)
                except OSError:
                    pass

    def _report_cache_error(self, operation: str) -> None:
        if self._on_cache_error is not None:
            try:
                self._on_cache_error(operation)
            except Exception:
                pass

    async def _get_avatar(self, self_id: int) -> str | None:
        future: asyncio.Future[Any] | None = None
        try:
            fetcher = self._avatar_fetcher or self._fetch_remote_avatar
            future = asyncio.ensure_future(fetcher(self_id))
            future.add_done_callback(self._discard_avatar_result)
            # Keep cancellation of this update independent of a fetcher that
            # swallows CancelledError; a replaced connection must still stop.
            result = await asyncio.wait_for(
                asyncio.shield(future), timeout=self._avatar_timeout
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            return None
        finally:
            if future is not None and not future.done():
                future.cancel()
        return _avatar_result_to_data_uri(result)

    @staticmethod
    def _discard_avatar_result(future: asyncio.Future[Any]) -> None:
        """Consume late results from timed-out or cancelled avatar requests."""
        if not future.cancelled():
            future.exception()

    async def _fetch_remote_avatar(self, self_id: int) -> tuple[str, bytes] | None:
        timeout = aiohttp.ClientTimeout(total=self._avatar_timeout)
        url = QQ_AVATAR_URL.format(self_id=self_id)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url) as response:
                if response.status != 200:
                    return None
                content_length = response.headers.get("Content-Length")
                try:
                    declared_length = _parse_content_length(content_length)
                    if (
                        declared_length is not None
                        and declared_length > MAX_AVATAR_BYTES
                    ):
                        return None
                except ValueError:
                    return None
                chunks: list[bytes] = []
                total = 0
                while True:
                    remaining = MAX_AVATAR_BYTES + 1 - total
                    if remaining <= 0:
                        return None
                    chunk = await response.content.read(
                        min(AVATAR_READ_CHUNK_BYTES, remaining)
                    )
                    if not chunk:
                        break
                    total += len(chunk)
                    if total > MAX_AVATAR_BYTES:
                        return None
                    chunks.append(chunk)
                data = b"".join(chunks)
                if declared_length is not None and len(data) != declared_length:
                    return None
                if _detect_image_type(data) is None:
                    return None
                return response.headers.get("Content-Type", ""), data


def _valid_self_id(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def _is_fresh(updated_at: float | None, now: float, ttl: float) -> bool:
    return updated_at is not None and 0 <= now - updated_at < ttl


def _valid_timestamp(value: Any) -> bool:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    try:
        return math.isfinite(value) and value >= 0
    except OverflowError:
        return False


def _validated_cached_profile(payload: Any, self_id: int) -> _CachedProfile | None:
    if not isinstance(payload, dict) or set(payload) != {
        "_schema_version",
        "self_id",
        "nickname",
        "avatar_data_uri",
        "nickname_updated_at",
        "avatar_updated_at",
    }:
        return None
    if type(payload["_schema_version"]) is not int or payload["_schema_version"] != 1:
        return None
    if not _valid_self_id(payload["self_id"]) or payload["self_id"] != self_id:
        return None
    nickname = payload["nickname"]
    avatar = payload["avatar_data_uri"]
    nickname_at = payload["nickname_updated_at"]
    avatar_at = payload["avatar_updated_at"]
    if nickname is None:
        if nickname_at is not None:
            return None
    elif sanitize_nickname(nickname) != nickname or not _valid_timestamp(nickname_at):
        return None
    if avatar is None:
        if avatar_at is not None:
            return None
    elif not data_uri_is_valid(avatar) or not _valid_timestamp(avatar_at):
        return None
    if nickname is None and avatar is None:
        return None
    return _CachedProfile(
        ProfileSnapshot(self_id, nickname, avatar), nickname_at, avatar_at
    )


def sanitize_nickname(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = "".join(
        char for char in value.strip() if ord(char) >= 32 and ord(char) != 127
    )
    return cleaned[:64] or None


def _avatar_result_to_data_uri(result: Any) -> str | None:
    content_type = ""
    data: bytes
    if isinstance(result, tuple) and len(result) == 2:
        content_type, data = result
        if not isinstance(content_type, str) or not isinstance(data, bytes):
            return None
    elif isinstance(result, bytes):
        data = result
    else:
        return None

    if len(data) == 0 or len(data) > MAX_AVATAR_BYTES:
        return None
    image_type = _detect_image_type(data)
    if image_type is None:
        return None
    normalized_type = content_type.split(";", 1)[0].strip().lower()
    if normalized_type and normalized_type not in {
        "image/png",
        "image/jpeg",
        "image/jpg",
        "image/webp",
    }:
        return None
    if normalized_type and not _content_type_matches(normalized_type, image_type):
        return None
    return f"data:image/{image_type};base64,{base64.b64encode(data).decode('ascii')}"


def _content_type_matches(content_type: str, image_type: str) -> bool:
    if image_type == "jpeg":
        return content_type in {"image/jpeg", "image/jpg"}
    return content_type == f"image/{image_type}"


def _detect_image_type(data: bytes) -> str | None:
    if data.startswith(b"\x89PNG\r\n\x1a\n") and _is_complete_png(data):
        return "png"
    if _is_complete_jpeg(data):
        return "jpeg"
    if _is_complete_webp(data):
        return "webp"
    return None


def _parse_content_length(value: Any) -> int | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.isdigit():
        raise ValueError("invalid Content-Length")
    return int(value)


def _is_complete_jpeg(data: bytes) -> bool:
    if len(data) < 4 or not data.startswith(b"\xff\xd8"):
        return False
    meaningful = data.rstrip(b"\x00\t\n\r ")
    return len(meaningful) >= 4 and meaningful.endswith(b"\xff\xd9")


def _is_complete_png(data: bytes) -> bool:
    signature_length = 8
    if len(data) <= signature_length:
        return False

    offset = signature_length
    saw_iend = False
    while offset < len(data):
        if len(data) - offset < 12:
            return False
        chunk_length = int.from_bytes(data[offset : offset + 4], "big")
        chunk_start = offset + 8
        chunk_end = chunk_start + chunk_length
        end = chunk_end + 4
        if chunk_end < chunk_start or end > len(data):
            return False
        chunk_type = data[offset + 4 : offset + 8]
        if not chunk_type.isalpha() or not all(byte < 128 for byte in chunk_type):
            return False
        offset = end
        if chunk_type == b"IEND":
            if chunk_length != 0 or offset != len(data):
                return False
            saw_iend = True
            break
    return saw_iend


def _is_complete_webp(data: bytes) -> bool:
    if len(data) < 12 or data[:4] != b"RIFF" or data[8:12] != b"WEBP":
        return False
    declared_file_size = int.from_bytes(data[4:8], "little")
    return declared_file_size + 8 == len(data)


def data_uri_is_valid(value: Any) -> bool:
    """Validate only local image data URIs accepted by the template."""
    if (
        not isinstance(value, str)
        or len(value) > 32 + 4 * ((MAX_AVATAR_BYTES + 2) // 3)
        or ";base64," not in value
    ):
        return False
    prefix, encoded = value.split(",", 1)
    if prefix not in {
        "data:image/png;base64",
        "data:image/jpeg;base64",
        "data:image/jpg;base64",
        "data:image/webp;base64",
    }:
        return False
    try:
        data = base64.b64decode(encoded, validate=True)
    except (ValueError, binascii.Error):
        return False
    if not data or len(data) > MAX_AVATAR_BYTES:
        return False
    image_type = _detect_image_type(data)
    return image_type is not None and _content_type_matches(
        prefix[5:].split(";", 1)[0], image_type
    )

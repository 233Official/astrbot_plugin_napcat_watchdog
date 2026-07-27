"""Best-effort in-memory NapCat profile and QQ avatar enrichment."""

from __future__ import annotations

import asyncio
import base64
import binascii
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import aiohttp

MAX_AVATAR_BYTES = 512 * 1024
AVATAR_READ_CHUNK_BYTES = 64 * 1024
DEFAULT_AVATAR_TTL = 300.0
DEFAULT_AVATAR_TIMEOUT = 2.0
QQ_AVATAR_URL = "https://q1.qlogo.cn/g?b=qq&nk={self_id}&s=640"


@dataclass(frozen=True)
class ProfileSnapshot:
    """Safe display-only profile data; never persisted."""

    self_id: int
    nickname: str | None = None
    avatar_data_uri: str | None = None


AvatarFetcher = Callable[[int], Awaitable[Any]]


class ProfileCache:
    """Cache nicknames and short-lived, validated avatar data URIs."""

    def __init__(
        self,
        *,
        avatar_fetcher: AvatarFetcher | None = None,
        avatar_ttl: float = DEFAULT_AVATAR_TTL,
        avatar_timeout: float = DEFAULT_AVATAR_TIMEOUT,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._avatar_fetcher = avatar_fetcher
        self._avatar_ttl = max(1.0, float(avatar_ttl))
        self._avatar_timeout = max(0.05, min(float(avatar_timeout), 10.0))
        self._clock = clock
        self._profiles: dict[int, ProfileSnapshot] = {}
        self._avatars: dict[int, tuple[float, str | None]] = {}

    def get(self, self_id: int) -> ProfileSnapshot | None:
        return self._profiles.get(self_id)

    def clear(self, self_id: int | None = None) -> None:
        if self_id is None:
            self._profiles.clear()
            self._avatars.clear()
            return
        self._profiles.pop(self_id, None)
        self._avatars.pop(self_id, None)

    async def update(self, self_id: int, nickname: str) -> ProfileSnapshot:
        """Store a safe nickname and enrich it with a best-effort avatar."""
        safe_nickname = sanitize_nickname(nickname)
        avatar_data_uri = await self._get_avatar(self_id)
        profile = ProfileSnapshot(
            self_id=self_id,
            nickname=safe_nickname,
            avatar_data_uri=avatar_data_uri,
        )
        self._profiles[self_id] = profile
        return profile

    async def _get_avatar(self, self_id: int) -> str | None:
        now = self._clock()
        cached = self._avatars.get(self_id)
        if cached is not None and cached[0] > now:
            return cached[1]

        result: Any = None
        try:
            if self._avatar_fetcher is not None:
                result = await asyncio.wait_for(
                    self._avatar_fetcher(self_id), timeout=self._avatar_timeout
                )
            else:
                result = await asyncio.wait_for(
                    self._fetch_remote_avatar(self_id), timeout=self._avatar_timeout
                )
        except asyncio.CancelledError:
            raise
        except Exception:
            result = None

        avatar_data_uri = _avatar_result_to_data_uri(result)
        self._avatars[self_id] = (now + self._avatar_ttl, avatar_data_uri)
        return avatar_data_uri

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
    if not isinstance(value, str) or ";base64," not in value:
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
    return _avatar_result_to_data_uri((prefix[5:].split(";", 1)[0], data)) is not None

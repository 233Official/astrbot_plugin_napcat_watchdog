"""Deterministic profile and avatar validation tests."""

from __future__ import annotations

import asyncio
import base64
from collections.abc import Iterable
from typing import Any

import pytest

import core.profile as profile_module
from core.profile import MAX_AVATAR_BYTES, ProfileCache, data_uri_is_valid

_PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII="
)
_JPEG = b"\xff\xd8\xff\xd9"
_WEBP = (
    b"RIFF"
    + (16).to_bytes(4, "little")
    + b"WEBPVP8X"
    + (4).to_bytes(4, "little")
    + b"\x00\x00\x00\x00"
)


class _FakeContent:
    def __init__(self, chunks: Iterable[bytes]) -> None:
        self._chunks = list(chunks)

    async def read(self, size: int = -1) -> bytes:
        if not self._chunks:
            return b""
        chunk = self._chunks[0]
        if size < 0 or len(chunk) <= size:
            self._chunks.pop(0)
            return chunk
        self._chunks[0] = chunk[size:]
        return chunk[:size]


class _FakeResponse:
    def __init__(
        self,
        chunks: Iterable[bytes],
        content_type: str,
        content_length: str | None = None,
    ) -> None:
        self.status = 200
        self.headers: dict[str, str] = {"Content-Type": content_type}
        if content_length is not None:
            self.headers["Content-Length"] = content_length
        self.content = _FakeContent(chunks)

    async def __aenter__(self) -> _FakeResponse:
        return self

    async def __aexit__(self, *_args: Any) -> None:
        return None


class _FakeSession:
    def __init__(self, response: _FakeResponse) -> None:
        self._response = response

    async def __aenter__(self) -> _FakeSession:
        return self

    async def __aexit__(self, *_args: Any) -> None:
        return None

    def get(self, _url: str) -> _FakeResponse:
        return self._response


def _patch_remote_response(
    monkeypatch: pytest.MonkeyPatch,
    chunks: Iterable[bytes],
    content_type: str,
    content_length: str | None = None,
) -> None:
    response = _FakeResponse(chunks, content_type, content_length)
    monkeypatch.setattr(
        profile_module.aiohttp,
        "ClientSession",
        lambda **_kwargs: _FakeSession(response),
    )


@pytest.mark.asyncio
async def test_valid_png_and_jpeg_are_converted_to_data_uri() -> None:
    responses = [("image/png", _PNG), ("image/jpeg", _JPEG)]

    async def fetcher(self_id: int):
        return responses.pop(0)

    cache = ProfileCache(avatar_fetcher=fetcher)
    png = await cache.update(1, "账号一")
    cache.clear(1)
    jpeg = await cache.update(1, "账号二")

    assert png.avatar_data_uri is not None
    assert png.avatar_data_uri.startswith("data:image/png;base64,")
    assert jpeg.avatar_data_uri is not None
    assert jpeg.avatar_data_uri.startswith("data:image/jpeg;base64,")
    assert data_uri_is_valid(png.avatar_data_uri)
    assert data_uri_is_valid(jpeg.avatar_data_uri)


@pytest.mark.asyncio
async def test_remote_avatar_stream_joins_partial_chunks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    split = [_PNG[:3], _PNG[3:19], _PNG[19:]]
    _patch_remote_response(
        monkeypatch,
        split,
        "image/png",
        str(len(_PNG)),
    )

    cache = ProfileCache()
    result = await cache._fetch_remote_avatar(1)

    assert result == ("image/png", _PNG)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("content_type", "payload"),
    [
        ("image/jpeg", _JPEG[:-1]),
        ("image/png", _PNG[:-12]),
        ("image/webp", _WEBP[:-1]),
    ],
)
async def test_remote_truncated_images_are_rejected(
    monkeypatch: pytest.MonkeyPatch,
    content_type: str,
    payload: bytes,
) -> None:
    _patch_remote_response(monkeypatch, [payload[:2], payload[2:]], content_type)

    cache = ProfileCache()
    assert await cache._fetch_remote_avatar(1) is None


@pytest.mark.asyncio
async def test_remote_content_length_mismatch_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_remote_response(
        monkeypatch,
        [_PNG],
        "image/png",
        str(len(_PNG) + 1),
    )

    cache = ProfileCache()
    assert await cache._fetch_remote_avatar(1) is None


@pytest.mark.asyncio
async def test_remote_over_limit_stream_is_rejected_without_prefix_acceptance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_remote_response(
        monkeypatch,
        [_JPEG, b"x" * MAX_AVATAR_BYTES],
        "image/jpeg",
    )

    cache = ProfileCache()
    assert await cache._fetch_remote_avatar(1) is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("content_type", "payload", "prefix"),
    [
        ("image/png", _PNG, "data:image/png;base64,"),
        ("image/jpeg", _JPEG, "data:image/jpeg;base64,"),
        ("image/webp", _WEBP, "data:image/webp;base64,"),
    ],
)
async def test_complete_remote_images_are_accepted(
    monkeypatch: pytest.MonkeyPatch,
    content_type: str,
    payload: bytes,
    prefix: str,
) -> None:
    _patch_remote_response(
        monkeypatch,
        [payload[:1], payload[1:]],
        content_type,
        str(len(payload)),
    )

    cache = ProfileCache()
    result = await cache._fetch_remote_avatar(1)
    assert result == (content_type, payload)
    data_uri = profile_module._avatar_result_to_data_uri(result)
    assert data_uri is not None
    assert data_uri_is_valid(data_uri)
    assert data_uri.startswith(prefix)


@pytest.mark.asyncio
async def test_invalid_oversized_and_error_avatar_results_fall_back() -> None:
    calls = 0

    async def fetcher(self_id: int):
        nonlocal calls
        calls += 1
        if calls == 1:
            return "text/plain", b"not-image"
        if calls == 2:
            return "image/png", b"x" * (512 * 1024 + 1)
        raise RuntimeError("avatar failure")

    cache = ProfileCache(avatar_fetcher=fetcher, avatar_ttl=1)
    assert (await cache.update(1, "a")).avatar_data_uri is None
    cache.clear(1)
    assert (await cache.update(1, "b")).avatar_data_uri is None
    cache.clear(1)
    assert (await cache.update(1, "c")).avatar_data_uri is None


@pytest.mark.asyncio
async def test_avatar_timeout_and_short_lived_cache() -> None:
    calls = 0

    async def slow_fetcher(self_id: int):
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.2)
        return _PNG

    cache = ProfileCache(avatar_fetcher=slow_fetcher, avatar_timeout=0.05)
    assert (await cache.update(1, "slow")).avatar_data_uri is None
    assert (await cache.update(1, "cached-failure")).avatar_data_uri is None
    assert calls == 1


@pytest.mark.asyncio
async def test_avatar_cache_reuses_valid_data_without_refetch() -> None:
    calls = 0

    async def fetcher(self_id: int):
        nonlocal calls
        calls += 1
        return _PNG

    cache = ProfileCache(avatar_fetcher=fetcher, avatar_ttl=30)
    first = await cache.update(1, "first")
    second = await cache.update(1, "second")

    assert calls == 1
    assert first.avatar_data_uri == second.avatar_data_uri
    assert second.nickname == "second"


def test_data_uri_validation_rejects_remote_and_malformed_values() -> None:
    valid = "data:image/png;base64," + base64.b64encode(_PNG).decode("ascii")
    assert data_uri_is_valid(valid)
    assert not data_uri_is_valid("https://qlogo.cn/avatar.png")
    assert not data_uri_is_valid("data:image/png;base64,not-base64")

"""Deterministic profile and avatar validation tests."""

from __future__ import annotations

import asyncio
import base64
import json
import stat
import threading
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pytest

import core.profile as profile_module
from core.profile import (
    DEFAULT_AVATAR_TTL,
    DEFAULT_REFRESH_COOLDOWN,
    MAX_AVATAR_BYTES,
    MAX_PROFILE_BYTES,
    MAX_RESTORED_PROFILES,
    ProfileCache,
    data_uri_is_valid,
)

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


@pytest.mark.asyncio
async def test_restart_reuses_successful_profile_and_clear_keeps_disk(
    tmp_path: Path,
) -> None:
    now = [1000.0]
    calls = 0

    async def fetcher(self_id: int):
        nonlocal calls
        calls += 1
        return _PNG

    cache = ProfileCache(
        storage_dir=tmp_path / "profiles", avatar_fetcher=fetcher, clock=lambda: now[0]
    )
    expected = await cache.update(1, "  cached\x00 nickname  ")
    path = tmp_path / "profiles" / "1.json"
    assert stat.S_IMODE(path.parent.stat().st_mode) == 0o700
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    cache.clear()
    assert cache.get(1) is None
    await asyncio.to_thread(cache.load, [1])
    assert cache.get(1) == expected

    restarted = ProfileCache(
        storage_dir=tmp_path / "profiles", avatar_fetcher=fetcher, clock=lambda: now[0]
    )
    await asyncio.to_thread(restarted.load, [1])
    assert restarted.get(1) == expected
    assert not restarted.should_refresh(1)
    assert (
        await restarted.update(1, "new nickname")
    ).avatar_data_uri == expected.avatar_data_uri
    assert calls == 1
    now[0] += DEFAULT_AVATAR_TTL + 1
    assert restarted.get(1) is not None
    assert restarted.get(1).avatar_data_uri == expected.avatar_data_uri
    assert restarted.should_refresh(1)


def test_refresh_claims_suppress_reconnects_until_failure_cooldown_expires() -> None:
    now = [1000.0]
    cache = ProfileCache(clock=lambda: now[0])
    assert cache.should_refresh(1)
    assert not cache.should_refresh(1)
    now[0] += DEFAULT_REFRESH_COOLDOWN - 1
    assert not cache.should_refresh(1)
    now[0] += 1
    assert cache.should_refresh(1)
    assert not cache.should_refresh(1)
    assert cache.should_refresh(2)


@pytest.mark.asyncio
async def test_stale_avatar_survives_failed_refresh_and_restart(tmp_path: Path) -> None:
    now = [1000.0]
    calls = 0

    async def fetcher(self_id: int):
        nonlocal calls
        calls += 1
        if calls == 1:
            return _PNG
        raise RuntimeError("synthetic avatar failure")

    cache = ProfileCache(
        storage_dir=tmp_path / "profiles", avatar_fetcher=fetcher, clock=lambda: now[0]
    )
    first = await cache.update(1, "first")
    now[0] += DEFAULT_AVATAR_TTL + 1
    assert cache.should_refresh(1)
    second = await cache.update(1, "second")
    assert second.nickname == "second"
    assert second.avatar_data_uri == first.avatar_data_uri
    assert not cache.should_refresh(1)
    now[0] += DEFAULT_REFRESH_COOLDOWN
    assert cache.should_refresh(1)

    restarted = ProfileCache(storage_dir=tmp_path / "profiles", clock=lambda: now[0])
    await asyncio.to_thread(restarted.load, [1])
    assert restarted.get(1) == second
    # The newer nickname must not make an old failed avatar look fresh.
    assert restarted.should_refresh(1)


@pytest.mark.asyncio
async def test_nickname_is_visible_and_durable_while_avatar_is_pending(
    tmp_path: Path,
) -> None:
    started = asyncio.Event()
    release = asyncio.Event()

    async def fetcher(self_id: int):
        started.set()
        await release.wait()
        return _PNG

    cache = ProfileCache(storage_dir=tmp_path / "profiles", avatar_fetcher=fetcher)
    task = asyncio.create_task(cache.update(1, "  early\x00 nickname  "))
    try:
        await asyncio.wait_for(started.wait(), timeout=1)
        pending = cache.get(1)
        assert pending is not None
        assert pending.nickname == "early nickname"
        assert pending.avatar_data_uri is None
        restarted = ProfileCache(storage_dir=tmp_path / "profiles")
        await asyncio.to_thread(restarted.load, [1])
        assert restarted.get(1) == pending
        release.set()
        completed = await task
        assert completed.nickname == pending.nickname
        assert data_uri_is_valid(completed.avatar_data_uri)
    finally:
        release.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_overlapping_updates_do_not_restore_older_nickname_or_avatar(
    tmp_path: Path,
) -> None:
    started = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    async def fetcher(self_id: int):
        nonlocal calls
        calls += 1
        if calls == 1:
            started.set()
            await release.wait()
            return _PNG
        return _JPEG

    cache = ProfileCache(storage_dir=tmp_path / "profiles", avatar_fetcher=fetcher)
    older = asyncio.create_task(cache.update(1, "older"))
    newer: asyncio.Task | None = None
    try:
        await asyncio.wait_for(started.wait(), timeout=1)
        newer = asyncio.create_task(cache.update(1, "newer"))
        await asyncio.sleep(0)
        assert cache.get(1).nickname == "newer"
        release.set()
        await asyncio.gather(older, newer)
        latest = cache.get(1)
        assert latest.nickname == "newer"
        assert latest.avatar_data_uri.startswith("data:image/jpeg;base64,")
        restarted = ProfileCache(storage_dir=tmp_path / "profiles")
        await asyncio.to_thread(restarted.load, [1])
        assert restarted.get(1) == latest
    finally:
        release.set()
        tasks = [older] + ([newer] if newer is not None else [])
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.asyncio
async def test_cancelled_writer_cannot_overwrite_newer_persisted_profile(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    started = threading.Event()
    release = threading.Event()
    finished = threading.Event()

    async def fetcher(self_id: int):
        return None

    cache = ProfileCache(storage_dir=tmp_path / "profiles", avatar_fetcher=fetcher)
    original_write = cache._write_profile

    def delayed_write(self_id, revision, cached):
        if cached.snapshot.nickname == "older":
            started.set()
            try:
                if not release.wait(timeout=5):
                    raise RuntimeError("test writer was not released")
                original_write(self_id, revision, cached)
            finally:
                finished.set()
        else:
            original_write(self_id, revision, cached)

    monkeypatch.setattr(cache, "_write_profile", delayed_write)
    older = asyncio.create_task(cache.update(1, "older"))
    try:
        assert await asyncio.to_thread(started.wait, 1)
        older.cancel()
        with pytest.raises(asyncio.CancelledError):
            await older
        latest = await cache.update(1, "newer")
        release.set()
        assert await asyncio.to_thread(finished.wait, 1)
        restarted = ProfileCache(storage_dir=tmp_path / "profiles")
        await asyncio.to_thread(restarted.load, [1])
        assert restarted.get(1) == latest
    finally:
        release.set()
        if not older.done():
            older.cancel()
        await asyncio.gather(older, return_exceptions=True)
        await asyncio.to_thread(finished.wait, 1)


@pytest.mark.asyncio
async def test_clear_invalidates_pending_avatar_without_erasing_durable_nickname(
    tmp_path: Path,
) -> None:
    started = asyncio.Event()
    release = asyncio.Event()

    async def fetcher(self_id: int):
        started.set()
        await release.wait()
        return _PNG

    cache = ProfileCache(storage_dir=tmp_path / "profiles", avatar_fetcher=fetcher)
    pending = asyncio.create_task(cache.update(1, "cached nickname"))
    try:
        await asyncio.wait_for(started.wait(), timeout=1)
        cache.clear(1)
        release.set()
        await pending
        assert cache.get(1) is None
        restarted = ProfileCache(storage_dir=tmp_path / "profiles")
        await asyncio.to_thread(restarted.load, [1])
        assert restarted.get(1).nickname == "cached nickname"
        assert restarted.get(1).avatar_data_uri is None
    finally:
        release.set()
        if not pending.done():
            pending.cancel()
        await asyncio.gather(pending, return_exceptions=True)


@pytest.mark.parametrize(
    "invalid_fields",
    [
        {"self_id": True},
        {"self_id": "2"},
        {"self_id": 1},
        {"nickname_updated_at": float("nan")},
        {"nickname_updated_at": float("inf")},
        {"nickname_updated_at": -1},
        {"nickname_updated_at": True},
        {"nickname_updated_at": 10**1000},
        {"nickname": "unclean\x00"},
        {
            "avatar_data_uri": "https://example.invalid/avatar.png",
            "avatar_updated_at": 1000,
        },
        {
            "avatar_data_uri": "data:image/png;base64," + "A" * MAX_PROFILE_BYTES,
            "avatar_updated_at": 1000,
        },
    ],
)
def test_invalid_cached_account_is_isolated_from_valid_accounts(
    tmp_path: Path, invalid_fields: dict
) -> None:
    directory = tmp_path / "profiles"
    directory.mkdir()
    good = {
        "_schema_version": 1,
        "self_id": 1,
        "nickname": "valid cached nickname",
        "avatar_data_uri": None,
        "nickname_updated_at": 1000.0,
        "avatar_updated_at": None,
    }
    (directory / "1.json").write_text(json.dumps(good), encoding="utf-8")
    bad = {**good, "self_id": 2, **invalid_fields}
    (directory / "2.json").write_text(json.dumps(bad), encoding="utf-8")
    (directory / "3.json").write_bytes(b"x" * (MAX_PROFILE_BYTES + 1))
    (directory / "4.json").write_bytes(b"{\xff")
    warnings: list[str] = []
    cache = ProfileCache(storage_dir=directory, on_cache_error=warnings.append)
    cache.load([1, 2, 3, 4])
    assert cache.get(1).nickname == "valid cached nickname"
    assert all(cache.get(self_id) is None for self_id in (2, 3, 4))
    assert warnings == ["read", "read", "read"]


def test_load_reads_only_bounded_known_accounts(tmp_path: Path) -> None:
    directory = tmp_path / "profiles"
    directory.mkdir()
    for self_id in range(1, MAX_RESTORED_PROFILES + 3):
        payload = {
            "_schema_version": 1,
            "self_id": self_id,
            "nickname": f"cached {self_id}",
            "avatar_data_uri": None,
            "nickname_updated_at": 1000.0,
            "avatar_updated_at": None,
        }
        (directory / f"{self_id}.json").write_text(
            json.dumps(payload), encoding="utf-8"
        )
    cache = ProfileCache(storage_dir=directory)
    cache.load(iter(range(1, MAX_RESTORED_PROFILES + 2)))
    assert all(
        cache.get(self_id) is not None
        for self_id in range(1, MAX_RESTORED_PROFILES + 1)
    )
    assert cache.get(MAX_RESTORED_PROFILES + 1) is None
    assert cache.get(MAX_RESTORED_PROFILES + 2) is None


@pytest.mark.asyncio
async def test_persistence_failure_preserves_memory_and_previous_atomic_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def fetcher(self_id: int):
        return _PNG

    warnings: list[str] = []
    cache = ProfileCache(
        storage_dir=tmp_path / "profiles",
        avatar_fetcher=fetcher,
        on_cache_error=warnings.append,
    )
    original = await cache.update(1, "durable")

    def fail_replace(*args):
        raise PermissionError("synthetic private path must not escape")

    monkeypatch.setattr(profile_module.os, "replace", fail_replace)
    latest = await cache.update(1, "memory remains usable")
    assert cache.get(1) == latest
    assert latest.avatar_data_uri == original.avatar_data_uri
    assert warnings == ["write"]
    assert not list((tmp_path / "profiles").glob("*.tmp"))
    restarted = ProfileCache(storage_dir=tmp_path / "profiles")
    await asyncio.to_thread(restarted.load, [1])
    assert restarted.get(1) == original


@pytest.mark.asyncio
async def test_expired_nickname_refresh_reuses_independently_fresh_avatar() -> None:
    now = [1000.0]
    calls = 0

    async def fetcher(self_id: int):
        nonlocal calls
        calls += 1
        return _PNG

    cache = ProfileCache(
        avatar_fetcher=fetcher, nickname_ttl=100, avatar_ttl=1000, clock=lambda: now[0]
    )
    first = await cache.update(1, "first")
    now[0] += DEFAULT_REFRESH_COOLDOWN + 1
    assert cache.should_refresh(1)
    second = await cache.update(1, "second")
    assert second.nickname == "second"
    assert second.avatar_data_uri == first.avatar_data_uri
    assert calls == 1
    assert not cache.should_refresh(1)


@pytest.mark.asyncio
async def test_cancelled_update_cannot_publish_late_avatar_after_replacement(
    tmp_path: Path,
) -> None:
    started = asyncio.Event()
    cancelled = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    async def fetcher(self_id: int):
        nonlocal calls
        calls += 1
        if calls == 1:
            started.set()
            try:
                await release.wait()
            except asyncio.CancelledError:
                cancelled.set()
                await release.wait()
            return _PNG
        return _JPEG

    cache = ProfileCache(
        storage_dir=tmp_path / "profiles", avatar_fetcher=fetcher, avatar_timeout=5
    )
    older = asyncio.create_task(cache.update(1, "older"))
    newer: asyncio.Task | None = None
    try:
        await asyncio.wait_for(started.wait(), timeout=1)
        older.cancel()
        await asyncio.wait_for(cancelled.wait(), timeout=1)
        newer = asyncio.create_task(cache.update(1, "newer"))
        await asyncio.sleep(0)
        assert cache.get(1).nickname == "newer"
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await older
        latest = await newer
        assert latest.nickname == "newer"
        assert latest.avatar_data_uri.startswith("data:image/jpeg;base64,")
        restarted = ProfileCache(storage_dir=tmp_path / "profiles")
        await asyncio.to_thread(restarted.load, [1])
        assert restarted.get(1) == latest
    finally:
        release.set()
        tasks = [older] + ([newer] if newer is not None else [])
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

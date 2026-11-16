"""Focused reverse-WS login profile correlation tests."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import aiohttp
import pytest

from core.profile import DEFAULT_AVATAR_TTL, ProfileCache
from core.ws_server import WatchdogWSServer

TOKEN = "profile-test-token"


def ws_url(server: WatchdogWSServer) -> str:
    assert server.bound_port is not None
    return f"ws://127.0.0.1:{server.bound_port}/napcat-watchdog/ws"


async def admit(self_id: int) -> int:
    return 1


async def running_profile_server(
    callback,
    *,
    timeout: float = 0.2,
) -> WatchdogWSServer:
    server = WatchdogWSServer(
        host="127.0.0.1",
        port=0,
        access_token=TOKEN,
        profile_timeout=timeout,
    )
    server.set_admission_handler(admit)
    server.set_profile_callback(callback)
    await server.start()
    return server


@pytest.mark.asyncio
async def test_login_info_request_echo_and_response_correlation() -> None:
    profiles: list[tuple[int, int, str]] = []
    profile_event = asyncio.Event()
    events: list[dict] = []

    async def profile_callback(self_id: int, generation: int, nickname: str) -> None:
        profiles.append((self_id, generation, nickname))
        profile_event.set()

    async def event_callback(self_id: int, generation: int, data: dict) -> None:
        events.append(data)

    server = await running_profile_server(profile_callback)
    server.set_event_callback(event_callback)
    try:
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(
                ws_url(server),
                headers={"Authorization": f"Bearer {TOKEN}", "X-Self-ID": "123"},
            ) as ws:
                request = await asyncio.wait_for(ws.receive_json(), timeout=1)
                assert request["action"] == "get_login_info"
                assert request["params"] == {}
                assert request["echo"]
                await ws.send_json(
                    {
                        "status": "ok",
                        "retcode": 0,
                        "data": {"user_id": 123, "nickname": "测试账号"},
                        "echo": request["echo"],
                    }
                )
                await asyncio.wait_for(profile_event.wait(), timeout=1)
                assert profiles == [(123, 1, "测试账号")]

                await ws.send_json(
                    {
                        "post_type": "meta_event",
                        "meta_event_type": "heartbeat",
                        "status": {"online": True},
                        "self_id": 123,
                    }
                )
                await asyncio.sleep(0.05)
                assert len(events) == 1
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_invalid_profile_response_does_not_interfere_with_heartbeat() -> None:
    profiles: list[str] = []
    events: list[dict] = []

    async def profile_callback(self_id: int, generation: int, nickname: str) -> None:
        profiles.append(nickname)

    async def event_callback(self_id: int, generation: int, data: dict) -> None:
        events.append(data)

    server = await running_profile_server(profile_callback)
    server.set_event_callback(event_callback)
    try:
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(
                ws_url(server),
                headers={"Authorization": f"Bearer {TOKEN}", "X-Self-ID": "456"},
            ) as ws:
                request = await ws.receive_json()
                await ws.send_json(
                    {
                        "status": "failed",
                        "retcode": 100,
                        "data": {"user_id": 999, "nickname": "bad"},
                        "echo": request["echo"],
                    }
                )
                await ws.send_json(
                    {
                        "post_type": "meta_event",
                        "meta_event_type": "heartbeat",
                        "status": {"online": True},
                        "self_id": 456,
                    }
                )
                await asyncio.sleep(0.05)
                assert profiles == []
                assert len(events) == 1
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_profile_pending_cleanup_on_replacement_disconnect_and_stop() -> None:
    async def profile_callback(self_id: int, generation: int, nickname: str) -> None:
        return None

    generations = 0

    async def admit_with_generations(self_id: int) -> int:
        nonlocal generations
        generations += 1
        return generations

    server = await running_profile_server(profile_callback, timeout=5)
    server.set_admission_handler(admit_with_generations)
    try:
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(
                ws_url(server),
                headers={"Authorization": f"Bearer {TOKEN}", "X-Self-ID": "789"},
            ) as ws1:
                await ws1.receive_json()
                assert server._pending_actions
                async with session.ws_connect(
                    ws_url(server),
                    headers={"Authorization": f"Bearer {TOKEN}", "X-Self-ID": "789"},
                ) as ws2:
                    await ws2.receive_json()
                    await asyncio.sleep(0.05)
                    assert all(key[1] != 1 for key in server._pending_actions)
                    await ws2.close()
                    await asyncio.sleep(0.05)
                    assert not server._pending_actions
                    assert not server._profile_tasks
    finally:
        await server.stop()
    assert not server._pending_actions
    assert not server._profile_tasks


def test_profile_validator_rejects_mismatch_and_malformed_payloads() -> None:
    from core.ws_server import _validated_login_nickname

    valid = {
        "status": "ok",
        "retcode": 0,
        "data": {"user_id": 1, "nickname": "ok"},
    }
    assert _validated_login_nickname(valid, 1) == "ok"
    assert _validated_login_nickname({**valid, "retcode": 1}, 1) is None
    assert (
        _validated_login_nickname(
            {**valid, "data": {"user_id": 2, "nickname": "wrong"}}, 1
        )
        is None
    )
    assert (
        _validated_login_nickname(
            {**valid, "data": {"user_id": 1, "nickname": "\n"}}, 1
        )
        is None
    )
    assert _validated_login_nickname(json.loads("{}"), 1) is None


@pytest.mark.asyncio
async def test_fresh_profiles_suppress_reconnects_and_stale_profiles_refresh_once(
    tmp_path: Path,
) -> None:
    now = [1000.0]
    generation = 0
    disconnected = asyncio.Event()
    heartbeat = asyncio.Event()

    async def fetcher(self_id: int):
        return b"\xff\xd8\xff\xd9"

    cache = ProfileCache(
        storage_dir=tmp_path / "profiles", avatar_fetcher=fetcher, clock=lambda: now[0]
    )

    async def profile_callback(self_id: int, generation: int, nickname: str) -> None:
        await cache.update(self_id, nickname)

    async def admission(self_id: int) -> int:
        nonlocal generation
        generation += 1
        return generation

    async def on_disconnect(self_id: int, generation: int) -> None:
        disconnected.set()

    async def on_event(self_id: int, generation: int, data: dict) -> None:
        heartbeat.set()

    server = await running_profile_server(profile_callback, timeout=5)
    server.set_admission_handler(admission)
    server.set_disconnect_callback(on_disconnect)
    server.set_event_callback(on_event)
    server.set_profile_refresh_predicate(cache.should_refresh)
    headers = {"Authorization": f"Bearer {TOKEN}", "X-Self-ID": "123"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(ws_url(server), headers=headers) as first:
                request = await asyncio.wait_for(first.receive_json(), timeout=1)
                await first.send_json(
                    {
                        "status": "ok",
                        "retcode": 0,
                        "data": {"user_id": 123, "nickname": "cached"},
                        "echo": request["echo"],
                    }
                )
                await server.wait_for_profile(123)
                assert cache.get(123).nickname == "cached"
            await asyncio.wait_for(disconnected.wait(), timeout=1)
            disconnected.clear()
            async with session.ws_connect(ws_url(server), headers=headers) as second:
                await second.send_json(
                    {
                        "post_type": "meta_event",
                        "meta_event_type": "heartbeat",
                        "status": {"online": True},
                        "self_id": 123,
                    }
                )
                await asyncio.wait_for(heartbeat.wait(), timeout=1)
                server.refresh_profiles()
                assert not server._profile_tasks
                now[0] += DEFAULT_AVATAR_TTL + 1
                server.refresh_profiles()
                in_flight = dict(server._profile_tasks)
                server.refresh_profiles()
                assert len(in_flight) == 1
                assert server._profile_tasks == in_flight
                request = await asyncio.wait_for(second.receive_json(), timeout=1)
                await second.send_json(
                    {
                        "status": "ok",
                        "retcode": 0,
                        "data": {"user_id": 123, "nickname": "refreshed"},
                        "echo": request["echo"],
                    }
                )
                await server.wait_for_profile(123)
                assert cache.get(123).nickname == "refreshed"
            await asyncio.wait_for(disconnected.wait(), timeout=1)
            server.refresh_profiles()
            assert not server._profile_tasks
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_waiter_timeout_and_cancellation_leave_avatar_refresh_running() -> None:
    avatar_started = asyncio.Event()
    avatar_release = asyncio.Event()

    async def fetcher(self_id: int):
        avatar_started.set()
        await avatar_release.wait()
        return b"\xff\xd8\xff\xd9"

    cache = ProfileCache(avatar_fetcher=fetcher, avatar_timeout=5)

    async def profile_callback(self_id: int, generation: int, nickname: str) -> None:
        await cache.update(self_id, nickname)

    server = await running_profile_server(profile_callback, timeout=5)
    try:
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(
                ws_url(server),
                headers={"Authorization": f"Bearer {TOKEN}", "X-Self-ID": "123"},
            ) as ws:
                request = await asyncio.wait_for(ws.receive_json(), timeout=1)
                await ws.send_json(
                    {
                        "status": "ok",
                        "retcode": 0,
                        "data": {"user_id": 123, "nickname": "visible early"},
                        "echo": request["echo"],
                    }
                )
                await asyncio.wait_for(avatar_started.wait(), timeout=1)
                assert cache.get(123).nickname == "visible early"
                assert cache.get(123).avatar_data_uri is None
                refresh = server._profile_tasks[(123, 1)]
                with pytest.raises(asyncio.TimeoutError):
                    await asyncio.wait_for(server.wait_for_profile(123), timeout=0.01)
                assert not refresh.done()
                waiter = asyncio.create_task(server.wait_for_profile(123))
                await asyncio.sleep(0)
                waiter.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await waiter
                assert not refresh.done()
                avatar_release.set()
                await asyncio.wait_for(server.wait_for_profile(123), timeout=1)
                assert cache.get(123).avatar_data_uri is not None
                assert not server._profile_tasks
    finally:
        avatar_release.set()
        await server.stop()


@pytest.mark.asyncio
async def test_waiters_follow_generation_through_cancellation() -> None:
    profiles: list[tuple[int, str]] = []
    generation = 0

    async def profile_callback(self_id: int, generation: int, nickname: str) -> None:
        profiles.append((generation, nickname))

    async def admission(self_id: int) -> int:
        nonlocal generation
        generation += 1
        return generation

    server = await running_profile_server(profile_callback, timeout=5)
    server.set_admission_handler(admission)
    headers = {"Authorization": f"Bearer {TOKEN}", "X-Self-ID": "123"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(ws_url(server), headers=headers) as first:
                old_request = await asyncio.wait_for(first.receive_json(), timeout=1)
                old_waiter = asyncio.create_task(server.wait_for_profile(123))
                await asyncio.sleep(0)
                async with session.ws_connect(
                    ws_url(server), headers=headers
                ) as second:
                    request = await asyncio.wait_for(second.receive_json(), timeout=1)
                    await asyncio.wait_for(old_waiter, timeout=1)
                    waiter = asyncio.create_task(server.wait_for_profile(123))
                    await asyncio.sleep(0)
                    await second.send_json(
                        {
                            "status": "ok",
                            "retcode": 0,
                            "data": {"user_id": 123, "nickname": "stale"},
                            "echo": old_request["echo"],
                        }
                    )
                    await second.send_json(
                        {
                            "status": "ok",
                            "retcode": 0,
                            "data": {"user_id": 123, "nickname": "current"},
                            "echo": request["echo"],
                        }
                    )
                    await asyncio.wait_for(waiter, timeout=1)
                    assert profiles == [(2, "current")]
                    server.refresh_profiles()
                    await asyncio.wait_for(second.receive_json(), timeout=1)
                    disconnected_waiter = asyncio.create_task(
                        server.wait_for_profile(123)
                    )
                    await asyncio.sleep(0)
                    await second.close()
                    await asyncio.wait_for(disconnected_waiter, timeout=1)
                    assert not server._pending_actions
                    assert not server._profile_tasks
        await server.wait_for_profile(123)
        await server.wait_for_profile(999)
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_response_resolved_during_replacement_never_publishes_stale_profile() -> (
    None
):
    profiles: list[str] = []
    server = WatchdogWSServer(access_token=TOKEN)

    async def callback(self_id: int, generation: int, nickname: str) -> None:
        profiles.append(nickname)

    class ReplacingSocket:
        closed = False

        async def send_json(self, request: dict) -> None:
            # Resolve the old echo, then replace the connection before the
            # request task resumes from its response wait.
            server._resolve_action_response(
                123,
                1,
                {
                    "status": "ok",
                    "retcode": 0,
                    "data": {"user_id": 123, "nickname": "stale"},
                    "echo": request["echo"],
                },
            )
            server._connections[123] = (replacement, 2)

    original = ReplacingSocket()
    replacement = ReplacingSocket()
    server.set_profile_callback(callback)
    server._connections[123] = (original, 1)
    server.refresh_profiles()
    await server.wait_for_profile(123)
    assert profiles == []
    assert not server._pending_actions
    assert not server._profile_tasks


@pytest.mark.asyncio
async def test_caller_cancellation_is_preserved_when_refresh_is_also_cancelled() -> (
    None
):
    server = WatchdogWSServer(access_token=TOKEN)
    release = asyncio.Event()
    refresh = asyncio.create_task(release.wait())
    server._connections[123] = (object(), 1)
    server._profile_tasks[(123, 1)] = refresh
    waiter = asyncio.create_task(server.wait_for_profile(123))
    try:
        await asyncio.sleep(0)
        refresh.cancel()
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
    finally:
        refresh.cancel()
        waiter.cancel()
        await asyncio.gather(refresh, waiter, return_exceptions=True)

"""Focused reverse-WS login profile correlation tests."""

from __future__ import annotations

import asyncio
import json

import aiohttp
import pytest

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

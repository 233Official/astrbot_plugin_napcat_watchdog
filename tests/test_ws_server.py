"""Integration tests for WatchdogWSServer using real aiohttp WS connections.

All tests bind to 127.0.0.1 with OS-assigned port 0 to avoid conflicts.
"""

from __future__ import annotations

import asyncio
from typing import Any

import aiohttp
import pytest

from core.token import ensure_access_token
from core.ws_server import WatchdogWSServer

TEST_TOKEN = "test-token-123"
ALT_TOKEN = "alt-token-456"


# ---- Test helpers ----


class _ConfigStub:
    """Minimal stand-in for AstrBotConfig used in token persistence tests."""

    def __init__(self, initial_token: str = "") -> None:
        self._store: dict[str, object] = {"access_token": initial_token}
        self.save_call_count: int = 0
        self.save_fail: bool = False

    def get(self, key: str, default: object = None) -> object:
        return self._store.get(key, default)

    def __setitem__(self, key: str, value: object) -> None:
        self._store[key] = value

    def save_config(self) -> None:
        self.save_call_count += 1
        if self.save_fail:
            msg = "模拟 save_config 写入失败"
            raise OSError(msg)


# ---- Fixtures ----


@pytest.fixture
def token() -> str:
    return TEST_TOKEN


@pytest.fixture
def server(token: str) -> WatchdogWSServer:
    return WatchdogWSServer(
        host="127.0.0.1",
        port=0,
        access_token=token,
    )


@pytest.fixture
async def running_server(server: WatchdogWSServer) -> WatchdogWSServer:
    """Start server with a simple admission handler that returns gen=1."""
    server.set_admission_handler(_simple_admit)
    await server.start()
    yield server
    await server.stop()


@pytest.fixture
async def client_session() -> aiohttp.ClientSession:
    async with aiohttp.ClientSession() as session:
        yield session


async def _simple_admit(self_id: int) -> int:
    """Always admit with generation=1."""
    return 1


def _ws_url(srv: WatchdogWSServer) -> str:
    assert srv.bound_port is not None
    return f"ws://127.0.0.1:{srv.bound_port}/napcat-watchdog/ws"


# ---- Token persistence (real plugin logic) ----


class TestAccessTokenPersistence:
    """Call the actual :func:`ensure_access_token` with a config stub."""

    def test_empty_token_generates_and_persists(self) -> None:
        cfg = _ConfigStub(initial_token="")
        result = ensure_access_token(cfg)

        assert isinstance(result, str)
        assert len(result) >= 43
        assert cfg.save_call_count == 1
        cfg2 = _ConfigStub(initial_token="")
        result2 = ensure_access_token(cfg2)
        assert result != result2

    def test_existing_token_does_not_save(self) -> None:
        cfg = _ConfigStub(initial_token="my-existing-token")
        result = ensure_access_token(cfg)
        assert result == "my-existing-token"
        assert cfg.save_call_count == 0

    def test_save_failure_resets_token_and_raises(self) -> None:
        cfg = _ConfigStub(initial_token="")
        cfg.save_fail = True

        with pytest.raises(RuntimeError, match="access_token persistence failed"):
            ensure_access_token(cfg)

        stored: object = cfg.get("access_token", "")
        assert stored == ""


# ---- Auth (401) ----


class TestAuth:
    """Bearer token authentication tests."""

    async def test_auth_success(
        self,
        running_server: WatchdogWSServer,
        client_session: aiohttp.ClientSession,
    ) -> None:
        async with client_session.ws_connect(
            _ws_url(running_server),
            headers={
                "Authorization": f"Bearer {TEST_TOKEN}",
                "X-Self-ID": "12345",
            },
        ) as ws:
            assert not ws.closed

    async def test_missing_token_returns_401(
        self,
        running_server: WatchdogWSServer,
        client_session: aiohttp.ClientSession,
    ) -> None:
        with pytest.raises(aiohttp.WSServerHandshakeError) as exc:
            async with client_session.ws_connect(
                _ws_url(running_server),
                headers={"X-Self-ID": "12345"},
            ):
                pass
        assert exc.value.status == 401
        hdrs = exc.value.headers
        assert hdrs is not None
        assert hdrs.get("WWW-Authenticate") == "Bearer"

    async def test_wrong_token_returns_401(
        self,
        running_server: WatchdogWSServer,
        client_session: aiohttp.ClientSession,
    ) -> None:
        with pytest.raises(aiohttp.WSServerHandshakeError) as exc:
            async with client_session.ws_connect(
                _ws_url(running_server),
                headers={
                    "Authorization": "Bearer wrong-token",
                    "X-Self-ID": "12345",
                },
            ):
                pass
        assert exc.value.status == 401

    async def test_malformed_auth_header_returns_401(
        self,
        running_server: WatchdogWSServer,
        client_session: aiohttp.ClientSession,
    ) -> None:
        with pytest.raises(aiohttp.WSServerHandshakeError) as exc:
            async with client_session.ws_connect(
                _ws_url(running_server),
                headers={
                    "Authorization": "Basic xyz",
                    "X-Self-ID": "12345",
                },
            ):
                pass
        assert exc.value.status == 401
        hdrs = exc.value.headers
        assert hdrs is not None
        assert hdrs.get("WWW-Authenticate") == "Bearer"


# ---- X-Self-ID (400) ----


class TestSelfID:
    """X-Self-ID validation tests."""

    async def test_missing_self_id_returns_400(
        self,
        running_server: WatchdogWSServer,
        client_session: aiohttp.ClientSession,
    ) -> None:
        with pytest.raises(aiohttp.WSServerHandshakeError) as exc:
            async with client_session.ws_connect(
                _ws_url(running_server),
                headers={"Authorization": f"Bearer {TEST_TOKEN}"},
            ):
                pass
        assert exc.value.status == 400

    async def test_empty_self_id_returns_400(
        self,
        running_server: WatchdogWSServer,
        client_session: aiohttp.ClientSession,
    ) -> None:
        with pytest.raises(aiohttp.WSServerHandshakeError) as exc:
            async with client_session.ws_connect(
                _ws_url(running_server),
                headers={
                    "Authorization": f"Bearer {TEST_TOKEN}",
                    "X-Self-ID": "",
                },
            ):
                pass
        assert exc.value.status == 400

    async def test_non_digit_self_id_returns_400(
        self,
        running_server: WatchdogWSServer,
        client_session: aiohttp.ClientSession,
    ) -> None:
        with pytest.raises(aiohttp.WSServerHandshakeError) as exc:
            async with client_session.ws_connect(
                _ws_url(running_server),
                headers={
                    "Authorization": f"Bearer {TEST_TOKEN}",
                    "X-Self-ID": "abc",
                },
            ):
                pass
        assert exc.value.status == 400

    async def test_negative_self_id_returns_400(
        self,
        running_server: WatchdogWSServer,
        client_session: aiohttp.ClientSession,
    ) -> None:
        with pytest.raises(aiohttp.WSServerHandshakeError) as exc:
            async with client_session.ws_connect(
                _ws_url(running_server),
                headers={
                    "Authorization": f"Bearer {TEST_TOKEN}",
                    "X-Self-ID": "-1",
                },
            ):
                pass
        assert exc.value.status == 400

    async def test_zero_self_id_returns_400(
        self,
        running_server: WatchdogWSServer,
        client_session: aiohttp.ClientSession,
    ) -> None:
        with pytest.raises(aiohttp.WSServerHandshakeError) as exc:
            async with client_session.ws_connect(
                _ws_url(running_server),
                headers={
                    "Authorization": f"Bearer {TEST_TOKEN}",
                    "X-Self-ID": "0",
                },
            ):
                pass
        assert exc.value.status == 400


# ---- Connection tracking & replacement ----


class TestConnectionTracking:
    """Connection management and same-self_id replacement."""

    async def test_multiple_self_ids_tracked_separately(
        self,
        running_server: WatchdogWSServer,
        client_session: aiohttp.ClientSession,
    ) -> None:
        url = _ws_url(running_server)
        headers = {"Authorization": f"Bearer {TEST_TOKEN}"}

        async with (
            client_session.ws_connect(
                url, headers={**headers, "X-Self-ID": "111"}
            ) as ws1,
            client_session.ws_connect(
                url, headers={**headers, "X-Self-ID": "222"}
            ) as ws2,
        ):
            assert not ws1.closed
            assert not ws2.closed
            assert running_server.connection_count == 2

    async def test_same_self_id_replaces_old_connection(
        self,
        running_server: WatchdogWSServer,
        client_session: aiohttp.ClientSession,
    ) -> None:
        """New connection with same self_id → old connection closed."""
        url = _ws_url(running_server)
        headers = {
            "Authorization": f"Bearer {TEST_TOKEN}",
            "X-Self-ID": "12345",
        }

        async with client_session.ws_connect(url, headers=headers) as ws1:
            async with client_session.ws_connect(url, headers=headers) as ws2:
                await ws1.receive()
                assert ws1.closed
                assert ws1.close_code == 1001
                assert not ws2.closed
                assert running_server.connection_count == 1

    async def test_replacement_order_new_first(
        self,
        running_server: WatchdogWSServer,
        client_session: aiohttp.ClientSession,
    ) -> None:
        """New connection is written to _connections BEFORE old is closed.

        The server stores ``web.WebSocketResponse`` objects which are
        different from the client's ``ClientWebSocketResponse`` objects.
        We verify the ordering by checking that ``ws1`` (first)
        receives a close frame (meaning it was replaced).
        """
        url = _ws_url(running_server)
        headers = {
            "Authorization": f"Bearer {TEST_TOKEN}",
            "X-Self-ID": "12345",
        }

        async with client_session.ws_connect(url, headers=headers) as ws1:
            async with client_session.ws_connect(url, headers=headers) as ws2:
                # ws1 should have received a close frame (replaced)
                msg = await ws1.receive()
                assert msg.type == aiohttp.WSMsgType.CLOSE
                assert msg.data == 1001
                assert ws1.close_code == 1001
                # ws2 should still be open
                assert not ws2.closed
                # There should be exactly 1 connection tracked
                assert running_server.connection_count == 1

    async def test_old_connection_finally_does_not_remove_new(
        self,
        running_server: WatchdogWSServer,
        client_session: aiohttp.ClientSession,
    ) -> None:
        """Old WS finally block does not delete the new connection.

        This is guaranteed by the replacement order: new is written
        first, so old's finally finds that _connections[self_id] is
        the new ws (not the old one).
        """
        url = _ws_url(running_server)
        headers = {
            "Authorization": f"Bearer {TEST_TOKEN}",
            "X-Self-ID": "12345",
        }

        # Use _simple_admit which always returns gen=1 per connect
        # Track connection count across the replacement
        async with client_session.ws_connect(url, headers=headers) as ws1:
            async with client_session.ws_connect(url, headers=headers) as ws2:
                # Wait for ws1 to receive the close frame (replaced)
                msg = await ws1.receive()
                assert msg.type == aiohttp.WSMsgType.CLOSE
                assert msg.data == 1001
                assert ws1.closed
                assert ws1.close_code == 1001
                # ws2 should be active
                assert not ws2.closed
                # Exactly 1 connection tracked
                assert running_server.connection_count == 1

        # After both contexts exit, connection_count should be 0
        await asyncio.sleep(0.05)
        assert running_server.connection_count == 0, (
            f"Expected 0 connections, got {running_server.connection_count}"
        )


# ---- Token rotation ----


class TestTokenRotation:
    """Token rotation closes existing connections and switches auth."""

    async def test_rotation_closes_existing_connections(
        self,
        running_server: WatchdogWSServer,
        client_session: aiohttp.ClientSession,
    ) -> None:
        headers = {
            "Authorization": f"Bearer {TEST_TOKEN}",
            "X-Self-ID": "12345",
        }
        async with client_session.ws_connect(
            _ws_url(running_server), headers=headers
        ) as ws:
            await running_server.update_access_token(ALT_TOKEN)
            msg = await ws.receive()
            assert msg.type == aiohttp.WSMsgType.CLOSE
            assert msg.data == 1001
            assert ws.close_code == 1001

    async def test_old_token_rejected_after_rotation(
        self,
        running_server: WatchdogWSServer,
        client_session: aiohttp.ClientSession,
    ) -> None:
        await running_server.update_access_token(ALT_TOKEN)
        with pytest.raises(aiohttp.WSServerHandshakeError) as exc:
            async with client_session.ws_connect(
                _ws_url(running_server),
                headers={
                    "Authorization": f"Bearer {TEST_TOKEN}",
                    "X-Self-ID": "12345",
                },
            ):
                pass
        assert exc.value.status == 401

    async def test_new_token_accepted_after_rotation(
        self,
        running_server: WatchdogWSServer,
        client_session: aiohttp.ClientSession,
    ) -> None:
        await running_server.update_access_token(ALT_TOKEN)
        async with client_session.ws_connect(
            _ws_url(running_server),
            headers={
                "Authorization": f"Bearer {ALT_TOKEN}",
                "X-Self-ID": "12345",
            },
        ) as ws:
            assert not ws.closed

    async def test_connection_count_cleared_after_rotation(
        self,
        running_server: WatchdogWSServer,
        client_session: aiohttp.ClientSession,
    ) -> None:
        headers = {
            "Authorization": f"Bearer {TEST_TOKEN}",
            "X-Self-ID": "12345",
        }
        async with client_session.ws_connect(_ws_url(running_server), headers=headers):
            await running_server.update_access_token(ALT_TOKEN)
            assert running_server.connection_count == 0


# ---- Stop ----


class TestServerStop:
    """Server stop tears down connections and frees the port."""

    async def test_stop_closes_all_connections(
        self,
        server: WatchdogWSServer,
        client_session: aiohttp.ClientSession,
    ) -> None:
        server.set_admission_handler(_simple_admit)
        await server.start()
        headers = {
            "Authorization": f"Bearer {TEST_TOKEN}",
            "X-Self-ID": "12345",
        }
        async with client_session.ws_connect(_ws_url(server), headers=headers) as ws:
            await server.stop()
            msg = await ws.receive()
            assert msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSE)
            assert ws.close_code == 1001

    async def test_stop_releases_port(
        self,
        server: WatchdogWSServer,
    ) -> None:
        server.set_admission_handler(_simple_admit)
        await server.start()
        port = server.bound_port
        assert port is not None
        await server.stop()

        srv2 = WatchdogWSServer(
            host="127.0.0.1",
            port=port,
            access_token=TEST_TOKEN,
        )
        try:
            srv2.set_admission_handler(_simple_admit)
            await srv2.start()
            assert srv2.bound_port == port
        finally:
            await srv2.stop()

    async def test_stop_is_idempotent(
        self,
        running_server: WatchdogWSServer,
    ) -> None:
        await running_server.stop()
        await running_server.stop()
        assert running_server.bound_port is None

    async def test_server_can_restart_after_stop(
        self,
        server: WatchdogWSServer,
        client_session: aiohttp.ClientSession,
    ) -> None:
        server.set_admission_handler(_simple_admit)
        await server.start()
        await server.stop()

        server.set_admission_handler(_simple_admit)
        await server.start()
        headers = {
            "Authorization": f"Bearer {TEST_TOKEN}",
            "X-Self-ID": "12345",
        }
        async with client_session.ws_connect(_ws_url(server), headers=headers) as ws:
            assert not ws.closed
        await server.stop()


# ---- Non-JSON & binary messages ----


class TestMessageHandling:
    """Non-JSON and binary messages should not crash the server."""

    async def test_non_json_text_does_not_crash(
        self,
        running_server: WatchdogWSServer,
        client_session: aiohttp.ClientSession,
    ) -> None:
        headers = {
            "Authorization": f"Bearer {TEST_TOKEN}",
            "X-Self-ID": "12345",
        }
        async with client_session.ws_connect(
            _ws_url(running_server), headers=headers
        ) as ws:
            await ws.send_str("this is not json")
            await ws.send_json({"type": "heartbeat"})
            assert not ws.closed

    async def test_binary_message_does_not_crash(
        self,
        running_server: WatchdogWSServer,
        client_session: aiohttp.ClientSession,
    ) -> None:
        headers = {
            "Authorization": f"Bearer {TEST_TOKEN}",
            "X-Self-ID": "12345",
        }
        async with client_session.ws_connect(
            _ws_url(running_server), headers=headers
        ) as ws:
            await ws.send_bytes(b"binary data")
            assert not ws.closed


# ---- Strict heartbeat validation via event callback ----


class TestHeartbeatValidation:
    """Only strict heartbeats reach the event callback."""

    async def test_valid_heartbeat_dispatched(
        self,
        running_server: WatchdogWSServer,
        client_session: aiohttp.ClientSession,
    ) -> None:
        received: list[tuple[int, int, dict[str, Any]]] = []
        received_event = asyncio.Event()

        async def callback(self_id: int, generation: int, data: dict[str, Any]) -> None:
            received.append((self_id, generation, data))
            received_event.set()

        running_server.set_event_callback(callback)

        headers = {
            "Authorization": f"Bearer {TEST_TOKEN}",
            "X-Self-ID": "12345",
        }
        async with client_session.ws_connect(
            _ws_url(running_server), headers=headers
        ) as ws:
            await ws.send_json(
                {
                    "post_type": "meta_event",
                    "meta_event_type": "heartbeat",
                    "status": {"online": True},
                    "self_id": 12345,
                }
            )
            await asyncio.wait_for(received_event.wait(), timeout=2)

        assert len(received) == 1
        assert received[0][0] == 12345  # self_id
        assert received[0][1] == 1  # generation
        assert received[0][2]["status"]["online"] is True

    async def test_invalid_post_type_not_dispatched(
        self,
        running_server: WatchdogWSServer,
        client_session: aiohttp.ClientSession,
    ) -> None:
        received: list[tuple[int, int, dict[str, Any]]] = []
        received_event = asyncio.Event()

        async def callback(self_id: int, generation: int, data: dict[str, Any]) -> None:
            received.append((self_id, generation, data))
            received_event.set()

        running_server.set_event_callback(callback)

        headers = {
            "Authorization": f"Bearer {TEST_TOKEN}",
            "X-Self-ID": "12345",
        }
        async with client_session.ws_connect(
            _ws_url(running_server), headers=headers
        ) as ws:
            await ws.send_json(
                {
                    "post_type": "message",
                    "meta_event_type": "heartbeat",
                    "status": {"online": True},
                }
            )
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(received_event.wait(), timeout=0.2)

        assert len(received) == 0

    async def test_self_id_mismatch_not_dispatched(
        self,
        running_server: WatchdogWSServer,
        client_session: aiohttp.ClientSession,
    ) -> None:
        received: list[tuple[int, int, dict[str, Any]]] = []
        received_event = asyncio.Event()

        async def callback(self_id: int, generation: int, data: dict[str, Any]) -> None:
            received.append((self_id, generation, data))
            received_event.set()

        running_server.set_event_callback(callback)

        headers = {
            "Authorization": f"Bearer {TEST_TOKEN}",
            "X-Self-ID": "12345",
        }
        async with client_session.ws_connect(
            _ws_url(running_server), headers=headers
        ) as ws:
            await ws.send_json(
                {
                    "post_type": "meta_event",
                    "meta_event_type": "heartbeat",
                    "status": {"online": True},
                    "self_id": 99999,  # Mismatch!
                }
            )
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(received_event.wait(), timeout=0.2)

        assert len(received) == 0

    async def test_status_not_dict_not_dispatched(
        self,
        running_server: WatchdogWSServer,
        client_session: aiohttp.ClientSession,
    ) -> None:
        received: list[tuple[int, int, dict[str, Any]]] = []
        received_event = asyncio.Event()

        async def callback(self_id: int, generation: int, data: dict[str, Any]) -> None:
            received.append((self_id, generation, data))
            received_event.set()

        running_server.set_event_callback(callback)

        headers = {
            "Authorization": f"Bearer {TEST_TOKEN}",
            "X-Self-ID": "12345",
        }
        async with client_session.ws_connect(
            _ws_url(running_server), headers=headers
        ) as ws:
            await ws.send_json(
                {
                    "post_type": "meta_event",
                    "meta_event_type": "heartbeat",
                    "status": "not_a_dict",
                }
            )
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(received_event.wait(), timeout=0.2)

        assert len(received) == 0

    async def test_online_not_bool_not_dispatched(
        self,
        running_server: WatchdogWSServer,
        client_session: aiohttp.ClientSession,
    ) -> None:
        received: list[tuple[int, int, dict[str, Any]]] = []
        received_event = asyncio.Event()

        async def callback(self_id: int, generation: int, data: dict[str, Any]) -> None:
            received.append((self_id, generation, data))
            received_event.set()

        running_server.set_event_callback(callback)

        headers = {
            "Authorization": f"Bearer {TEST_TOKEN}",
            "X-Self-ID": "12345",
        }
        async with client_session.ws_connect(
            _ws_url(running_server), headers=headers
        ) as ws:
            await ws.send_json(
                {
                    "post_type": "meta_event",
                    "meta_event_type": "heartbeat",
                    "status": {"online": 1},  # int, not bool!
                }
            )
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(received_event.wait(), timeout=0.2)

        assert len(received) == 0

    async def test_generation_passed_to_callback(
        self,
        client_session: aiohttp.ClientSession,
    ) -> None:
        """Generation from admission handler is passed through to event callback."""
        server = WatchdogWSServer(host="127.0.0.1", port=0, access_token=TEST_TOKEN)

        async def admit(self_id: int) -> int:
            return 42  # custom generation

        server.set_admission_handler(admit)
        await server.start()

        received: list[tuple[int, int, dict[str, Any]]] = []
        received_event = asyncio.Event()

        async def callback(self_id: int, generation: int, data: dict[str, Any]) -> None:
            received.append((self_id, generation, data))
            received_event.set()

        server.set_event_callback(callback)

        headers = {
            "Authorization": f"Bearer {TEST_TOKEN}",
            "X-Self-ID": "12345",
        }
        async with client_session.ws_connect(_ws_url(server), headers=headers) as ws:
            await ws.send_json(
                {
                    "post_type": "meta_event",
                    "meta_event_type": "heartbeat",
                    "status": {"online": True},
                    "self_id": 12345,
                }
            )
            await asyncio.wait_for(received_event.wait(), timeout=2)

        await server.stop()

        assert len(received) == 1
        assert received[0][1] == 42  # generation passed through


# ---- Event callback - non-dict JSON discarded ----


class TestNonDictJson:
    """JSON arrays/strings/numbers/bools/null are not dispatched."""

    @pytest.mark.parametrize(
        ("payload", "desc"),
        [
            ("[1, 2, 3]", "array"),
            ('"hello"', "string"),
            ("42", "number"),
            ("true", "boolean"),
            ("null", "null"),
        ],
    )
    async def test_non_dict_json_not_dispatched(
        self,
        running_server: WatchdogWSServer,
        client_session: aiohttp.ClientSession,
        payload: str,
        desc: str,
    ) -> None:
        received: list[tuple[int, int, dict[str, Any]]] = []
        received_event = asyncio.Event()

        async def callback(self_id: int, generation: int, data: dict[str, Any]) -> None:
            received.append((self_id, generation, data))
            received_event.set()

        running_server.set_event_callback(callback)

        headers = {
            "Authorization": f"Bearer {TEST_TOKEN}",
            "X-Self-ID": "12345",
        }
        async with client_session.ws_connect(
            _ws_url(running_server), headers=headers
        ) as ws:
            await ws.send_str(payload)
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(received_event.wait(), timeout=0.2)

        assert len(received) == 0


# ---- start() raises when already running ----


class TestStartIdempotency:
    async def test_double_start_raises(
        self,
        running_server: WatchdogWSServer,
    ) -> None:
        with pytest.raises(RuntimeError, match="already running"):
            await running_server.start()


# ---- bound_port ----


class TestBoundPort:
    async def test_bound_port_none_before_start(self) -> None:
        srv = WatchdogWSServer(access_token=TEST_TOKEN)
        assert srv.bound_port is None

    async def test_bound_port_after_start(
        self,
        running_server: WatchdogWSServer,
    ) -> None:
        assert running_server.bound_port is not None
        assert isinstance(running_server.bound_port, int)
        assert running_server.bound_port > 0

    async def test_bound_port_none_after_stop(
        self,
        running_server: WatchdogWSServer,
    ) -> None:
        await running_server.stop()
        assert running_server.bound_port is None


# ---- Empty token rejection ----


class TestEmptyToken:
    def test_empty_token_raises_on_construction(self) -> None:
        with pytest.raises(ValueError, match="access_token must not be empty"):
            WatchdogWSServer(access_token="")

    def test_default_token_raises_on_construction(self) -> None:
        with pytest.raises(ValueError, match="access_token must not be empty"):
            WatchdogWSServer()

    async def test_update_empty_token_raises(
        self,
        running_server: WatchdogWSServer,
    ) -> None:
        with pytest.raises(ValueError, match="access_token must not be empty"):
            await running_server.update_access_token("")

    async def test_token_preserved_after_empty_update_rejected(
        self,
        running_server: WatchdogWSServer,
    ) -> None:
        assert running_server.access_token == TEST_TOKEN
        with pytest.raises(ValueError):
            await running_server.update_access_token("")
        assert running_server.access_token == TEST_TOKEN


# ---- Cancel capacity handler (reservation release) ----


class TestCancelCapacity:
    """Cancel capacity handler releases reservation on failures."""

    async def test_cancel_handler_is_wired_and_callable(
        self,
        server: WatchdogWSServer,
    ) -> None:
        """Cancel capacity handler is properly set and callable."""
        cancel_called: list[int] = []

        async def cancel_cb(self_id: int) -> None:
            cancel_called.append(self_id)

        async def can_reg(self_id: int) -> bool:
            return True

        server.set_can_register_handler(can_reg)
        server.set_cancel_capacity_handler(cancel_cb)
        await server.start()

        assert server._cancel_capacity_handler is not None

        # Directly invoke the handler to verify it works
        await server._cancel_capacity_handler(42)
        assert cancel_called == [42]

        await server.stop()

    async def test_admission_raise_does_not_trigger_cancel_capacity(
        self,
        server: WatchdogWSServer,
        client_session: aiohttp.ClientSession,
    ) -> None:
        """Admission raise does NOT trigger cancel capacity (reservation
        has been consumed by confirm_connection)."""
        cancel_called: list[int] = []

        async def cancel_cb(self_id: int) -> None:
            cancel_called.append(self_id)

        async def can_reg(self_id: int) -> bool:
            return True

        async def failing_admit(self_id: int) -> int:
            raise RuntimeError("admission failed")

        server.set_can_register_handler(can_reg)
        server.set_cancel_capacity_handler(cancel_cb)
        server.set_admission_handler(failing_admit)
        await server.start()

        async with client_session.ws_connect(
            _ws_url(server),
            headers={
                "Authorization": f"Bearer {TEST_TOKEN}",
                "X-Self-ID": "12345",
            },
        ) as ws:
            msg = await ws.receive()
            assert msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED)

        # Cancel should NOT be called for admission failures
        # (reservation was consumed by confirm_connection inside admission)
        assert len(cancel_called) == 0

        await server.stop()


# ---- Admission handler failures ----


class TestAdmissionFailure:
    """When admission handler raises, the connection is closed after upgrade.

    The WebSocket upgrade succeeds (401/400 checks pass), but the
    admission handler rejects the connection post-upgrade, sending
    a close frame with code 1011.  The client sees a successful
    upgrade followed by close, *not* a handshake error.
    """

    async def test_admission_raise_closes_connection(
        self,
        server: WatchdogWSServer,
        client_session: aiohttp.ClientSession,
    ) -> None:
        async def failing_admit(self_id: int) -> int:
            raise RuntimeError("admission failed")

        server.set_admission_handler(failing_admit)
        await server.start()

        # Upgrade should succeed (auth passes), then close with 1011
        async with client_session.ws_connect(
            _ws_url(server),
            headers={
                "Authorization": f"Bearer {TEST_TOKEN}",
                "X-Self-ID": "12345",
            },
        ) as ws:
            msg = await ws.receive()
            assert msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED)
            assert ws.close_code == 1011

        await server.stop()

    async def test_no_admission_handler_closes_connection(
        self,
        server: WatchdogWSServer,
        client_session: aiohttp.ClientSession,
    ) -> None:
        """Without admission handler, connection is closed after upgrade."""
        await server.start()

        async with client_session.ws_connect(
            _ws_url(server),
            headers={
                "Authorization": f"Bearer {TEST_TOKEN}",
                "X-Self-ID": "12345",
            },
        ) as ws:
            msg = await ws.receive()
            assert msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED)

        await server.stop()


# ---- Disconnect callback ----


class TestDisconnectCallback:
    """Disconnect callback fires with correct self_id and generation."""

    async def test_disconnect_callback_fires(
        self,
        running_server: WatchdogWSServer,
        client_session: aiohttp.ClientSession,
    ) -> None:
        disconnects: list[tuple[int, int]] = []
        disconnect_event = asyncio.Event()

        async def on_disconnect(self_id: int, generation: int) -> None:
            disconnects.append((self_id, generation))
            disconnect_event.set()

        running_server.set_disconnect_callback(on_disconnect)

        headers = {
            "Authorization": f"Bearer {TEST_TOKEN}",
            "X-Self-ID": "12345",
        }
        async with client_session.ws_connect(_ws_url(running_server), headers=headers):
            pass  # close on exit

        await asyncio.wait_for(disconnect_event.wait(), timeout=2)
        assert len(disconnects) == 1
        assert disconnects[0][0] == 12345
        assert disconnects[0][1] == 1  # generation from simple_admit

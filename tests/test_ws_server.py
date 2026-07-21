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
    await server.start()
    yield server
    await server.stop()


@pytest.fixture
async def client_session() -> aiohttp.ClientSession:
    async with aiohttp.ClientSession() as session:
        yield session


def _ws_url(srv: WatchdogWSServer) -> str:
    assert srv.bound_port is not None
    return f"ws://127.0.0.1:{srv.bound_port}/napcat-watchdog/ws"


# ---- Token persistence (real plugin logic) ----


class TestAccessTokenPersistence:
    """Call the actual :func:`ensure_access_token` with a config stub.

    These tests replace the earlier ``TestTokenGeneration`` that only
    verified ``secrets.token_urlsafe`` in isolation.
    """

    def test_empty_token_generates_and_persists(self) -> None:
        """Empty access_token → token generated, save_config called once."""
        cfg = _ConfigStub(initial_token="")
        result = ensure_access_token(cfg)

        assert isinstance(result, str)
        assert len(result) >= 43  # ceil(32*4/3) = 43
        assert cfg.save_call_count == 1
        # Verify it's actually random
        cfg2 = _ConfigStub(initial_token="")
        result2 = ensure_access_token(cfg2)
        assert result != result2

    def test_existing_token_does_not_save(self) -> None:
        """Non-empty access_token → returned as-is, save_config NOT called."""
        cfg = _ConfigStub(initial_token="my-existing-token")
        result = ensure_access_token(cfg)

        assert result == "my-existing-token"
        assert cfg.save_call_count == 0

    def test_save_failure_resets_token_and_raises(self) -> None:
        """save_config() exception → token reset to '' and RuntimeError."""
        cfg = _ConfigStub(initial_token="")
        cfg.save_fail = True

        with pytest.raises(RuntimeError, match="access_token persistence failed"):
            ensure_access_token(cfg)

        # In-memory token must be empty so the caller refuses to start
        stored: object = cfg.get("access_token", "")
        assert stored == ""


# ---- Schema defaults ----


class TestSchemaDefaults:
    """Verify schema default values match requirements."""

    def test_defaults_loaded(self) -> None:
        """Server defaults should match the specification."""
        srv = WatchdogWSServer(access_token=TEST_TOKEN)
        assert srv._host == "0.0.0.0"
        assert srv._port == 19090
        assert srv._path == "/napcat-watchdog/ws"
        assert srv._access_token == TEST_TOKEN


# ---- Auth (401) ----


class TestAuth:
    """Bearer token authentication tests."""

    async def test_auth_success(
        self,
        running_server: WatchdogWSServer,
        client_session: aiohttp.ClientSession,
    ) -> None:
        """Correct token + valid X-Self-ID → WebSocket upgrade succeeds."""
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
        """Missing Authorization header → 401 + WWW-Authenticate."""
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
        """Wrong Bearer token → 401 + WWW-Authenticate."""
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
        hdrs = exc.value.headers
        assert hdrs is not None
        assert hdrs.get("WWW-Authenticate") == "Bearer"

    async def test_malformed_auth_header_returns_401(
        self,
        running_server: WatchdogWSServer,
        client_session: aiohttp.ClientSession,
    ) -> None:
        """Authorization header without 'Bearer ' prefix → 401 + WWW-Authenticate."""
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
        """Missing X-Self-ID header → 400."""
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
        """Empty X-Self-ID → 400."""
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
        """Non-numeric X-Self-ID → 400."""
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
        """Negative integer X-Self-ID → 400."""
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
        """Zero X-Self-ID → 400 (positive integer required)."""
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
        """Two different self_ids → both connected."""
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
                # Consume the close frame sent by the server
                await ws1.receive()
                assert ws1.closed
                assert ws1.close_code == 1001
                assert not ws2.closed
                assert running_server.connection_count == 1

    async def test_old_connection_replaced_after_reconnect(
        self,
        running_server: WatchdogWSServer,
        client_session: aiohttp.ClientSession,
    ) -> None:
        """Replacement: old WS receives close frame with reason."""
        url = _ws_url(running_server)
        headers = {
            "Authorization": f"Bearer {TEST_TOKEN}",
            "X-Self-ID": "12345",
        }

        async with client_session.ws_connect(url, headers=headers) as ws1:
            async with client_session.ws_connect(url, headers=headers):
                # ws1 should have received a close frame
                msg = await ws1.receive()
                assert msg.type == aiohttp.WSMsgType.CLOSE
                assert msg.data == 1001
                assert ws1.close_code == 1001


# ---- Token rotation ----


class TestTokenRotation:
    """Token rotation closes existing connections and switches auth."""

    async def test_rotation_closes_existing_connections(
        self,
        running_server: WatchdogWSServer,
        client_session: aiohttp.ClientSession,
    ) -> None:
        """After rotation, existing connections receive close frame."""
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
        """After rotation, old token → 401."""
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
        """After rotation, new token → connection succeeds."""
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
        """After rotation, connection count drops to 0."""
        headers = {
            "Authorization": f"Bearer {TEST_TOKEN}",
            "X-Self-ID": "12345",
        }
        async with client_session.ws_connect(_ws_url(running_server), headers=headers):
            await running_server.update_access_token(ALT_TOKEN)
            # _close_all clears the dict synchronously before any await,
            # so connection_count is 0 immediately after return.
            assert running_server.connection_count == 0


# ---- Stop ----


class TestServerStop:
    """Server stop tears down connections and frees the port."""

    async def test_stop_closes_all_connections(
        self,
        server: WatchdogWSServer,
        client_session: aiohttp.ClientSession,
    ) -> None:
        """Server stop → all WS connections receive close frame."""
        await server.start()
        headers = {
            "Authorization": f"Bearer {TEST_TOKEN}",
            "X-Self-ID": "12345",
        }
        async with client_session.ws_connect(_ws_url(server), headers=headers) as ws:
            await server.stop()
            msg = await ws.receive()
            # CLOSED or CLOSE frame depending on timing
            assert msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSE)
            assert ws.close_code == 1001

    async def test_stop_releases_port(
        self,
        server: WatchdogWSServer,
    ) -> None:
        """After stop, the same port can be reused."""
        await server.start()
        port = server.bound_port
        assert port is not None
        await server.stop()

        # Start another server on the same port (should succeed now)
        srv2 = WatchdogWSServer(
            host="127.0.0.1",
            port=port,
            access_token=TEST_TOKEN,
        )
        try:
            await srv2.start()
            assert srv2.bound_port == port
        finally:
            await srv2.stop()

    async def test_stop_is_idempotent(
        self,
        running_server: WatchdogWSServer,
    ) -> None:
        """Calling stop multiple times does not raise."""
        await running_server.stop()
        await running_server.stop()  # second call should be safe
        assert running_server.bound_port is None

    async def test_server_can_restart_after_stop(
        self,
        server: WatchdogWSServer,
        client_session: aiohttp.ClientSession,
    ) -> None:
        """After stop → start, the server is functional again."""
        await server.start()
        await server.stop()

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
        """Sending non-JSON text → server stays up and responds to next message."""
        headers = {
            "Authorization": f"Bearer {TEST_TOKEN}",
            "X-Self-ID": "12345",
        }
        async with client_session.ws_connect(
            _ws_url(running_server), headers=headers
        ) as ws:
            await ws.send_str("this is not json")
            await ws.send_json({"type": "heartbeat"})
            # If we get here without error, the server survived
            assert not ws.closed

    async def test_binary_message_does_not_crash(
        self,
        running_server: WatchdogWSServer,
        client_session: aiohttp.ClientSession,
    ) -> None:
        """Sending binary data → server stays up."""
        headers = {
            "Authorization": f"Bearer {TEST_TOKEN}",
            "X-Self-ID": "12345",
        }
        async with client_session.ws_connect(
            _ws_url(running_server), headers=headers
        ) as ws:
            await ws.send_bytes(b"binary data")
            # Still connected
            assert not ws.closed


# ---- Event callback ----


class TestEventCallback:
    """Optional async callback for received JSON events."""

    async def test_callback_receives_json_events(
        self,
        running_server: WatchdogWSServer,
        client_session: aiohttp.ClientSession,
    ) -> None:
        """JSON messages are dispatched to the registered callback."""
        received: list[tuple[int, dict[str, Any]]] = []
        received_event = asyncio.Event()

        async def callback(self_id: int, data: dict[str, Any]) -> None:
            received.append((self_id, data))
            received_event.set()

        running_server.set_event_callback(callback)

        headers = {
            "Authorization": f"Bearer {TEST_TOKEN}",
            "X-Self-ID": "12345",
        }
        async with client_session.ws_connect(
            _ws_url(running_server), headers=headers
        ) as ws:
            await ws.send_json({"type": "heartbeat", "online": True})
            await asyncio.wait_for(received_event.wait(), timeout=2)

        assert len(received) == 1
        assert received[0][0] == 12345
        assert received[0][1] == {"type": "heartbeat", "online": True}

    async def test_non_json_not_dispatched(
        self,
        running_server: WatchdogWSServer,
        client_session: aiohttp.ClientSession,
    ) -> None:
        """Non-JSON text messages are not dispatched to callback."""
        received: list[tuple[int, dict[str, Any]]] = []
        received_event = asyncio.Event()

        async def callback(self_id: int, data: dict[str, Any]) -> None:
            received.append((self_id, data))
            received_event.set()

        running_server.set_event_callback(callback)

        headers = {
            "Authorization": f"Bearer {TEST_TOKEN}",
            "X-Self-ID": "12345",
        }
        async with client_session.ws_connect(
            _ws_url(running_server), headers=headers
        ) as ws:
            await ws.send_str("not json")
            # Give the server a chance NOT to call the callback
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(received_event.wait(), timeout=0.2)

        assert len(received) == 0

    async def test_no_callback_safe_discard(
        self,
        running_server: WatchdogWSServer,
        client_session: aiohttp.ClientSession,
    ) -> None:
        """Without a callback, JSON events are safely discarded."""
        headers = {
            "Authorization": f"Bearer {TEST_TOKEN}",
            "X-Self-ID": "12345",
        }
        async with client_session.ws_connect(
            _ws_url(running_server), headers=headers
        ) as ws:
            await ws.send_json({"type": "heartbeat"})
            assert not ws.closed


# ---- start() raises when already running ----


class TestStartIdempotency:
    """start() raises RuntimeError if already running."""

    async def test_double_start_raises(
        self,
        running_server: WatchdogWSServer,
    ) -> None:
        """Calling start() again raises RuntimeError."""
        with pytest.raises(RuntimeError, match="already running"):
            await running_server.start()


# ---- bound_port ----


class TestBoundPort:
    """bound_port property works correctly."""

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
    """WatchdogWSServer rejects empty access_token in constructor and update."""

    def test_empty_token_raises_on_construction(self) -> None:
        """Constructing with empty token raises ValueError."""
        with pytest.raises(ValueError, match="access_token must not be empty"):
            WatchdogWSServer(access_token="")

    def test_default_token_raises_on_construction(self) -> None:
        """Constructing with default empty token raises ValueError."""
        with pytest.raises(ValueError, match="access_token must not be empty"):
            WatchdogWSServer()

    async def test_update_empty_token_raises(
        self,
        running_server: WatchdogWSServer,
    ) -> None:
        """update_access_token('') raises ValueError."""
        with pytest.raises(ValueError, match="access_token must not be empty"):
            await running_server.update_access_token("")

    async def test_token_preserved_after_empty_update_rejected(
        self,
        running_server: WatchdogWSServer,
    ) -> None:
        """After rejected update, the original token is unchanged."""
        assert running_server.access_token == TEST_TOKEN
        with pytest.raises(ValueError):
            await running_server.update_access_token("")
        assert running_server.access_token == TEST_TOKEN


# ---- Non-dict JSON ----


class TestNonDictJson:
    """JSON arrays/strings/numbers/bools/null are not dispatched to callback."""

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
        """JSON {desc} is not dispatched to callback."""
        received: list[tuple[int, dict[str, Any]]] = []
        received_event = asyncio.Event()

        async def callback(self_id: int, data: dict[str, Any]) -> None:
            received.append((self_id, data))
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

    async def test_dict_json_still_dispatched(
        self,
        running_server: WatchdogWSServer,
        client_session: aiohttp.ClientSession,
    ) -> None:
        """JSON object is still dispatched to callback (sanity check)."""
        received: list[tuple[int, dict[str, Any]]] = []
        received_event = asyncio.Event()

        async def callback(self_id: int, data: dict[str, Any]) -> None:
            received.append((self_id, data))
            received_event.set()

        running_server.set_event_callback(callback)

        headers = {
            "Authorization": f"Bearer {TEST_TOKEN}",
            "X-Self-ID": "12345",
        }
        async with client_session.ws_connect(
            _ws_url(running_server), headers=headers
        ) as ws:
            await ws.send_json({"type": "heartbeat"})
            await asyncio.wait_for(received_event.wait(), timeout=2)

        assert len(received) == 1
        assert received[0][1] == {"type": "heartbeat"}


# ---- Start() failure cleanup ----


class TestStartFailure:
    """start() failure leaves server in a clean, non-started state."""

    async def test_start_failure_cleans_up(self) -> None:
        """Port conflict → runner/site cleaned up, bound_port is None."""
        srv1 = WatchdogWSServer(host="127.0.0.1", port=0, access_token=TEST_TOKEN)
        await srv1.start()
        occupied_port = srv1.bound_port
        assert occupied_port is not None

        srv2 = WatchdogWSServer(
            host="127.0.0.1",
            port=occupied_port,
            access_token=TEST_TOKEN,
        )
        with pytest.raises(Exception):
            await srv2.start()

        assert srv2._app is None
        assert srv2._runner is None
        assert srv2._site is None
        assert srv2.bound_port is None

        await srv1.stop()

    async def test_failed_start_not_marked_running(self) -> None:
        """After failed start, start can be called again (on free port)."""
        srv1 = WatchdogWSServer(host="127.0.0.1", port=0, access_token=TEST_TOKEN)
        await srv1.start()
        occupied_port = srv1.bound_port
        assert occupied_port is not None

        srv2 = WatchdogWSServer(
            host="127.0.0.1",
            port=occupied_port,
            access_token=TEST_TOKEN,
        )
        with pytest.raises(Exception):
            await srv2.start()

        # srv2 should not think it's running — no "already running" error
        # Create a new instance bound to port 0 for a clean retry
        srv3 = WatchdogWSServer(host="127.0.0.1", port=0, access_token=TEST_TOKEN)
        await srv3.start()
        assert srv3.bound_port is not None
        assert srv3.bound_port != occupied_port
        await srv3.stop()

        await srv1.stop()

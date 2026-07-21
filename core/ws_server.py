"""Self-contained aiohttp WebSocket server for NapCat watchdog connections.

This module implements the WS server independently of AstrBot.
It handles authentication, connection tracking, event dispatch,
disconnect notification, and generation-based stale event isolation.

**Design notes**:

- Generation is **not** maintained by this server.  It is obtained from
  the :class:`~core.state_machine.StateMachine` via the admission
  callback and stored per-connection.
- Connection replacement order: new WS is written into
  ``_connections[self_id]`` **first**, then the old WS is closed.  This
  ensures the old WS ``finally`` block never deletes the active
  connection.
- All callbacks are ``async``.
"""

from __future__ import annotations

import json
import logging
import secrets
from collections.abc import Awaitable, Callable
from typing import Any

import aiohttp
from aiohttp import web

logger = logging.getLogger(__name__)

# ---- Callback type aliases ----

CanRegisterHandler = Callable[[int], Awaitable[bool]]
"""Async predicate ``fn(self_id) → bool`` for capacity check."""

AdmissionHandler = Callable[[int], Awaitable[int]]
"""Async admission callback ``fn(self_id) → generation``.

Called after WS upgrade succeeds.  Must return the connection generation
from the state machine.  Raise to reject the connection.
"""

EventCallback = Callable[[int, int, dict[str, Any]], Awaitable[None]]
"""Async event callback ``fn(self_id, generation, data)``.

The ``generation`` is the authoritative generation from the state
machine, allowing the receiver to filter stale events.
"""

DisconnectCallback = Callable[[int, int], Awaitable[None]]
"""Async disconnect callback ``fn(self_id, generation)``."""

CancelCapacityHandler = Callable[[int], Awaitable[None]]
"""Async capacity-rollback callback ``fn(self_id)``.

Called when a WebSocket upgrade fails after :meth:`CanRegisterHandler`
returned ``True``, so the caller can release any reservation.
"""


class WatchdogWSServer:
    """A self-contained aiohttp WebSocket server for NapCat watchdog.

    Provides start/stop lifecycle, Bearer token authentication,
    ``X-Self-ID``-based connection tracking with generation-based stale
    event isolation, and optional async callbacks for admission, events,
    and disconnection.
    """

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 19090,
        path: str = "/napcat-watchdog/ws",
        access_token: str = "",
    ) -> None:
        if not access_token:
            raise ValueError("access_token must not be empty")
        self._host = host
        self._port = port
        self._path = path
        self._access_token = access_token
        self._app: web.Application | None = None
        self._runner: web.AppRunner | None = None
        self._site: web.TCPSite | None = None
        # self_id → (web.WebSocketResponse, generation)
        self._connections: dict[int, tuple[web.WebSocketResponse, int]] = {}
        self._bound_port: int | None = None

        # Optional async callbacks
        self._can_register_handler: CanRegisterHandler | None = None
        self._cancel_capacity_handler: CancelCapacityHandler | None = None
        self._admission_handler: AdmissionHandler | None = None
        self._event_callback: EventCallback | None = None
        self._disconnect_callback: DisconnectCallback | None = None

    # ---- Public API ----

    @property
    def access_token(self) -> str:
        """Current access token in use."""
        return self._access_token

    @property
    def bound_port(self) -> int | None:
        """Actual bound port.  Cached at start time; reset on stop."""
        return self._bound_port

    @property
    def connection_count(self) -> int:
        """Number of currently tracked WS connections."""
        return len(self._connections)

    def set_can_register_handler(self, handler: CanRegisterHandler | None) -> None:
        """Register an async predicate for capacity checks.

        Called before WS upgrade with ``self_id``.  If it returns
        ``False``, the connection is rejected with 429.
        """
        self._can_register_handler = handler

    def set_cancel_capacity_handler(
        self, handler: CancelCapacityHandler | None
    ) -> None:
        """Register an async capacity-rollback callback.

        Called when the WebSocket upgrade fails after the capacity
        check passed, so the caller can release a reservation.
        """
        self._cancel_capacity_handler = handler

    def set_admission_handler(self, handler: AdmissionHandler | None) -> None:
        """Register an async admission callback.

        Called **after** WS upgrade succeeds.  Must return the
        connection generation.  If it raises, the connection is closed.
        """
        self._admission_handler = handler

    def set_event_callback(self, callback: EventCallback | None) -> None:
        """Register an async callback for received JSON events.

        The callback receives ``(self_id, generation, data)`` for each
        successfully parsed JSON object.  Non-JSON text, binary messages,
        and non-dict JSON values are silently discarded.
        """
        self._event_callback = callback

    def set_disconnect_callback(self, callback: DisconnectCallback | None) -> None:
        """Register an async callback for connection close.

        Receives ``(self_id, generation)`` where generation is the
        authoritative value from the state machine at admission time.
        """
        self._disconnect_callback = callback

    async def update_access_token(self, new_token: str) -> None:
        """Replace the access token and close all existing connections.

        After this call only clients using *new_token* can connect.

        Raises:
            ValueError: if *new_token* is empty.
        """
        if not new_token:
            raise ValueError("access_token must not be empty")
        self._access_token = new_token
        await self._close_all(code=1001, message=b"Token rotated")

    async def start(self) -> None:
        """Start the WebSocket server.

        Raises:
            RuntimeError: if the server is already running.
            OSError: if the address is already in use (port conflict).
            aiohttp.web.GracefulExit: on runner/setup failure.
        """
        if self._app is not None:
            msg = "Watchdog WS server is already running"
            raise RuntimeError(msg)

        _app = web.Application()
        _app.router.add_get(self._path, self._ws_handler)

        _runner = web.AppRunner(_app)

        try:
            await _runner.setup()
            _site = web.TCPSite(_runner, self._host, self._port)
            await _site.start()
        except Exception:
            await _runner.cleanup()
            raise

        self._app = _app
        self._runner = _runner
        self._site = _site
        # Cache bound port — one-time access to aiohttp internals
        try:
            if _site._server is not None and _site._server.sockets:
                self._bound_port = _site._server.sockets[0].getsockname()[1]
        except Exception:
            pass

        logger.info(
            "Watchdog WS server listening on %s:%s%s",
            self._host,
            self._bound_port or self._port,
            self._path,
        )

    async def stop(self) -> None:
        """Stop the WebSocket server and close all connections.

        Idempotent — safe to call multiple times.
        """
        await self._close_all(code=1001, message=b"Server shutting down")

        if self._runner is not None:
            await self._runner.cleanup()

        self._app = None
        self._runner = None
        self._site = None
        self._bound_port = None

        logger.info("Watchdog WS server stopped")

    # ---- Internal helpers ----

    async def _close_all(self, code: int = 1001, message: bytes = b"") -> None:
        """Close every tracked WebSocket connection."""
        ws_list = [ws for ws, _ in self._connections.values()]
        self._connections.clear()

        for ws in ws_list:
            if not ws.closed:
                try:
                    await ws.close(code=code, message=message)
                except Exception:
                    logger.debug("Error closing WS connection", exc_info=True)

    async def _ws_handler(self, request: web.Request) -> web.WebSocketResponse:
        """Handle an incoming WebSocket upgrade request."""
        # ---- Authentication ----
        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            return web.Response(
                status=401,
                text="Unauthorized",
                headers={"WWW-Authenticate": "Bearer"},
            )
        token = auth_header[len("Bearer ") :]
        if not secrets.compare_digest(token, self._access_token):
            return web.Response(
                status=401,
                text="Unauthorized",
                headers={"WWW-Authenticate": "Bearer"},
            )

        # ---- X-Self-ID validation ----
        self_id_str = request.headers.get("X-Self-ID", "")
        if not self_id_str or not self_id_str.isdigit():
            return web.Response(status=400, text="Bad Request")
        self_id = int(self_id_str)
        if self_id <= 0:
            return web.Response(status=400, text="Bad Request")

        # ---- Capacity check (registration limit) ----
        capacity_reserved = False
        if self._can_register_handler is not None:
            allowed = await self._can_register_handler(self_id)
            if not allowed:
                logger.warning(
                    "Connection rejected for %s: registration limit reached",
                    self_id,
                )
                return web.Response(status=429, text="Too Many QQs")
            capacity_reserved = True

        # ---- WebSocket upgrade ----
        ws = web.WebSocketResponse()
        try:
            await ws.prepare(request)
        except Exception:
            logger.exception(
                "WebSocket prepare failed for self_id=%s; releasing capacity",
                self_id,
            )
            if capacity_reserved and self._cancel_capacity_handler is not None:
                await self._cancel_capacity_handler(self_id)
            raise

        # ---- Admission (state machine confirm_connection) ----
        generation = 0
        if self._admission_handler is not None:
            try:
                generation = await self._admission_handler(self_id)
            except Exception:
                logger.exception(
                    "Admission callback failed for self_id=%s; closing connection",
                    self_id,
                )
                if not ws.closed:
                    await ws.close(code=1011, message=b"Admission failed")
                return ws

        if generation <= 0:
            # Fallback: no admission handler or it returned invalid generation
            if not ws.closed:
                await ws.close(code=1011, message=b"Admission failed")
            return ws

        # ---- Replace existing connection (new first, then close old) ----
        old_entry = self._connections.get(self_id)

        # Write new connection FIRST
        self._connections[self_id] = (ws, generation)

        # Then close old connection (if any)
        if old_entry is not None:
            old_ws, old_gen = old_entry
            if not old_ws.closed:
                try:
                    await old_ws.close(code=1001, message=b"Replaced by new connection")
                except Exception:
                    logger.debug("Error closing replaced WS", exc_info=True)

        logger.info(
            "NapCat %s connected (generation=%s, total=%s)",
            self_id,
            generation,
            len(self._connections),
        )

        # ---- Receive loop ----
        try:
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    await self._on_text(self_id, generation, msg.data)
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    break
        except (ConnectionResetError, aiohttp.WebSocketError):
            pass
        finally:
            # Only remove if this connection is still the current one
            current = self._connections.get(self_id)
            if current is not None and current[0] is ws:
                self._connections.pop(self_id, None)

            logger.info(
                "NapCat %s disconnected (generation=%s, total=%s)",
                self_id,
                generation,
                len(self._connections),
            )

            # Notify disconnect callback
            if self._disconnect_callback is not None:
                try:
                    await self._disconnect_callback(self_id, generation)
                except Exception:
                    logger.exception(
                        "Disconnect callback error for self_id=%s generation=%s",
                        self_id,
                        generation,
                    )

        return ws

    async def _on_text(self, self_id: int, generation: int, text: str) -> None:
        """Process a received text message.

        Only JSON ``dict`` results are dispatched to the event callback;
        JSON arrays, strings, numbers, booleans, and null are silently
        discarded — the same as non-JSON text.
        """
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            return

        if not isinstance(data, dict):
            return

        # ---- Strict heartbeat parsing ----
        # We only dispatch valid heartbeats; other events are discarded.
        if not _is_valid_heartbeat(data, self_id):
            return

        # Dispatch with authoritative generation
        if self._event_callback is not None:
            try:
                await self._event_callback(self_id, generation, data)
            except Exception:
                logger.exception(
                    "Event callback error for self_id=%s generation=%s",
                    self_id,
                    generation,
                )


# ---- Strict heartbeat validation ----


def _is_valid_heartbeat(data: dict[str, Any], expected_self_id: int) -> bool:
    """Return ``True`` if *data* is a strictly valid OneBot 11 heartbeat.

    Rules:
    - ``post_type == "meta_event"``
    - ``meta_event_type == "heartbeat"``
    - ``status`` is a dict
    - ``status.online`` is an exact ``bool``
    - If ``data`` contains ``self_id``, it must match *expected_self_id*
      exactly (as int).
    """
    if data.get("post_type") != "meta_event":
        return False
    if data.get("meta_event_type") != "heartbeat":
        return False

    status = data.get("status")
    if not isinstance(status, dict):
        return False

    online = status.get("online")
    if not isinstance(online, bool):
        return False

    # Optional self_id must match if present
    event_self_id = data.get("self_id")
    if event_self_id is not None:
        if not isinstance(event_self_id, int) or event_self_id != expected_self_id:
            return False

    return True

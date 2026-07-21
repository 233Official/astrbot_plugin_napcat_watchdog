"""Self-contained aiohttp WebSocket server for NapCat watchdog connections.

This module implements the WS server independently of AstrBot.
It handles authentication, connection tracking, and event dispatch.
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


class WatchdogWSServer:
    """A self-contained aiohttp WebSocket server for NapCat watchdog.

    Provides start/stop lifecycle, Bearer token authentication,
    X-Self-ID based connection tracking, and an optional async
    event callback for received JSON messages.
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
        self._connections: dict[int, web.WebSocketResponse] = {}
        self._event_callback: (
            Callable[[int, dict[str, Any]], Awaitable[None]] | None
        ) = None
        self._bound_port: int | None = None

    # ---- Public API ----

    @property
    def access_token(self) -> str:
        """Current access token in use."""
        return self._access_token

    @property
    def bound_port(self) -> int | None:
        """Actual bound port. Cached at start time; reset on stop."""
        return self._bound_port

    @property
    def connection_count(self) -> int:
        """Number of currently tracked WS connections."""
        return len(self._connections)

    def set_event_callback(
        self,
        callback: Callable[[int, dict[str, Any]], Awaitable[None]],
    ) -> None:
        """Register an async callback for received JSON events.

        The callback receives (self_id: int, data: dict) for each
        successfully parsed JSON text message. Non-JSON and binary
        messages are silently discarded.
        """
        self._event_callback = callback

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
        ws_list = list(self._connections.values())
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

        # ---- WebSocket upgrade ----
        ws = web.WebSocketResponse()
        await ws.prepare(request)

        # Replace existing connection for the same self_id
        old_ws = self._connections.get(self_id)
        if old_ws is not None and not old_ws.closed:
            await old_ws.close(code=1001, message=b"Replaced by new connection")

        self._connections[self_id] = ws
        logger.info("NapCat %s connected (total: %s)", self_id, len(self._connections))

        # ---- Receive loop ----
        try:
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    await self._on_text(self_id, msg.data)
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    break
        except (ConnectionResetError, aiohttp.WebSocketError):
            pass
        finally:
            # Only remove if we haven't been replaced already
            if self._connections.get(self_id) is ws:
                self._connections.pop(self_id, None)
            logger.info(
                "NapCat %s disconnected (total: %s)", self_id, len(self._connections)
            )

        return ws

    async def _on_text(self, self_id: int, text: str) -> None:
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

        if self._event_callback is not None:
            try:
                await self._event_callback(self_id, data)
            except Exception:
                logger.exception("Event callback error for self_id=%s", self_id)

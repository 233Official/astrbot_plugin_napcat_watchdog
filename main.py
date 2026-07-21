"""AstrBot NapCat 存活监控插件 — WebSocket 服务端与状态机集成阶段。"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from astrbot.api import AstrBotConfig
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, StarTools

from .core import (
    PersistenceError,
    QQStatus,
    StateMachine,
    TransitionKind,
    WatchdogWSServer,
    load_snapshot,
    save_snapshot,
    snapshot_exists,
)
from .core.token import ensure_access_token

logger = logging.getLogger(__name__)

_SWEEP_INTERVAL = 30.0
"""Seconds between heartbeat timeout sweep cycles."""

_SNAPSHOT_FILENAME = "napcat_watchdog_state.json"


class NapCatWatchdogPlugin(Star):
    """NapCat QQ 存活监控插件。

    集成状态机、持久化、WebSocket 服务端和心跳超时扫描的全生命周期。
    """

    def __init__(self, context: Context, config: AstrBotConfig) -> None:
        super().__init__(context, config)
        self.config = config
        self._ws_server: WatchdogWSServer | None = None
        self._sm: StateMachine | None = None
        self._sweep_task: asyncio.Task[None] | None = None
        self._shutting_down: bool = False
        self._snapshot_path: Path | None = None

    async def initialize(self) -> None:
        """初始化并启动所有组件。

        流程：
        1. 确保 access_token 已设置并持久化。
        2. 确定持久化目录和快照路径。
        3. 尝试加载 JSON 快照；损坏/不兼容时 fail closed（不启动 WS）。
        4. 创建状态机并加载快照。
        5. 创建 WS 服务端并绑定回调。
        6. 启动 WS 服务端和心跳超时扫描任务。
        7. 启动重启宽限计时器。
        """
        # ---- 1. Token ----
        token: str = ensure_access_token(self.config, logger)

        # ---- 2. Data directory ----
        try:
            data_dir = Path(StarTools.get_data_dir("astrbot_plugin_napcat_watchdog"))
        except Exception as e:
            logger.error("Failed to get data directory: %s", e)
            raise
        data_dir.mkdir(parents=True, exist_ok=True)
        self._snapshot_path = data_dir / _SNAPSHOT_FILENAME

        # ---- 3. Load snapshot (fail closed on corruption) ----
        loaded_snapshot: dict | None = None
        try:
            if snapshot_exists(self._snapshot_path):
                loaded_snapshot = load_snapshot(self._snapshot_path)
                logger.info("Loaded state snapshot with %s QQs", len(loaded_snapshot))
        except PersistenceError as e:
            logger.error(
                "State snapshot corrupt — WS server will NOT start: %s",
                e,
            )
            raise RuntimeError("Corrupt state snapshot; refusing startup") from e
        except FileNotFoundError:
            pass

        # ---- 4. State machine ----
        timeout = float(int(self.config.get("offline_timeout_seconds", 90) or 90))
        self._sm = StateMachine(offline_timeout=timeout)

        # Register transition callback for persistence
        self._sm.set_transition_callback(self._on_transition)

        if loaded_snapshot is not None:
            self._sm.load_snapshot(loaded_snapshot)

        # ---- 5. WS server ----
        host: str = self.config.get("listen_host", "0.0.0.0") or "0.0.0.0"
        port: int = int(self.config.get("listen_port", 19090) or 19090)
        path: str = (
            self.config.get("ws_path", "/napcat-watchdog/ws") or "/napcat-watchdog/ws"
        )

        self._ws_server = WatchdogWSServer(
            host=host,
            port=port,
            path=path,
            access_token=token,
        )

        # Wire callbacks
        self._ws_server.set_can_register_handler(self._can_register)
        self._ws_server.set_cancel_capacity_handler(self._cancel_registration)
        self._ws_server.set_admission_handler(self._admit_connection)
        self._ws_server.set_event_callback(self._on_event)
        self._ws_server.set_disconnect_callback(self._on_disconnect)

        # ---- 6. Start WS server ----
        try:
            await self._ws_server.start()
        except Exception as e:
            logger.error("Watchdog WS server failed to start: %s", e)
            self._ws_server = None
            raise

        # ---- 7. Start heartbeat sweep ----
        self._sweep_task = asyncio.create_task(self._sweep_loop())

        # ---- 8. Start grace timers ----
        self._sm.start_grace_timers()

    async def terminate(self) -> None:
        """停止所有组件。

        顺序：
        1. 设置 shutdown 标志（阻止掉线 transition）。
        2. 取消 sweep 任务。
        3. 取消所有离线计时器。
        4. 停止 WS 服务端（关闭所有连接）。
        """
        self._shutting_down = True

        if self._sweep_task is not None:
            self._sweep_task.cancel()
            try:
                await self._sweep_task
            except asyncio.CancelledError:
                pass
            self._sweep_task = None

        if self._sm is not None:
            self._sm.cancel_all_timers()

        if self._ws_server is not None:
            try:
                await self._ws_server.stop()
            except Exception as e:
                logger.error("Error stopping Watchdog WS server: %s", e)
            self._ws_server = None

    # ---- Coordinator callbacks ----

    async def _can_register(self, self_id: int) -> bool:
        """Capacity check — delegates to state machine."""
        if self._sm is None:
            return False
        return await self._sm.try_reserve(self_id)

    async def _cancel_registration(self, self_id: int) -> None:
        """Release reservation on WS upgrade failure."""
        if self._sm is not None:
            self._sm.release_reservation(self_id)

    async def _admit_connection(self, self_id: int) -> int:
        """Atomic admission: confirm + persist (if state change).

        Only persists on actual transitions (FIRST_ONLINE or RECOVERED).
        ONLINE replacement skips persistence.
        Raises if persistence fails; rolls back state machine.
        """
        if self._sm is None:
            raise RuntimeError("State machine not initialized")

        gen = self._sm.confirm_connection(self_id)

        # Check whether this was a transition that needs persistence
        kind = self._sm.get_confirm_transition(self_id)
        if kind is not None and kind != TransitionKind.SHUTDOWN:
            try:
                self._save_snapshot()
            except PersistenceError:
                self._sm.undo_confirm(self_id)
                raise
            # Fire event only after successful persist
            new_status = self._sm.get_view(self_id)
            actual_status = (
                QQStatus.ONLINE
                if new_status and new_status["status"] != "offline"
                else QQStatus.OFFLINE
            )
            self._sm.fire_transition_event(self_id, actual_status, kind)

        return gen

    async def _on_event(self, self_id: int, generation: int, data: dict) -> None:
        """Process a valid heartbeat event."""
        if self._sm is None:
            return

        status = data.get("status", {})
        online = bool(status.get("online", False))

        kind = self._sm.on_heartbeat(self_id, generation, online)

        # If heartbeat caused OFFLINE→ONLINE recovery, persist + fire
        if kind == TransitionKind.RECOVERED:
            try:
                self._save_snapshot()
            except PersistenceError:
                self._sm.rollback_heartbeat_recovery(self_id)
                return
            self._sm.fire_transition_event(
                self_id, QQStatus.ONLINE, TransitionKind.RECOVERED
            )

    async def _on_disconnect(self, self_id: int, generation: int) -> None:
        """Handle WebSocket disconnection."""
        if self._sm is None or self._shutting_down:
            return

        self._sm.on_disconnect(self_id, generation)

    async def _on_transition(
        self,
        self_id: int,
        new_status: QQStatus,
        kind: TransitionKind,
    ) -> None:
        """Transition callback — persistence is handled by coordinator.

        This callback only fires for SHUTDOWN (final save in terminate).
        All other transitions are persisted by the coordinator before
        calling :meth:`fire_transition_event`.
        """
        # No-op — persistence is handled at the call site.
        pass

    # ---- Sweep loop ----

    async def _sweep_loop(self) -> None:
        """Periodic heartbeat timeout sweep.

        For each timeout transition:
        1. Persist snapshot.
        2. On success, fire transition event.
        3. On failure, rollback in state machine so signals are
           preserved for next sweep cycle.
        """
        while True:
            try:
                await asyncio.sleep(_SWEEP_INTERVAL)
                if self._sm is None:
                    continue

                self._sm.sweep_heartbeat_timeouts()
                transitions = self._sm.check_timeouts()

                if transitions:
                    try:
                        self._save_snapshot()
                    except PersistenceError:
                        for self_id, _ in transitions:
                            self._sm.rollback_timeout(self_id)
                        logger.warning(
                            "Timeout persist failed for %s QQs; "
                            "batch rolled back for retry",
                            len(transitions),
                        )
                    else:
                        for self_id, kind in transitions:
                            self._sm.fire_transition_event(
                                self_id,
                                QQStatus.OFFLINE,
                                kind,
                            )
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Error in heartbeat sweep loop")

    # ---- Snapshot persistence ----

    def _save_snapshot(self) -> None:
        """Persist current state snapshot."""
        if self._snapshot_path is None or self._sm is None:
            return
        snapshot = self._sm.get_snapshot()
        if snapshot:
            save_snapshot(self._snapshot_path, snapshot)

    # ---- Command ----

    @filter.command("napcat_watchdog_status")
    async def status(self, event: AstrMessageEvent):
        """返回插件当前阶段状态。"""
        if self._ws_server is not None and self._ws_server.bound_port is not None:
            sm_info = ""
            if self._sm is not None:
                sm_info = (
                    f" | 登记 QQ: {self._sm.registered_count}"
                    f" 在线: {self._sm.online_count}"
                    f" 待确认: {self._sm.pending_offline_count}"
                    f" 离线: {self._sm.offline_count}"
                )
            yield event.plain_result(f"NapCat Watchdog — 运行中。{sm_info}")
        else:
            yield event.plain_result("NapCat Watchdog — WS 服务端未运行。")

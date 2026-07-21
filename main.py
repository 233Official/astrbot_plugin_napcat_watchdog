"""AstrBot NapCat 存活监控插件 — WebSocket 服务端与命令集成阶段。"""

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
    SubscriptionError,
    SubscriptionStore,
    TransitionKind,
    WatchdogWSServer,
    load_snapshot,
    load_subscriptions,
    save_snapshot,
    snapshot_exists,
    subscription_exists,
)
from .core.token import ensure_access_token

logger = logging.getLogger(__name__)

_SWEEP_INTERVAL = 30.0
"""Seconds between heartbeat timeout sweep cycles."""

_SNAPSHOT_FILENAME = "napcat_watchdog_state.json"
"""State-machine snapshot file name (Issue #3)."""

_SUBSCRIPTION_FILENAME = "subscriptions.json"
"""Subscription store file name (Issue #4)."""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mask_sensitive(text: str, visible_tail: int = 4) -> str:
    """Mask all but the last *visible_tail* characters with asterisks.

    The result length equals the original length.
    Non-empty strings shorter than or equal to *visible_tail* are
    fully masked (never returned in the clear).
    Empty strings are returned as-is.
    """
    if not text:
        return text
    if len(text) <= visible_tail:
        return "*" * len(text)
    return "*" * (len(text) - visible_tail) + text[-visible_tail:]


# ---------------------------------------------------------------------------
# Plugin
# ---------------------------------------------------------------------------


class NapCatWatchdogPlugin(Star):
    """NapCat QQ 存活监控插件。

    集成状态机、持久化、WebSocket 服务端、心跳超时扫描和订阅管理。
    """

    def __init__(self, context: Context, config: AstrBotConfig) -> None:
        super().__init__(context, config)
        self.config = config
        self._ws_server: WatchdogWSServer | None = None
        self._sm: StateMachine | None = None
        self._sub_store: SubscriptionStore | None = None
        self._sweep_task: asyncio.Task[None] | None = None
        self._shutting_down: bool = False
        self._snapshot_path: Path | None = None
        self._subscription_path: Path | None = None

    async def initialize(self) -> None:
        """初始化并启动所有组件。

        流程：
        1. 确保 access_token 已设置并持久化。
        2. 确定持久化目录。
        3. 加载状态快照（损坏时 fail closed）。
        4. 加载订阅文件（损坏时 fail closed）。
        5. 创建状态机并加载快照。
        6. 创建订阅存储并加载数据。
        7. 创建 WS 服务端并绑定回调。
        8. 启动 WS 服务端和心跳超时扫描任务。
        9. 启动重启宽限计时器。
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
        self._subscription_path = data_dir / _SUBSCRIPTION_FILENAME

        # ---- 3. Load state snapshot (fail closed on corruption) ----
        loaded_snapshot: dict | None = None
        try:
            if snapshot_exists(self._snapshot_path):
                loaded_snapshot = load_snapshot(self._snapshot_path)
                logger.info(
                    "Loaded state snapshot with %s QQs",
                    len(loaded_snapshot),
                )
        except PersistenceError as e:
            logger.error(
                "State snapshot corrupt — WS server will NOT start: %s",
                e,
            )
            raise RuntimeError("Corrupt state snapshot; refusing startup") from e
        except FileNotFoundError:
            pass

        # ---- 4. Load subscriptions (fail closed on corruption) ----
        self._sub_store = SubscriptionStore()
        try:
            if subscription_exists(self._subscription_path):
                loaded_subs = load_subscriptions(self._subscription_path)
                self._sub_store.load_dict(loaded_subs)
                logger.info("Loaded %s subscriptions", len(loaded_subs))
        except SubscriptionError as e:
            logger.error(
                "Subscription file corrupt — WS server will NOT start: %s",
                e,
            )
            raise RuntimeError("Corrupt subscription file; refusing startup") from e

        # ---- 5. State machine ----
        timeout = float(int(self.config.get("offline_timeout_seconds", 90) or 90))
        self._sm = StateMachine(offline_timeout=timeout)

        # Register transition callback for persistence
        self._sm.set_transition_callback(self._on_transition)

        if loaded_snapshot is not None:
            self._sm.load_snapshot(loaded_snapshot)

        # ---- 6. WS server ----
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

        # ---- 7. Start WS server ----
        try:
            await self._ws_server.start()
        except Exception as e:
            logger.error("Watchdog WS server failed to start: %s", e)
            self._ws_server = None
            raise

        # ---- 8. Start heartbeat sweep ----
        self._sweep_task = asyncio.create_task(self._sweep_loop())

        # ---- 9. Start grace timers ----
        self._sm.start_grace_timers()

    async def terminate(self) -> None:
        """停止所有组件。

        不做无条件重复写盘——订阅文件只由 subscribe/unsubscribe 写入。
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
        """Atomic admission: confirm + persist (if state change)."""
        if self._sm is None:
            raise RuntimeError("State machine not initialized")

        gen = self._sm.confirm_connection(self_id)

        kind = self._sm.get_confirm_transition(self_id)
        if kind is not None and kind != TransitionKind.SHUTDOWN:
            try:
                self._save_snapshot()
            except PersistenceError:
                self._sm.undo_confirm(self_id)
                raise
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
        """Transition callback — no-op, persistence handled at call site."""
        pass

    # ---- Sweep loop ----

    async def _sweep_loop(self) -> None:
        """Periodic heartbeat timeout sweep."""
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

    # ---- Subscription persistence helpers ----

    def _save_subscriptions(self) -> None:
        """Persist current subscription store.

        Raises
        ------
        SubscriptionError
            If ``_subscription_path`` or ``_sub_store`` is not initialized
            (e.g. before :meth:`initialize` completes), or if the write
            fails.
        """
        if self._subscription_path is None:
            raise SubscriptionError("Subscription path not initialized")
        if self._sub_store is None:
            raise SubscriptionError("Subscription store not initialized")
        self._sub_store.save(self._subscription_path)

    # ---- Instance summary for command responses ----

    def _build_instance_summary(self) -> str:
        """Build a status line about currently registered NapCat instances.

        QQ IDs are masked (only last 4 digits visible).  Returns a
        human-readable string safe for command output.
        """
        if self._sm is None or self._sm.registered_count == 0:
            return "当前状态: 暂无已登记实例"

        lines: list[str] = ["当前已登记实例:"]
        for sid_str in sorted(self._sm.get_all_views(), key=int):
            view = self._sm.get_view(int(sid_str))
            if view is None:
                continue
            masked = _mask_sensitive(str(view["self_id"]), 4)
            status_label = view["status"]
            hb = view.get("last_heartbeat_at")
            hb_str = f"{hb:.0f}" if hb is not None else "未知"
            lines.append(f"  QQ {masked}: {status_label} | 最后心跳: {hb_str}")
        return "\n".join(lines)

    # ---- Command group ----

    @filter.command_group("napcat_watchdog")
    def napcat_watchdog(self) -> None:
        """NapCat Watchdog 命令组。"""

    @napcat_watchdog.command("subscribe")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def subscribe(self, event: AstrMessageEvent):  # type: ignore[arg-type]
        """订阅当前群的离线通知。"""
        if event.is_private_chat():
            yield event.plain_result("⚠️ 订阅设置不支持在私聊中操作，请在群聊中使用。")
            return

        umo: str | None = event.unified_msg_origin
        if not umo:
            yield event.plain_result("⚠️ 无法获取消息来源，订阅失败。")
            return
        umo = umo.strip()
        if not umo:
            yield event.plain_result("⚠️ 消息来源为空，订阅失败。")
            return

        owner_id: str = event.get_sender_id().strip()
        if not owner_id:
            yield event.plain_result("⚠️ 发送者ID为空，订阅失败。")
            return

        # Capture pre-mutation state for rollback
        before = self._sub_store.get_snapshot() if self._sub_store else {}
        try:
            if self._sub_store is None:
                yield event.plain_result("⚠️ 订阅存储未初始化，请稍后重试。")
                return
            rec, created = self._sub_store.subscribe(umo, owner_id)
            self._save_subscriptions()
        except SubscriptionError:
            if self._sub_store is not None:
                self._sub_store.load_dict(before)
            logger.exception(
                "Failed to save subscription for UMO=%s",
                _mask_sensitive(umo, 6),
            )
            yield event.plain_result("⚠️ 订阅保存失败，请稍后重试。")
            return

        action = "已订阅" if created else "已更新订阅"
        msg_parts: list[str] = [
            f"✅ {action}本群的离线通知。",
        ]

        summary = self._build_instance_summary()
        msg_parts.append(summary)

        yield event.plain_result("\n".join(msg_parts))

    @napcat_watchdog.command("unsubscribe")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def unsubscribe(self, event: AstrMessageEvent):  # type: ignore[arg-type]
        """取消当前群的离线通知订阅。"""
        if event.is_private_chat():
            yield event.plain_result("⚠️ 取消订阅不支持在私聊中操作，请在群聊中使用。")
            return

        umo: str | None = event.unified_msg_origin
        if not umo:
            yield event.plain_result("⚠️ 无法获取消息来源。")
            return

        # Check existence before mutating
        if self._sub_store is None:
            yield event.plain_result("⚠️ 订阅存储未初始化。")
            return
        existing = self._sub_store.get(umo)
        if existing is None:
            yield event.plain_result("ℹ️ 本群未订阅通知，无需取消。")
            return

        # Capture pre-mutation state for rollback
        before = self._sub_store.get_snapshot()
        self._sub_store.unsubscribe(umo)
        try:
            self._save_subscriptions()
        except SubscriptionError:
            self._sub_store.load_dict(before)
            logger.exception(
                "Failed to save after unsubscribe for UMO=%s",
                _mask_sensitive(umo, 6),
            )
            yield event.plain_result("⚠️ 取消订阅保存失败，请稍后重试。")
            return

        yield event.plain_result("✅ 已取消本群的离线通知订阅。")

    @napcat_watchdog.command("status")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def status(self, event: AstrMessageEvent):  # type: ignore[arg-type]
        """返回插件当前运行时状态。"""
        lines: list[str] = []

        # WS listening status
        if self._ws_server is not None and self._ws_server.bound_port is not None:
            lines.append("WebSocket 服务端: 运行中")
            lines.append(f"当前连接数: {self._ws_server.connection_count}")
        else:
            lines.append("WebSocket 服务端: 未运行")
            lines.append("当前连接数: 0")

        # Registered instances
        sm_count = self._sm.registered_count if self._sm is not None else 0
        lines.append(f"已登记实例数: {sm_count}")

        if self._sm is not None and sm_count > 0:
            for sid_str in sorted(self._sm.get_all_views(), key=int):
                view = self._sm.get_view(int(sid_str))
                if view is None:
                    continue
                masked = _mask_sensitive(str(view["self_id"]), 4)
                status_label = view["status"]
                hb = view.get("last_heartbeat_at")
                hb_str = f"{hb:.0f}" if hb is not None else "未知"
                lines.append(f"  QQ {masked}: {status_label} | 最后心跳: {hb_str}")

        # Subscriptions
        sub_count = self._sub_store.count if self._sub_store is not None else 0
        lines.append(f"订阅群数量: {sub_count}")

        if sub_count > 0 and self._sub_store is not None:
            # Masked subscription summary
            masked_subs = [_mask_sensitive(s.umo, 6) for s in self._sub_store.all()]
            lines.append(f"订阅摘要: {', '.join(masked_subs)}")

        # Catch-up count — placeholder for Issue #5
        lines.append("待补发数量: 0")

        yield event.plain_result("\n".join(lines))

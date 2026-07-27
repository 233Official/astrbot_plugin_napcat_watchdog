"""AstrBot NapCat 存活监控插件 — WebSocket 服务端与命令集成阶段。"""

from __future__ import annotations

import asyncio
import inspect
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from astrbot.api import AstrBotConfig
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, StarTools

from .core import (
    NotificationManager,
    NotificationPersistenceError,
    PersistenceError,
    ProfileCache,
    QQStatus,
    StateMachine,
    SubscriptionError,
    SubscriptionStore,
    TransitionKind,
    WatchdogWSServer,
    load_snapshot,
    load_subscriptions,
    render_html_image,
    save_snapshot,
    snapshot_exists,
    subscription_exists,
)
from .core.duration import format_natural_duration
from .core.notifications import _RENDER_OPTIONS
from .core.notifications import _mask_qq as _mask_qq_for_dashboard
from .core.profile import data_uri_is_valid, sanitize_nickname
from .core.token import ensure_access_token

logger = logging.getLogger(__name__)

_SWEEP_INTERVAL = 30.0
"""Seconds between heartbeat timeout sweep cycles."""

_SNAPSHOT_FILENAME = "napcat_watchdog_state.json"
"""State-machine snapshot file name (Issue #3)."""

_SUBSCRIPTION_FILENAME = "subscriptions.json"
"""Subscription store file name (Issue #4)."""

_PENDING_FILENAME = "pending_notifications.json"
"""Pending notification file name (PRD 11)."""

_STATUS_TEMPLATE = (
    Path(__file__).resolve().parent / "templates" / "status_dashboard.html"
)
_STATUS_LABELS = {
    "online": "在线",
    "pending_offline": "待确认",
    "offline": "离线",
}
_STATUS_DURATION_LABELS = {
    "online": "在线时长",
    "pending_offline": "待确认时长",
    "offline": "离线时长",
}


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


def _format_status_time(value: float) -> str:
    """Format a wall-clock value without exposing its raw epoch form."""
    try:
        return datetime.fromtimestamp(float(value)).strftime("%Y-%m-%d %H:%M:%S")
    except (OverflowError, OSError, TypeError, ValueError):
        return "时间未知"


def _human_heartbeat(value: Any, now: float) -> str:
    """Return a readable relative heartbeat time, never the raw timestamp."""
    try:
        heartbeat = float(value)
        current = float(now)
    except (TypeError, ValueError):
        return "暂无记录"
    if heartbeat < 0 or current < 0:
        return "时间未知"
    elapsed = max(0.0, current - heartbeat)
    if elapsed < 10:
        return "刚刚"
    if elapsed < 60:
        return f"{int(elapsed)} 秒前"
    if elapsed < 3600:
        return f"{int(elapsed // 60)} 分钟前"
    if elapsed < 86400:
        return f"{int(elapsed // 3600)} 小时前"
    return _format_status_time(heartbeat)


def _format_status_duration(seconds: Any) -> str:
    """Format a nonnegative duration as compact natural Chinese text."""
    return format_natural_duration(seconds)


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
        self._pending_path: Path | None = None
        self._notification_manager: NotificationManager | None = None
        self._profile_cache: ProfileCache | None = None
        self._notification_tasks: set[asyncio.Task[None]] = set()

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
        self._pending_path = data_dir / _PENDING_FILENAME

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

        # ---- 4b. Pending notifications ----
        self._profile_cache = ProfileCache()
        self._notification_manager = NotificationManager(
            self.context,
            self._sub_store,
            self._pending_path,
            html_render=getattr(self, "html_render", None),
            profile_provider=self._get_profile,
            instance_label=str(self.config.get("instance_label", "") or ""),
        )
        try:
            self._notification_manager.load()
        except NotificationPersistenceError as e:
            logger.error(
                "Pending notification file corrupt — WS server will NOT start: %s",
                e,
            )
            raise RuntimeError(
                "Corrupt pending notification state; refusing startup"
            ) from e

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
        self._ws_server.set_profile_callback(self._on_profile)

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

        # Catch-up is deliberately after subscriptions are loaded and before
        # accepting new WS traffic.  Removed targets are dropped here.
        try:
            await self._notification_manager.catch_up()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Pending notification catch-up failed")

    async def terminate(self) -> None:
        """停止所有组件。

        订阅文件只由 subscribe/unsubscribe 写入；待补发和状态快照在
        停止前分别安全持久化。
        """
        self._shutting_down = True

        # Transition callbacks are dispatched by the state machine as tasks;
        # wait for or cancel them explicitly so exceptions are not lost during
        # plugin shutdown.
        tasks = list(self._notification_tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._notification_tasks.clear()

        if self._notification_manager is not None:
            try:
                self._notification_manager.persist()
            except NotificationPersistenceError:
                logger.exception("Failed to persist pending notifications on shutdown")
        if self._profile_cache is not None:
            self._profile_cache.clear()

        if self._sweep_task is not None:
            self._sweep_task.cancel()
            try:
                await self._sweep_task
            except asyncio.CancelledError:
                pass
            self._sweep_task = None

        if self._sm is not None:
            self._sm.cancel_all_timers()
            try:
                self._save_snapshot()
            except PersistenceError:
                logger.exception("Failed to persist state snapshot on shutdown")

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

        already_registered = self._sm.get_view(self_id) is not None
        gen = self._sm.confirm_connection(self_id)

        if not already_registered:
            try:
                self._save_snapshot()
            except PersistenceError:
                self._sm.undo_confirm(self_id)
                raise

        # Admission only persists a brand-new registration.  FIRST_ONLINE and
        # RECOVERED are emitted by a current-generation valid heartbeat, never
        # by WebSocket reconnect churn.

        view = self._sm.get_view(self_id)
        if view is not None and view["status"] == "offline":
            logger.info(
                "NapCat %s reconnected but remains confirmed offline "
                "until a valid online heartbeat",
                _mask_sensitive(str(self_id), 4),
            )

        return gen

    async def _on_event(self, self_id: int, generation: int, data: dict) -> None:
        """Process a valid heartbeat event."""
        if self._sm is None:
            return

        status = data.get("status", {})
        online = bool(status.get("online", False))

        kind = self._sm.on_heartbeat(self_id, generation, online)

        if kind in {TransitionKind.FIRST_ONLINE, TransitionKind.RECOVERED}:
            try:
                self._save_snapshot()
            except PersistenceError:
                self._sm.rollback_heartbeat_recovery(self_id)
                return
            self._sm.fire_transition_event(self_id, QQStatus.ONLINE, kind)

    async def _on_disconnect(self, self_id: int, generation: int) -> None:
        """Handle WebSocket disconnection."""
        if self._sm is None or self._shutting_down:
            return

        self._sm.on_disconnect(self_id, generation)

    def _get_profile(self, self_id: int):
        if self._profile_cache is None:
            return None
        return self._profile_cache.get(self_id)

    async def _on_profile(self, self_id: int, generation: int, nickname: str) -> None:
        """Best-effort profile enrichment; never affects watchdog state."""
        cache = self._profile_cache
        if cache is None:
            return
        try:
            await cache.update(self_id, nickname)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.warning(
                "NapCat profile enrichment failed self_id=%s generation=%s",
                _mask_sensitive(str(self_id), 4),
                generation,
            )

    async def _on_transition(
        self,
        self_id: int,
        new_status: QQStatus,
        kind: TransitionKind,
    ) -> None:
        """Deliver notifications for a committed state-machine transition."""
        manager = self._notification_manager
        if manager is None or self._shutting_down:
            return
        if kind not in {
            TransitionKind.FIRST_ONLINE,
            TransitionKind.RECOVERED,
            TransitionKind.OFFLINE_TIMEOUT,
        }:
            return

        if kind == TransitionKind.OFFLINE_TIMEOUT:
            view = self._sm.get_view(self_id) if self._sm is not None else None
            if view is not None and not view.get("online_confirmed", True):
                # A newly admitted connection that never produced a valid
                # online heartbeat is not allowed to create an offline alert.
                return

        current_task = asyncio.current_task()
        if current_task is not None:
            self._notification_tasks.add(current_task)
        try:
            details: dict[str, float | int | None] = {}
            if self._sm is not None:
                details = self._sm.get_transition_context(self_id, kind)
            episode_id = details.get("episode_id")
            if isinstance(episode_id, bool) or not isinstance(episode_id, int):
                episode_id = None
            await manager.handle_transition(
                self_id,
                new_status,
                kind,
                event_time=details.get("event_time"),
                offline_since=details.get("offline_since"),
                online_since=details.get("online_since"),
                episode_id=episode_id,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "NapCat transition notification failed self_id=%s kind=%s",
                _mask_sensitive(str(self_id), 4),
                kind.value,
            )
        finally:
            if current_task is not None:
                self._notification_tasks.discard(current_task)

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

    # ---- Status dashboard data and rendering ----

    def _build_status_data(
        self,
        event: AstrMessageEvent,
        *,
        now: float | None = None,
    ) -> dict[str, Any]:
        """Build the allow-listed, display-only status dashboard context."""
        current_time = time.time() if now is None else now
        views = self._sm.get_all_views() if self._sm is not None else {}
        instance_label = sanitize_nickname(self.config.get("instance_label", ""))
        instances: list[dict[str, Any]] = []

        for sid_str in sorted(views, key=lambda value: int(value)):
            view = views.get(sid_str)
            if not isinstance(view, dict):
                continue
            try:
                self_id = int(view["self_id"])
            except (KeyError, TypeError, ValueError):
                continue
            status = view.get("status")
            if status not in _STATUS_LABELS:
                status = "offline"
            nickname, avatar_data_uri = self._status_profile_values(self_id)
            instances.append(
                {
                    "masked_qq": _mask_qq_for_dashboard(self_id),
                    "nickname": nickname,
                    "avatar_data_uri": avatar_data_uri,
                    "instance_label": instance_label,
                    "status": status,
                    "status_label": _STATUS_LABELS[status],
                    "duration_label": _STATUS_DURATION_LABELS[status],
                    "status_duration_text": _format_status_duration(
                        view.get("status_duration_seconds", 0)
                    ),
                    "last_heartbeat_text": _human_heartbeat(
                        view.get("last_heartbeat_at"), current_time
                    ),
                }
            )

        counts = {
            "online": 0,
            "pending_offline": 0,
            "offline": 0,
        }
        for instance in instances:
            counts[instance["status"]] += 1

        websocket = self._ws_server
        if websocket is None or websocket.bound_port is None:
            service_running = False
            connection_count = 0
        else:
            service_running = True
            connection_count = websocket.connection_count
        subscription_count = self._sub_store.count if self._sub_store is not None else 0
        pending_delivery_count = (
            self._notification_manager.pending_count
            if self._notification_manager is not None
            else 0
        )

        return {
            "service_running": service_running,
            "connection_count": connection_count,
            "registered_count": len(instances),
            "online_count": counts["online"],
            "pending_count": counts["pending_offline"],
            "offline_count": counts["offline"],
            "subscription_count": subscription_count,
            "pending_delivery_count": pending_delivery_count,
            "current_group_subscribed": self._current_group_subscription(event),
            "updated_at": _format_status_time(current_time),
            "instances": instances,
        }

    def _status_profile_values(self, self_id: int) -> tuple[str | None, str | None]:
        """Return only sanitized, cached profile display fields."""
        profile = self._get_profile(self_id)
        if profile is None:
            return None, None
        if isinstance(profile, dict):
            nickname = profile.get("nickname")
            avatar_data_uri = profile.get("avatar_data_uri")
        else:
            nickname = getattr(profile, "nickname", None)
            avatar_data_uri = getattr(profile, "avatar_data_uri", None)
        safe_avatar = avatar_data_uri if data_uri_is_valid(avatar_data_uri) else None
        return sanitize_nickname(nickname), safe_avatar

    def _current_group_subscription(self, event: AstrMessageEvent) -> bool | None:
        """Check only the exact group UMO supplied by the current event."""
        try:
            if event.is_private_chat():
                return None
        except (AttributeError, TypeError):
            return None
        umo = getattr(event, "unified_msg_origin", None)
        if not isinstance(umo, str) or not umo.strip() or self._sub_store is None:
            return None
        return self._sub_store.get(umo) is not None

    async def _render_status_dashboard(self, data: dict[str, Any]) -> Any | None:
        """Render the designer-owned dashboard with notification-safe options."""
        return await render_html_image(
            getattr(self, "html_render", None),
            _STATUS_TEMPLATE,
            data,
            options=_RENDER_OPTIONS,
            diagnostic_label="NapCat status dashboard rendering",
        )

    @staticmethod
    def _status_text(data: dict[str, Any]) -> str:
        """Build the concise safe text fallback from already-sanitized data."""
        service = "运行中" if data["service_running"] else "未运行"
        lines = [
            f"WebSocket 服务端: {service}",
            f"当前连接数: {data['connection_count']}",
            f"已登记实例数: {data['registered_count']}",
            f"在线实例数: {data['online_count']}",
            f"待确认实例数: {data['pending_count']}",
            f"离线实例数: {data['offline_count']}",
        ]
        for instance in data["instances"]:
            label = (
                f"（{instance['instance_label']}）"
                if instance["instance_label"]
                else ""
            )
            lines.append(
                f"  QQ {instance['masked_qq']}{label}: "
                f"{instance['status_label']} | "
                f"最后心跳: {instance['last_heartbeat_text']} | "
                f"{instance['duration_label']}: {instance['status_duration_text']}"
            )
        group_state = data["current_group_subscribed"]
        group_text = (
            "已订阅"
            if group_state is True
            else "未订阅"
            if group_state is False
            else "未提供当前群信息"
        )
        lines.extend(
            [
                f"订阅群数量: {data['subscription_count']}",
                f"当前群订阅状态: {group_text}",
                f"待补发数量: {data['pending_delivery_count']}",
            ]
        )
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

        if self._notification_manager is not None:
            self._notification_manager.drop_for_umo(umo)

        yield event.plain_result("✅ 已取消本群的离线通知订阅。")

    @napcat_watchdog.command("status")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def status(self, event: AstrMessageEvent):  # type: ignore[arg-type]
        """Return a dashboard image, with a concise safe text fallback."""
        data = self._build_status_data(event)
        image_result = await self._render_status_dashboard(data)
        if image_result is not None:
            image_api = getattr(event, "image_result", None)
            if callable(image_api):
                try:
                    result = image_api(image_result)
                    if inspect.isawaitable(result):
                        result = await result
                    yield result
                    return
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("NapCat status image result API failed")
            else:
                logger.warning("NapCat status image result API unavailable")
        yield event.plain_result(self._status_text(data))

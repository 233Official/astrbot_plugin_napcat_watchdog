"""AstrBot NapCat 存活监控插件 — WebSocket 服务端阶段。"""

from __future__ import annotations

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star

from .core import WatchdogWSServer
from .core.token import ensure_access_token


class NapCatWatchdogPlugin(Star):
    """NapCat QQ 存活监控插件，当前完成 WebSocket 服务端接入阶段。"""

    def __init__(self, context: Context, config: AstrBotConfig) -> None:
        super().__init__(context, config)
        self.config = config
        self._ws_server: WatchdogWSServer | None = None

    async def initialize(self) -> None:
        """初始化并启动 WebSocket 服务。

        - access_token 为空时自动生成 32 字节安全随机 Token 并写回配置。
        - 持久化失败时重置 Token 为空并向上抛出 RuntimeError，阻止 WS 启动。
        - 启动失败时记录错误，不阻塞插件加载。
        """
        # ---- 确保 access_token 已设置且持久化（fail closed） ----
        token: str = ensure_access_token(self.config, logger)

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

        try:
            await self._ws_server.start()
        except Exception as e:
            logger.error("Watchdog WS server failed to start: %s", e)
            self._ws_server = None

    async def terminate(self) -> None:
        """停止 WebSocket 服务并释放所有资源。"""
        if self._ws_server is not None:
            try:
                await self._ws_server.stop()
            except Exception as e:
                logger.error("Error stopping Watchdog WS server: %s", e)
            self._ws_server = None

    @filter.command("napcat_watchdog_status")
    async def status(self, event: AstrMessageEvent):
        """返回插件当前阶段状态。"""
        if self._ws_server is not None and self._ws_server.bound_port is not None:
            yield event.plain_result("NapCat Watchdog — WS 服务端阶段已实现。")
        else:
            yield event.plain_result("NapCat Watchdog — WS 服务端未运行。")

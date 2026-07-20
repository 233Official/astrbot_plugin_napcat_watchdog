from __future__ import annotations

from astrbot.api import AstrBotConfig
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star


class NapCatWatchdogPlugin(Star):
    """NapCat QQ 存活监控插件骨架。"""

    def __init__(self, context: Context, config: AstrBotConfig) -> None:
        super().__init__(context, config)
        self.config = config

    async def initialize(self) -> None:
        """完成插件骨架初始化，暂不启动任何监控任务。"""

    async def terminate(self) -> None:
        """终止插件骨架，当前没有需要释放的监控资源。"""

    @filter.command("napcat_watchdog_status")
    async def status(self, event: AstrMessageEvent):
        """返回插件当前开发状态。"""
        yield event.plain_result("插件骨架已加载，监控功能将在 PRD 确认后实现。")

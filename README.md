# NapCat QQ 存活监控

`astrbot_plugin_napcat_watchdog` 计划用于在 AstrBot 中检查 NapCat QQ 实例的存活状态，并在异常时向约定目标发送通知。

## 当前状态

项目当前处于**需求设计阶段**，仅包含可被 AstrBot 加载的最小插件骨架和只读状态命令，尚未实现真实监控、告警或恢复操作。

已对齐、待实现的正式需求见 [产品需求文档](docs/PRD.md)。

> 本插件目前不可用于生产环境，也不应作为 NapCat QQ 实例的可用性保障手段。

---

## 当前可用功能

- 通过 AstrBot 的 `Star` 子类自动发现机制加载插件。
- 当前骨架暂时提供只读命令 `/napcat_watchdog_status`，仅用于确认插件已经加载；正式实现将迁移为 `/napcat_watchdog status` 子命令格式。
- 当前骨架仍使用空配置 schema，正式监控配置尚未实现。

---

## 预期能力

以下能力仅为后续设计目标，不代表当前版本已经实现：

- 插件自建 `aiohttp` WebSocket 服务端，由 1–20 个 NapCat OneBot 11 WebSocket Client 通过 Caddy WSS/TLS 主动连接。
- 所有 NapCat 共用自动生成或手动配置的全局 Bearer Token，并通过 `X-Self-ID` 自动登记，无需实例白名单。
- 仅消费 lifecycle 与 heartbeat；WS 断开、心跳缺失或持续 `online=false` 达到默认 90 秒后，统一通知“NapCat 掉线”。
- AstrBot 管理员在 QQ 群内订阅全部当前和未来 NapCat 状态；掉线通知提及该群订阅负责人，上线通知不提及。
- 使用轻量 JSON 原子持久化状态、群订阅和待补发摘要，避免重启重复通知。
- WebUI 仅管理监听地址、公网 WSS 地址、全局 Token 和掉线超时等最小配置。

完整范围、兼容基线和验收标准见 [产品需求文档](docs/PRD.md)。

---

## 最终命令

正式实现计划提供以下仅管理员可用的命令：

- `/napcat_watchdog subscribe`：在当前 QQ 群订阅全部 NapCat 状态。
- `/napcat_watchdog unsubscribe`：取消当前 QQ 群订阅。
- `/napcat_watchdog status`：查看连接、状态和订阅摘要。

其中 `subscribe` 和 `unsubscribe` 仅可在群聊中执行。上述命令尚未实现，当前骨架验证仍使用 `/napcat_watchdog_status`。

---

## 开发环境安装

1. 准备可运行的 AstrBot 开发环境。
2. 将本仓库克隆到 AstrBot 的 `data/plugins` 目录：

   ```bash
   cd AstrBot/data/plugins
   git clone https://github.com/233Official/astrbot_plugin_napcat_watchdog.git
   ```

3. 启动 AstrBot，或在 WebUI 的插件管理页面重载插件。
4. 在可用会话中发送 `/napcat_watchdog_status` 验证骨架是否加载。

当前版本没有第三方 Python 依赖。

---

## 本地验证

在插件仓库根目录执行：

```bash
python -m pytest
ruff check .
ruff format --check .
```

结构测试使用 Python AST，不要求本地安装 AstrBot。

---

## 许可证

本项目使用 MIT License，详见 [LICENSE](LICENSE)。

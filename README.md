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
- 提供空配置 schema，避免在 PRD 确认前固化实例或通知目标结构。

---

## 预期能力

以下能力仅为后续设计目标，不代表当前版本已经实现：

- 由 1–20 个 NapCat OneBot 11 WebSocket Client 主动连接插件提供的 WS 服务，公网 WSS/TLS 由 Caddy 终止。
- 仅消费 lifecycle 与 heartbeat，基于心跳、防抖和重连窗口区分 QQ 离线、连接丢失与从未连接。
- 使用实例独立 Token、`X-Self-ID` 和预配置白名单完成鉴权与身份校验。
- 通过唯一 AstrBot 通知账号向多个目标群发送一次异常和一次恢复通知，并持久化状态与待补发摘要。
- 通过 WebUI 管理配置，并提供仅管理员可用的 `/napcat_watchdog status` 只读状态命令。

完整范围、兼容基线和验收标准见 [产品需求文档](docs/PRD.md)。

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

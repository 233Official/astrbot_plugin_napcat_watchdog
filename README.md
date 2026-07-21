# NapCat QQ 存活监控

`astrbot_plugin_napcat_watchdog` 计划用于在 AstrBot 中检查 NapCat QQ 实例的存活状态，并在异常时向约定目标发送通知。

## 当前状态

项目当前处于 **WebSocket 服务端接入阶段**。已完成以下工作：

- 插件在 AstrBot 内自建 `aiohttp` WebSocket 服务端。
- 支持 Bearer Token 鉴权和 `X-Self-ID` 自动登记。
- Token 自动生成（空时基于 32 字节安全随机源）与轮换。
- 连接替换、优雅关闭和基本事件回调接口。

尚未实现状态监控、掉线防抖、群订阅和通知推送（计划中的后续里程碑）。正式需求见 [产品需求文档](docs/PRD.md)。

> 本插件目前不可用于生产环境，也不应作为 NapCat QQ 实例的可用性保障手段。

---

## 当前可用功能

- 通过 AstrBot 的 `Star` 子类自动发现机制加载插件。
- `aiohttp` WebSocket 服务端，支持：
  - Bearer Token 鉴权（`secrets.compare_digest` 安全比较）。
  - `X-Self-ID` 验证与自动登记。
  - 同一 QQ 的新连接替换旧连接。
  - Token 轮换时关闭所有现有连接。
  - 可注册异步 JSON 事件回调（供后续功能使用）。
- 当 `access_token` 为空时，首次启动自动生成基于 32 字节安全随机数据的 Token，并持久化到插件配置。
- 提供只读命令 `/napcat_watchdog_status` 查看服务端运行状态。

---

## 预期能力

以下能力仅为后续设计目标，不代表当前版本已经实现：

- 插件自建 `aiohttp` WebSocket 服务端，由 1–20 个 NapCat OneBot 11 WebSocket Client 通过 Caddy WSS/TLS 主动连接。
- 所有 NapCat 共用自动生成或手动配置的全局 Bearer Token，并通过 `X-Self-ID` 自动登记，无需实例白名单。
- 仅消费 lifecycle 与 heartbeat；WS 断开、心跳缺失或持续 `online=false` 达到默认 90 秒后，统一通知"NapCat 掉线"。
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

其中 `subscribe` 和 `unsubscribe` 仅可在群聊中执行。上述命令尚未实现，当前仍使用 `/napcat_watchdog_status` 查看服务端状态。

---

## 开发环境安装

1. 准备可运行的 AstrBot 开发环境。
2. 将本仓库克隆到 AstrBot 的 `data/plugins` 目录：

   ```bash
   cd AstrBot/data/plugins
   git clone https://github.com/233Official/astrbot_plugin_napcat_watchdog.git
   ```

3. 安装 Python 依赖：

   ```bash
   pip install aiohttp>=3.9.0
   ```

   或在插件目录执行：

   ```bash
   pip install -r requirements.txt
   ```

4. 启动 AstrBot，或在 WebUI 的插件管理页面重载插件。
5. 在可用会话中发送 `/napcat_watchdog_status` 验证插件是否已加载。

当前运行时依赖为 `aiohttp>=3.9.0`。

---

## 本地验证

在插件仓库根目录执行：

```bash
python -m pytest -q
ruff check .
ruff format --check .
```

结构测试使用 Python AST，不要求本地安装 AstrBot。WebSocket 集成测试使用 `aiohttp` 真实连接，绑定 `127.0.0.1` 与操作系统分配端口。

---

## 许可证

本项目使用 MIT License，详见 [LICENSE](LICENSE)。

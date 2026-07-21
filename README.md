# NapCat QQ 存活监控

`astrbot_plugin_napcat_watchdog` 用于通过 NapCat OneBot 11 反向 WebSocket 监控多个 QQ 实例的存活状态，并为 AstrBot 管理员提供群订阅和状态查询能力。

## 当前状态

项目当前处于 **生产联调前阶段**。已完成以下工作：

- 插件在 AstrBot 内自建 `aiohttp` WebSocket 服务端。
- 支持 Bearer Token 鉴权和 `X-Self-ID` 自动登记。
- Token 自动生成（空时基于 32 字节安全随机源）与轮换。
- connection generation 隔离、连接替换和优雅关闭。
- 90 秒掉线防抖、重启宽限和严格 heartbeat 校验。
- 状态与群订阅的独立 JSON 原子持久化。
- AstrBot 管理员群订阅、取消订阅和状态查询命令。

尚未实现上下线群通知、负责人提及、失败重试和待补发队列。正式需求见 [产品需求文档](docs/PRD.md)。

> 当前版本正在进行生产 smoke test，通知链路完成前不应作为 NapCat QQ 实例的唯一可用性保障手段。

---

## 当前可用功能

- 通过 AstrBot 的 `Star` 子类自动发现机制加载插件。
- `aiohttp` WebSocket 服务端，支持：
  - Bearer Token 鉴权（`secrets.compare_digest` 安全比较）。
  - `X-Self-ID` 验证与自动登记。
  - 同一 QQ 的新连接替换旧连接。
  - Token 轮换时关闭所有现有连接。
  - 仅消费结构合法的 OneBot 11 heartbeat，其他事件安静丢弃。
- 当 `access_token` 为空时，首次启动自动生成基于 32 字节安全随机数据的 Token，并持久化到插件配置。
- 最多自动登记 20 个 NapCat QQ 实例；20 仅为安全上限，实际可按需接入少量实例。
- 支持 WebSocket 断连、heartbeat 缺失和持续 `online=false` 的 90 秒掉线防抖。
- 提供仅 AstrBot 管理员可用的命令组：
  - `/napcat_watchdog subscribe`
  - `/napcat_watchdog unsubscribe`
  - `/napcat_watchdog status`
- `subscribe` 和 `unsubscribe` 仅允许在群聊执行，并持久化完整 UMO 与当前负责人。

---

## 后续能力

以下能力将在后续阶段实现：

- 掉线时向全部订阅群发送通知并提及该群当前负责人。
- 首次接入和确认掉线后的恢复发送不带提及的上线通知。
- 多群独立发送、失败重试和最小待补发摘要。
- Caddy WSS/TLS 与 NapCat WebSocket Client 的正式部署文档和行为测试。

完整范围、兼容基线和验收标准见 [产品需求文档](docs/PRD.md)。

---

## 命令

以下命令仅 AstrBot 管理员可用：

- `/napcat_watchdog subscribe`：在当前 QQ 群订阅全部 NapCat 状态。
- `/napcat_watchdog unsubscribe`：取消当前 QQ 群订阅。
- `/napcat_watchdog status`：查看连接、状态和订阅摘要。

其中 `subscribe` 和 `unsubscribe` 仅可在群聊中执行；`status` 可在 AstrBot 支持的命令上下文中查询只读摘要。

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
5. 由 AstrBot 管理员在可用会话中发送 `/napcat_watchdog status` 验证插件是否已加载。

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

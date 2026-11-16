# NapCat QQ 存活监控

`astrbot_plugin_napcat_watchdog` 通过 OneBot 11 反向 WebSocket 监控 NapCat、SnowLuma 等兼容客户端，并为 AstrBot 管理员提供群订阅和状态查询能力。监控失联不等同于 QQ 账号本身离线。

## 当前状态

已实现以下功能；目标环境是否具备这些行为，以实际安装包版本和验收结果为准：

- 插件在 AstrBot 内自建 `aiohttp` WebSocket 服务端。
- 支持 Bearer Token 鉴权和 `X-Self-ID` 自动登记。
- Token 自动生成（空时基于 32 字节安全随机源）与轮换。
- connection generation 隔离、连接替换和优雅关闭。
- 90 秒掉线防抖、重启宽限和严格 heartbeat 校验。
- 状态与群订阅的独立 JSON 原子持久化。
- AstrBot 管理员群订阅、取消订阅和状态查询命令；`status` 优先返回 HTML 仪表盘图片，渲染不可用时回退为安全文本。
- 确认掉线、首次上线和恢复上线通知，使用脱敏 HTML 图片卡片及安全文本回退。
- 多群独立投递、最多三次重试、待补发摘要持久化和重启补发。
- 持久化账号昵称和有效头像，通知优先使用上次成功资料；过期后后台刷新，失败不清空旧资料。

> 本地代码和测试通过不代表生产环境已更新。部署后应分别核对资料缓存恢复、监控状态通知和消息投递结果。

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

## 后续工作

- Caddy WSS/TLS 与 NapCat WebSocket Client 的正式部署文档和行为测试。

完整范围、兼容基线和验收标准见 [产品需求文档](docs/PRD.md)。

---

## 命令

以下命令仅 AstrBot 管理员可用：

- `/napcat_watchdog subscribe`：在当前 QQ 群订阅全部 NapCat 状态。
- `/napcat_watchdog unsubscribe`：取消当前 QQ 群订阅。
- `/napcat_watchdog status`：查看 HTML 仪表盘图片形式的连接、状态和订阅摘要；渲染失败时使用安全文本回退。

其中 `subscribe` 和 `unsubscribe` 仅可在群聊中执行；`status` 可在 AstrBot 支持的命令上下文中查询只读摘要。

### 资料缓存与通知语义

- 账号资料保存在 AstrBot 插件数据目录的 `profiles/` 下，按已登记账号独立保存，不随插件重载丢失。缓存包含昵称、经过校验的本地头像数据及各自更新时间，不包含 Token、群订阅或聊天内容。
- 昵称和头像默认保持新鲜 24 小时；有效期内重连不重复查询资料。过期缓存仍可直接用于通知和状态查询，在线连接通过后台任务刷新，刷新失败后至少间隔 5 分钟再尝试。
- 昵称取得后立即可读，不等待头像下载。头像失败或超时不会覆盖上次成功头像。
- 恢复通知有可用缓存时立即渲染；完全没有资料时最多等待已有后台补全任务 3 秒，超时后用已取得的资料或占位内容发送，不阻塞心跳处理，也不取消后台刷新。失联通知不等待网络。
- 卡片和文本使用 OneBot 监控名称。“监控失联”“失联时长”表示监控服务未能确认客户端正常的状态和持续时间，不据此判断 QQ 是否一直离线。
- 缓存文件损坏、超限或不可读时仅舍弃该账号的展示缓存，不阻止监控启动，也不清空状态、订阅或通知记录。缓存文件不进入发布包或版本库。

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

## 打包与发布

插件支持标准打包流程，生成适用于 AstrBot WebUI 上传安装的 ZIP 包。

```bash
# 正式包 (dist/astrbot_plugin_napcat_watchdog-v0.1.0.zip)
python scripts/package_plugin.py

# 测试包 (dist/astrbot_plugin_napcat_watchdog-v0.1.0.dev<timestamp>.zip)
python scripts/package_plugin.py --dev-version

# 旧版扁平包 (无顶层插件目录)
python scripts/package_plugin.py --dev-version --flat
```

正式包满足 WebUI 上传要求：ZIP 内包含 `astrbot_plugin_napcat_watchdog/` 顶层目录。打包脚本会自动校验 `metadata.yaml` 与 `pyproject.toml` 的版本一致性，仅在包内修改版本号（不修改工作区源文件）。

> 注：当前为开发阶段，尚未发布正式 Release。

---

## 许可证

本项目使用 MIT License，详见 [LICENSE](LICENSE)。

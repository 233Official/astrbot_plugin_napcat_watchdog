# NapCat QQ 存活监控

`astrbot_plugin_napcat_watchdog` 计划用于在 AstrBot 中检查 NapCat QQ 实例的存活状态，并在异常时向约定目标发送通知。

## 当前状态

项目当前处于**需求设计阶段**，仅包含可被 AstrBot 加载的最小插件骨架和只读状态命令，尚未实现真实监控、告警或恢复操作。

> 本插件目前不可用于生产环境，也不应作为 NapCat QQ 实例的可用性保障手段。

---

## 当前可用功能

- 通过 AstrBot 的 `Star` 子类自动发现机制加载插件。
- 提供只读命令 `/napcat_watchdog_status`，用于确认插件骨架已经加载。
- 提供空配置 schema，避免在 PRD 确认前固化实例或通知目标结构。

---

## 预期能力

以下能力仅为后续设计目标，不代表当前版本已经实现：

- 检查一个或多个 NapCat QQ 实例的存活状态。
- 区分连接失败、认证失败、接口异常和实例离线等状态。
- 按可控频率执行检查，并提供超时、重试与抑制重复告警的策略。
- 将异常与恢复通知发送到经过配置的 AstrBot 会话。
- 提供只读状态查询与必要的诊断信息。

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

## PRD 待讨论事项

- NapCat 实例的唯一标识、连接方式和多实例配置结构。
- 存活判定使用的接口、认证方式、超时与重试策略。
- 目标群、私聊或其他 AstrBot 会话的配置表达方式。
- 告警触发、恢复通知、重复告警抑制和静默窗口。
- 凭据保存、日志脱敏和最小权限边界。
- 插件与 AstrBot、NapCat 版本之间的兼容范围。
- 状态持久化、重启后的告警连续性与诊断信息范围。

---

## 许可证

本项目使用 MIT License，详见 [LICENSE](LICENSE)。

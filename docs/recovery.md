# Recovery

## Facade restart

The MCP facade is stateless. Restart it with the same `ZCODE_AGENTD_SOCKET`, then use `zcode_subagent_poll` or `zcode_subagent_result` with the durable `agent_id`. Hook provenance and service generation are optional integration metadata and are not required for daemon startup. Do not confuse a daemon restart with a facade restart or a task identity.

## Daemon restart

Stop the daemon with SIGTERM or SIGINT and wait for its exact socket to disappear. Restart with the same canonical database and socket paths. Startup reconciliation runs before publication. Live runtime reconnect is unsupported; interrupted work becomes runtime-lost or orphaned without signaling an unverified PID or process group.

After restart, call `zcode_subagent_list` with explicit repository, feature, or ownership scope. Inspect tasks with `poll` and `result`, then close them after verifying durable state. Start a new Agent for further work.

## Data and terminal history

For a consistent SQLite backup, stop the sole daemon and preserve the database with any WAL/SHM companions. Terminal history is read through `zcode_subagent_result` and running state through poll. 恢复已结束的 session 暂不可用；发送恢复失败时保留原终态历史，具体限制见下文。 Never read private stored locators directly.

This product intentionally has no compatibility framework or migration for removed unpublished records. Use `cleanup-legacy --yes` only for explicit deletion; it never imports or aliases legacy data.

## Session 恢复暂不可用

截至 2026-09-07，本产品不支持通过追加消息恢复已结束的 Agent。任务完成后
runtime 会被回收，即使 `closed=false`，再次发送也属于冷恢复。此限制不影响
运行中的追加消息、MCP facade 重启后重新连接正在运行的任务，或查询终态历史。
继续工作请 `spawn` 新 Agent，并显式提供需要的上下文；新任务不会自动继承旧会话。

已验证环境：ZCode Desktop 3.11.2，内置 CLI 0.16.5，macOS ARM64；官方
`zcode.cjs` SHA-256 为
`e9f1868c0fdb863537ed910ee3828b9be96b8c2fd805473f63b439e1113266b8`。
此前 3.10.1 也观察到同类失败；未验证其他版本是否受影响。

真实调用结果：`session/resume` 与 `session/subscribe` 成功，但
`session/send` 被官方 runtime 拒绝，原始错误为
`-32031 / ZCODE_RUNTIME_MODEL_UNAVAILABLE`。本产品可能显示通用
`daemon_unavailable` 调用错误，消息最终为 `FAILED / SESSION_SEND_FAILED`。
原任务的结果会保留；旧结果中的 `COMPLETED` 不代表追加消息成功。

同一官方 runtime、session、工作区与已有配置下，直接使用官方 CLI
`--resume <sessionId> --prompt <text>` 完成了真实 Read，退出码为 0；再走
app-server 路径仍失败。因此没有证据表明 session 丢失或账号失效。

源码与启动日志显示，CLI 恢复使用本地配置的正常模型适配器；app-server
冷恢复在未收到 `runtimeModel` 且进程内 `workspaceModelCatalogs` 没有匹配
配置时，创建空 provider registry 的延迟适配器并设置 `restoreWarning`，
随后在 send 前拒绝。现阶段按 app-server 冷恢复兼容性问题记录，尚未取得
上游对预期接口行为的确认。本产品没有绕过该检查、转发 provider 配置或
将 CLI 恢复作为自动降级路径。

入站反向请求携带 `trace` 元数据导致解析失败的问题已单独修复；这能让
恢复进入发送阶段，但不能解决上述模型初始化拒绝。

正式复现命令（真实模型调用，需本地 daemon 和 ZCode 可用）：

```sh
python3 tests/live-agent/non-git-based/real_terminal_send_case.py
```

用例先完成真实文件读取，再重启自己的 MCP facade，向同一未 close 的
已完成 Agent 发送追加读取，记录重试、消息状态、结果与清理。失败证据保留在
`tests/live-agent/workspace/real-terminal-send-*`，不会作为成功验收。

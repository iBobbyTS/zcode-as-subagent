# Recovery

## Facade restart

The MCP facade is stateless. Restart it with the same `ZCODE_AGENTD_SOCKET`, then use `zcode_subagent_poll` or `zcode_subagent_result` with the durable `agent_id`. Hook provenance and service generation are optional integration metadata and are not required for daemon startup. Do not confuse a daemon restart with a facade restart or a task identity.

## Daemon restart

Stop the daemon with SIGTERM or SIGINT and wait for its exact socket to disappear. Restart with the same canonical database and socket paths. Startup reconciliation runs before publication. Live runtime reconnect is unsupported; interrupted work becomes runtime-lost or orphaned without signaling an unverified PID or process group.

After restart, call `zcode_subagent_list` with explicit repository, feature, or ownership scope. Inspect tasks with `poll` and `result`, then close them after verifying durable state. Start a new Agent for further work.

Observation snapshots are intentionally memory-only. After a daemon restart,
`zcode_subagent_observe` can return an empty bounded snapshot for a retained
task only with `tool_history_complete=false` and
`reasoning_complete=false`; it never presents the missing pre-restart history
as a complete empty lifetime. A runtime path or SHA-256 mismatch makes the
observation source unavailable and sets the status capability to false. Use
`poll` and `result` for durable lifecycle facts.

Within one daemon lifetime, observation retains at most 64 KiB of accumulated
public reasoning source so the 200-character projection can be maintained
without retaining unlimited history. When that budget is reached, the tracker
evicts old source, records a coverage gap with `reasoning_complete=false`, and
continues updating the latest 200-character tail. Message content is not
rewritten here; the two Agent layers own redaction before messages reach this
middle layer. The tracker never reconstructs the discarded prefix or stores an
unlimited reasoning history.

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
`-32031 / ZCODE_RUNTIME_MODEL_UNAVAILABLE`。本产品将 runtime 命令失败显示为
`runtime_command_failed`，消息最终为 `FAILED / SESSION_SEND_FAILED`。
旧版 facade 曾将此错误误报为 `daemon_unavailable`。发送失败诊断保留
`operation`、`remote_code` 和有界的 `remote_message`（不做消息内容改写）；后续进程清理状态
另列为 `cleanup_result`，不覆盖最初拒绝原因。
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

## 调用错误与恢复动作

- `conflict: WORKSPACE_BUSY`：工作区已有活动任务；可查询 `active_agent_id`。
- `conflict: MESSAGE_ID_CONFLICT`：消息 ID 已绑定其他 Agent 或内容。新消息使用新 ID；只有同一消息的重试才复用 ID。
- `runtime_command_failed`：daemon 已处理请求，但 runtime 命令失败。先用 `poll`、`result` 和 `diagnose` 查看状态；此错误不表示 daemon 离线。
- `daemon_unavailable`：MCP 无法通过 socket 联系 daemon。检查服务与 socket；不要因上述两类业务拒绝自动重启服务。

先比较 `status.identity.daemon` 与 `status.identity.facade` 中来源为
`running_executable` 的路径、hash 和采集时间，再比较 CLI diagnose 中来源为
`distributed_payload` 的磁盘产物。磁盘新文件不能证明旧进程已重启；CLI
未经过 MCP facade 时会把 facade running identity 明确报告为未观察。

按 Agent 导出的结构化诊断在 16 KiB 序列化预算内保留有效 JSON：元数据使用有界前缀，`stderr_tail` 优先保留末尾，并明确标记 `truncated`。全局日志尾部与按 Agent 的保留窗口查询仍是两个不同范围。

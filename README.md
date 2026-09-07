# zcode-as-subagent

`zcode-as-subagent` is a local npm package that provides the `zas` CLI and MCP facade for running
one durable Agent in a caller-selected workspace. The package exposes the same
Agent lifecycle through `zas` and the `zcode_subagent_*` MCP
tools; there is no compatibility alias or migration layer.

## Install and use

```bash
npm install -g zcode-as-subagent
zas help
zas init --dry-run
zas install-mcp
# Hooks are opt-in:
zas init --install-hooks
# Or install them independently:
zas hooks install
zas status
```

`install-mcp` installs the `zcode_as_subagent` MCP server into
`$CODEX_HOME/config.toml`, or `~/.codex/config.toml` when `CODEX_HOME` is not
set. It preserves unrelated Codex configuration and can be run repeatedly.
Use `zas install-mcp --uninstall` to remove only this managed
MCP entry.

The macOS runtime is probed only at the fixed bundle location
`/Applications/ZCode.app/Contents/Resources/glm/zcode.cjs`. Windows supports
only `help` and `version`; business commands return structured
`UNSUPPORTED_PLATFORM` without touching HOME. The npm package never downloads,
upgrades, or manages providers, credentials, GUI clients, remote daemons,
multi-tenant services, Windows daemons, Rosetta, Git, worktrees, or a second
supervisor.

## Public MCP catalog

The ten tools are `zcode_subagent_status`, `zcode_subagent_spawn`,
`zcode_subagent_poll`, `zcode_subagent_list`, `zcode_subagent_observe`, `zcode_subagent_send`,
`zcode_subagent_respond`, `zcode_subagent_cancel`, `zcode_subagent_result`,
and `zcode_subagent_close`. Spawn accepts only `build`, `edit`, `plan`, or
`yolo` permission modes (default `build`). `write_manifest` is optional for
`build`, `edit`, and `yolo`; when omitted the
daemon uses the protected repository workspace scope, while `plan` remains
read-only. A canonical workspace has one active Agent; a collision is reported
as `WORKSPACE_BUSY` with the active id. Terminal results expose stable outcome,
partial status, task reason code, and bounded result segments; use
`offset`/`limit` (default and maximum 81920 bytes) for large text.

`zcode_subagent_observe` is a suspicion-only, read-only snapshot. Call it with
only `agent_id` when recent behavior may be looping; ordinary progress remains
on `poll`. It returns at most the top three tool names by lifetime invocation
count, the latest five calls per returned tool with bounded redacted arguments
and no results, plus the newest 200 Unicode characters from the locally
verified public reasoning stream. It does not classify progress or cancel a
task. The same daemon projection is available as
`zas observe --json '{"agent_id":"..."}'`. Status reports the observation
protocol, bounds, default public collection, and whether the configured runtime
source matches the verified local path and SHA-256.

## 已知限制：暂不支持恢复已结束的 session

当前版本不能通过 `zcode_subagent_send` 恢复已结束的 Agent 并继续执行，
包括 `COMPLETED` 但尚未调用 `close` 的情况。首次任务结束后 runtime 已回收，
追加消息会进入冷恢复路径；发送失败时仍可查询原终态结果。请新建 Agent
并显式提供所需上下文，不要把旧 `COMPLETED` 结果视为追加消息执行成功。

此限制在 ZCode Desktop 3.11.2 / 内置 CLI 0.16.5 上仍可复现；官方 CLI
`--resume` 的同会话对照成功，失败范围是本产品使用的 app-server 冷恢复路径。
运行中发送消息及重启 MCP facade 后读取历史不属于此限制。
详见 [恢复限制与复现说明](docs/recovery.md#session-恢复暂不可用)。

## Data, cleanup, and safety

`uninstall` removes service registration but retains data. `purge --yes` is the
only destructive data operation. `cleanup-legacy --yes` removes an old,
unpublished installation without importing or aliasing its data. Optional
file-scope Hooks enforce only the declared workspace boundary; Bash permission
decisions remain with the official runtime and caller response. PostToolUse
records observed metadata-only facts and never re-evaluates policy.

The default `init` does not modify ZCode hook configuration. Hook installation
is explicit via `init --install-hooks` or `hooks install`.

The distributable public request catalog is versioned at
[`schema/zcode-subagent-public-api.json`](schema/zcode-subagent-public-api.json).
See [docs/setup.md](docs/setup.md), [docs/operations.md](docs/operations.md),
[docs/recovery.md](docs/recovery.md), and the
[plugin validation guide](plugins/zcode-subagent-mcp/docs/VALIDATION.md).

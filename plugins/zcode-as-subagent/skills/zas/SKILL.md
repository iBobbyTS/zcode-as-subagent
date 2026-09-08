---
name: zas
description: Safely operate the local ZCode-as-subagent MCP lifecycle.
---

# ZCode-as-subagent

Use this skill as a protocol guide; the MCP server is the source of runtime truth.

## Preflight and planning

默认先执行一次目标明确的 `zcode_subagent_spawn`；不要为了例行预检先调用
`status`/`list`。只有 spawn 返回错误、没有返回可用 `agent_id`、或任务无法推进时，
才调用 `zcode_subagent_status` 和 repository-scoped `zcode_subagent_list`（使用精确仓库路径；
`workspace` 是别名）进行诊断，并据此核对既有任务。spawn 不幂等：保留返回的
`agent_id`，不得盲目重试。

Plans must be explicit, small, and complete. Every plan prompt MUST say: **不得执行
Bash 命令**. If Bash is needed, edit the plan first, then issue a separately authorized
call. Never use `write_manifest` or silently escalate a failed plan to edit/build/yolo.
Read only the relevant sections, but do not call a plan reviewed until all requested
pages are read and hashes/paths are recorded.

## Lifecycle

Use `zcode_subagent_send` with the task ID and preserve the returned receipt. `queued`
means accepted for delivery; `delivered` means the remote agent acknowledged it; these
are distinct outcomes. Poll with `zcode_subagent_poll` and a maximum 5-second timeout.
Use `zcode_subagent_result` to read the complete result, following its pagination until
the end before summarizing. Keep completion state, semantic verdict (for example CLEAN),
and telemetry/health as separate facts; none alone proves review or business success.

`zcode_subagent_observe` is suspicion-only evidence for a suspected meaningless loop,
not a heartbeat and not a success signal. Respect permission responses and
`unsupported_input` without retry storms. Use `cancel` for an active task and `close`
to release a terminal session; verify the terminal receipt. Terminal resume is valid
only when the current session reports it, and a rejected resume remains terminal.

## Evidence and boundaries

Record exact IDs, repository, paths, command/model provenance, receipts, status
transitions, pagination boundaries, timeout values, and bounded diagnostic tails. Do not
restart the daemon automatically on an error. A complete runtime result is evidence to
interpret, not permission to claim semantic acceptance; a reviewer must independently
decide CLEAN/NOT-CLEAN from the full requested scope.

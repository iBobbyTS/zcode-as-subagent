# Operations

## CLI daemon calls

The `zas` CLI forwards business commands to the configured daemon
Unix socket (`ZCODE_AGENTD_SOCKET`, or the product socket under Application
Support). Pass a JSON object with `--json` or on stdin. `create`/`spawn` submit
the general task contract; `get`/`poll` read task progress. The remaining
commands map to the corresponding daemon task RPCs:

```sh
zas create --json '{"repository":"/abs/repo","prompt":"..."}'
zas poll --json '{"agent_id":"...","timeout_ms":5000}'
zas result --json '{"agent_id":"..."}'
```

Successful responses are structured JSON. Daemon errors preserve their `code`,
`message`, and (when present) `agent_id`; an unavailable socket is reported as
`SOCKET_UNAVAILABLE`.

## Generic lifecycle

1. Call `zcode_subagent_spawn` with a canonical workspace and one of `build|edit|plan|yolo` (`build` is the default).
2. Call `zcode_subagent_poll` with the returned `agent_id`, `after_revision`, and bounded `timeout_ms`.
3. Reuse `next_revision`; do not restart polling from zero.
4. Answer only daemon-published typed requests through `zcode_subagent_respond`.
5. Queue clarification for a running Agent with `zcode_subagent_send`. 恢复已结束的 session 暂不可用（包括尚未 close 的 COMPLETED 任务）；继续工作请新建 Agent 并提供所需上下文。失败的追加发送不改变原终态结果，详见 [恢复限制](recovery.md#session-恢复暂不可用)。
6. Read final text through `zcode_subagent_result`; use poll for running progress.
7. Use `zcode_subagent_cancel` for authoritative stop/kill/reap and `zcode_subagent_close` for idempotent cleanup.

`zcode_subagent_list` requires a repository scope. The CLI also accepts
`workspace` as a JSON-input alias and normalizes it to `repository`; neither is
a direct command-line flag. Filtering occurs in the Store before the limit.

## Activity

Poll exposes bounded visible text, active tool classes, model request clocks, and rolling 60-second counts. Reasoning content, tool arguments, cwd, command output, and absolute internal paths are never public. Runtime activity is liveness evidence, not semantic progress.

Pending requests and terminal transitions wake long polls immediately. Unknown telemetry shapes degrade telemetry status without failing the Agent.

## Completion and bounded control waits

A matching `turn.completed` converges the task to `TERMINAL` after runtime cleanup. The terminal response text is the authoritative final text.

The adapter does not stop a task because a tool, model stream, approval request,
or the runtime has been quiet for an adapter-selected interval. Finite waits are
limited to connection and handshake work, individual control RPCs, cancellation,
and process recovery/reaping. Explicit cancellation still fences late events
before terminal persistence.

`COMPLETED` means the runtime turn ended and daemon finalization succeeded. It does not mean a review is clean, a patch is correct, or the change is mergeable.

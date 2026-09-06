# Operations

## CLI daemon calls

The `zcode-as-subagent` CLI forwards business commands to the configured daemon
Unix socket (`ZCODE_AGENTD_SOCKET`, or the product socket under Application
Support). Pass a JSON object with `--json` or on stdin. `create`/`spawn` submit
the general task contract; `get`/`poll` read task progress. The remaining
commands map to the corresponding daemon task RPCs:

```sh
zcode-as-subagent create --json '{"repository":"/abs/repo","prompt":"..."}'
zcode-as-subagent poll --json '{"agent_id":"...","timeout_ms":5000}'
zcode-as-subagent result --json '{"agent_id":"..."}'
```

Successful responses are structured JSON. Daemon errors preserve their `code`,
`message`, and (when present) `agent_id`; an unavailable socket is reported as
`SOCKET_UNAVAILABLE`.

## Generic lifecycle

1. Call `zcode_subagent_spawn` with a canonical workspace and one of `build|edit|plan|yolo` (`build` is the default).
2. Call `zcode_subagent_poll` with the returned `agent_id`, `after_revision`, and bounded `timeout_ms`.
3. Reuse `next_revision`; do not restart polling from zero.
4. Answer only daemon-published typed requests through `zcode_subagent_respond`.
5. Queue clarification with `zcode_subagent_send`. For a closed session the daemon attempts a send-based resume; a failed resume returns a typed error and preserves the terminal history.
6. Read final text through `zcode_subagent_result`; use poll for running progress.
7. Use `zcode_subagent_cancel` for authoritative stop/kill/reap and `zcode_subagent_close` for idempotent cleanup.

`zcode_subagent_list` requires repository, feature, or ownership scope. Filtering occurs in the Store before the limit.

## Activity

Poll exposes bounded visible text, active tool classes, model request clocks, and rolling 60-second counts. Reasoning content, tool arguments, cwd, command output, and absolute internal paths are never public. Runtime activity is liveness evidence, not semantic progress.

Pending requests and terminal transitions wake long polls immediately. Unknown telemetry shapes degrade telemetry status without failing the Agent.

## Completion and timeouts

A matching `turn.completed` converges the task to `TERMINAL` after runtime cleanup. The terminal response text is the authoritative final text.

Timeout classes are `RUNTIME_ACTIVITY_IDLE_TIMEOUT`, `MODEL_STREAM_IDLE_TIMEOUT`, `TOOL_CALL_TIMEOUT`, `INPUT_WAIT_TIMEOUT`, and `WALL_TIME_DEADLINE_EXCEEDED`. Timeout and cancellation fence late events before terminal persistence.

`COMPLETED` means the runtime turn ended and daemon finalization succeeded. It does not mean a review is clean, a patch is correct, or the change is mergeable.

---
name: zcode-as-subagent-debugging
description: Debug this repository's zcode-as-subagent MCP, daemon, runtime, and lifecycle failures using source-to-process evidence. Use when spawn/poll/result behavior, validation, stale binaries, logs, permissions, or MCP transport state is unclear.
---

# Zcode As Subagent Debugging

Use this skill for diagnosis and narrowly scoped repairs in `/Users/ibobby/Projects/zcode-mcp`.

## Establish the active implementation

- Inspect the working tree and initialize/sync CodeGraph when the repository is indexed.
- Trace the request through `crates/zcode-subagent-mcp`, `crates/zcode-agentd`, and `crates/zcode-agent-preparation`; distinguish facade validation, daemon validation, runtime Hook policy, and finalization.
- Before attributing behavior to new source, inspect the live process command, LaunchAgent plist, executable modification time, and SHA-256. The LaunchAgent normally uses `npm/native/darwin-arm64/zcode-as-subagentd`; Codex's MCP process may be a separately supervised `npm/native/darwin-arm64/zcode-as-subagent-mcp` child.
- Do not infer request-level evidence from old logs or from a binary whose hash does not match the built target.

## MCP lifecycle evidence

- When the user requires MCP, use the `zcode_as_subagent` MCP tools; do not replace the experiment with the CLI.
- `zcode_subagent_spawn` requires an absolute repository. `plan` is read-only and omits `write_manifest`; `build`, `edit`, and `yolo` require a non-empty repository-relative `write_manifest`.
- Record the returned `agent_id` and `revision`. Poll with `after_revision` set to the previous response's `next_revision`, respecting the bounded timeout. Continue until `task.phase` is terminal and `result_available` is true; inspect pending typed requests before declaring failure.
- Read the terminal result through `zcode_subagent_result`. Report `outcome`, `final_text`, `partial`, and `residual_gaps`; a generic `validation: request validation failed` is facade-level evidence, not proof that ZCode executed.

## Permission and write-manifest boundary

`write_manifest` is enforced by this project, not supplied as an intrinsic ZCode model feature. The preparation layer rejects empty write manifests for workspace-write modes. The daemon passes the normalized list to the child as `ZCODE_AGENT_WRITE_MANIFEST`; the runtime Hook denies out-of-scope writes. If a task is direct-workspace, do not claim a finalization allowlist failure unless the current source actually contains that check: verify the source and active binary first.

## Logs and persistence

- On macOS, inspect `~/Library/Logs/zcode-as-subagent/daemon.log` and `daemon-error.log`; the paths are defined in `cli/paths.mjs` and the LaunchAgent plist.
- Treat these as process stdout/stderr, usually startup diagnostics, not a complete MCP request log. A recent SQLite/WAL timestamp proves persistence activity only; it does not prove a particular MCP call or runtime turn.
- Never expose private stored locators, credentials, raw reasoning, or unredacted task payloads.

## Rebuild and restart

- For Rust rebuilds, follow this repository's rule: run `cargo clean` before `cargo build --release -p zcode-agentd -p zcode-subagent-mcp`.
- Replace the active native binary only when the user authorizes runtime refresh, then verify hashes before and after `launchctl bootout/bootstrap` using the actual user plist at `~/Library/LaunchAgents/com.zcode-as-subagent.daemon.plist`.
- Do not create or retain historical binary backups for this workflow. Preserve the SQLite database and its WAL/SHM companions.
- A daemon restart is independently verifiable with `launchctl print`. The MCP facade is owned by Codex app-server; killing it can close the MCP transport and is not a reliable in-thread restart. Report that limitation and require host-side MCP reload when necessary.

## Report format

End with: active executable paths and hashes, process/LaunchAgent state, relevant log timestamps and messages, MCP calls actually completed, final task result, source-vs-runtime version conclusion, and checks not run. Separate observed facts from inferred causes.

Relevant repository references: `docs/operations.md`, `docs/recovery.md`, `docs/protocol-compatibility.md`, and `tests/live-agent/README.md`.

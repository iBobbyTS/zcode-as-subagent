# zcode-as-subagent setup

## Build

Source builds require Rust 1.97 or compatible and Cargo. The distributed product requires Node.js 20 and macOS.

```text
cargo build --release -p zcode-agentd -p zcode-subagent-mcp
```

## Daemon

Use private absolute database and socket paths outside the target repository:

```text
export ZCODE_AGENTD_STORE=/absolute/private/zcode-agent.sqlite3
export ZCODE_AGENTD_SOCKET=/absolute/private/zcode-agent.sock
./target/release/zcode-as-subagentd
```

The daemon and Store are the sole durable lifecycle owner. The runtime owner keeps child process, stdio, session, turn, stop, and reap authority. `--database`, `--socket`, and `--runtime` are the daemon options. The MCP facade executable is `./target/release/zcode-as-subagent-mcp`.
Hooks are optional. The daemon starts without hook configuration or provenance;
`ZCODE_AGENT_HOOK_PROVENANCE` is only used by explicit hook
installation/checking workflows.
The npm product always uses `/Applications/ZCode.app/Contents/Resources/glm/zcode.cjs`; it does not search PATH or expose a runtime override.

## Codex MCP

Install the MCP entry into `$CODEX_HOME/config.toml`, falling back to
`~/.codex/config.toml`, with:

```text
zas install-mcp
```

The command preserves unrelated configuration and replaces only its managed
`mcp_servers.zcode_as_subagent` tables. Use `install-mcp codex --dry-run` to
inspect the resolved paths without writing. Use `install-mcp --uninstall` to
remove only the managed tables while retaining the Codex config file and all
unrelated settings. The binary has one startup-static catalog:

```text
zcode_subagent_status
zcode_subagent_spawn
zcode_subagent_poll
zcode_subagent_list
zcode_subagent_send
zcode_subagent_respond
zcode_subagent_cancel
zcode_subagent_result
zcode_subagent_close
```

Review is a normal read-only Agent invocation. Put review instructions in `prompt`; no review task type or continuation identity exists.

```json
{
  "repository": "/absolute/repository",
  "prompt": "Review base..HEAD and report concrete findings.",
  "permission_mode": "build",
  "write_manifest": ["src", "tests"]
}
```

`permission_mode` defaults to `build`. For `build`, `edit`, and `yolo`,
`write_manifest` is optional; when omitted the daemon uses the protected
repository workspace as the write scope. A provided list narrows that scope to
repository-relative paths. It is propagated to the runtime Hook through
`ZCODE_AGENT_WRITE_MANIFEST`; writes outside the list are denied during tool
execution. `plan` is read-only and must omit the list. Terminal results expose
 bounded result segments (`offset`/`limit`, default and maximum 262144 UTF-8 bytes) for machine-readable reads. Callers cannot submit
programs, arguments, cwd, shell, or environment.

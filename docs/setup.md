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
./target/release/zcode-agentd
```

The daemon and Store are the sole durable lifecycle owner. The runtime owner keeps child process, stdio, session, turn, stop, and reap authority. `--database`, `--socket`, `--runtime`, and `--command-catalog` are equivalent CLI options.
Hooks are optional. The daemon starts without hook configuration or provenance;
`ZCODE_AGENT_HOOK_PROVENANCE` and `ZCODE_AGENT_SERVICE_GENERATION` are only
used by explicit hook installation/checking workflows.
The npm product always uses `/Applications/ZCode.app/Contents/Resources/glm/zcode.cjs`; it does not search PATH or expose a runtime override.

## Codex MCP

Merge values from `config/codex-zcode-subagent-mcp.toml` into Codex configuration only when explicitly requested. The binary has one startup-static catalog:

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

`permission_mode` defaults to `build`. For write modes, `write_manifest` is a caller-provided list of relative paths inside `repository`; the daemon never expands an omitted list to the whole workspace. Plan mode is read-only. Callers cannot submit programs, arguments, cwd, shell, or environment.

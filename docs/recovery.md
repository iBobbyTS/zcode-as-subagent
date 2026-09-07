# Recovery

## Facade restart

The MCP facade is stateless. Restart it with the same `ZCODE_AGENTD_SOCKET`, then use `zcode_subagent_poll` or `zcode_subagent_result` with the durable `agent_id`. Hook provenance and service generation are optional integration metadata and are not required for daemon startup. Do not confuse a daemon restart with a facade restart or a task identity.

## Daemon restart

Stop the daemon with SIGTERM or SIGINT and wait for its exact socket to disappear. Restart with the same canonical database and socket paths. Startup reconciliation runs before publication. Live runtime reconnect is unsupported; interrupted work becomes runtime-lost or orphaned without signaling an unverified PID or process group.

After restart, call `zcode_subagent_list` with explicit repository, feature, or ownership scope. Inspect tasks with `poll` and `result`, then close them after verifying durable state. Start a new Agent for further work.

## Data and terminal history

For a consistent SQLite backup, stop the sole daemon and preserve the database with any WAL/SHM companions. Terminal history is read through `zcode_subagent_result` and running state through poll. A send to a closed session attempts resume; failure is typed and leaves the original terminal row unchanged. Never read private stored locators directly.

This product intentionally has no compatibility framework or migration for removed unpublished records. Use `cleanup-legacy --yes` only for explicit deletion; it never imports or aliases legacy data.

# Security boundary

`zcode-as-subagentd` owns durable task identity, workspace occupancy, pending requests,
runtime processes, and cleanup. `zcode-as-subagent-mcp` is a
stateless local projection: it validates bounded public inputs, calls the
private Unix RPC, and returns only approved fields. Do not expose the private
socket to untrusted local users; keep its directory and the SQLite database
outside target repositories with owner-only permissions.

The generic facade does not add separate projections for prompts, raw pending
payloads, private correlation/runtime/process identities, or the environment.
It may publish the bounded public reasoning tail and other allowlisted message
projections defined by the observation protocol. Those message values can
themselves contain workspace content, paths, commands, or other text supplied
by an Agent; the facade does not inspect or rewrite that content.

Message content is not redacted by this middle layer. Both Agent endpoints are
responsible for redacting content before they send it. The daemon and facade
only enforce bounded projections and protocol-level exclusions such as
`encrypted_content`; diagnostic reports likewise preserve message values within
their existing byte budgets. Event payloads outside those public projections
are reduced to stable activity categories and counters. Permission responses are
accepted only for typed daemon-published pending requests, and local policy may
override an external allow to deny.

Task listing is never daemon-wide: callers must provide at least one explicit
repository or group scope, which the Store applies before the bound. Stable
public IDs are not authentication tokens; this is a local,
single-user transport without remote or multi-tenant authorization.

The repo-local plugin and sample config expose only the fixed generic catalog
and forward the already configured socket. They do not copy credentials,
download runtimes, edit provider/account configuration, or start a second
daemon/service.

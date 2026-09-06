# Hook integration

The plugin root is the only installation surface. `hooks/hooks.json` registers
the Bash guard, file guard, and metadata-only Bash audit wrapper. The hook
scripts consume the verified record at `ZCODE_AGENT_HOOK_PROVENANCE`. The
daemon does not require hook provenance or caller-supplied generation; service
identity is generated automatically.

## Runtime environment

The daemon injects these values into each ZCode child:

```text
ZCODE_AGENT_POLICY=1
ZCODE_AGENT_WRITE_MANIFEST=["src"]
ZCODE_AGENT_BOOTSTRAP_ROOTS=/Applications/ZCode.app
```

The file guard canonicalizes roots, rejects traversal, outside-root paths,
symlink escapes, credentials, secrets, Git metadata, and `.agent-work/**`.
Write/Edit/Delete/Move require a path inside the serialized write manifest.
Bootstrap roots permit explicit absolute reads only and never grant writes.

## Installation lifecycle

`scripts/install-agent-hooks.mjs` updates only the requested config and writes a
provenance record. Unknown managed matchers are rejected without changing the
config. `scripts/preflight-agent-hooks.mjs` runs a safe read and a destructive
canary through the actual wrapper, then marks the generation active.
`scripts/check-agent-hooks.mjs` verifies every event, policy digest, wrapper,
file policy, and config digest. All three scripts are idempotent and intended
for isolated test configurations.

## Bash policy

The official runtime and caller own Bash permission decisions. The file hook
only enforces the declared workspace boundary; the daemon source hash
and JavaScript source hash are recorded in provenance. Shell composition,
redirection, command substitution, caller environment assignments, unknown
executables, path escapes, secret paths, symlink escapes, and Git ref mutation
are denied by default.

`ZCODE_AGENT_BASH_ROOT` optionally pins the Bash root,
`ZCODE_AGENT_BASH_TRUSTED_BIN_DIRS` controls the fixed executable directories,
and `ZCODE_AGENT_BASH_UNKNOWN_DECISION=ask` is allowed only for unsupported
non-dangerous commands. `ZCODE_PLUGIN_DATA` receives hashes and bounded audit
metadata only; raw command, output, cwd, and credentials are excluded.

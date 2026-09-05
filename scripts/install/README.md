# Installation ownership

The `zcode-as-subagent init` command is the sole owner of product directories,
model catalog provenance, and the LaunchAgent. It never downloads ZCode and it
always targets the fixed application-bundle runtime.

Hook configuration is deliberately outside the default install. Use
`zcode-as-subagent init --install-hooks` to opt in during initialization, or
`zcode-as-subagent hooks install` as a standalone operation.

# Installation ownership

The `zas init` command is the sole owner of product directories,
model catalog provenance, and the LaunchAgent. It never downloads ZCode and it
always targets the fixed application-bundle runtime.

Hook configuration is deliberately outside the default install. Use
`zas init --install-hooks` to opt in during initialization, or
`zas hooks install` as a standalone operation.

# Native release payloads

Release packaging places signed native binaries under the platform/architecture
directory. The supported payloads are `darwin-arm64/zcode-as-subagentd` and
`darwin-arm64/zcode-as-subagent-mcp`. `init`
fails closed before changing user state when its required payload is absent.

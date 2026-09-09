use std::{env, io, path::PathBuf};

#[tokio::main]
async fn main() -> io::Result<()> {
    let socket = env::var_os("ZCODE_AGENTD_SOCKET")
        .map(PathBuf::from)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "ZCODE_AGENTD_SOCKET is required"))?;
    if !socket.is_absolute() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "ZCODE_AGENTD_SOCKET must be absolute"));
    }
    // The installer keeps the stable daemon RPC socket in configuration;
    // MCP is exposed by the sibling `.mcp` stream endpoint.  Resolve that
    // endpoint when it is present while retaining the explicit socket for
    // isolated process tests and non-standard deployments.
    let endpoint = if socket.file_name().and_then(|name| name.to_str()) == Some("zcode-as-subagent.sock") {
        socket.with_extension("mcp")
    } else {
        socket
    };
    zcode_subagent_mcp::serve_stdio(endpoint).await
}

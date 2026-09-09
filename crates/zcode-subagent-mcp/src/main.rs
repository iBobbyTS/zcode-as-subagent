use std::{env, io, path::PathBuf};

#[tokio::main]
async fn main() -> io::Result<()> {
    let socket = env::var_os("ZCODE_AGENTD_SOCKET")
        .map(PathBuf::from)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "ZCODE_AGENTD_SOCKET is required"))?;
    if !socket.is_absolute() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "ZCODE_AGENTD_SOCKET must be absolute"));
    }
    zcode_subagent_mcp::serve_stdio(socket).await
}

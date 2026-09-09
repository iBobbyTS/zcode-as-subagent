//! Stateless stdio bridge for the shared daemon MCP listener.

use std::{io, path::Path};
use tokio::{
    io::{self as tokio_io, AsyncWriteExt},
    net::UnixStream,
};

/// Forward one client stdio session to the daemon MCP stream endpoint.
pub async fn serve_stdio(socket: impl AsRef<Path>) -> io::Result<()> {
    let socket = socket.as_ref();
    let stream = UnixStream::connect(socket).await.map_err(|error| {
        io::Error::new(
            error.kind(),
            format!(
                "failed to connect to daemon MCP socket {}: {error}",
                socket.display()
            ),
        )
    })?;
    let (mut daemon_read, mut daemon_write) = stream.into_split();
    let mut stdin = tokio_io::stdin();
    let mut stdout = tokio_io::stdout();
    let stdin_to_daemon = tokio::spawn(async move {
        let result = tokio_io::copy(&mut stdin, &mut daemon_write).await;
        let _ = daemon_write.shutdown().await;
        result
    });
    let daemon_to_stdout = tokio::spawn(async move { tokio_io::copy(&mut daemon_read, &mut stdout).await });
    tokio::pin!(stdin_to_daemon);
    tokio::pin!(daemon_to_stdout);
    tokio::select! {
        // If both directions finish at once, prefer the client's stdin EOF.
        // A client is allowed to half-close its request stream; this ends the
        // bridge without treating the client's normal EOF as a transport
        // failure. A daemon EOF while stdin is still live is handled below.
        biased;
        result = &mut stdin_to_daemon => {
            result
                .map_err(|error| io::Error::new(io::ErrorKind::Other, format!("stdio forwarding task failed: {error}")))?
                .map_err(|error| io::Error::new(error.kind(), format!("daemon MCP stream write failed: {error}")))?;
            // Client EOF owns this connection's lifetime. Do not wait for a
            // daemon session that may intentionally remain open for others.
            Ok(())
        }
        result = &mut daemon_to_stdout => {
            let result = result
                .map_err(|error| io::Error::new(io::ErrorKind::Other, format!("daemon MCP stream task failed: {error}")))?;
            match result {
                Ok(_) => {
                    // The daemon owns the transport error. Stop the
                    // still-running stdin forwarder so it cannot keep the
                    // bridge process alive while blocked on client input.
                    stdin_to_daemon.abort();
                    Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "daemon MCP stream closed unexpectedly (EOF)",
                    ))
                }
                Err(error) => {
                    stdin_to_daemon.abort();
                    Err(io::Error::new(error.kind(), format!("daemon MCP stream failed: {error}")))
                }
            }
        }
    }
}

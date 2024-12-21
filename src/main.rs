use anyhow::Result;
use simple_redis::{network, Backend};
use tokio::net::TcpListener;
use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let addr = "0.0.0.0:6379";
    info!("Mini Redis is listening on {}", addr);
    let listener = TcpListener::bind(addr).await?;

    let backend = Backend::new();
    loop {
        let (stream, peer_addr) = listener.accept().await?;
        info!("Accepted connection from {}", peer_addr);
        let cloned_backend = backend.clone();
        tokio::spawn(async move {
            match network::stream_handler(stream, cloned_backend).await {
                Ok(_) => info!("Connection from {} closed", peer_addr),
                Err(e) => warn!("Error {} occurs while handle {} connection", e, peer_addr),
            }
        });
    }
}

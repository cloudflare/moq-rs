use clap::Parser;

mod server;
use moq_api::ApiError;
use server::{Server, ServerConfig};

#[tokio::main]
async fn main() -> Result<(), ApiError> {
    // Initialize tracing with env filter (respects RUST_LOG environment variable)
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let config = ServerConfig::parse();
    let server = Server::new(config);
    server.run().await
}

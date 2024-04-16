use clap::Parser;
use redis_starter_rust::{cli::Cli, server, store::Store};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let info = cli.to_info();
    let store = Store::new();
    info.write(&store)?;
    let listener = tokio::net::TcpListener::bind(info.bind_address()).await?;
    server::run(listener, store.clone()).await?;

    Ok(())
}

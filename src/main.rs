use clap::Parser;
use redis_starter_rust::{cli::Cli, info::Info, store::Store};
use tokio::signal;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let info = cli.to_info();
    let store = Store::new();
    info.write(&store)?;
    let port: u16 = cli.port;

    let listener = tokio::net::TcpListener::bind(format!("{}", info.bind_address())).await?;

    //server::run(listener, signal::ctrl_c()).await?;

    Ok(())
}

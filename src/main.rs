use redis_starter_rust::config::Config;
use redis_starter_rust::server::run;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::default();
    run(config).await
}

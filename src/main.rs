use redis_starter_rust::config::Config;
use redis_starter_rust::server::run;
use std::env;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = env::args().collect::<Vec<String>>();

    let config = if args.len() == 3 && args[1] == "--port" {
        Config {
            port: args[2].to_string(),
            ..Default::default()
        }
    } else {
        Config::default()
    };

    run(config).await
}

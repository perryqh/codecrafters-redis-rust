use redis_starter_rust::{
    redis_args::RedisArgs, replica_slave::slave_hand_shake, server::run, store::Store,
};
use std::env;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = RedisArgs::parse(env::args().collect::<Vec<String>>())?;
    let store = Store::new();
    let info = args.to_info();
    info.write(&store)?;

    if info.is_slave() {
        slave_hand_shake(&store).await?;
    }

    run(&store).await
}

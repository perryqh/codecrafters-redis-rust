use tokio::net::TcpListener;

use crate::{
    command::Command, connection::Connection, info::Info, replicator::Replicator, store::Store,
};

pub async fn run(listener: TcpListener, store: Store) -> anyhow::Result<()> {
    let subscriber_store = store.clone();
    setup_subscriber(subscriber_store).await?;

    loop {
        let store = store.clone();
        let (socket, _) = listener.accept().await?;
        let mut handler = Handler {
            store,
            connection: Connection::new(socket),
        };
        tokio::spawn(async move {
            if let Err(err) = handler.run().await {
                eprintln!("connection error: {:?}", err);
            }
        });
    }
}

async fn setup_subscriber(store: Store) -> anyhow::Result<()> {
    let info = Info::from_store(&store)?;
    if info.is_replica() {
        let mut replica = Replicator::new(store, info);
        tokio::spawn(async move {
            if let Err(err) = replica.run().await {
                eprintln!("replication error: {:?}", err);
            }
        });
    }
    Ok(())
}

struct Handler {
    store: Store,
    connection: Connection,
}

impl Handler {
    async fn run(&mut self) -> anyhow::Result<()> {
        while let Some(frame) = self.connection.read_frame().await? {
            let command = Command::from_frame(frame)?;
            command.apply(&self.store, &mut self.connection).await?;
        }
        Ok(())
    }
}

use tokio::net::TcpListener;

use crate::{
    command::Command, connection::Connection, info::Info, publisher, replicator::Replicator,
    store::Store,
};

pub async fn run(listener: TcpListener, store: Store) -> anyhow::Result<()> {
    let subscriber_store = store.clone();
    setup_subscriber(subscriber_store).await?;

    loop {
        let store = store.clone();
        let (socket, _) = listener.accept().await?;
        let mut handler = Handler {};
        tokio::spawn(async move {
            if let Err(err) = handler.run(store, Connection::new(socket)).await {
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

struct Handler {}

impl Handler {
    async fn run(&mut self, store: Store, mut connection: Connection) -> anyhow::Result<()> {
        let mut subscriber = false;
        while let Some(frame) = connection.read_frame().await? {
            let command = Command::from_frame(frame)?;
            match &command {
                Command::Psync(_) => {
                    subscriber = true;
                }
                _ => {}
            }
            command.apply(&store, &mut connection).await?;
            if subscriber {
                let _ = publisher::add_connection(connection, &store).await;
                // TODO: for some reason, if we attempt to read another frame, the replicant errors out
                // specifically: `0 == self.stream.read_buf(&mut self.buffer).await?`
                break; 
            }
        }
        Ok(())
    }
}

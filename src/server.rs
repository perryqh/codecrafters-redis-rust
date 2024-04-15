use tokio::net::TcpListener;

use crate::{command::Command, connection::Connection, store::Store};

pub async fn run(listener: TcpListener, store: Store) -> anyhow::Result<()> {
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

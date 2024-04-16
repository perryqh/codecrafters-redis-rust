use anyhow::ensure;
use bytes::Bytes;

use crate::{connection::Connection, frame::Frame, info::Info, store::Store};

pub struct Replicator {
    store: Store,
    info: Info,
}

impl Replicator {
    pub fn new(store: Store, info: Info) -> Self {
        Self { store, info }
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        let master_address = self.info.replication.master_address()?;
        let stream = tokio::net::TcpStream::connect(master_address).await?;
        let mut master_connection = Connection::new(stream);

        hand_shake(
            &mut master_connection,
            &ping_fame(),
            Frame::Simple("PONG".into()),
        )
        .await?;

        hand_shake(
            &mut master_connection,
            &listening_port_frame(&self.info),
            Frame::Simple("OK".into()),
        )
        .await?;

        hand_shake(
            &mut master_connection,
            &capability_bytes(),
            Frame::Simple("OK".into()),
        )
        .await?;

        hand_shake(
            &mut master_connection,
            &psync_bytes().await,
            Frame::Simple("OK".into()),
        )
        .await?;

        // loop {
        //     if let Some(frame) = master_connection.read_frame().await? {
        //         let command = crate::command::Command::from_frame(frame)?;
        //         command.apply(&self.store, &mut master_connection).await?;
        //     }
        // }

        Ok(())
    }
}

async fn hand_shake(
    connection: &mut Connection,
    command: &Frame,
    expected_response: Frame,
) -> anyhow::Result<()> {
    connection.write_frame(command).await?;
    match connection.read_frame().await? {
        Some(response) => {
            eprintln!("response: {:?}", response);
            eprintln!("expected_response: {:?}", expected_response);
            ensure!(
                response == expected_response,
                "replicator received invalid response. Expected: {:?}, got: {:?}",
                expected_response,
                response
            )
        }
        None => anyhow::bail!(
            "connection reset by peer. Response frame not received for command: {:?}",
            command
        ),
    }

    Ok(())
}

fn ping_fame() -> Frame {
    let mut array = Frame::array();
    array.push_bulk(Bytes::from("PING"));

    array
}

fn listening_port_frame(info: &Info) -> Frame {
    let port = info.self_port;

    let mut array = Frame::array();
    array.push_bulk(Bytes::from("REPLCONF"));
    array.push_bulk(Bytes::from("listening-port"));
    array.push_bulk(Bytes::from(port.to_string()));
    array
}

fn capability_bytes() -> Frame {
    let mut array = Frame::array();
    array.push_bulk(Bytes::from("REPLCONF"));
    array.push_bulk(Bytes::from("capa"));
    array.push_bulk(Bytes::from("psync2"));
    array
}

async fn psync_bytes() -> Frame {
    let mut array = Frame::array();
    array.push_bulk(Bytes::from("PSYNC"));
    array.push_bulk(Bytes::from("?"));
    array.push_bulk(Bytes::from("-1"));
    array
}

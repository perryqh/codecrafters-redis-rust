use anyhow::{ensure, Context};
use bytes::Bytes;

use crate::{comms::Comms, connection::Connection, frame::Frame, info::Info, store::Store};

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
        let socket = tokio::net::TcpStream::connect(master_address).await?;
        let (reader, writer) = socket.into_split();
        let mut master_connection = Connection::new(reader, writer, true);

        hand_shake(
            &mut master_connection,
            &ping_fame()?,
            Frame::Simple("PONG".into()),
        )
        .await?;

        hand_shake(
            &mut master_connection,
            &listening_port_frame(&self.info)?,
            Frame::Simple("OK".into()),
        )
        .await?;

        hand_shake(
            &mut master_connection,
            &capability_bytes()?,
            Frame::Simple("OK".into()),
        )
        .await?;

        master_connection.write_frame(&psync_bytes().await?).await?;

        match master_connection.read_frame().await? {
            Some(Frame::Simple(response)) => {
                // TODO: do something with response
            }
            _ => anyhow::bail!("replicator received invalid response"),
        }

        loop {
            if let Some(frame) = master_connection.read_frame().await? {
                match &frame {
                    Frame::Array(_) => {
                        let command = crate::command::Command::from_frame(frame)
                            .context("expecting update replica commands")?;
                        command.apply(&self.store, &mut master_connection).await?;
                    }
                    _ => {
                        eprintln!("dropping rdb file {:?}", frame);
                    }
                }
            }
        }

        Ok(())
    }
}

async fn hand_shake<C: Comms>(
    comms: &mut C,
    command: &Frame,
    expected_response: Frame,
) -> anyhow::Result<()> {
    comms.write_frame(command).await?;
    match comms.read_frame().await? {
        Some(response) => {
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

fn ping_fame() -> anyhow::Result<Frame> {
    let mut array = Frame::array();
    array.push_bulk(Bytes::from("PING"))?;

    Ok(array)
}

fn listening_port_frame(info: &Info) -> anyhow::Result<Frame> {
    let port = info.self_port;

    let mut array = Frame::array();
    array.push_bulk(Bytes::from("REPLCONF"))?;
    array.push_bulk(Bytes::from("listening-port"))?;
    array.push_bulk(Bytes::from(port.to_string()))?;
    Ok(array)
}

fn capability_bytes() -> anyhow::Result<Frame> {
    let mut array = Frame::array();
    array.push_bulk(Bytes::from("REPLCONF"))?;
    array.push_bulk(Bytes::from("capa"))?;
    array.push_bulk(Bytes::from("psync2"))?;
    Ok(array)
}

async fn psync_bytes() -> anyhow::Result<Frame> {
    let mut array = Frame::array();
    array.push_bulk(Bytes::from("PSYNC"))?;
    array.push_bulk(Bytes::from("?"))?;
    array.push_bulk(Bytes::from("-1"))?;
    Ok(array)
}

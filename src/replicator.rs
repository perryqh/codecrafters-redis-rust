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
        let comms = Connection::new(reader, writer, true);

        self.run_replication(comms).await
    }

    async fn run_replication<C: Comms>(&mut self, mut comms: C) -> anyhow::Result<()> {
        hand_shake(&mut comms, &ping_fame()?, Frame::Simple("PONG".into())).await?;

        hand_shake(
            &mut comms,
            &listening_port_frame(&self.info)?,
            Frame::Simple("OK".into()),
        )
        .await?;

        hand_shake(&mut comms, &capability_bytes()?, Frame::Simple("OK".into())).await?;

        comms.write_frame(&psync_bytes().await?).await?;

        match comms.read_frame().await? {
            Some(Frame::Simple(response)) => {
                // TODO: do something with response
            }
            _ => anyhow::bail!("replicator received invalid response"),
        }

        loop {
            if let Some(frame) = comms.read_frame().await? {
                match &frame {
                    Frame::Array(_) => {
                        let command = crate::command::Command::from_frame(frame)
                            .context("expecting update replica commands")?;
                        command.apply(&self.store, &mut comms).await?;
                    }
                    _ => {
                        eprintln!("dropping rdb file {:?}", frame);
                    }
                }
            }
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    //#[tokio::test]
    // TODO: add shutdown support
    async fn test_run_replication() -> anyhow::Result<()> {
        let mut replicator = Replicator::new(Store::new(), Info::default());

        let reader = tokio_test::io::Builder::new()
            .read(b"+PONG\r\n")
            .read(b"+OK\r\n")
            .read(b"+OK\r\n")
            .build();
        let writer = tokio_test::io::Builder::new()
            .write(b"*1\r\n$4\r\nPING\r\n")
            .write(b"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6379\r\n")
            .write(b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n")
            .write(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
            .read(b"+yup\r\n")
            .build();

        let connection = Connection::new(reader, writer, true);

        replicator.run_replication(connection).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_ping_fame() -> anyhow::Result<()> {
        let frame = ping_fame()?;
        assert_eq!(frame.to_string(), "PING");

        Ok(())
    }

    #[tokio::test]
    async fn test_listening_port_frame() -> anyhow::Result<()> {
        let info = Info {
            self_port: 1234,
            ..Default::default()
        };

        let frame = listening_port_frame(&info)?;
        assert_eq!(frame.to_string(), "REPLCONF listening-port 1234");

        Ok(())
    }

    #[tokio::test]
    async fn test_capability_bytes() -> anyhow::Result<()> {
        let frame = capability_bytes()?;
        assert_eq!(frame.to_string(), "REPLCONF capa psync2");

        Ok(())
    }

    #[tokio::test]
    async fn test_psync_bytes() -> anyhow::Result<()> {
        let frame = psync_bytes().await?;
        assert_eq!(frame.to_string(), "PSYNC ? -1");

        Ok(())
    }
}

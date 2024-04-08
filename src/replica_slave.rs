use anyhow::ensure;
use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::{
    info::Info,
    resp_lexer::{RESPArray, RESPBulkString, RESPValue, Serialize},
    store::Store,
};

pub async fn slave_hand_shake(store: &Store) -> anyhow::Result<()> {
    let info = Info::from_store(store)?;
    let master_address = info.replication.master_address()?;
    let mut stream = tokio::net::TcpStream::connect(master_address).await?;

    let (mut reader, mut writer) = stream.split();

    let ping_bytes = ping_bytes().await?;
    writer.write_all(&ping_bytes).await?;
    writer.flush().await?;

    let mut buf = [0; 512];
    let byte_count = reader.read(&mut buf).await?;
    ensure!(byte_count > 0, "No data received from master");
    println!("Received: {:?}", &buf[..byte_count]);

    let listening_port_bytes = listening_port_bytes(&info).await?;
    writer.write_all(&listening_port_bytes).await?;
    writer.flush().await?;

    buf.fill(0);
    let byte_count = reader.read(&mut buf).await?;
    ensure!(byte_count > 0, "No data received from master");
    println!("Received: {:?}", &buf[..byte_count]);

    let cap_bytes = capability_bytes().await?;
    writer.write_all(&cap_bytes).await?;
    writer.flush().await?;

    buf.fill(0);
    let byte_count = reader.read(&mut buf).await?;
    ensure!(byte_count > 0, "No data received from master");
    println!("Received: {:?}", &buf[..byte_count]);

    let psync_bytes = psync_bytes().await?;
    writer.write_all(&psync_bytes).await?;
    writer.flush().await?;

    buf.fill(0);
    let byte_count = reader.read(&mut buf).await?;
    ensure!(byte_count > 0, "No data received from master");
    println!("Received: {:?}", &buf[..byte_count]);

    Ok(())
}
async fn ping_bytes() -> anyhow::Result<Bytes> {
    let bytes = RESPArray {
        data: vec![RESPValue::BulkString(RESPBulkString::new(Bytes::from(
            "PING",
        )))],
    }
    .serialize();
    Ok(bytes)
}

async fn listening_port_bytes(info: &Info) -> anyhow::Result<Bytes> {
    let port = info.self_port;
    let bytes = RESPArray {
        data: vec![
            RESPValue::BulkString(RESPBulkString::new(Bytes::from("REPLCONF"))),
            RESPValue::BulkString(RESPBulkString::new(Bytes::from("listening-port"))),
            RESPValue::BulkString(RESPBulkString::new(Bytes::from(port.to_string()))),
        ],
    }
    .serialize();
    Ok(bytes)
}

async fn capability_bytes() -> anyhow::Result<Bytes> {
    let bytes = RESPArray {
        data: vec![
            RESPValue::BulkString(RESPBulkString::new(Bytes::from("REPLCONF"))),
            RESPValue::BulkString(RESPBulkString::new(Bytes::from("capa"))),
            RESPValue::BulkString(RESPBulkString::new(Bytes::from("psync2"))),
        ],
    }
    .serialize();
    Ok(bytes)
}

async fn psync_bytes() -> anyhow::Result<Bytes> {
    let bytes = RESPArray {
        data: vec![
            RESPValue::BulkString(RESPBulkString::new(Bytes::from("PSYNC"))),
            RESPValue::BulkString(RESPBulkString::new(Bytes::from("?"))),
            RESPValue::BulkString(RESPBulkString::new(Bytes::from("-1"))),
        ],
    }
    .serialize();
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_ping_bytes() -> anyhow::Result<()> {
        let bytes = ping_bytes().await?;
        assert_eq!(bytes, Bytes::from("*1\r\n$4\r\nPING\r\n"));
        Ok(())
    }

    #[tokio::test]
    async fn test_listening_port_bytes() -> anyhow::Result<()> {
        let info = Info {
            self_port: 6380,
            ..Default::default()
        };
        let expected_bytes =
            Bytes::from("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n");
        let bytes = listening_port_bytes(&info).await?;
        assert_eq!(bytes, expected_bytes);
        Ok(())
    }

    #[tokio::test]
    async fn test_capability_bytes() -> anyhow::Result<()> {
        let expected_bytes = Bytes::from("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n");
        let bytes = capability_bytes().await?;
        assert_eq!(bytes, expected_bytes);
        Ok(())
    }

    #[tokio::test]
    async fn test_psync_bytes() -> anyhow::Result<()> {
        let expected_bytes = Bytes::from("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n");
        let bytes = psync_bytes().await?;
        assert_eq!(bytes, expected_bytes);
        Ok(())
    }

    // #[tokio::test]
    // async fn test_ping() {
    //     let reader = tokio_test::io::Builder::new().read(b"+PONG\r\n").build();
    //     let writer = tokio_test::io::Builder::new()
    //         .write(b"*1\r\n$4\r\nPING\r\n")
    //         .build();
    //     let _ = ping(reader, writer).await;
    // }
}

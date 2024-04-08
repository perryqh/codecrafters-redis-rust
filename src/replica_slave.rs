use anyhow::ensure;
use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::{
    resp_lexer::{RESPArray, RESPBulkString, RESPValue, Serialize},
    store::Store,
};

pub async fn slave_hand_shake(store: &Store) -> anyhow::Result<()> {
    let info = crate::info::Info::from_store(store)?;
    let master_address = info.replication.master_address()?;
    let mut stream = tokio::net::TcpStream::connect(master_address).await?;

    let (reader, writer) = stream.split();
    ping(reader, writer).await
}

async fn ping<Reader, Writer>(mut reader: Reader, mut writer: Writer) -> anyhow::Result<()>
where
    Reader: AsyncReadExt + Unpin,
    Writer: AsyncWriteExt + Unpin,
{
    let ping_bytes = ping_bytes().await?;
    writer.write_all(&ping_bytes).await?;

    let mut buf = [0; 512];
    let byte_count = reader.read(&mut buf).await?;
    ensure!(byte_count > 0, "No data received from master");
    let bytes = &buf[..byte_count];
    let mut lexer = crate::resp_lexer::Lexer::new(bytes.into());
    let pong = lexer.lex()?;
    println!("Received value: {:?}", &pong);

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
    async fn test_ping() {
        let reader = tokio_test::io::Builder::new().read(b"+PONG\r\n").build();
        let writer = tokio_test::io::Builder::new()
            .write(b"*1\r\n$4\r\nPING\r\n")
            .build();
        let _ = ping(reader, writer).await;
    }
}

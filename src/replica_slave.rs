use anyhow::ensure;
use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::{
    commands::parse_command,
    resp_lexer::{RESPArray, RESPBulkString, RESPValue, Serialize},
    store::Store,
};

pub async fn slave_hand_shake(store: &Store) -> anyhow::Result<()> {
    let info = crate::info::Info::from_store(store)?;
    let master_address = info.replication.master_address()?;
    let mut stream = tokio::net::TcpStream::connect(master_address).await?;

    let (reader, writer) = stream.split();
    ping(reader, writer, store).await
}

async fn ping<Reader, Writer>(
    mut reader: Reader,
    mut writer: Writer,
    store: &Store,
) -> anyhow::Result<()>
where
    Reader: AsyncReadExt + Unpin,
    Writer: AsyncWriteExt + Unpin,
{
    let ping_bytes = ping_bytes().await?;
    writer.write_all(&ping_bytes).await?;

    let mut buf = [0; 512];
    let byte_count = reader.read(&mut buf).await?;
    ensure!(byte_count > 0, "No data received from master");
    let command = parse_command(&buf[..byte_count], store.clone())?;
    println!("Received command: {:?}", &command);
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

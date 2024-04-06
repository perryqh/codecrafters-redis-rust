use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::commands::parse_command;
use crate::info::Info;
use crate::store::Store;

async fn process_socket(mut socket: TcpStream, store: Store) -> anyhow::Result<()> {
    let mut buf = [0; 512];
    while let Ok(byte_count) = socket.read(&mut buf).await {
        if byte_count == 0 {
            break;
        }
        let command = parse_command(&buf[..byte_count], store.clone())?;
        socket.write_all(&command.response_bytes()?).await?;
    }

    Ok(())
}

pub async fn run(store: &Store) -> anyhow::Result<()> {
    let info = Info::from_store(store)?;

    println!("Listening on {}", &info.bind_address());
    let listener = TcpListener::bind(&info.bind_address()).await?;
    loop {
        let store = store.clone();
        let (socket, _) = listener.accept().await?;
        tokio::spawn(async move {
            if let Err(err) = process_socket(socket, store).await {
                eprintln!("Error processing socket: {:?}", err);
            }
        });
    }
}

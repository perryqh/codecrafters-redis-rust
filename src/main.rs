// Uncomment this block to pass the first stage
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

async fn process_socket(mut socket: TcpStream) -> anyhow::Result<()> {
    let mut buf = [0; 512];
    while let Ok(byte_count) = socket.read(&mut buf).await {
        if byte_count == 0 {
            break;
        }
        socket.write_all(b"+PONG\r\n").await?;
    }

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        let (socket, _) = listener.accept().await?;
        process_socket(socket).await?;
    }
}

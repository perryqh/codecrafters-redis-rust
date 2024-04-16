use redis_starter_rust::{array_of_bulks, server};
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

async fn start_server() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let store = redis_starter_rust::store::Store::new();

    tokio::spawn(async move { server::run(listener, store).await });

    addr
}

#[tokio::test]
async fn send_error_unknown_command() {
    let addr = start_server().await;

    let mut stream = TcpStream::connect(addr).await.unwrap();

    stream
        .write_all(array_of_bulks!("FOO", "hello"))
        .await
        .unwrap();

    let mut response = [0; 28];

    stream.read_exact(&mut response).await.unwrap();

    assert_eq!(b"-ERR unknown command \'foo\'\r\n", &response);
}

#[tokio::test]
async fn send_ping_command() {
    let addr = start_server().await;

    let mut stream = TcpStream::connect(addr).await.unwrap();

    stream.write_all(array_of_bulks!("PING")).await.unwrap();

    let mut response = [0; 7];

    stream.read_exact(&mut response).await.unwrap();

    assert_eq!(b"+PONG\r\n", &response);
}

#[tokio::test]
async fn send_two_ping_commands() {
    let addr = start_server().await;

    let mut stream = TcpStream::connect(addr).await.unwrap();

    stream.write_all(array_of_bulks!("PING")).await.unwrap();

    let mut response = [0; 7];

    stream.read_exact(&mut response).await.unwrap();

    assert_eq!(b"+PONG\r\n", &response);

    stream.write_all(array_of_bulks!("PING")).await.unwrap();

    let mut response = [0; 7];

    stream.read_exact(&mut response).await.unwrap();

    assert_eq!(b"+PONG\r\n", &response);
}

#[tokio::test]
async fn echo() -> anyhow::Result<()> {
    let addr = start_server().await;

    // Establish a connection to the server
    let mut stream = TcpStream::connect(addr).await.unwrap();

    // Send a PING command
    stream
        .write_all(array_of_bulks!("echo", "hello"))
        .await
        .unwrap();

    let mut response = [0; 11];

    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(b"$5\r\nhello\r\n", &response);

    Ok(())
}

#[tokio::test]
async fn set_get() -> anyhow::Result<()> {
    let addr = start_server().await;

    let mut stream = TcpStream::connect(addr).await.unwrap();

    stream
        .write_all(array_of_bulks!("set", "hello", "world"))
        .await
        .unwrap();

    let mut buffer = [0; 5];

    stream.read_exact(&mut buffer).await.unwrap();

    assert_eq!(b"+OK\r\n", &buffer.as_slice());

    stream
        .write_all(array_of_bulks!("get", "hello"))
        .await
        .unwrap();

    let mut response = [0; 11];

    stream.read_exact(&mut response).await.unwrap();

    assert_eq!(b"$5\r\nworld\r\n", &response);

    Ok(())
}

#[tokio::test]
async fn get_not_found() -> anyhow::Result<()> {
    let addr = start_server().await;

    let mut stream = TcpStream::connect(addr).await.unwrap();

    stream
        .write_all(array_of_bulks!("get", "hello"))
        .await
        .unwrap();

    let mut response = [0; 5];

    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(b"$-1\r\n", &response);

    Ok(())
}

#[tokio::test]
async fn set_expired() -> anyhow::Result<()> {
    let addr = start_server().await;

    let mut stream = TcpStream::connect(addr).await.unwrap();

    stream
        .write_all(array_of_bulks!("set", "hello", "world", "PX", "1"))
        .await
        .unwrap();

    let mut response = [0; 5];

    stream.read_exact(&mut response).await.unwrap();

    assert_eq!(b"+OK\r\n", &response);

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    stream
        .write_all(array_of_bulks!("get", "hello"))
        .await
        .unwrap();

    let mut response = [0; 5];

    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(b"$-1\r\n", &response);

    Ok(())
}

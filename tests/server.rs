use redis_starter_rust::array_of_bulks;
use redis_starter_rust::info::DEFAULT_MASTER_REPLID;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
mod common;
use common::start_server;

#[tokio::test]
async fn send_error_unknown_command() {
    let (addr, _store) = start_server().await;

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
    let (addr, _store) = start_server().await;

    let mut stream = TcpStream::connect(addr).await.unwrap();

    stream.write_all(array_of_bulks!("PING")).await.unwrap();

    let mut response = [0; 7];

    stream.read_exact(&mut response).await.unwrap();

    assert_eq!(b"+PONG\r\n", &response);
}

#[tokio::test]
async fn send_two_ping_commands() {
    let (addr, _store) = start_server().await;

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
    let (addr, _store) = start_server().await;

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
    let (addr, _store) = start_server().await;

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
    let (addr, _store) = start_server().await;

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
    let (addr, _store) = start_server().await;

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

#[tokio::test]
async fn info() -> anyhow::Result<()> {
    let (addr, _store) = start_server().await;

    let mut stream = TcpStream::connect(addr).await.unwrap();

    stream
        .write_all(array_of_bulks!("info", "replication"))
        .await
        .unwrap();

    let mut response = [0; 94];

    stream.read_exact(&mut response).await.unwrap();
    let expected = format!(
        "$91\r\nrole:master\r\nmaster_replid:{}\r\nmaster_repl_offset:0",
        DEFAULT_MASTER_REPLID
    );

    assert_eq!(expected.as_bytes(), &response);

    Ok(())
}

#[tokio::test]
async fn repl_conf_listening_port() -> anyhow::Result<()> {
    let (addr, store) = start_server().await;

    let mut stream = TcpStream::connect(addr).await.unwrap();

    stream
        .write_all(array_of_bulks!("REPLCONF", "listening-port", "6380"))
        .await
        .unwrap();

    let mut response = [0; 5];

    stream.read_exact(&mut response).await.unwrap();

    assert_eq!(b"+OK\r\n", &response);

    Ok(())
}

#[tokio::test]
async fn repl_conf_capabilities() -> anyhow::Result<()> {
    let (addr, _store) = start_server().await;

    let mut stream = TcpStream::connect(addr).await.unwrap();

    stream
        .write_all(array_of_bulks!("REPLCONF", "capa", "eof", "capa", "psync2"))
        .await
        .unwrap();

    let mut response = [0; 5];

    stream.read_exact(&mut response).await.unwrap();

    assert_eq!(b"+OK\r\n", &response);

    Ok(())
}

#[tokio::test]
async fn test_psync() -> anyhow::Result<()> {
    let (addr, _store) = start_server().await;

    let mut stream = TcpStream::connect(addr).await.unwrap();

    stream
        .write_all(array_of_bulks!("PSYNC", "?", "-1"))
        .await
        .unwrap();

    let expected = format!("+FULLRESYNC {} {}\r\n", DEFAULT_MASTER_REPLID, 0);

    let mut response = [0; 56];

    stream.read_exact(&mut response).await.unwrap();
    let response_str = String::from_utf8(response.to_vec()).unwrap();
    assert_eq!(expected, response_str);
    Ok(())
}

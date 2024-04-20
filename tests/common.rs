use redis_starter_rust::server;
use redis_starter_rust::store::Store;
use std::net::SocketAddr;
use tokio::net::TcpListener;

pub const TEST_SERVER_HOST: &str = "127.0.0.1";
pub const TEST_SERVER_PORT: u16 = 0;

pub async fn start_server() -> (SocketAddr, Store) {
    let listener = TcpListener::bind(format!("{}:{}", TEST_SERVER_HOST, TEST_SERVER_PORT))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    let store = redis_starter_rust::store::Store::new();
    let return_store = store.clone();

    tokio::spawn(async move { server::run(listener, store.clone()).await });

    (addr, return_store)
}

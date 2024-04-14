use tokio::net::TcpListener;

#[derive(Debug)]
struct Listener {
    listener: TcpListener,
}

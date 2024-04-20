use tokio::io;

use crate::frame::Frame;

#[async_trait::async_trait]
pub trait Comms: Send + Sync {
    async fn write_frame(&mut self, frame: &Frame) -> io::Result<()>;
    async fn read_frame(&mut self) -> anyhow::Result<Option<Frame>>;
    fn is_follower_receiving_sync_request(&self) -> bool;
}

use bytes::Bytes;

use crate::{connection::Connection, frame::Frame, parse::Parse};

#[derive(Debug, Default)]
pub struct Echo {
    msg: Bytes,
}

impl Echo {
    pub fn new(msg: Bytes) -> Self {
        Self { msg }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> anyhow::Result<Echo> {
        let msg = parse.next_string()?;
        Ok(Echo::new(msg.into()))
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> anyhow::Result<()> {
        let response = Frame::Bulk(self.msg.clone());

        dst.write_frame(&response).await?;

        Ok(())
    }
}

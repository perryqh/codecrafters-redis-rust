use bytes::Bytes;

use crate::{connection::Connection, frame::Frame, parse::Parse, store::Store};

#[derive(Debug, Default)]
pub struct Get {
    key: Bytes,
}

impl Get {
    pub fn new(key: Bytes) -> Self {
        Self { key }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> anyhow::Result<Get> {
        let msg = parse.next_string()?;
        Ok(Get::new(msg.into()))
    }

    pub(crate) async fn apply(self, dst: &mut Connection, store: &Store) -> anyhow::Result<()> {
        let value = store.get(self.key);
        match value {
            Some(value) => {
                let response = Frame::Bulk(value);
                dst.write_frame(&response).await.map_err(|e| e.into())
            }
            None => {
                let response = Frame::Null;
                dst.write_frame(&response).await.map_err(|e| e.into())
            }
        }
    }
}

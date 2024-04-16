use std::time::Duration;

use bytes::Bytes;

use crate::{
    connection::Connection,
    frame::Frame,
    parse::Parse,
    store::{Store, DEFAULT_EXPIRY},
};

#[derive(Debug, Default)]
pub struct Set {
    key: Bytes,
    value: Bytes,
    expiry: Option<u64>,
}

impl Set {
    pub fn new(key: Bytes, value: Bytes, expiry: Option<u64>) -> Self {
        Self { key, value, expiry }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> anyhow::Result<Set> {
        let key = parse.next_string()?;
        let value = parse.next_string()?;
        let mut expiry = None;

        match parse.next_string() {
            Ok(s) if s == "PX" => {
                if s == "PX" {
                    expiry = Some(parse.next_int()?);
                }
            }
            _ => {}
        }

        Ok(Set::new(key.into(), value.into(), expiry))
    }

    pub(crate) async fn apply(self, dst: &mut Connection, store: &Store) -> anyhow::Result<()> {
        let ttl = self.expiry.unwrap_or(DEFAULT_EXPIRY);
        store.set(self.key, self.value, Duration::from_millis(ttl));

        let response = Frame::OK;
        dst.write_frame(&response).await.map_err(|e| e.into())
    }
}

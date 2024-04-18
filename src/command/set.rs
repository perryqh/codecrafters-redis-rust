use std::time::Duration;

use bytes::Bytes;

use crate::{
    connection::Connection,
    frame::Frame,
    parse::Parse,
    publisher::{publish, Action},
    store::{Store, DEFAULT_EXPIRY},
};

#[derive(Debug, Default, Clone, PartialEq)]
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
            Ok(s) if s.to_uppercase() == "PX" => {
                expiry = Some(parse.next_int()?);
            }
            _ => {}
        }

        Ok(Set::new(key.into(), value.into(), expiry))
    }

    pub(crate) async fn apply(
        self,
        dst: &mut Connection,
        store: &Store,
        respond: bool,
    ) -> anyhow::Result<()> {
        let ttl = self.expiry.unwrap_or(DEFAULT_EXPIRY);
        let cloned_self = self.clone();

        store.set(self.key, self.value, Duration::from_millis(ttl));

        let action = Action::Set {
            key: cloned_self.key,
            value: cloned_self.value,
            expiry: cloned_self.expiry,
        };
        publish(action).await?;

        if respond {
            let response = Frame::OK;
            dst.write_frame(&response).await.map_err(|e| e.into())
        } else {
            Ok(())
        }
    }
}

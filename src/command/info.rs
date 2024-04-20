use anyhow::bail;
use bytes::Bytes;

use crate::{comms::Comms, frame::Frame, parse::Parse, store::Store};

#[derive(Debug, Default)]
pub struct Info {
    kind: Bytes,
}

impl Info {
    pub fn new(kind: Bytes) -> Self {
        Self { kind }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> anyhow::Result<Info> {
        let kind = parse.next_string()?;
        Ok(Info::new(kind.into()))
    }

    pub(crate) async fn apply<C: Comms>(self, comms: &mut C, store: &Store) -> anyhow::Result<()> {
        let info = crate::info::Info::from_store(&store)?;

        let bulk_string = match info.replication.role.as_str() {
            "master" => {
                format!(
                    "role:master\r\nmaster_replid:{}\r\nmaster_repl_offset:{}\r\n",
                    info.replication
                        .master_replid
                        .as_ref()
                        .unwrap_or(&"".to_string()),
                    info.replication.master_repl_offset.as_ref().unwrap_or(&0)
                )
            }
            "slave" => "role:slave".to_string(),
            _ => bail!("Invalid role"),
        };
        let response = Frame::Bulk(bulk_string.into());
        comms.write_frame(&response).await.map_err(|e| e.into())
    }
}

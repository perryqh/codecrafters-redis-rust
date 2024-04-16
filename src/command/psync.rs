use anyhow::bail;

use crate::{connection::Connection, frame::Frame, parse::Parse, store::Store};

#[derive(Debug, Default)]
pub struct Psync {
    /// Comes from the follower. It must be sending the master_replid so we can confirm it's us
    master_replid: String,
    master_repl_offset: Option<i64>,
}

impl Psync {
    pub(crate) fn parse_frames(parse: &mut Parse) -> anyhow::Result<Psync> {
        let master_replid = parse.next_string()?;
        // -1 initially
        let _master_repl_offset = parse.next_string()?;

        Ok(Psync {
            master_replid,
            master_repl_offset: None,
        })
    }

    pub(crate) async fn apply(self, dst: &mut Connection, store: &Store) -> anyhow::Result<()> {
        let info = crate::info::Info::from_store(&store)?;
        let response = match info.replication.role.as_str() {
            "master" => Frame::Simple(format!(
                "FULLRESYNC {} 0",
                info.replication.master_replid.unwrap_or_default()
            )),
            _ => Frame::Error("Not a master server".to_string()),
        };

        dst.write_frame(&response).await.map_err(|e| e.into())
    }
}
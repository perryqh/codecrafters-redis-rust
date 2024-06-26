use crate::{comms::Comms, frame::Frame, parse::Parse, store::Store};

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

    pub(crate) async fn apply<C: Comms>(self, comms: &mut C, store: &Store) -> anyhow::Result<()> {
        let info = crate::info::Info::from_store(store)?;

        if info.is_replica() {
            let error = Frame::Error("Not a master server".to_string());
            comms
                .write_frame(&error)
                .await
                .map_err(anyhow::Error::from)?;
            return Ok(());
        }

        let response = Frame::Simple(format!(
            "FULLRESYNC {} 0",
            info.replication.master_replid.unwrap_or_default()
        ));

        comms
            .write_frame(&response)
            .await
            .map_err(anyhow::Error::from)?;

        Ok(())
    }
}

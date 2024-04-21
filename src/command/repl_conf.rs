use anyhow::bail;

use crate::{array_of_bulks, comms::Comms, frame::Frame, parse::Parse, store::Store};

#[derive(Debug, Default)]
pub struct ReplConf {
    /// The replication server listening port
    listening_port: Option<u16>,
    capabilities: Vec<String>,
    getack_option: Option<String>,
}

impl ReplConf {
    pub(crate) fn parse_frames(parse: &mut Parse) -> anyhow::Result<ReplConf> {
        let mut listening_port = None;
        let mut capabilities = vec![];
        let mut getack_option = None;

        while let Ok(arg) = parse.next_string() {
            match arg.to_lowercase().as_str() {
                "listening-port" => {
                    let port = parse
                        .next_string()
                        .map_err(|_| anyhow::anyhow!("expecting port"))?;
                    listening_port = Some(port.parse()?);
                }
                "capa" => {
                    let cap = parse
                        .next_string()
                        .map_err(|_| anyhow::anyhow!("expecting cap"))?;
                    capabilities.push(cap);
                }
                "getack" => {
                    let getack = parse
                        .next_string()
                        .map_err(|_| anyhow::anyhow!("expecting getack option"))?;
                    getack_option = Some(getack);
                }
                _ => bail!("expecting listening-port or cap, but got {:?}", arg),
            }
        }

        Ok(ReplConf {
            listening_port,
            capabilities,
            getack_option,
        })
    }

    pub(crate) async fn apply<C: Comms>(self, comms: &mut C, _store: &Store) -> anyhow::Result<()> {
        match self.getack_option {
            Some(_) => {
                let response = Frame::Array(vec![
                    Frame::Bulk("REPLCONF".into()),
                    Frame::Bulk("ACK".into()),
                    Frame::Bulk("0".into()),
                ]);
                comms.write_frame(&response).await.map_err(|e| e.into())
            }
            None => comms.write_frame(&Frame::OK).await.map_err(|e| e.into()),
        }
    }
}

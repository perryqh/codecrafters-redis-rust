use anyhow::bail;

use crate::{connection::Connection, frame::Frame, parse::Parse, store::Store};

#[derive(Debug, Default)]
pub struct ReplConf {
    /// The replication server listening port
    listening_port: Option<u16>,
    capabilities: Vec<String>,
}

impl ReplConf {
    pub(crate) fn parse_frames(parse: &mut Parse) -> anyhow::Result<ReplConf> {
        let mut listening_port = None;
        let mut capabilities = vec![];

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
                _ => bail!("expecting listening-port or cap, but got {:?}", arg),
            }
        }

        Ok(ReplConf {
            listening_port,
            capabilities,
        })
    }

    pub(crate) async fn apply(self, dst: &mut Connection, store: &Store) -> anyhow::Result<()> {
        dst.write_frame(&Frame::OK).await.map_err(|e| e.into())
    }
}

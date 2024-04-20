use bytes::Bytes;

use crate::{
    comms::Comms,
    frame::Frame,
    parse::{Parse, ParseError},
};

#[derive(Debug, Default)]
pub struct Ping {
    /// optional message to be returned
    msg: Option<Bytes>,
}

impl Ping {
    pub fn new(msg: Option<Bytes>) -> Ping {
        Ping { msg }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> anyhow::Result<Ping> {
        match parse.next_bytes() {
            Ok(msg) => Ok(Ping::new(Some(msg))),
            Err(ParseError::EndOfStream) => Ok(Ping::default()),
            Err(e) => Err(e.into()),
        }
    }

    pub(crate) async fn apply<C: Comms>(self, comms: &mut C) -> anyhow::Result<()> {
        let response = match self.msg {
            None => Frame::Simple("PONG".to_string()),
            Some(msg) => Frame::Bulk(msg),
        };

        comms.write_frame(&response).await?;

        Ok(())
    }
}

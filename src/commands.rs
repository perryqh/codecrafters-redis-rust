// https://redis.io/docs/reference/protocol-spec/
use crate::{
    info::Info,
    resp_lexer::{RESPBulkString, RESPSimpleString, Serialize},
    store::{Store, DEFAULT_EXPIRY},
};
use anyhow::bail;
use bytes::Bytes;
use std::time::Duration;

#[derive(Debug)]
pub enum Command {
    Echo(EchoCommand),
    Ping(PingCommand),
    Set(SetCommand),
    Get(GetCommand),
    Info(InfoCommand),
    ReplConf(ReplConfCommand),
    Psync(PsyncCommand),
}

impl CommandResponse for Command {
    fn response_bytes(&self) -> anyhow::Result<Bytes> {
        match self {
            Command::Echo(command) => command.response_bytes(),
            Command::Ping(command) => command.response_bytes(),
            Command::Set(command) => command.response_bytes(),
            Command::Get(command) => command.response_bytes(),
            Command::Info(command) => command.response_bytes(),
            Command::ReplConf(command) => command.response_bytes(),
            Command::Psync(command) => command.response_bytes(),
        }
    }
}

pub trait CommandResponse {
    fn response_bytes(&self) -> anyhow::Result<Bytes>;
}

#[derive(Debug)]
pub struct PsyncCommand {
    pub store: Store,
    pub master_replid: Option<String>,
    pub master_repl_offset: Option<u64>,
}

impl CommandResponse for PsyncCommand {
    fn response_bytes(&self) -> anyhow::Result<Bytes> {
        let info = Info::from_store(&self.store)?;
        let repl_offset = info.replication.master_repl_offset.as_ref().unwrap_or(&0);
        let master_repli_id = match info.replication.master_replid.as_ref() {
            Some(replid) => replid,
            None => bail!("Expected master replid to be set!"),
        };
        let response = format!("FULLRESYNC {} {}", master_repli_id, repl_offset);
        Ok(RESPSimpleString::new(response.into()).serialize())
    }
}

#[derive(Debug)]
pub struct ReplConfCommand {
    pub listening_port: Option<u16>,
    pub capabilities: Vec<String>,
    pub store: Store,
}

impl CommandResponse for ReplConfCommand {
    fn response_bytes(&self) -> anyhow::Result<Bytes> {
        Ok(RESPSimpleString::new("OK".into()).serialize())
    }
}

#[derive(Debug)]
pub struct InfoCommand {
    pub kind: Bytes,
    pub store: Store,
}

impl CommandResponse for InfoCommand {
    fn response_bytes(&self) -> anyhow::Result<Bytes> {
        let info = Info::from_store(&self.store)?;
        let bulk_str = match info.replication.role.as_str() {
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
        let bulk_string = RESPBulkString::new(bulk_str.into());
        Ok(bulk_string.serialize())
    }
}

#[derive(Debug)]
pub struct SetCommand {
    pub key: Bytes,
    pub value: Bytes,
    pub store: Store,
    pub expiry_in_milliseconds: Option<u64>,
}

impl CommandResponse for SetCommand {
    fn response_bytes(&self) -> anyhow::Result<Bytes> {
        let expiry = self.expiry_in_milliseconds.unwrap_or(DEFAULT_EXPIRY);
        self.store.set(
            self.key.clone(),
            self.value.clone(),
            Duration::from_millis(expiry),
        );
        Ok(RESPSimpleString::new("OK".into()).serialize())
    }
}
#[derive(Debug)]
pub struct GetCommand {
    pub key: Bytes,
    pub store: Store,
}

impl CommandResponse for GetCommand {
    fn response_bytes(&self) -> anyhow::Result<Bytes> {
        match self.store.get(self.key.clone()) {
            Some(value) => Ok(RESPBulkString::new(value).serialize()),
            None => Ok("$-1\r\n".into()),
        }
    }
}
#[derive(Debug)]
pub struct EchoCommand {
    pub message: Bytes,
}
#[derive(Debug)]
pub struct PingCommand;
impl CommandResponse for PingCommand {
    fn response_bytes(&self) -> anyhow::Result<Bytes> {
        Ok(RESPSimpleString::new("PONG".into()).serialize())
    }
}

impl CommandResponse for EchoCommand {
    fn response_bytes(&self) -> anyhow::Result<Bytes> {
        Ok(RESPSimpleString::new(self.message.clone()).serialize())
    }
}

#[cfg(test)]
mod tests {
    use tokio::time::sleep;

    use crate::{command_builder::parse_command, info::DEFAULT_MASTER_REPLID};

    use super::*;
    use bytes::Bytes;

    #[tokio::test]
    async fn test_parse_command_ping() -> anyhow::Result<()> {
        let command = b"*1\r\n$4\r\nping\r\n";
        let command = parse_command(command, Store::new())?;
        match command {
            Command::Ping(_) => {}
            _ => panic!("Expected ping"),
        }
        assert_eq!(command.response_bytes()?.as_ref(), b"+PONG\r\n");
        Ok(())
    }

    #[tokio::test]
    async fn test_parse_command_ping_with_mixed_case() -> anyhow::Result<()> {
        let command = b"*1\r\n$4\r\npInG\r\n";
        let command = parse_command(command, Store::new())?;
        match command {
            Command::Ping(_) => {}
            _ => panic!("Expected ping"),
        }
        assert_eq!(command.response_bytes()?.as_ref(), b"+PONG\r\n");
        Ok(())
    }

    #[tokio::test]
    async fn test_parse_command_echo() -> anyhow::Result<()> {
        let command = b"*2\r\n$4\r\necho\r\n$9\r\nvideogame\r\n";
        let command = parse_command(command, Store::new())?;
        match command {
            Command::Echo(ref msg) => {
                assert_eq!(msg.message, "videogame");
            }
            _ => panic!("Expected echo"),
        }
        assert_eq!(command.response_bytes()?.as_ref(), b"+videogame\r\n");
        Ok(())
    }

    #[tokio::test]
    async fn test_parse_command_set() -> anyhow::Result<()> {
        let command = b"*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        let store = Store::new();
        let command = parse_command(command, store.clone())?;
        match command {
            Command::Set(ref set) => {
                assert_eq!(set.key, "key");
                assert_eq!(set.value, "value");
                assert_eq!(set.expiry_in_milliseconds, Some(DEFAULT_EXPIRY as u64));
            }
            _ => panic!("Expected set"),
        }
        assert_eq!(command.response_bytes()?.as_ref(), b"+OK\r\n");
        assert_eq!(store.get("key".into()).unwrap(), Bytes::from("value"));
        Ok(())
    }

    #[tokio::test]
    async fn test_parse_command_get() -> anyhow::Result<()> {
        let command = b"*2\r\n$3\r\nget\r\n$3\r\nkey\r\n";
        let store = Store::new();
        store.set(
            "key".into(),
            "value".into(),
            Duration::from_millis(DEFAULT_EXPIRY as u64),
        );
        let command = parse_command(command, store)?;
        match command {
            Command::Get(ref get) => {
                assert_eq!(get.key, "key");
            }
            _ => panic!("Expected get"),
        }
        assert_eq!(
            command.response_bytes()?.as_ref(),
            b"$5\r\nvalue\r\n".as_ref()
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_parse_command_get_not_found() -> anyhow::Result<()> {
        let command = b"*2\r\n$3\r\nget\r\n$3\r\nkey\r\n";
        let store = Store::new();
        let command = parse_command(command, store)?;
        match command {
            Command::Get(ref get) => {
                assert_eq!(get.key, "key");
            }
            _ => panic!("Expected get"),
        }
        assert_eq!(command.response_bytes()?.as_ref(), b"$-1\r\n".as_ref());
        Ok(())
    }

    #[tokio::test]
    async fn test_parse_command_set_with_expiry() -> anyhow::Result<()> {
        let command = b"*5\r\n$3\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\nPX\r\n$4\r\n1000\r\n";
        let store = Store::new();
        let command = parse_command(command, store.clone())?;
        match command {
            Command::Set(ref set) => {
                dbg!(set.expiry_in_milliseconds);
                assert_eq!(set.key, "key");
                assert_eq!(set.value, "value");
                assert_eq!(set.expiry_in_milliseconds, Some(1000));
            }
            _ => panic!("Expected set"),
        }
        assert_eq!(command.response_bytes()?.as_ref(), b"+OK\r\n".as_ref());
        assert_eq!(store.get("key".into()).unwrap(), Bytes::from("value"));
        Ok(())
    }

    #[tokio::test]
    async fn test_parse_command_set_expired() -> anyhow::Result<()> {
        let command = b"*5\r\n$3\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\nPX\r\n$1\r\n1\r\n";
        let store = Store::new();
        let command = parse_command(command, store.clone())?;
        match command {
            Command::Set(ref set) => {
                assert_eq!(set.key, "key");
                assert_eq!(set.value, "value");
                assert_eq!(set.expiry_in_milliseconds, Some(1));
            }
            _ => panic!("Expected set"),
        }
        assert_eq!(command.response_bytes()?, "+OK\r\n");
        assert_eq!(store.get("key".into()), Some(Bytes::from("value")));

        sleep(Duration::from_secs(1)).await;
        let command = b"*2\r\n$3\r\nget\r\n$3\r\nkey\r\n";
        let command = parse_command(command, store)?;
        assert_eq!(command.response_bytes()?, Bytes::from("$-1\r\n"));
        Ok(())
    }

    #[tokio::test]
    async fn test_info_command() -> anyhow::Result<()> {
        let command = b"*1\r\n$4\r\ninfo\r\n";
        let command = parse_command(command, Store::new())?;
        match command {
            Command::Info(_) => {}
            _ => panic!("Expected info"),
        }
        assert_eq!(
            command.response_bytes()?,
            RESPBulkString {
                data: "role:master\r\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\nmaster_repl_offset:0\r\n".into(),
            }
            .serialize()
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_repl_conf_listening_port() -> anyhow::Result<()> {
        let array = RESPArray {
            data: vec![
                RESPValue::BulkString(RESPBulkString::new(Bytes::from("REPLCONF"))),
                RESPValue::BulkString(RESPBulkString::new(Bytes::from("listening-port"))),
                RESPValue::BulkString(RESPBulkString::new(Bytes::from("6380"))),
            ],
        };
        let command = array.serialize();
        let command = parse_command(command.as_ref(), Store::new())?;
        match &command {
            Command::ReplConf(repl_conf) => {
                assert_eq!(repl_conf.listening_port, Some(6380));
            }
            _ => panic!("Expected repl conf"),
        }
        assert_eq!(
            command.response_bytes()?,
            RESPSimpleString { data: "OK".into() }.serialize()
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_repl_conf_capabilities() -> anyhow::Result<()> {
        let array = RESPArray {
            data: vec![
                RESPValue::BulkString(RESPBulkString::new(Bytes::from("REPLCONF"))),
                RESPValue::BulkString(RESPBulkString::new(Bytes::from("capa"))),
                RESPValue::BulkString(RESPBulkString::new(Bytes::from("eof"))),
                RESPValue::BulkString(RESPBulkString::new(Bytes::from("capa"))),
                RESPValue::BulkString(RESPBulkString::new(Bytes::from("psync2"))),
            ],
        };
        let command = array.serialize();
        let command = parse_command(command.as_ref(), Store::new())?;
        match &command {
            Command::ReplConf(repl_conf) => {
                assert_eq!(repl_conf.listening_port, None);
                assert_eq!(
                    repl_conf.capabilities,
                    vec!["eof".to_string(), "psync2".to_string()]
                );
            }
            _ => panic!("Expected repl conf"),
        }
        assert_eq!(
            command.response_bytes()?,
            RESPSimpleString { data: "OK".into() }.serialize()
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_psync() -> anyhow::Result<()> {
        let command = b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
        let command = parse_command(command, Store::new())?;
        match &command {
            Command::Psync(psync) => {
                assert_eq!(psync.master_replid, None);
                assert_eq!(psync.master_repl_offset, None);
            }
            _ => panic!("Expected repl conf"),
        }

        let expected = format!("FULLRESYNC {} {}", DEFAULT_MASTER_REPLID, 0);

        assert_eq!(
            command.response_bytes()?,
            RESPSimpleString {
                data: expected.into()
            }
            .serialize()
        );
        Ok(())
    }
}

// https://redis.io/docs/reference/protocol-spec/
use crate::{
    resp_lexer::{Lexer, RESPArray, RESPBulkString, RESPSimpleString, RESPValue, Serialize},
    store::Store,
};
use anyhow::{bail, ensure};
use std::time::Duration;

pub enum Command {
    Echo(EchoCommand),
    Ping(PingCommand),
    Set(SetCommand),
    Get(GetCommand),
    Info(InfoCommand),
}

impl Command {
    pub fn response_bytes(&self) -> anyhow::Result<Vec<u8>> {
        match self {
            Command::Echo(command) => command.response_bytes(),
            Command::Ping(command) => command.response_bytes(),
            Command::Set(command) => command.response_bytes(),
            Command::Get(command) => command.response_bytes(),
            Command::Info(command) => command.response_bytes(),
        }
    }
}

pub struct InfoCommand {
    pub kind: String,
}

impl InfoCommand {
    fn response_bytes(&self) -> anyhow::Result<Vec<u8>> {
        let bulk_string = RESPBulkString::new("role:master");
        Ok(bulk_string.serialize())
    }
}

pub struct SetCommand {
    pub key: String,
    pub value: String,
    pub store: Store,
    pub expiry_in_milliseconds: Option<u64>,
}

const DEFAULT_EXPIRY: i64 = 1000 * 60 * 60 * 24 * 7; // 1 week

impl SetCommand {
    fn response_bytes(&self) -> anyhow::Result<Vec<u8>> {
        let expiry = self.expiry_in_milliseconds.unwrap_or(DEFAULT_EXPIRY as u64);
        self.store.set(
            self.key.clone(),
            self.value.clone().into(),
            Duration::from_millis(expiry),
        );
        Ok(RESPSimpleString::new("OK").serialize())
    }
}

pub struct GetCommand {
    pub key: String,
    pub store: Store,
}

impl GetCommand {
    fn response_bytes(&self) -> anyhow::Result<Vec<u8>> {
        match self.store.get(&self.key) {
            Some(value) => {
                let value = String::from_utf8(value.to_vec()).unwrap_or_default();
                Ok(RESPBulkString::new(&value).serialize())
            }
            None => Ok(b"$-1\r\n".to_vec()),
        }
    }
}

pub struct EchoCommand {
    pub message: String,
}

pub struct PingCommand;
impl PingCommand {
    fn response_bytes(&self) -> anyhow::Result<Vec<u8>> {
        Ok(RESPSimpleString::new("PONG").serialize())
    }
}

impl EchoCommand {
    fn response_bytes(&self) -> anyhow::Result<Vec<u8>> {
        Ok(RESPSimpleString::new(&self.message).serialize())
    }
}

pub fn parse_command(command: &[u8], store: Store) -> anyhow::Result<Command> {
    let mut lexer = Lexer::new(command.into());
    let value = lexer.lex()?;
    match value {
        RESPValue::Array(array) => build_command_from_array(array, store),
        _ => bail!("Expected RESPValue::Array found: {:?}", value),
    }
}

fn build_command_from_array(array: RESPArray, store: Store) -> anyhow::Result<Command> {
    let command = array
        .data
        .first()
        .ok_or_else(|| anyhow::anyhow!("Expected command"))?;
    let command = match command {
        RESPValue::BulkString(command) => command,
        _ => bail!("Expected RESPValue::BulkString found: {:?}", command),
    };

    match command.data.to_uppercase().as_str() {
        "PING" => Ok(Command::Ping(PingCommand)),
        "ECHO" => {
            ensure!(array.data.len() == 2, "echo 1 argument expected");
            let message = array
                .data
                .get(1)
                .ok_or_else(|| anyhow::anyhow!("Expected message"))?;
            match message {
                RESPValue::BulkString(message) => Ok(Command::Echo(EchoCommand {
                    message: message.data.clone(),
                })),
                _ => bail!("Expected RESPValue::BulkString found: {:?}", message),
            }
        }
        "SET" => {
            ensure!(array.data.len() >= 3, "set 2 or more arguments expected");
            let key = array
                .data
                .get(1)
                .ok_or_else(|| anyhow::anyhow!("Expected key"))?;
            let value = array
                .data
                .get(2)
                .ok_or_else(|| anyhow::anyhow!("Expected value"))?;
            let expiry = if array.data.len() >= 5 {
                let expiry = array
                    .data
                    .get(4)
                    .ok_or_else(|| anyhow::anyhow!("Expected expiry"))?;
                match expiry {
                    RESPValue::Integer(expiry) => Some(*expiry as u64),
                    RESPValue::BulkString(expiry) => Some(expiry.data.parse::<u64>()?),
                    _ => bail!("Expected RESPValue::Integer found: {:?}", expiry),
                }
            } else {
                Some(DEFAULT_EXPIRY as u64)
            };

            match (key, value) {
                (RESPValue::BulkString(key), RESPValue::BulkString(value)) => {
                    Ok(Command::Set(SetCommand {
                        key: key.data.clone(),
                        value: value.data.clone(),
                        expiry_in_milliseconds: expiry,
                        store,
                    }))
                }
                _ => bail!(
                    "Expected RESPValue::BulkString found: {:?} {:?}",
                    key,
                    value
                ),
            }
        }
        "GET" => {
            ensure!(array.data.len() == 2, "get 1 argument expected");
            let key = array
                .data
                .get(1)
                .ok_or_else(|| anyhow::anyhow!("Expected key"))?;
            match key {
                RESPValue::BulkString(key) => Ok(Command::Get(GetCommand {
                    key: key.data.clone(),
                    store,
                })),
                _ => bail!("Expected RESPValue::BulkString found: {:?}", key),
            }
        }
        "INFO" => Ok(Command::Info(InfoCommand {
            kind: "replication".to_string(),
        })),
        _ => bail!("Not implemented command: {:?}", command.data),
    }
}

#[cfg(test)]
mod tests {
    use tokio::time::sleep;

    use super::*;

    #[tokio::test]
    async fn test_parse_command_ping() -> anyhow::Result<()> {
        let command = b"*1\r\n$4\r\nping\r\n";
        let command = parse_command(command, Store::new())?;
        match command {
            Command::Ping(_) => {}
            _ => panic!("Expected ping"),
        }
        assert_eq!(command.response_bytes()?, b"+PONG\r\n");
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
        assert_eq!(command.response_bytes()?, b"+PONG\r\n");
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
        assert_eq!(command.response_bytes()?, b"+videogame\r\n");
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
        assert_eq!(command.response_bytes()?, b"+OK\r\n");
        assert_eq!(
            String::from_utf8(store.get("key").unwrap().to_vec()).unwrap(),
            "value"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_parse_command_get() -> anyhow::Result<()> {
        let command = b"*2\r\n$3\r\nget\r\n$3\r\nkey\r\n";
        let store = Store::new();
        store.set(
            "key".to_string(),
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
        assert_eq!(command.response_bytes()?, b"$5\r\nvalue\r\n");
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
        assert_eq!(command.response_bytes()?, b"$-1\r\n");
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
        assert_eq!(command.response_bytes()?, b"+OK\r\n");
        assert_eq!(
            String::from_utf8(store.get("key").unwrap().to_vec()).unwrap(),
            "value"
        );
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
        assert_eq!(command.response_bytes()?, b"+OK\r\n");
        assert_eq!(store.get("key"), Some("value".into()));

        sleep(Duration::from_secs(1)).await;
        let command = b"*2\r\n$3\r\nget\r\n$3\r\nkey\r\n";
        let command = parse_command(command, store)?;
        assert_eq!(command.response_bytes()?, b"$-1\r\n");
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
        assert_eq!(command.response_bytes()?, RESPBulkString{data: "role:master".to_string()}.serialize());
        Ok(())
    }
}

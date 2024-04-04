// https://redis.io/docs/reference/protocol-spec/
use crate::{
    resp_lexer::{Lexer, RESPArray, RESPValue},
    store::Store,
};
use anyhow::{bail, ensure};

pub enum Command {
    Echo(EchoCommand),
    Ping(PingCommand),
    Set(SetCommand),
    Get(GetCommand),
}

impl Command {
    pub fn response_bytes(&self) -> anyhow::Result<Vec<u8>> {
        match self {
            Command::Echo(command) => command.response_bytes(),
            Command::Ping(command) => command.response_bytes(),
            Command::Set(command) => command.response_bytes(),
            Command::Get(command) => command.response_bytes(),
        }
    }
}

pub struct SetCommand {
    pub key: String,
    pub value: String,
    pub store: Store,
}

impl SetCommand {
    fn response_bytes(&self) -> anyhow::Result<Vec<u8>> {
        self.store.set(self.key.clone(), self.value.clone().into());
        Ok(b"+OK\r\n".to_vec())
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
                Ok(format!("${}\r\n{}\r\n", value.len(), value).into_bytes())
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
        Ok(b"+PONG\r\n".to_vec())
    }
}

impl EchoCommand {
    fn response_bytes(&self) -> anyhow::Result<Vec<u8>> {
        Ok(format!("+{}\r\n", self.message).into_bytes())
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
            ensure!(array.data.len() == 3, "set 2 arguments expected");
            let key = array
                .data
                .get(1)
                .ok_or_else(|| anyhow::anyhow!("Expected key"))?;
            let value = array
                .data
                .get(2)
                .ok_or_else(|| anyhow::anyhow!("Expected value"))?;
            match (key, value) {
                (RESPValue::BulkString(key), RESPValue::BulkString(value)) => {
                    Ok(Command::Set(SetCommand {
                        key: key.data.clone(),
                        value: value.data.clone(),
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
        _ => bail!("Not implemented command: {:?}", command.data),
    }
}

#[cfg(test)]
mod tests {
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
        store.set("key".to_string(), "value".into());
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
}

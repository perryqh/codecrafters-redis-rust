// https://redis.io/docs/reference/protocol-spec/
use crate::resp_lexer::{Lexer, RESPArray, RESPValue};
use anyhow::{bail, ensure};

pub enum Command {
    Echo(EchoCommand),
    Ping(PingCommand),
}

impl Command {
    pub fn response_bytes(&self) -> anyhow::Result<Vec<u8>> {
        match self {
            Command::Echo(command) => command.response_bytes(),
            Command::Ping(command) => command.response_bytes(),
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

pub fn parse_command(command: &[u8]) -> anyhow::Result<Command> {
    let mut lexer = Lexer::new(command.into());
    let value = lexer.lex()?;
    match value {
        RESPValue::Array(array) => build_command_from_array(array),
        _ => bail!("Expected RESPValue::Array found: {:?}", value),
    }
}

fn build_command_from_array(array: RESPArray) -> anyhow::Result<Command> {
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
        _ => bail!("Not implemented"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_parse_command_ping() -> anyhow::Result<()> {
        let command = b"*1\r\n$4\r\nping\r\n";
        let command = parse_command(command)?;
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
        let command = parse_command(command)?;
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
        let command = parse_command(command)?;
        match command {
            Command::Echo(ref msg) => {
                assert_eq!(msg.message, "videogame");
            }
            _ => panic!("Expected echo"),
        }
        assert_eq!(command.response_bytes()?, b"+videogame\r\n");
        Ok(())
    }
}

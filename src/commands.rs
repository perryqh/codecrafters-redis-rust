// https://redis.io/docs/reference/protocol-spec/
use anyhow::ensure;

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
    let command_string = std::str::from_utf8(command)?;
    let parts = command_string.split("\r\n").filter(|part| !part.is_empty());
    build_command_from_parts(parts)
}

pub fn build_command_from_parts<'a>(
    mut parts: impl Iterator<Item = &'a str>,
) -> anyhow::Result<Command> {
    let command_type = parts
        .next()
        .ok_or_else(|| anyhow::anyhow!("Expected command"))?;
    match command_type
        .chars()
        .next()
        .ok_or_else(|| anyhow::anyhow!("Expected char prefix"))?
    {
        '*' => {
            let num_args = command_type
                .chars()
                .skip(1)
                .collect::<String>()
                .parse::<usize>()?;
            let mut command_parts = vec![];
            for _ in 0..num_args {
                let arg_type = parts
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("Expected argument type"))?;
                ensure!(
                    arg_type.starts_with('$'),
                    "Expected argument type to start with $, but is '{}'",
                    arg_type
                );
                let arg_len = arg_type
                    .chars()
                    .skip(1)
                    .collect::<String>()
                    .parse::<usize>()?;
                let arg = parts
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("Expected argument"))?;
                ensure!(
                    arg.len() == arg_len,
                    "Argument length does not match expected length. Expected: {}, Actual: {}",
                    arg_len,
                    arg.len()
                );
                command_parts.push(arg);
            }
            build_command_from_array(command_parts)
        }
        _ => panic!("'{}' Not implemented", command_type),
    }
}

fn build_command_from_array(parts: Vec<&str>) -> anyhow::Result<Command> {
    let command = *parts
        .first()
        .ok_or_else(|| anyhow::anyhow!("Expected command"))?;
    match command {
        "ping" => Ok(Command::Ping(PingCommand)),
        "echo" => {
            let message = parts
                .get(1)
                .ok_or_else(|| anyhow::anyhow!("Expected message"))?;
            Ok(Command::Echo(EchoCommand {
                message: message.to_string(),
            }))
        }
        _ => panic!("Not implemented"),
    }
}

enum RESPDataType {
    BulkString(RESPBulkString),
    Array(RESPArray),
}

struct RESPBulkString {
    data: String,
}

struct RESPArray {
    data: Vec<RESPDataType>,
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

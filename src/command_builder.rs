use anyhow::{bail, ensure, Context};

use crate::{
    commands::{
        Command, EchoCommand, GetCommand, InfoCommand, PingCommand, PsyncCommand, ReplConfCommand,
        SetCommand,
    },
    resp_lexer::{Lexer, RESPArray, RESPValue},
    store::{Store, DEFAULT_EXPIRY},
};

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

    match String::from_utf8(command.data.to_vec())
        .context("Expected command data to be utf8")?
        .to_uppercase()
        .as_str()
    {
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
                    RESPValue::BulkString(expiry) => {
                        Some(String::from_utf8(expiry.data.to_vec())?.parse::<u64>()?)
                    }
                    _ => bail!(
                        "Expected RESPValue::Integer or RESPValue::BulkString found: {:?}",
                        expiry
                    ),
                }
            } else {
                Some(DEFAULT_EXPIRY)
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
            kind: "replication".into(),
            store,
        })),
        "PSYNC" => {
            ensure!(array.data.len() == 3, "psync 2 arguments expected");

            Ok(Command::Psync(PsyncCommand {
                store,
                master_repl_offset: None,
                master_replid: None,
            }))
        }
        "REPLCONF" => {
            let mut listening_port = None;
            let mut capabilities = vec![];
            for i in (1..array.data.len()).step_by(2) {
                let key = array
                    .data
                    .get(i)
                    .ok_or_else(|| anyhow::anyhow!("Expected key"))?;
                let value = array
                    .data
                    .get(i + 1)
                    .ok_or_else(|| anyhow::anyhow!("Expected value"))?;
                match (key, value) {
                    (RESPValue::BulkString(key), RESPValue::BulkString(value)) => {
                        match String::from_utf8(key.data.to_vec())
                            .context("Expected key data to be utf8")?
                            .to_lowercase()
                            .as_str()
                        {
                            "listening-port" => {
                                let port = String::from_utf8(value.data.to_vec())
                                    .context("Expected value data to be utf8")?
                                    .parse::<u16>()?;
                                listening_port = Some(port);
                            }
                            "capa" => {
                                let capa = String::from_utf8(value.data.to_vec())
                                    .context("Expected value data to be utf8")?;
                                capabilities.push(capa);
                            }
                            _ => bail!("Invalid key: {:?}", key),
                        }
                    }
                    _ => bail!(
                        "Expected RESPValue::BulkString found: {:?} {:?}",
                        key,
                        value
                    ),
                }
            }
            Ok(Command::ReplConf(ReplConfCommand {
                listening_port,
                capabilities,
                store,
            }))
        }
        _ => bail!("Not implemented command: {:?}", command.data),
    }
}

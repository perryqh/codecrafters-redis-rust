use crate::{comms::Comms, frame::Frame, parse::Parse, store::Store};
pub mod ping;
use anyhow::Context;
use ping::Ping;
pub mod echo;
use echo::Echo;
pub mod unknown;
use unknown::Unknown;
pub mod get;
use get::Get;
pub mod set;
use set::Set;
pub mod info;
use info::Info;
pub mod repl_conf;
use repl_conf::ReplConf;
pub mod psync;
use psync::Psync;

#[derive(Debug)]
pub enum Command {
    Ping(Ping),
    Echo(Echo),
    Unknown(Unknown),
    Get(Get),
    Set(Set),
    Info(Info),
    ReplConf(ReplConf),
    Psync(Psync),
}

impl Command {
    pub fn from_frame(frame: Frame) -> anyhow::Result<Command> {
        let mut parse = Parse::new(frame).context("erroring parsing frame")?;
        let command_name = parse.next_string()?.to_lowercase();
        let command = match command_name.to_lowercase().as_str() {
            "ping" => Command::Ping(Ping::parse_frames(&mut parse)?),
            "echo" => Command::Echo(Echo::parse_frames(&mut parse)?),
            "get" => Command::Get(Get::parse_frames(&mut parse)?),
            "set" => Command::Set(Set::parse_frames(&mut parse)?),
            "info" => Command::Info(Info::parse_frames(&mut parse)?),
            "replconf" => Command::ReplConf(ReplConf::parse_frames(&mut parse)?),
            "psync" => Command::Psync(Psync::parse_frames(&mut parse)?),
            _ => {
                return Ok(Command::Unknown(Unknown::new(command_name)));
            }
        };
        parse.finish()?; // if any remaining frames, return an error

        Ok(command)
    }

    pub async fn apply<C: Comms>(self, store: &Store, comms: &mut C) -> anyhow::Result<()> {
        match self {
            Command::Echo(cmd) => cmd.apply(comms).await,
            Command::Unknown(cmd) => cmd.apply(comms).await,
            Command::Get(cmd) => cmd.apply(comms, store).await,
            Command::Set(cmd) => cmd.apply(comms, store).await,
            Command::Info(cmd) => cmd.apply(comms, store).await,
            Command::ReplConf(cmd) => cmd.apply(comms, store).await,
            Command::Ping(cmd) => cmd.apply(comms).await,
            Command::Psync(cmd) => cmd.apply(comms, store).await,
        }
    }
}

#[macro_export]
macro_rules! simple_string {
    ($x: expr) => {
        format!("+{}\r\n", $x).as_bytes()
    };
}

#[macro_export]
macro_rules! array_of_bulks {
    ($($arg:expr),*) => {{
        let mut command = Vec::new();
        command.push(format!("*{}\r\n", $crate::count_redis_input_command_args!(@COUNT; $($arg),*)));
        $(
            command.push(format!("${}\r\n{}\r\n", $arg.len(), $arg));
        )*
        command.concat().as_bytes()
    }};
}

#[macro_export]
macro_rules! count_redis_input_command_args {
    (@COUNT; $($arg:expr),*) => {
        0 $(+ {let _ = $arg; 1})*
    };
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_array_of_bulks() {
        assert_eq!(
            array_of_bulks!("get", "key"),
            b"*2\r\n$3\r\nget\r\n$3\r\nkey\r\n"
        );
        assert_eq!(array_of_bulks!("info"), b"*1\r\n$4\r\ninfo\r\n");
    }

    #[test]
    fn test_simple_string() {
        assert_eq!(simple_string!("foo"), b"+foo\r\n");
    }
}

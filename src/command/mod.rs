use crate::{connection::Connection, frame::Frame, parse::Parse, store::Store};
pub mod ping;
use ping::Ping;
pub mod echo;
use echo::Echo;
pub mod unknown;
use unknown::Unknown;

#[derive(Debug)]
pub enum Command {
    Ping(Ping),
    Echo(Echo),
    Unknown(Unknown),
}

impl Command {
    pub fn from_frame(frame: Frame) -> anyhow::Result<Command> {
        let mut parse = Parse::new(frame)?;
        let command_name = parse.next_string()?.to_lowercase();
        let command = match command_name.to_lowercase().as_str() {
            "ping" => Command::Ping(Ping::parse_frames(&mut parse)?),
            "echo" => Command::Echo(Echo::parse_frames(&mut parse)?),
            _ => {
                return Ok(Command::Unknown(Unknown::new(command_name)));
            }
        };
        parse.finish()?; // if any remaining frames, return an error

        Ok(command)
    }

    pub async fn apply(self, store: &Store, connection: &mut Connection) -> anyhow::Result<()> {
        match self {
            Command::Ping(cmd) => cmd.apply(connection).await,
            Command::Echo(cmd) => cmd.apply(connection).await,
            Command::Unknown(cmd) => cmd.apply(connection).await,
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

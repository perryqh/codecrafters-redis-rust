use anyhow::ensure;
use bytes::{Bytes, BytesMut};

#[derive(Debug, PartialEq)]
pub enum RESPValue {
    BulkString(RESPBulkString),
    SimpleString(RESPSimpleString),
    Array(RESPArray),
    Integer(i64),
}

pub trait Serialize {
    fn serialize(&self) -> Bytes;
}

#[derive(Debug, PartialEq)]
pub struct RESPBulkString {
    pub data: Bytes,
}

impl RESPSimpleString {
    pub fn new(data: Bytes) -> Self {
        Self { data }
    }
}

impl RESPBulkString {
    pub fn new(data: Bytes) -> Self {
        Self { data }
    }
}

#[derive(Debug, PartialEq)]
pub struct RESPSimpleString {
    pub data: Bytes,
}

impl Serialize for RESPValue {
    fn serialize(&self) -> Bytes {
        match self {
            RESPValue::SimpleString(simple_string) => simple_string.serialize(),
            RESPValue::BulkString(bulk_string) => bulk_string.serialize(),
            RESPValue::Array(array) => array.serialize(),
            RESPValue::Integer(int_value) => int_value.serialize(),
        }
    }
}

impl Serialize for i64 {
    fn serialize(&self) -> Bytes {
        let mut buffer = BytesMut::new();
        buffer.extend_from_slice(b":");
        buffer.extend_from_slice(self.to_string().as_bytes());
        buffer.extend_from_slice(b"\r\n");
        buffer.freeze()
    }
}

impl Serialize for RESPArray {
    fn serialize(&self) -> Bytes {
        let mut buffer = BytesMut::new();
        buffer.extend_from_slice(b"*");
        buffer.extend_from_slice(self.data.len().to_string().as_bytes());
        buffer.extend_from_slice(b"\r\n");
        for value in &self.data {
            buffer.extend_from_slice(&value.serialize());
        }
        buffer.freeze()
    }
}

impl Serialize for RESPSimpleString {
    fn serialize(&self) -> Bytes {
        let mut buffer = BytesMut::new();
        buffer.extend_from_slice(b"+");
        buffer.extend_from_slice(&self.data);
        buffer.extend_from_slice(b"\r\n");
        buffer.freeze()
    }
}

impl Serialize for RESPBulkString {
    fn serialize(&self) -> Bytes {
        let mut buffer = BytesMut::new();
        buffer.extend_from_slice(b"$");
        buffer.extend_from_slice(self.data.len().to_string().as_bytes());
        buffer.extend_from_slice(b"\r\n");
        buffer.extend_from_slice(&self.data);
        buffer.extend_from_slice(b"\r\n");
        buffer.freeze()
    }
}

#[derive(Debug, PartialEq)]
pub struct RESPArray {
    pub data: Vec<RESPValue>,
}

#[derive(Debug, PartialEq)]
pub struct Lexer {
    data: BytesMut,
}

impl Lexer {
    pub fn new(data: BytesMut) -> Self {
        Self { data }
    }

    pub fn lex(&mut self) -> anyhow::Result<RESPValue> {
        match self.lex_into_resp_values(0) {
            Ok((resp_value, _)) => Ok(resp_value),
            Err(e) => Err(e),
        }
    }

    fn lex_into_resp_values(&self, current_position: usize) -> anyhow::Result<(RESPValue, usize)> {
        match self.data[current_position] {
            b'*' => self.lex_array(current_position + 1),
            b'$' => self.lex_bulk_string(current_position + 1),
            b':' => self.lex_int_value(current_position + 1),
            b'+' => self.lex_simple_string(current_position + 1),
            _ => Err(anyhow::anyhow!("Invalid RESP value: {:?}", self.data)),
        }
    }

    fn lex_int_value(&self, current_position: usize) -> anyhow::Result<(RESPValue, usize)> {
        let (line, current_position) = match self.read_until_crlf(current_position) {
            Some((line, current_position)) => (line, current_position),
            None => return Err(anyhow::anyhow!("Invalid integer format {:?}", self.data)),
        };

        let int_value = self.lex_int(line)?;
        Ok((RESPValue::Integer(int_value), current_position))
    }

    fn lex_simple_string(&self, current_position: usize) -> anyhow::Result<(RESPValue, usize)> {
        let (line, current_position) = match self.read_until_crlf(current_position) {
            Some((line, current_position)) => (line, current_position),
            None => {
                return Err(anyhow::anyhow!(
                    "Invalid simple string format {:?}",
                    self.data
                ))
            }
        };

        Ok((
            RESPValue::SimpleString(RESPSimpleString {
                data: Bytes::copy_from_slice(line),
            }),
            current_position,
        ))
    }

    fn lex_bulk_string(&self, current_position: usize) -> anyhow::Result<(RESPValue, usize)> {
        let (line, current_position) = match self.read_until_crlf(current_position) {
            Some((line, current_position)) => (line, current_position),
            None => {
                return Err(anyhow::anyhow!(
                    "Invalid bulk string format {:?}",
                    self.data
                ))
            }
        };

        let bulk_string_length = self.lex_int(line)?;
        ensure!(
            bulk_string_length >= 0,
            "Bulk string length must be greater than or equal to 0"
        );

        let bulk_string =
            &self.data[current_position..(current_position + bulk_string_length as usize)];
        let bulk_string = Bytes::copy_from_slice(bulk_string);
        let end_position = current_position + bulk_string_length as usize + 2;
        ensure!(
            self.data.len() >= end_position
                && self.data[end_position - 2] == b'\r'
                && self.data[end_position - 1] == b'\n',
            "Invalid bulk string format {:?}. Expected bulk string to end with CRLF.",
            self.data
        );

        Ok((
            RESPValue::BulkString(RESPBulkString { data: bulk_string }),
            end_position,
        ))
    }

    fn lex_array(&self, current_position: usize) -> anyhow::Result<(RESPValue, usize)> {
        let (line, current_position) = match self.read_until_crlf(current_position) {
            Some((line, current_position)) => (line, current_position),
            None => return Err(anyhow::anyhow!("Invalid array format {:?}", self.data)),
        };

        let array_length = self.lex_int(line)?;
        ensure!(array_length > 0, "Array length must be greater than 0");
        let mut array: Vec<RESPValue> = Vec::new();
        let mut current_position = current_position;

        for _ in 0..array_length {
            let (resp_value, pos) = self.lex_into_resp_values(current_position)?;
            array.push(resp_value);
            current_position = pos;
        }

        Ok((
            RESPValue::Array(RESPArray { data: array }),
            current_position,
        ))
    }

    fn lex_int(&self, buffer: &[u8]) -> anyhow::Result<i64> {
        Ok(String::from_utf8(buffer.to_vec())?.parse::<i64>()?)
    }

    fn read_until_crlf(&self, current_position: usize) -> Option<(&[u8], usize)> {
        for i in (current_position + 1)..self.data.len() {
            if self.data[i - 1] == b'\r' && self.data[i] == b'\n' {
                return Some((&self.data[current_position..(i - 1)], i + 1));
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lex_array_ping() -> anyhow::Result<()> {
        let input = b"*1\r\n$4\r\nping\r\n";
        let mut lexer = Lexer::new(BytesMut::from(&input[..]));
        let result = lexer.lex()?;
        assert_eq!(
            result,
            RESPValue::Array(RESPArray {
                data: vec![RESPValue::BulkString(RESPBulkString {
                    data: "ping".into(),
                })]
            })
        );

        Ok(())
    }

    #[test]
    fn test_lex_array_echo() -> anyhow::Result<()> {
        let input = b"*2\r\n$4\r\necho\r\n$5\r\nhello\r\n";
        let mut lexer = Lexer::new(BytesMut::from(&input[..]));
        let result = lexer.lex()?;
        assert_eq!(
            result,
            RESPValue::Array(RESPArray {
                data: vec![
                    RESPValue::BulkString(RESPBulkString {
                        data: "echo".into(),
                    }),
                    RESPValue::BulkString(RESPBulkString {
                        data: "hello".into()
                    })
                ]
            })
        );

        Ok(())
    }

    #[test]
    fn test_serialize_bulk_string() -> anyhow::Result<()> {
        let input = "role:master".to_string();
        let bulk_string = RESPBulkString { data: input.into() };
        let expected: Bytes = "$11\r\nrole:master\r\n".into();
        assert_eq!(bulk_string.serialize(), expected);

        Ok(())
    }

    #[test]
    fn test_lex_bulk_string_with_crlf() -> anyhow::Result<()> {
        let input = "role:master\r\nmaster_replid:878S\r\nmaster_repl_offset:0";
        let input_as_bytes = format!("${}\r\n{}\r\n", input.len(), input);
        let mut lexer = Lexer::new(BytesMut::from(&input_as_bytes[..]));
        let result = lexer.lex()?;
        assert_eq!(
            result,
            RESPValue::BulkString(RESPBulkString {
                data: "role:master\r\nmaster_replid:878S\r\nmaster_repl_offset:0".into()
            })
        );

        Ok(())
    }

    #[test]
    fn test_lex_bulk_string_with_crlf_without_termination() -> anyhow::Result<()> {
        let input = "role:master\r\nmaster_replid:878S\r\nmaster_repl_offset:0";
        let input_as_bytes = format!("${}\r\n{}", input.len(), input);
        let mut lexer = Lexer::new(BytesMut::from(&input_as_bytes[..]));
        let result = lexer.lex();
        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn test_lex_bulk_string_with_crlf_without_valid_termination() -> anyhow::Result<()> {
        let input = "role:master\r\nmaster_replid:878S\r\nmaster_repl_offset:0\r\r";
        let input_as_bytes = format!("${}\r\n{}", input.len(), input);
        let mut lexer = Lexer::new(BytesMut::from(&input_as_bytes[..]));
        let result = lexer.lex();
        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn test_simple_string() -> anyhow::Result<()> {
        let bytes: BytesMut = "+PONG\r\n".into();
        let mut lexer = Lexer::new(bytes);
        let result = lexer.lex()?;
        assert_eq!(
            result,
            RESPValue::SimpleString(RESPSimpleString {
                data: "PONG".into()
            })
        );

        Ok(())
    }
}

//! Provides a type representing a Redis protocol frame as well as utilities for
//! parsing frames from a byte array.

use anyhow::bail;
use bytes::{Buf, Bytes};
use std::convert::TryInto;
use std::fmt;
use std::io::Cursor;
use std::num::TryFromIntError;
use std::string::FromUtf8Error;

/// A frame in the Redis protocol.
#[derive(Clone, Debug, PartialEq)]
pub enum Frame {
    Simple(String),
    Error(String),
    Integer(u64),
    Bulk(Bytes),
    Null,
    OK,
    Array(Vec<Frame>),
    RdbFile(Bytes),
}

#[derive(Debug)]
pub enum Error {
    /// Not enough data is available to parse a message
    Incomplete,

    /// Invalid message encoding
    Other(anyhow::Error),
}

impl Frame {
    /// Returns an empty array
    pub(crate) fn array() -> Frame {
        Frame::Array(vec![])
    }

    pub(crate) fn push_bulk(&mut self, bytes: Bytes) -> anyhow::Result<()> {
        match self {
            Frame::Array(vec) => {
                vec.push(Frame::Bulk(bytes));
                Ok(())
            }
            _ => bail!("not an array frame"),
        }
    }

    /// Push an "integer" frame into the array. `self` must be an Array frame.
    ///
    /// # Panics
    ///
    /// panics if `self` is not an array
    pub(crate) fn push_int(&mut self, value: u64) -> anyhow::Result<()> {
        match self {
            Frame::Array(vec) => {
                vec.push(Frame::Integer(value));
                Ok(())
            }
            _ => bail!("not an array frame"),
        }
    }

    /// Checks if an entire message can be decoded from `src`
    pub fn check(src: &mut Cursor<&[u8]>) -> Result<(), Error> {
        match get_u8(src)? {
            b'+' => {
                get_line(src)?;
                Ok(())
            }
            b'-' => {
                get_line(src)?;
                Ok(())
            }
            b':' => {
                let _ = get_decimal(src)?;
                Ok(())
            }
            b'$' => {
                if b'-' == peek_u8(src)? {
                    // Skip '-1\r\n'
                    skip(src, 4)
                } else {
                    // Read the bulk string
                    let len: usize = get_decimal(src)?.try_into()?;

                    match skip(src, len) {
                        Ok(_) => {
                            // special case for RDB, which does not have trailing \r\n
                            match peek_u8(src) {
                                Ok(b'\r') => skip(src, 2),
                                _ => Ok(()),
                            }
                        }
                        Err(_) => return Err(Error::Incomplete),
                    }
                }
            }
            b'*' => {
                let len = get_decimal(src)?;

                for _ in 0..len {
                    Frame::check(src)?;
                }

                Ok(())
            }
            actual => Err(format!("protocol error; invalid frame type byte `{}`", actual).into()),
        }
    }

    /// The message has already been validated with `check`.
    pub fn parse(src: &mut Cursor<&[u8]>) -> Result<Frame, Error> {
        match get_u8(src)? {
            b'+' => {
                // Read the line and convert it to `Vec<u8>`
                let line = get_line(src)?.to_vec();

                // Convert the line to a String
                let string = String::from_utf8(line)?;

                Ok(Frame::Simple(string))
            }
            b'-' => {
                // Read the line and convert it to `Vec<u8>`
                let line = get_line(src)?.to_vec();

                // Convert the line to a String
                let string = String::from_utf8(line)?;

                Ok(Frame::Error(string))
            }
            b':' => {
                let len = get_decimal(src)?;
                Ok(Frame::Integer(len))
            }
            b'$' => {
                if b'-' == peek_u8(src)? {
                    let line = get_line(src)?;

                    if line != b"-1" {
                        return Err("protocol error; invalid frame format".into());
                    }

                    Ok(Frame::Null)
                } else {
                    // Read the bulk string
                    let len = get_decimal(src)?.try_into()?;

                    let data = Bytes::copy_from_slice(&src.chunk()[..len]);

                    skip(src, len)?;
                    match peek_u8(src) {
                        Ok(b'\r') => {
                            skip(src, 2)?;
                        }
                        _ => {}
                    }

                    Ok(Frame::Bulk(data))
                }
            }
            b'*' => {
                let len = get_decimal(src)?.try_into()?;
                let mut out = Vec::with_capacity(len);

                for _ in 0..len {
                    out.push(Frame::parse(src)?);
                }

                Ok(Frame::Array(out))
            }
            _ => unimplemented!(),
        }
    }

    /// Converts the frame to an "unexpected frame" error
    pub(crate) fn to_error(&self) -> anyhow::Error {
        anyhow::Error::new(std::fmt::Error).context(format!("unexpected frame: {}", self))
    }
}

impl PartialEq<&str> for Frame {
    fn eq(&self, other: &&str) -> bool {
        match self {
            Frame::Simple(s) => s.eq(other),
            Frame::Bulk(s) => s.eq(other),
            _ => false,
        }
    }
}

impl fmt::Display for Frame {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        use std::str;

        match self {
            Frame::Simple(response) => response.fmt(fmt),
            Frame::Error(msg) => write!(fmt, "error: {}", msg),
            Frame::Integer(num) => num.fmt(fmt),
            Frame::Bulk(msg) => match str::from_utf8(msg) {
                Ok(string) => string.fmt(fmt),
                Err(_) => write!(fmt, "{:?}", msg),
            },
            Frame::Null => "(nil)".fmt(fmt),
            Frame::OK => "OK".fmt(fmt),
            Frame::Array(parts) => {
                for (i, part) in parts.iter().enumerate() {
                    if i > 0 {
                        // use space as the array element display separator
                        write!(fmt, " ")?;
                    }

                    part.fmt(fmt)?;
                }

                Ok(())
            }
            Frame::RdbFile(_) => write!(fmt, "RDB file"),
        }
    }
}

fn peek_u8(src: &mut Cursor<&[u8]>) -> Result<u8, Error> {
    if !src.has_remaining() {
        return Err(Error::Incomplete);
    }

    Ok(src.chunk()[0])
}

fn get_u8(src: &mut Cursor<&[u8]>) -> Result<u8, Error> {
    if !src.has_remaining() {
        return Err(Error::Incomplete);
    }

    Ok(src.get_u8())
}

fn skip(src: &mut Cursor<&[u8]>, n: usize) -> Result<(), Error> {
    if src.remaining() < n {
        return Err(Error::Incomplete);
    }

    src.advance(n);
    Ok(())
}

/// Read a new-line terminated decimal
fn get_decimal(src: &mut Cursor<&[u8]>) -> Result<u64, Error> {
    use atoi::atoi;

    let line = get_line(src)?;

    atoi::<u64>(line).ok_or_else(|| "protocol error; invalid frame format".into())
}

/// Find a line
fn get_line<'a>(src: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], Error> {
    // Scan the bytes directly
    let start = src.position() as usize;
    // Scan to the second to last byte
    let end = src.get_ref().len() - 1;

    for i in start..end {
        if src.get_ref()[i] == b'\r' && src.get_ref()[i + 1] == b'\n' {
            // We found a line, update the position to be *after* the \n
            src.set_position((i + 2) as u64);

            // Return the line
            return Ok(&src.get_ref()[start..i]);
        }
    }

    Err(Error::Incomplete)
}

impl From<String> for Error {
    fn from(src: String) -> Error {
        Error::Other(anyhow::Error::msg(src))
    }
}

impl From<&str> for Error {
    fn from(src: &str) -> Error {
        src.to_string().into()
    }
}

impl From<FromUtf8Error> for Error {
    fn from(_src: FromUtf8Error) -> Error {
        "protocol error; invalid frame format".into()
    }
}

impl From<TryFromIntError> for Error {
    fn from(_src: TryFromIntError) -> Error {
        "protocol error; invalid frame format".into()
    }
}

impl std::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Incomplete => "stream ended early".fmt(fmt),
            Error::Other(err) => err.fmt(fmt),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn array() {
        assert_eq!(Frame::array(), Frame::Array(vec![]))
    }

    #[test]
    fn push_bulk_when_array() -> anyhow::Result<()> {
        let mut array = Frame::array();
        array.push_bulk(Bytes::from("hello".as_bytes()))?;

        match array {
            Frame::Array(vec) => {
                assert_eq!(vec.len(), 1);
                assert_eq!(vec[0], Frame::Bulk(Bytes::from("hello".as_bytes())));
            }
            _ => unreachable!(),
        }

        Ok(())
    }

    #[test]
    fn push_bulk_when_not_array() -> anyhow::Result<()> {
        let mut frame = Frame::Simple("hello".to_string());
        let result = frame.push_bulk(Bytes::from("world".as_bytes()));

        assert!(result.is_err());
        assert_eq!(frame, Frame::Simple("hello".to_string()));

        Ok(())
    }

    #[test]
    fn push_int_when_array() -> anyhow::Result<()> {
        let mut array = Frame::array();
        array.push_int(42)?;

        match array {
            Frame::Array(vec) => {
                assert_eq!(vec.len(), 1);
                assert_eq!(vec[0], Frame::Integer(42));
            }
            _ => unreachable!(),
        }

        Ok(())
    }

    #[test]
    fn push_int_when_not_array() -> anyhow::Result<()> {
        let mut frame = Frame::Simple("hello".to_string());
        let result = frame.push_int(42);

        assert!(result.is_err());
        assert_eq!(frame, Frame::Simple("hello".to_string()));

        Ok(())
    }

    #[test]
    fn check_simple() {
        let mut cursor: Cursor<&[u8]> = Cursor::new(b"+simple\r\n");
        let result = Frame::check(&mut cursor);
        assert!(result.is_ok());
    }

    #[test]
    fn check_error() {
        let mut cursor: Cursor<&[u8]> = Cursor::new(b"-myerror\r\n");
        let result = Frame::check(&mut cursor);
        assert!(result.is_ok());
    }

    #[test]
    fn check_integer() {
        let mut cursor: Cursor<&[u8]> = Cursor::new(b":42\r\n");
        let result = Frame::check(&mut cursor);
        assert!(result.is_ok());
    }

    #[test]
    fn check_bulk() {
        let mut cursor: Cursor<&[u8]> = Cursor::new(b"$5\r\nhello\r\n");
        let result = Frame::check(&mut cursor);
        assert!(result.is_ok());
    }

    #[test]
    fn check_array() {
        let mut cursor: Cursor<&[u8]> = Cursor::new(b"*2\r\n+simple\r\n:42\r\n");
        let result = Frame::check(&mut cursor);
        assert!(result.is_ok());
    }

    #[test]
    fn parse_simple() {
        let mut cursor: Cursor<&[u8]> = Cursor::new(b"+simple\r\n");
        let result = Frame::parse(&mut cursor);
        assert_eq!(result.unwrap(), Frame::Simple("simple".to_string()));
    }

    #[test]
    fn parse_bulk() {
        let mut cursor: Cursor<&[u8]> = Cursor::new(b"$5\r\nhello\r\n");
        let result = Frame::parse(&mut cursor);
        assert_eq!(
            result.unwrap(),
            Frame::Bulk(Bytes::from("hello".as_bytes()))
        );
    }

    #[test]
    fn parse_array() {
        let mut cursor: Cursor<&[u8]> = Cursor::new(b"*2\r\n+simple\r\n:42\r\n");
        let result = Frame::parse(&mut cursor);
        assert_eq!(
            result.unwrap(),
            Frame::Array(vec![
                Frame::Simple("simple".to_string()),
                Frame::Integer(42)
            ])
        );
    }

    #[test]
    fn parse_integer() {
        let mut cursor: Cursor<&[u8]> = Cursor::new(b":42\r\n");
        let result = Frame::parse(&mut cursor);
        assert_eq!(result.unwrap(), Frame::Integer(42));
    }
}

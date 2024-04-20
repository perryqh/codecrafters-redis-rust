use crate::{
    comms::Comms,
    frame::{self, Frame},
};

use anyhow::ensure;
use bytes::{Buf, BytesMut};
use std::io::{self, Cursor};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};

#[derive(Debug)]
pub struct Connection<R: AsyncReadExt + Unpin, W: AsyncWriteExt + Unpin> {
    writer: BufWriter<W>,
    reader: BufReader<R>,
    buffer: BytesMut,
    is_follower_receiving_sync_request: bool,
}

#[async_trait::async_trait]
impl<R: AsyncReadExt + Unpin + Send + Sync, W: AsyncWriteExt + Unpin + Send + Sync> Comms for Connection<R, W> {
    async fn write_frame(&mut self, frame: &Frame) -> io::Result<()> {
        match frame {
            Frame::Array(val) => {
                self.writer.write_u8(b'*').await?;

                self.write_decimal(val.len() as u64).await?;

                for entry in &**val {
                    self.write_value(entry).await?;
                }
            }
            _ => self.write_value(frame).await?,
        }

        self.writer.flush().await
    }

    async fn read_frame(&mut self) -> anyhow::Result<Option<Frame>> {
        loop {
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }

            if 0 == self.reader.read_buf(&mut self.buffer).await? {
                ensure!(self.buffer.is_empty(), "connection reset by peer");

                return Ok(None);
            }
        }
    }

    fn is_follower_receiving_sync_request(&self) -> bool {
        self.is_follower_receiving_sync_request
    }
}

impl<R: AsyncReadExt + Unpin, W: AsyncWriteExt + Unpin> Connection<R, W> {
    pub fn new(reader: R, writer: W, is_follower_receiving_sync_request: bool) -> Connection<R, W> {
        Connection {
            writer: BufWriter::new(writer),
            reader: BufReader::new(reader),
            buffer: BytesMut::with_capacity(4 * 1024),
            is_follower_receiving_sync_request,
        }
    }

    fn parse_frame(&mut self) -> anyhow::Result<Option<Frame>> {
        use frame::Error::Incomplete;
        let mut buf = Cursor::new(&self.buffer[..]);

        match Frame::check(&mut buf) {
            Ok(_) => {
                let len = buf.position() as usize;

                buf.set_position(0);

                let frame = Frame::parse(&mut buf)?;

                self.buffer.advance(len);

                Ok(Some(frame))
            }
            Err(Incomplete) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    async fn write_value(&mut self, frame: &Frame) -> io::Result<()> {
        match frame {
            Frame::Simple(val) => {
                self.writer.write_u8(b'+').await?;
                self.writer.write_all(val.as_bytes()).await?;
                self.writer.write_all(b"\r\n").await?;
            }
            Frame::Error(val) => {
                self.writer.write_u8(b'-').await?;
                self.writer.write_all(val.as_bytes()).await?;
                self.writer.write_all(b"\r\n").await?;
            }
            Frame::Integer(val) => {
                self.writer.write_u8(b':').await?;
                self.write_decimal(*val).await?;
            }
            Frame::Null => {
                self.writer.write_all(b"$-1\r\n").await?;
            }
            Frame::OK => {
                self.writer.write_all(b"+OK\r\n").await?;
            }
            Frame::Bulk(val) => {
                let len = val.len();

                self.writer.write_u8(b'$').await?;
                self.write_decimal(len as u64).await?;
                self.writer.write_all(val).await?;
                self.writer.write_all(b"\r\n").await?;
            }
            Frame::RdbFile(file_bytes) => {
                let len = file_bytes.len();

                self.writer.write_u8(b'$').await?;
                self.write_decimal(len as u64).await?;
                self.writer.write_all(file_bytes).await?;
                // no \r\n for rdb files
            }
            Frame::Array(_val) => unreachable!(),
        }

        Ok(())
    }

    async fn write_decimal(&mut self, val: u64) -> io::Result<()> {
        use std::io::Write;

        let mut buf = [0u8; 20];
        let mut buf = Cursor::new(&mut buf[..]);
        write!(&mut buf, "{}", val)?;

        let pos = buf.position() as usize;
        self.writer.write_all(&buf.get_ref()[..pos]).await?;
        self.writer.write_all(b"\r\n").await?;

        Ok(())
    }
}

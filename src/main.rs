// Uncomment this block to pass the first stage
use std::{io::Write, net::TcpListener};

use anyhow::Context;

fn main() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").context("unable to bind tcp listener")?;
    
    for stream in listener.incoming() {
         match stream {
             Ok(mut stream) => {
                 println!("accepted new connection");
                 let pong = b"+PONG\r\n";
                 stream.write_all(pong).context("unable to write to stream")?;
             }
             Err(e) => {
                 println!("error: {}", e);
             }
         }
     }
     Ok(())
}

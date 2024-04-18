use bytes::Bytes;
use once_cell::sync::Lazy;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::{connection::Connection, frame::Frame, store::Store};

static SUBSCRIBERS: Lazy<Mutex<Vec<Arc<Mutex<Connection>>>>> = Lazy::new(|| Mutex::new(Vec::new()));

pub enum Action {
    Set {
        key: Bytes,
        value: Bytes,
        expiry: Option<u64>,
    },
    //Remove{key: Bytes},
}

pub async fn publish(action: Action) -> anyhow::Result<()> {
    match action {
        Action::Set { key, value, expiry } => {
            let mut array = Frame::array();
            array.push_bulk(Bytes::from("set"))?;
            array.push_bulk(key.clone())?;
            array.push_bulk(value.clone())?;
            if let Some(expiry) = expiry {
                array.push_bulk("PX".into())?;
                array.push_bulk(expiry.to_string().into())?;
            }
            publish_frame(array).await
        }
    }
}

async fn publish_frame(frame: Frame) -> anyhow::Result<()> {
    let subscribers = SUBSCRIBERS.lock().await;
    for connection in subscribers.iter() {
        let mut connection_lock = connection.lock().await;
        connection_lock.write_frame(&frame).await?;
    }

    Ok(())
}

pub async fn add_connection(connection: Connection, store: &Store) -> anyhow::Result<()> {
    let mut subscribers = SUBSCRIBERS.lock().await;
    subscribers.push(Arc::new(Mutex::new(connection)));

    let rdb = store.as_rdb();
    let rdb = Frame::RdbFile(rdb);

    let mut connection = subscribers.last().unwrap().lock().await;
    connection
        .write_frame(&rdb)
        .await
        .map_err(anyhow::Error::from)
}

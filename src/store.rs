use bytes::Bytes;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

type Db = Arc<Mutex<HashMap<String, Bytes>>>;

#[derive(Clone, Debug, Default)]
pub struct Store {
    data: Db,
}

impl Store {
    pub fn new() -> Self {
        Self {
            data: Default::default(),
        }
    }

    pub fn set(&self, key: String, value: Bytes) {
        let mut data = self.data.lock().unwrap();
        data.insert(key, value);
    }

    pub fn get(&self, key: &str) -> Option<Bytes> {
        let data = self.data.lock().unwrap();
        data.get(key).cloned()
    }

    pub fn del(&self, key: &str) {
        let mut data = self.data.lock().unwrap();
        data.remove(key);
    }
}

// https://tokio.rs/tokio/tutorial/shared-state

use bytes::Bytes;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

struct ValueWithExpiry {
    value: Bytes,
    expiry: Instant,
}

type Db = Arc<Mutex<HashMap<String, ValueWithExpiry>>>;

#[derive(Clone, Default)]
pub struct Store {
    data: Db,
}

impl Store {
    pub fn new() -> Self {
        Self {
            data: Default::default(),
        }
    }

    pub fn set(&self, key: String, value: Bytes, expiry_duration: Duration) {
        let mut data = self.data.lock().unwrap();
        let expiry = Instant::now() + expiry_duration;
        data.insert(key, ValueWithExpiry { value, expiry });
    }

    pub fn get(&self, key: &str) -> Option<Bytes> {
        let mut data = self.data.lock().unwrap();
        if let Some(value_with_expiry) = data.get(key) {
            if Instant::now() < value_with_expiry.expiry {
                return Some(value_with_expiry.value.clone());
            } else {
                data.remove(key);
            }
        }
        None
    }

    pub fn del(&self, key: &str) {
        let mut data = self.data.lock().unwrap();
        data.remove(key);
    }
}

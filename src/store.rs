use bytes::Bytes;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Debug)]
struct ValueWithExpiry {
    value: Bytes,
    expiry: Instant,
}

type Db = Arc<Mutex<HashMap<Bytes, ValueWithExpiry>>>;

#[derive(Debug, Clone, Default)]
pub struct Store {
    data: Db,
}

pub const DEFAULT_EXPIRY: u64 = 1000 * 60 * 60 * 24 * 7; // 1 week

impl Store {
    pub fn new() -> Self {
        Self {
            data: Default::default(),
        }
    }

    pub fn set_with_default_expiry(&self, key: Bytes, value: Bytes) {
        self.set(key, value, Duration::from_secs(DEFAULT_EXPIRY));
    }

    pub fn set(&self, key: Bytes, value: Bytes, expiry_duration: Duration) {
        let mut data = self.data.lock().unwrap();
        let expiry = Instant::now() + expiry_duration;
        data.insert(key, ValueWithExpiry { value, expiry });
    }

    pub fn get(&self, key: Bytes) -> Option<Bytes> {
        let mut data = self.data.lock().unwrap();
        if let Some(value_with_expiry) = data.get(&key) {
            if Instant::now() < value_with_expiry.expiry {
                return Some(value_with_expiry.value.clone());
            } else {
                data.remove(&key);
            }
        }
        None
    }

    pub fn del(&self, key: Bytes) {
        let mut data = self.data.lock().unwrap();
        data.remove(&key);
    }
}

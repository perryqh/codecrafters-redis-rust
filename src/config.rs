#[derive(Debug)]
pub struct Config {
    pub bind_address: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bind_address: "127.0.0.1:6379".to_string(),
        }
    }
}

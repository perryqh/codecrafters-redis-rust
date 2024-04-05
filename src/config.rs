#[derive(Debug)]
pub struct Config {
    pub host: String,
    pub port: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: "6379".to_string(),
        }
    }
}

impl Config {
    pub fn bind_address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

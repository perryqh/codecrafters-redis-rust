use anyhow::Context;

use crate::store::Store;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Info {
    pub self_host: String,
    pub self_port: u16,
    pub replication: Replication,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Replication {
    pub role: String,
    pub replication_of_host: Option<String>,
    pub replication_of_port: Option<u16>,
}

impl Default for Info {
    fn default() -> Self {
        Self {
            self_host: DEFAULT_HOST.to_string(),
            self_port: DEFAULT_PORT,
            replication: Default::default(),
        }
    }
}

impl Default for Replication {
    fn default() -> Self {
        Self {
            role: DEFAULT_ROLE.to_string(),
            replication_of_host: None,
            replication_of_port: None,
        }
    }
}

const DEFAULT_ROLE: &str = "master";
const DEFAULT_HOST: &str = "127.0.0.1";
const DEFAULT_PORT: u16 = 6379;
const STORE_PREFIX: &str = "INFO:";

impl Info {
    pub fn new(self_host: String, self_port: u16, replication: Replication) -> Self {
        Self {
            self_host,
            self_port,
            replication,
        }
    }

    pub fn bind_address(&self) -> String {
        format!("{}:{}", self.self_host, self.self_port)
    }

    pub fn builder() -> InfoBuilder {
        InfoBuilder::default()
    }

    pub fn from_store(store: &Store) -> anyhow::Result<Self> {
        let self_host =
            if let Some(self_host) = store.get(format!("{}SELF_HOST", STORE_PREFIX).into()) {
                String::from_utf8(self_host.to_vec()).context("invalid self_host bytes")?
            } else {
                DEFAULT_HOST.to_string()
            };
        let self_port =
            if let Some(self_port) = store.get(format!("{}SELF_PORT", STORE_PREFIX).into()) {
                String::from_utf8(self_port.to_vec())
                    .context("invalid self_port bytes")?
                    .parse::<u16>()
                    .context("invalid self_port u16")?
            } else {
                DEFAULT_PORT
            };
        let replication_role = if let Some(replication_role) =
            store.get(format!("{}REPLICATION:ROLE", STORE_PREFIX).into())
        {
            String::from_utf8(replication_role.to_vec())
                .context("invalid replication_role bytes")?
        } else {
            DEFAULT_ROLE.to_string()
        };
        let replication_of_host = if let Some(replication_of_host) =
            store.get(format!("{}REPLICATION:REPLICATION_OF_HOST", STORE_PREFIX).into())
        {
            Some(
                String::from_utf8(replication_of_host.to_vec())
                    .context("invalid replication_of_host bytes")?,
            )
        } else {
            None
        };
        let replication_of_port = if let Some(replication_of_port) =
            store.get(format!("{}REPLICATION:REPLICATION_OF_PORT", STORE_PREFIX).into())
        {
            Some(
                String::from_utf8(replication_of_port.to_vec())
                    .context("invalid replication_of_port bytes")?
                    .parse::<u16>()
                    .context("invalid replication_of_port u16")?,
            )
        } else {
            None
        };
        let replication = Replication {
            role: replication_role,
            replication_of_host,
            replication_of_port,
        };

        Ok(Self {
            self_host,
            self_port,
            replication,
        })
    }

    pub fn write(&self, store: &Store) -> anyhow::Result<()> {
        store.set_with_default_expiry(
            format!("{}SELF_HOST", STORE_PREFIX).into(),
            self.self_host.clone().into(),
        );
        store.set_with_default_expiry(
            format!("{}SELF_PORT", STORE_PREFIX).into(),
            self.self_port.to_string().into(),
        );
        store.set_with_default_expiry(
            format!("{}REPLICATION:ROLE", STORE_PREFIX).into(),
            self.replication.role.clone().into(),
        );
        if let Some(replication_of_host) = &self.replication.replication_of_host {
            store.set_with_default_expiry(
                format!("{}REPLICATION:REPLICATION_OF_HOST", STORE_PREFIX).into(),
                replication_of_host.clone().into(),
            );
        }
        if let Some(replication_of_port) = &self.replication.replication_of_port {
            store.set_with_default_expiry(
                format!("{}REPLICATION:REPLICATION_OF_PORT", STORE_PREFIX).into(),
                replication_of_port.to_string().into(),
            );
        }
        Ok(())
    }
}

#[derive(Debug, Default, PartialEq)]
pub struct InfoBuilder {
    self_host: Option<String>,
    self_port: Option<u16>,
    replication_role: Option<String>,
    replication_of_host: Option<String>,
    replication_of_port: Option<u16>,
}

impl InfoBuilder {
    pub fn self_host(mut self, self_host: Option<String>) -> Self {
        if let Some(host) = self_host {
            self.self_host = Some(host);
        }
        self
    }

    pub fn self_port(mut self, self_port: Option<u16>) -> Self {
        if let Some(port) = self_port {
            self.self_port = Some(port);
        }
        self
    }

    pub fn replication_role(mut self, replication_role: Option<String>) -> Self {
        if let Some(role) = replication_role {
            self.replication_role = Some(role);
        }
        self
    }

    pub fn replication_of_host(mut self, replication_of_host: Option<String>) -> Self {
        if let Some(host) = replication_of_host {
            self.replication_of_host = Some(host);
        }
        self
    }

    pub fn replication_of_port(mut self, replication_of_port: Option<u16>) -> Self {
        if let Some(port) = replication_of_port {
            self.replication_of_port = Some(port);
        }
        self
    }

    pub fn build(self) -> Info {
        Info {
            self_host: self.self_host.unwrap_or_else(|| DEFAULT_HOST.to_string()),
            self_port: self.self_port.unwrap_or(DEFAULT_PORT),
            replication: Replication {
                role: self
                    .replication_role
                    .unwrap_or_else(|| DEFAULT_ROLE.to_string()),
                replication_of_host: self.replication_of_host,
                replication_of_port: self.replication_of_port,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_info_default() -> anyhow::Result<()> {
        let info = Info::default();
        assert_eq!(info.self_host, "127.0.0.1");
        assert_eq!(info.self_port, 6379);
        assert_eq!(info.replication.role, "master");
        assert_eq!(info.replication.replication_of_host, None);
        assert_eq!(info.replication.replication_of_port, None);
        Ok(())
    }

    #[test]
    fn test_info_new() -> anyhow::Result<()> {
        let info = Info::new("127.0.0.1".to_string(), 6380, Replication::default());
        assert_eq!(info.self_host, "127.0.0.1");
        assert_eq!(info.self_port, 6380);
        assert_eq!(info.replication.role, "master");
        assert_eq!(info.replication.replication_of_host, None);
        assert_eq!(info.replication.replication_of_port, None);
        Ok(())
    }

    #[test]
    fn test_info_write() -> anyhow::Result<()> {
        let info = Info {
            self_host: "localhost".to_string(),
            self_port: 1234,
            replication: Replication {
                role: "slave".to_string(),
                replication_of_host: Some("master.host".to_string()),
                replication_of_port: Some(5678),
            },
        };
        let store = Store::new();
        info.write(&store)?;

        let saved_info = Info::from_store(&store)?;
        assert_eq!(saved_info, info);

        Ok(())
    }
}

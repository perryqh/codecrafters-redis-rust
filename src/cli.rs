use clap::Parser;

use crate::info::Info;

#[derive(Parser, Debug)]
#[clap(name = "redis-rust", version, author, about = "A limited Redis server")]
pub struct Cli {
    #[clap(short, long, default_value = "6379")]
    pub port: u16,

    #[clap(long, value_delimiter = ' ', num_args = 2)]
    pub replicaof: Option<Vec<String>>,
}

impl Cli {
    pub fn to_info(&self) -> Info {
        Info::builder()
            .self_port(Some(self.port))
            .replication_of_host(self.replicaof.as_ref().map(|v| v[0].clone()))
            .replication_of_port(self.replicaof.as_ref().and_then(|v| v[1].parse().ok()))
            .build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_port() {
        let cli = Cli::parse();
        assert_eq!(cli.port, 6379);
    }

    #[test]
    fn test_custom_port() {
        let cli = Cli::parse_from(&["redis-rust", "--port", "1234"]);
        assert_eq!(cli.port, 1234);
    }

    #[test]
    fn test_with_invalid_port() {
        let result = Cli::try_parse_from(&["redis-rust", "--port", "not-a-number"]);
        assert!(result.is_err());
    }

    #[test]
    fn test_replica_of() {
        let cli = Cli::parse_from(&["redis-rust", "--replicaof", "host.com", "4321"]);
        assert_eq!(
            cli.replicaof,
            Some(vec!["host.com".to_string(), "4321".to_string()])
        );
    }

    #[test]
    fn test_to_info() {
        let cli = Cli::parse_from(&[
            "redis-rust",
            "--port",
            "1234",
            "--replicaof",
            "host.com",
            "4321",
        ]);
        let info = cli.to_info();
        assert_eq!(info.self_port, 1234);
        assert_eq!(
            info.replication.replication_of_host,
            Some("host.com".to_string())
        );
        assert_eq!(info.replication.replication_of_port, Some(4321));
    }
}

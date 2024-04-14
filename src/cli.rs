use clap::Parser;

#[derive(Parser, Debug)]
#[clap(name = "redis-rust", version, author, about = "A limited Redis server")]
struct Cli {
    #[clap(short, long, default_value = "6379")]
    port: u16,

    #[clap(long, value_delimiter = ' ', num_args = 2)]
    replicaof: Option<Vec<String>>,
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
}

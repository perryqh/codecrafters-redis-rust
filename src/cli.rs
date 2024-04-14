use clap::Parser;

#[derive(Parser, Debug)]
#[clap(name = "redis-rust", version, author, about = "A limited Redis server")]
struct Cli {
    #[clap(short, long, default_value = "6379")]
    port: u16,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_port() {
        let cli = Cli::parse();
        assert_eq!(cli.port, 6379);
    }
}

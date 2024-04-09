use anyhow::{bail, Context};

use crate::info::{Info, DEFAULT_MASTER_REPLID};

#[derive(Debug, Default, PartialEq)]
pub struct RedisArgs {
    port: Option<u16>,
    replicaof: Option<(String, u16)>,
}

impl RedisArgs {
    pub fn parse(args: Vec<String>) -> anyhow::Result<Self> {
        let mut args = args.into_iter();
        let mut port = None;
        let mut replicaof = None;
        while let Some(arg) = args.next() {
            if arg == "--port" {
                let p = match args.next() {
                    Some(port_str) => port_str.parse::<u16>().context("Invalid port")?,
                    None => bail!("port expected"),
                };
                port = Some(p);
            } else if arg == "--replicaof" {
                let host = args.next().context("replicaof host expected")?;
                let port_str = args.next().context("replicaof port expected")?;
                let port = port_str.parse::<u16>().context("Invalid replicaof port")?;
                replicaof = Some((host, port));
            }
        }
        Ok(RedisArgs { port, replicaof })
    }

    pub fn to_info(&self) -> Info {
        let mut info = Info::builder();
        if let Some(port) = self.port {
            info = info.self_port(Some(port));
        }
        info = info.self_host(Some("127.0.0.1".to_string()));
        if let Some(replicaof) = &self.replicaof {
            info = info
                .replication_role(Some("slave".to_string()))
                .replication_of_host(Some(replicaof.0.clone()))
                .replication_of_port(Some(replicaof.1));
        } else {
            info = info
                .replication_role(Some("master".to_string()))
                .master_replid(Some(DEFAULT_MASTER_REPLID.to_string()))
                .master_repl_offset(Some(0));
        }
        info.build()
    }
}

#[cfg(test)]
mod tests {
    use crate::redis_args::RedisArgs;

    #[test]
    fn without_args() -> anyhow::Result<()> {
        let args = RedisArgs::parse(vec!["self".to_string()])?;
        assert_eq!(args, RedisArgs::default());
        Ok(())
    }

    #[test]
    fn with_port_arg() -> anyhow::Result<()> {
        let args = RedisArgs::parse(vec![
            "self".to_string(),
            "--port".to_string(),
            "1234".to_string(),
        ])?;
        assert_eq!(args.port, Some(1234));
        assert_eq!(args.replicaof, None);
        Ok(())
    }

    #[test]
    fn with_replicaof_arg() -> anyhow::Result<()> {
        let args = RedisArgs::parse(vec![
            "self".to_string(),
            "--replicaof".to_string(),
            "host.com".to_string(),
            "1234".to_string(),
        ])?;
        assert_eq!(args.port, None);
        assert_eq!(args.replicaof, Some(("host.com".to_string(), 1234)));
        Ok(())
    }

    #[test]
    fn with_port_and_replicaof_args() -> anyhow::Result<()> {
        let args = RedisArgs::parse(vec![
            "self".to_string(),
            "--port".to_string(),
            "1234".to_string(),
            "--replicaof".to_string(),
            "host.com".to_string(),
            "5678".to_string(),
        ])?;
        assert_eq!(args.port, Some(1234));
        assert_eq!(args.replicaof, Some(("host.com".to_string(), 5678)));
        Ok(())
    }

    #[test]
    fn with_port_and_replicaof_args_with_invalid_port() -> anyhow::Result<()> {
        let args = RedisArgs::parse(vec![
            "self".to_string(),
            "--port".to_string(),
            "1234".to_string(),
            "--replicaof".to_string(),
            "host.com".to_string(),
            "invalid".to_string(),
        ]);
        assert!(args.is_err());
        Ok(())
    }

    #[test]
    fn with_invalid_port_and_replicaof_args_with_valid_replicaof_port() -> anyhow::Result<()> {
        let args = RedisArgs::parse(vec![
            "self".to_string(),
            "--port".to_string(),
            "invalid".to_string(),
            "--replicaof".to_string(),
            "host.com".to_string(),
            "invalid".to_string(),
        ]);
        assert!(args.is_err());
        Ok(())
    }
}

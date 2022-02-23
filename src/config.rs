use std::{
    collections::HashMap,
    env,
    net::{IpAddr, SocketAddr, ToSocketAddrs},
    process::exit,
    str::FromStr,
};

use store_rocksdb::RocksDBStoreConfig;
use tracing::error;

pub struct EnvSettings {
    pub args: HashMap<String, String>,
}

impl Default for EnvSettings {
    fn default() -> Self {
        Self::new()
    }
}

impl EnvSettings {
    pub fn new() -> Self {
        let mut args = HashMap::new();
        let mut current_key: Option<String> = None;

        for arg in env::args().into_iter().skip(1) {
            if arg.contains('=') {
                let mut parts = arg.splitn(2, '=');
                let key = parts.next().unwrap();
                let value = parts.next().unwrap();

                if let Some(key) = key.strip_prefix("--") {
                    args.insert(key.to_lowercase(), value.to_string());
                } else {
                    error!("Invalid command line argument: {}", key);
                    exit(1);
                }
            } else if let Some(key) = std::mem::take(&mut current_key) {
                args.insert(key, arg);
            } else if let Some(key) = arg.strip_prefix("--") {
                current_key = Some(key.to_lowercase());
            } else {
                error!("Invalid command line argument: {}", arg);
                exit(1);
            }
        }

        EnvSettings { args }
    }

    pub fn get(&self, name: &str) -> Option<String> {
        if let Some(value) = self.args.get(name) {
            Some(value.clone())
        } else if let Ok(value) = env::var(name.replace("-", "_").to_uppercase()) {
            Some(value)
        } else {
            None
        }
    }

    pub fn parse<T>(&self, name: &str) -> Option<T>
    where
        T: FromStr,
    {
        if let Some(value) = self.get(name) {
            if let Ok(value) = value.parse::<T>() {
                Some(value)
            } else {
                error!("Failed to parse environment variable: {}", name);
                exit(1);
            }
        } else {
            None
        }
    }

    pub fn parse_list(&self, name: &str) -> Option<Vec<String>> {
        if let Some(value) = self.get(name) {
            value
                .split(if value.contains(';') { ';' } else { ',' })
                .map(|v| v.to_string())
                .collect::<Vec<String>>()
                .into()
        } else {
            None
        }
    }

    pub fn parse_ipaddr(&self, name: &str, default: &str) -> IpAddr {
        self.get(name)
            .unwrap_or_else(|| default.to_string())
            .parse()
            .map_err(|e| {
                error!("Failed to parse address in parameter '{}': {}", name, e);
                std::process::exit(1);
            })
            .unwrap()
    }

    pub fn parse_socketaddr(&self, name: &str, default: &str) -> SocketAddr {
        if let Some(value) = self.get(name) {
            value
                .to_socket_addrs()
                .map_err(|e| {
                    error!("Failed to parse address in parameter '{}': {}", name, e);
                    std::process::exit(1);
                })
                .unwrap()
                .next()
                .unwrap_or_else(|| {
                    error!("Failed to parse address in parameter '{}'.", name);
                    std::process::exit(1);
                })
        } else {
            default.to_socket_addrs().unwrap().next().unwrap()
        }
    }
}

impl From<&EnvSettings> for RocksDBStoreConfig {
    fn from(settings: &EnvSettings) -> Self {
        RocksDBStoreConfig::default_config(
            &settings
                .get("db-path")
                .unwrap_or_else(|| "stalwart-jmap".to_string()),
        )
    }
}

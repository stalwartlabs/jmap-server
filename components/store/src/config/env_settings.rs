use std::{
    collections::HashMap,
    env,
    net::{IpAddr, SocketAddr, ToSocketAddrs},
    str::FromStr,
};

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
                    panic!("Invalid command line argument: {}", key);
                }
            } else if let Some(key) = std::mem::take(&mut current_key) {
                args.insert(key, arg);
            } else if let Some(key) = arg.strip_prefix("--") {
                current_key = Some(key.to_lowercase());
            } else {
                panic!("Invalid command line argument: {}", arg);
            }
        }

        EnvSettings { args }
    }

    pub fn get(&self, name: &str) -> Option<String> {
        if let Some(value) = self.args.get(name) {
            Some(value.clone())
        } else if let Ok(value) = env::var(name.replace('-', "_").to_uppercase()) {
            Some(value)
        } else {
            None
        }
    }

    pub fn contains_key(&self, name: &str) -> bool {
        self.args.contains_key(name) || env::var(name.replace('-', "_").to_uppercase()).is_ok()
    }

    pub fn parse<T>(&self, name: &str) -> Option<T>
    where
        T: FromStr,
    {
        if let Some(value) = self.get(name) {
            if let Ok(value) = value.parse::<T>() {
                Some(value)
            } else {
                panic!("Failed to parse environment variable: {}", name);
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
                panic!("Failed to parse address in parameter '{}': {}", name, e);
            })
            .unwrap()
    }

    pub fn parse_socketaddr(&self, name: &str, default: &str) -> SocketAddr {
        if let Some(value) = self.get(name) {
            value
                .to_socket_addrs()
                .map_err(|e| {
                    panic!("Failed to parse address in parameter '{}': {}", name, e);
                })
                .unwrap()
                .next()
                .unwrap_or_else(|| {
                    panic!("Failed to parse address in parameter '{}'.", name);
                })
        } else {
            default.to_socket_addrs().unwrap().next().unwrap()
        }
    }

    pub fn set_value(&mut self, name: String, value: String) {
        self.args.insert(name, value);
    }
}

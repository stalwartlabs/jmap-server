use std::{
    env,
    io::BufRead,
    net::{IpAddr, SocketAddr, ToSocketAddrs},
    str::FromStr,
};

use ahash::AHashMap;

#[derive(Debug)]
pub struct EnvSettings {
    pub args: AHashMap<String, String>,
}

impl Default for EnvSettings {
    fn default() -> Self {
        Self::new()
    }
}

impl EnvSettings {
    pub fn new() -> Self {
        let mut args = AHashMap::default();
        let mut current_key: Option<String> = None;

        for arg in env::args().into_iter().skip(1) {
            if let Some((key, value)) = arg.split_once('=') {
                if let Some(key) = key.strip_prefix("--") {
                    args.insert(key.to_lowercase(), value.to_string());
                } else {
                    soft_panic(&format!("Invalid command line argument: {}", key));
                }
            } else if let Some(key) = std::mem::take(&mut current_key) {
                args.insert(key, arg);
            } else if let Some(key) = arg.strip_prefix("--") {
                current_key = Some(key.to_lowercase());
            } else {
                soft_panic(&format!("Invalid command line argument: {}", arg));
            }
        }

        // Read config file if it was provided
        if let Some(config_path) = args.remove("config") {
            std::fs::read(&config_path)
                .unwrap_or_else(|err| {
                    soft_panic(&format!(
                        "Failed to read config file {}: {}",
                        config_path, err
                    ));
                })
                .lines()
                .for_each(|line| {
                    let line = line.unwrap_or_else(|err| {
                        soft_panic(&format!(
                            "Failed to read config file {}: {}",
                            config_path, err
                        ));
                    });
                    let line = line.trim();
                    if !line.is_empty() && !line.starts_with('#') {
                        if let Some((key, value)) = line.split_once(':') {
                            let key = key.trim();
                            if !args.contains_key(key) {
                                let value = value
                                    .rsplit_once(" #")
                                    .or_else(|| value.split_once("\t#"))
                                    .map(|v| v.0)
                                    .unwrap_or(value)
                                    .trim();

                                if !value.is_empty() {
                                    args.insert(key.to_string(), value.to_string());
                                }
                            }
                        } else {
                            soft_panic(&format!("Invalid config file line: {}", line));
                        }
                    }
                });
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
                soft_panic(&format!("Failed to parse argument: {}", name));
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
                soft_panic(&format!(
                    "Failed to parse address in parameter '{}': {}",
                    name, e
                ));
            })
            .unwrap()
    }

    pub fn parse_socketaddr(&self, name: &str, default: &str) -> SocketAddr {
        if let Some(value) = self.get(name) {
            value
                .to_socket_addrs()
                .map_err(|e| {
                    soft_panic(&format!(
                        "Failed to parse address in parameter '{}': {}",
                        name, e
                    ));
                })
                .unwrap()
                .next()
                .unwrap_or_else(|| {
                    soft_panic(&format!("Failed to parse address in parameter '{}'.", name));
                })
        } else {
            default.to_socket_addrs().unwrap().next().unwrap()
        }
    }

    pub fn set_value(&mut self, name: String, value: String) {
        self.args.insert(name, value);
    }
}

pub fn soft_panic(message: &str) -> ! {
    println!("{}", message);
    std::process::exit(1);
}

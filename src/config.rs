use std::fs;
use std::net::IpAddr;
use std::path::Path;

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub data_dir: String,
    pub bind_addr: String,
    #[serde(default = "default_tick_ms")]
    pub tick_ms: u64,
    #[serde(default = "default_ready_channel_capacity")]
    pub ready_channel_capacity: usize,
    #[serde(default = "default_connection_write_channel")]
    pub connection_write_channel: usize,
    #[serde(default = "default_retry_delay_ms")]
    pub retry_delay_ms: u64,
    #[serde(default = "default_allowed_hosts")]
    pub allowed_hosts: Vec<String>,
}

impl Config {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let raw = fs::read_to_string(&path)
            .with_context(|| format!("read config file: {}", path.as_ref().display()))?;
        let mut cfg: Config = serde_json::from_str(&raw).context("parse config json")?;
        if cfg.allowed_hosts.is_empty() {
            cfg.allowed_hosts = default_allowed_hosts();
        }
        Ok(cfg)
    }

    pub fn allows_ip(&self, ip: IpAddr) -> bool {
        if ip.is_loopback() {
            return true;
        }
        self.allowed_hosts
            .iter()
            .filter_map(|addr| addr.parse::<IpAddr>().ok())
            .any(|allowed| allowed == ip)
    }
}

fn default_tick_ms() -> u64 {
    10
}

fn default_ready_channel_capacity() -> usize {
    2048
}

fn default_connection_write_channel() -> usize {
    256
}

fn default_retry_delay_ms() -> u64 {
    1_000
}

fn default_allowed_hosts() -> Vec<String> {
    vec!["127.0.0.1".into(), "::1".into()]
}

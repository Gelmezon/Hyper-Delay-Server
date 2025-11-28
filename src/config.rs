use std::fs;
use std::net::IpAddr;
use std::path::Path;

use anyhow::{Context, Result};
use serde::Deserialize;

// 服务运行配置，从 config.json 读取。字段均提供合理默认值，便于快速启动。
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    // 数据持久化目录（sled 存储路径）
    pub data_dir: String,
    // gRPC 监听地址（建议在容器内配置为 0.0.0.0:7000）
    pub bind_addr: String,
    #[serde(default = "default_tick_ms")]
    // 时间轮推进的 tick 间隔（毫秒）
    pub tick_ms: u64,
    #[serde(default = "default_ready_channel_capacity")]
    // 就绪任务通道容量（任务到达后进入该通道分发）
    pub ready_channel_capacity: usize,
    #[serde(default = "default_connection_write_channel")]
    // 每个连接的出站消息通道容量（gRPC 服务器到客户端）
    pub connection_write_channel: usize,
    #[serde(default = "default_retry_delay_ms")]
    // 回调失败后的重试基础延迟（毫秒）
    pub retry_delay_ms: u64,
    #[serde(default = "default_max_delay_days")]
    // 允许的最大延迟天数（默认 30 天），用于限制过远的 fire_at，避免资源占用与异常数据
    pub max_delay_days: u64,
    #[serde(default = "default_max_delivery_retries")]
    // 投递失败最大重试次数（超过后不再重试，任务可选择进入死信处理，当前实现仅日志）
    pub max_delivery_retries: u32,
    #[serde(default = "default_allowed_hosts")]
    // 允许连接的来源 IP 白名单（字符串形式），不在列表将被拒绝
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
        // 本地回环地址默认放行，便于本机部署与调试
        if ip.is_loopback() {
            return true;
        }
        self.allowed_hosts
            .iter()
            .filter_map(|addr| addr.parse::<IpAddr>().ok())
            .any(|allowed| allowed == ip)
    }

    // 将最大延迟从“天”为单位转换为“毫秒”
    pub fn max_delay_ms(&self) -> u64 {
        // days * 24h * 60m * 60s * 1000ms
        self.max_delay_days.saturating_mul(24).saturating_mul(60).saturating_mul(60).saturating_mul(1000)
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

fn default_max_delay_days() -> u64 {
    30
}

fn default_max_delivery_retries() -> u32 {
    10
}
fn default_allowed_hosts() -> Vec<String> {
    vec!["127.0.0.1".into(), "::1".into()]
}

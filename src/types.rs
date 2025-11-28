use anyhow::Context;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

use crate::pb::delay;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Compression {
    None,
    Lz4,
}

impl Default for Compression {
    fn default() -> Self {
        Compression::None
    }
}

#[allow(dead_code)]
impl Compression {
    pub fn compress(&self, payload: &[u8]) -> anyhow::Result<Vec<u8>> {
        match self {
            Compression::None => Ok(payload.to_vec()),
            Compression::Lz4 => Ok(lz4_flex::block::compress_prepend_size(payload)),
        }
    }

    pub fn decompress(&self, payload: &[u8]) -> anyhow::Result<Vec<u8>> {
        match self {
            Compression::None => Ok(payload.to_vec()),
            Compression::Lz4 => lz4_flex::block::decompress_size_prepended(payload)
                .map_err(|e| anyhow::anyhow!("lz4 decompress: {e}")),
        }
    }
}

impl From<delay::Compression> for Compression {
    fn from(value: delay::Compression) -> Self {
        match value {
            delay::Compression::Lz4 => Compression::Lz4,
            _ => Compression::None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ScheduledTask {
    pub id: Uuid,
    pub fire_at_epoch_ms: u64,
    pub payload: Bytes,
    pub compression: Compression,
    pub route_key: String,
    pub attempts: u32, // 已尝试投递次数（成功投递前递增）
}

impl ScheduledTask {
    pub fn from_proto(req: &delay::ScheduleRequest, connection_id: u64) -> anyhow::Result<Self> {
        let id = if req.id.is_empty() {
            Uuid::new_v4()
        } else {
            Uuid::parse_str(&req.id).context("parse request id")?
        };
        let route_key = if req.route_key.is_empty() {
            format!("conn-{connection_id}")
        } else {
            req.route_key.clone()
        };
        Ok(Self {
            id,
            fire_at_epoch_ms: req.fire_at_epoch_ms,
            payload: Bytes::from(req.payload.clone()),
            compression: Compression::from(req.compression()),
            route_key,
            attempts: 0,
        })
    }

    pub fn to_persisted(&self) -> PersistedTask {
        PersistedTask {
            id: self.id,
            fire_at_epoch_ms: self.fire_at_epoch_ms,
            payload: self.payload.clone().to_vec(),
            compression: self.compression.clone(),
            route_key: self.route_key.clone(),
            attempts: self.attempts,
        }
    }

    pub fn cloned_with_delay(&self, delay: Duration) -> Self {
        let next_fire = now_epoch_ms() + delay.as_millis() as u64;
        Self {
            id: self.id,
            fire_at_epoch_ms: next_fire,
            payload: self.payload.clone(),
            compression: self.compression.clone(),
            route_key: self.route_key.clone(),
            attempts: self.attempts + 1,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedTask {
    pub id: Uuid,
    pub fire_at_epoch_ms: u64,
    #[serde(with = "serde_bytes")]
    pub payload: Vec<u8>,
    pub compression: Compression,
    pub route_key: String,
    #[serde(default)]
    pub attempts: u32,
}

impl PersistedTask {
    pub fn into_scheduled(self, _connection_id: u64) -> ScheduledTask {
        ScheduledTask {
            id: self.id,
            fire_at_epoch_ms: self.fire_at_epoch_ms,
            payload: Bytes::from(self.payload),
            compression: self.compression,
            route_key: self.route_key,
            attempts: self.attempts,
        }
    }

    pub fn into_scheduled_requeued(self) -> Arc<ScheduledTask> {
        Arc::new(self.into_scheduled(0))
    }
}

pub fn now_epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_millis() as u64
}

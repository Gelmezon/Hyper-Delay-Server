use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleRequest {
    #[serde(default = "Uuid::nil", skip_serializing_if = "Uuid::is_nil")]
    pub id: Uuid,
    pub fire_at_epoch_ms: u64,
    #[serde(with = "serde_bytes")]
    pub payload: Vec<u8>,
    #[serde(default)]
    pub compression: Option<Compression>,
    #[serde(default)]
    pub route_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AckResponse {
    pub id: Uuid,
    pub fire_at_epoch_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterAck {
    pub route_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliveryPayload {
    pub id: Uuid,
    pub fire_at_epoch_ms: u64,
    #[serde(with = "serde_bytes")]
    pub payload: ByteBuf,
    pub compression: Compression,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterRequest {
    pub route_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum InboundMessage {
    Schedule(ScheduleRequest),
    Register(RegisterRequest),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OutboundMessage {
    Ack(AckResponse),
    Delivery(DeliveryPayload),
    Error(ErrorResponse),
    Registered(RegisterAck),
}

#[derive(Debug, Clone)]
pub struct ScheduledTask {
    pub id: Uuid,
    pub fire_at_epoch_ms: u64,
    pub payload: Bytes,
    pub compression: Compression,
    pub route_key: String,
}

impl ScheduledTask {
    pub fn from_request(req: ScheduleRequest, connection_id: u64) -> anyhow::Result<Self> {
        let id = if req.id.is_nil() {
            Uuid::new_v4()
        } else {
            req.id
        };
        let compression = req.compression.unwrap_or_default();
        let bytes = Bytes::from(req.payload);
        let route_key = req
            .route_key
            .unwrap_or_else(|| format!("conn-{connection_id}"));
        Ok(Self {
            id,
            fire_at_epoch_ms: req.fire_at_epoch_ms,
            payload: bytes,
            compression,
            route_key,
        })
    }

    pub fn to_persisted(&self) -> PersistedTask {
        PersistedTask {
            id: self.id,
            fire_at_epoch_ms: self.fire_at_epoch_ms,
            payload: self.payload.clone().to_vec(),
            compression: self.compression.clone(),
            route_key: self.route_key.clone(),
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
}

impl PersistedTask {
    pub fn into_scheduled(self, _connection_id: u64) -> ScheduledTask {
        ScheduledTask {
            id: self.id,
            fire_at_epoch_ms: self.fire_at_epoch_ms,
            payload: Bytes::from(self.payload),
            compression: self.compression,
            route_key: self.route_key,
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

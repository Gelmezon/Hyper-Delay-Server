use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use futures_core::Stream;
use tokio::sync::{RwLock, mpsc};
use tokio_stream::{StreamExt, wrappers::ReceiverStream};
use tonic::{Request, Response, Status};
use tracing::{info, warn};

use crate::config::Config;
use crate::pb::delay;
use crate::persistence::SledStorage;
use crate::timer::TimerWheel;
use crate::types::{Compression, ScheduledTask};
use uuid::Uuid;

type ConnectStream =
    Pin<Box<dyn Stream<Item = Result<delay::ServerMessage, Status>> + Send + Sync + 'static>>;

#[derive(Default)]
struct RegistryState {
    connections: HashMap<u64, mpsc::Sender<delay::ServerMessage>>,
    routes: HashMap<String, u64>,
}

#[derive(Clone)]
pub struct ConnectionRegistry {
    inner: Arc<RwLock<RegistryState>>,
}

impl ConnectionRegistry {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(RegistryState::default())),
        }
    }

    pub async fn register_connection(&self, id: u64, tx: mpsc::Sender<delay::ServerMessage>) {
        self.inner.write().await.connections.insert(id, tx);
    }

    pub async fn bind_route(&self, route_key: String, id: u64) {
        self.inner.write().await.routes.insert(route_key, id);
    }

    pub async fn unregister_connection(&self, id: u64) {
        let mut guard = self.inner.write().await;
        guard.connections.remove(&id);
        guard.routes.retain(|_, conn_id| *conn_id != id);
    }

    pub async fn send_to_connection(&self, id: u64, message: delay::ServerMessage) -> Result<()> {
        let sender = { self.inner.read().await.connections.get(&id).cloned() };
        if let Some(tx) = sender {
            tx.send(message).await.context("send to connection channel")
        } else {
            bail!("connection {id} not found")
        }
    }

    pub async fn send_to_route(
        &self,
        route_key: &str,
        message: delay::ServerMessage,
    ) -> Result<()> {
        let sender = {
            let guard = self.inner.read().await;
            guard
                .routes
                .get(route_key)
                .and_then(|id| guard.connections.get(id))
                .cloned()
        };
        if let Some(tx) = sender {
            tx.send(message).await.context("send to route channel")
        } else {
            bail!("route {route_key} not found")
        }
    }
}

pub struct ConnectionHub {
    wheel: Arc<TimerWheel>,
    storage: Arc<SledStorage>,
    registry: ConnectionRegistry,
    id_seq: AtomicU64,
    config: Arc<Config>,
}

impl ConnectionHub {
    pub fn new(wheel: Arc<TimerWheel>, storage: Arc<SledStorage>, config: Arc<Config>) -> Self {
        Self {
            wheel,
            storage,
            registry: ConnectionRegistry::new(),
            id_seq: AtomicU64::new(0),
            config,
        }
    }

    pub fn registry(&self) -> ConnectionRegistry {
        self.registry.clone()
    }

    pub fn next_connection_id(&self) -> u64 {
        self.id_seq.fetch_add(1, Ordering::Relaxed) + 1
    }

    pub async fn process_schedule(
        &self,
        req: delay::ScheduleRequest,
        connection_id: u64,
    ) -> Result<()> {
        // 基本校验：禁止过去时间与超过最大允许延迟的任务
        let now = crate::types::now_epoch_ms();
        if req.fire_at_epoch_ms <= now {
            warn!(connection_id, fire_at = req.fire_at_epoch_ms, now, "reject schedule: fire_at in the past");
            // 当 proto 重新生成后，ErrorResponse 将包含 code+message 字段
            let err = delay::ServerMessage { message: Some(delay::server_message::Message::Error(delay::ErrorResponse { code: "INVALID_FIRE_AT".into(), message: "fire_at in the past".into() })) };
            let _ = self.registry.send_to_connection(connection_id, err).await;
            bail!("fire_at in the past");
        }
        let max_ms = self.config.max_delay_ms();
        if req.fire_at_epoch_ms - now > max_ms {
            warn!(connection_id, fire_at = req.fire_at_epoch_ms, now, max_ms, "reject schedule: fire_at exceeds max delay");
            let err = delay::ServerMessage { message: Some(delay::server_message::Message::Error(delay::ErrorResponse { code: "EXCEEDS_MAX_DELAY".into(), message: "fire_at exceeds max delay".into() })) };
            let _ = self.registry.send_to_connection(connection_id, err).await;
            bail!("fire_at exceeds max delay");
        }
        let task = ScheduledTask::from_proto(&req, connection_id)?;
        self.registry
            .bind_route(task.route_key.clone(), connection_id)
            .await;
        self.storage.persist_task(&task).await?;
        self.wheel.schedule_task(Arc::new(task.clone())).await;
        let ack = delay::ServerMessage {
            message: Some(delay::server_message::Message::Ack(delay::AckResponse {
                id: task.id.to_string(),
                fire_at_epoch_ms: task.fire_at_epoch_ms,
            })),
        };
        self.registry.send_to_connection(connection_id, ack).await?;
        Ok(())
    }

    pub async fn handle_register(
        &self,
        req: delay::RegisterRequest,
        connection_id: u64,
    ) -> Result<()> {
        self.registry
            .bind_route(req.route_key.clone(), connection_id)
            .await;
        let ack = delay::ServerMessage {
            message: Some(delay::server_message::Message::Registered(
                delay::Registered {
                    route_key: req.route_key,
                },
            )),
        };
        self.registry.send_to_connection(connection_id, ack).await?;
        Ok(())
    }

    pub async fn handle_ack(&self, req: delay::AckRequest) -> Result<()> {
        let id = Uuid::parse_str(&req.id).context("parse ack id")?;
        self.storage.remove_task(&id).await?;
        Ok(())
    }
}

pub fn spawn_ready_dispatcher(
    mut ready_rx: mpsc::Receiver<Arc<ScheduledTask>>,
    registry: ConnectionRegistry,
    wheel: Arc<TimerWheel>,
    retry_delay_ms: u64,
) {
    let retry_delay = Duration::from_millis(retry_delay_ms);
    tokio::spawn(async move {
        while let Some(task) = ready_rx.recv().await {
            let message = delay::ServerMessage {
                message: Some(delay::server_message::Message::Delivery(delay::Delivery {
                    id: task.id.to_string(),
                    fire_at_epoch_ms: task.fire_at_epoch_ms,
                    payload: task.payload.clone().to_vec().into(),
                    compression: match task.compression {
                        Compression::None => delay::Compression::None as i32,
                        Compression::Lz4 => delay::Compression::Lz4 as i32,
                    },
                    route_key: task.route_key.clone(),
                })),
            };
            match registry.send_to_route(&task.route_key, message).await {
                Ok(_) => {
                    info!(task_id = %task.id, route = %task.route_key, fire_at = task.fire_at_epoch_ms, "delivery enqueued; awaiting client ack");
                    // wait for explicit ack from client to remove storage entry
                }
                Err(err) => {
                    warn!(task_id = %task.id, route = %task.route_key, error = %err, "delivery failed, will retry");
                    let retry = Arc::new(task.cloned_with_delay(retry_delay));
                    wheel.schedule_task(retry).await;
                }
            }
        }
    });
}

#[derive(Clone)]
pub struct DelaySchedulerService {
    hub: Arc<ConnectionHub>,
    config: Arc<Config>,
}

impl DelaySchedulerService {
    pub fn new(hub: Arc<ConnectionHub>, config: Arc<Config>) -> Self {
        Self { hub, config }
    }
}

#[tonic::async_trait]
impl delay::delay_scheduler_server::DelayScheduler for DelaySchedulerService {
    type ConnectStream = ConnectStream;

    async fn connect(
        &self,
        request: Request<tonic::Streaming<delay::ClientMessage>>,
    ) -> Result<Response<Self::ConnectStream>, Status> {
        if let Some(addr) = request.remote_addr() {
            let ip = addr.ip();
            if !self.config.allows_ip(ip) {
                warn!(peer = %ip, allowed = ?self.config.allowed_hosts, "reject connection: host not allowed");
                return Err(Status::permission_denied("host not allowed"));
            }
            info!(peer = %ip, "accept connection");
        }
        let connection_id = self.hub.next_connection_id();
        let (tx, rx) = mpsc::channel(self.config.connection_write_channel);
        self.hub
            .registry()
            .register_connection(connection_id, tx)
            .await;
        info!(connection_id, "stream connected");
        let mut inbound = request.into_inner();
        let hub = self.hub.clone();
        let registry = self.hub.registry();
        tokio::spawn(async move {
            loop {
                match inbound.message().await {
                    Ok(Some(msg)) => match msg.message {
                        Some(delay::client_message::Message::Register(req)) => {
                            if let Err(err) = hub.handle_register(req.clone(), connection_id).await {
                                warn!(connection_id, error = %err, route = %req.route_key, "register failed");
                            } else {
                                info!(connection_id, route = %req.route_key, "route registered");
                            }
                        }
                        Some(delay::client_message::Message::Schedule(req)) => {
                            if let Err(err) = hub.process_schedule(req.clone(), connection_id).await {
                                warn!(connection_id, error = %err, route = %req.route_key, "schedule failed");
                            } else {
                                info!(connection_id, route = %req.route_key, fire_at = req.fire_at_epoch_ms, payload_len = req.payload.len(), "task scheduled");
                            }
                        }
                        Some(delay::client_message::Message::Ack(req)) => {
                            if let Err(err) = hub.handle_ack(req.clone()).await {
                                warn!(connection_id, error = %err, ack_id = %req.id, "ack failed");
                            } else {
                                info!(connection_id, ack_id = %req.id, "task acked; removed from storage");
                            }
                        }
                        None => {}
                    },
                    Ok(None) => break,
                    Err(err) => {
                        warn!(connection_id, error = %err, "client stream error");
                        break;
                    }
                }
            }
            registry.unregister_connection(connection_id).await;
            info!(connection_id, "stream closed");
        });
        let stream = ReceiverStream::new(rx).map(Ok);
        let boxed: ConnectStream = Box::pin(stream);
        Ok(Response::new(boxed))
    }
}

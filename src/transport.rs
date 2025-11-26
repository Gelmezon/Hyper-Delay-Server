use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{RwLock, mpsc};
use tracing::{error, info, warn};

use crate::config::Config;
use crate::persistence::SledStorage;
use crate::timer::TimerWheel;
use crate::types::{
    AckResponse, DeliveryPayload, ErrorResponse, InboundMessage, OutboundMessage, RegisterAck,
    RegisterRequest, ScheduleRequest, ScheduledTask,
};

#[derive(Default)]
struct RegistryState {
    connections: HashMap<u64, mpsc::Sender<OutboundMessage>>,
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

    /// 记录连接 id -> 写通道，用于主动推送
    pub async fn register_connection(&self, id: u64, tx: mpsc::Sender<OutboundMessage>) {
        self.inner.write().await.connections.insert(id, tx);
    }

    /// route_key 与连接之间存在多对一映射，客户端重连后通过同 key 找回
    pub async fn bind_route(&self, route_key: String, id: u64) {
        self.inner.write().await.routes.insert(route_key, id);
    }

    /// 连接关闭时清理写通道与关联路由
    pub async fn unregister_connection(&self, id: u64) {
        let mut guard = self.inner.write().await;
        guard.connections.remove(&id);
        guard.routes.retain(|_, conn_id| *conn_id != id);
    }

    /// 针对指定连接推送消息（用于 Ack/错误等单播）
    pub async fn send_to_connection(&self, id: u64, message: OutboundMessage) -> Result<()> {
        let sender = {
            let guard = self.inner.read().await;
            guard.connections.get(&id).cloned()
        };

        if let Some(tx) = sender {
            tx.send(message).await.context("send to connection channel")
        } else {
            bail!("connection {id} not found");
        }
    }

    /// 按逻辑 route 推送数据，delivery dispatcher 依赖该方法
    pub async fn send_to_route(&self, route_key: &str, message: OutboundMessage) -> Result<()> {
        let sender = {
            let guard = self.inner.read().await;
            if let Some(id) = guard.routes.get(route_key).copied() {
                guard.connections.get(&id).cloned()
            } else {
                None
            }
        };

        if let Some(tx) = sender {
            tx.send(message).await.context("send to route channel")
        } else {
            bail!("route {route_key} not found");
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
    /// 封装网络入口，组合时间轮与持久化
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

    /// 主循环：监听 TCP 并为每个连接起协程
    pub async fn run(self: Arc<Self>, bind_addr: &str) -> Result<()> {
        let listener = TcpListener::bind(bind_addr)
            .await
            .with_context(|| format!("bind {bind_addr}"))?;
        info!(%bind_addr, "listening for scheduling clients");

        loop {
            let (socket, addr) = listener.accept().await.context("accept connection")?;
            if !self.config.allows_ip(addr.ip()) {
                warn!(%addr, "rejecting connection from disallowed host");
                continue;
            }
            let connection_id = self.id_seq.fetch_add(1, Ordering::Relaxed) + 1;
            info!(%addr, %connection_id, "accepted client");
            let hub = self.clone();
            tokio::spawn(async move {
                if let Err(err) = hub.handle_connection(socket, connection_id).await {
                    warn!(%connection_id, error = %err, "connection ended with error");
                }
            });
        }
    }

    /// 每条连接拆分读写，读协程解析协议，写协程由 channel 驱动
    async fn handle_connection(
        self: Arc<Self>,
        socket: TcpStream,
        connection_id: u64,
    ) -> Result<()> {
        let (reader, writer) = socket.into_split();
        let (tx, rx) = mpsc::channel(self.config.connection_write_channel);
        self.registry
            .register_connection(connection_id, tx.clone())
            .await;

        tokio::spawn(writer_loop(
            rx,
            writer,
            connection_id,
            self.registry.clone(),
        ));

        let mut reader = BufReader::new(reader);
        let mut line = String::new();

        loop {
            line.clear();
            let read = reader.read_line(&mut line).await.context("read line")?;
            if read == 0 {
                break;
            }
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }

            let inbound = serde_json::from_str::<InboundMessage>(trimmed).or_else(|_| {
                serde_json::from_str::<ScheduleRequest>(trimmed).map(InboundMessage::Schedule)
            });

            // 同时兼容“包裹格式”和旧的裸 ScheduleRequest
            match inbound {
                Ok(InboundMessage::Schedule(req)) => {
                    if let Err(err) = self.process_request(req, connection_id).await {
                        warn!(%connection_id, error = %err, "failed to process request");
                        let _ = tx
                            .send(OutboundMessage::Error(ErrorResponse {
                                message: err.to_string(),
                            }))
                            .await;
                    }
                }
                Ok(InboundMessage::Register(req)) => {
                    if let Err(err) = self.handle_register(req, connection_id).await {
                        warn!(%connection_id, error = %err, "failed to register route");
                        let _ = tx
                            .send(OutboundMessage::Error(ErrorResponse {
                                message: err.to_string(),
                            }))
                            .await;
                    }
                }
                Err(err) => {
                    let _ = tx
                        .send(OutboundMessage::Error(ErrorResponse {
                            message: format!("invalid json: {err}"),
                        }))
                        .await;
                }
            }
        }

        self.registry.unregister_connection(connection_id).await;
        info!(%connection_id, "client disconnected");
        Ok(())
    }

    /// 收到调度请求：落盘 -> 投递到时间轮 -> Ack 客户端
    async fn process_request(&self, req: ScheduleRequest, connection_id: u64) -> Result<()> {
        let task = Arc::new(ScheduledTask::from_request(req, connection_id)?);
        self.registry
            .bind_route(task.route_key.clone(), connection_id)
            .await;
        self.storage.persist_task(&task).await?;
        self.wheel.schedule_task(task.clone()).await;
        let ack = OutboundMessage::Ack(AckResponse {
            id: task.id,
            fire_at_epoch_ms: task.fire_at_epoch_ms,
        });
        self.registry.send_to_connection(connection_id, ack).await?;
        Ok(())
    }

    /// 显式注册 route，通常用于客户端重连自报身份
    async fn handle_register(&self, req: RegisterRequest, connection_id: u64) -> Result<()> {
        self.registry
            .bind_route(req.route_key.clone(), connection_id)
            .await;
        let ack = OutboundMessage::Registered(RegisterAck {
            route_key: req.route_key,
        });
        self.registry.send_to_connection(connection_id, ack).await?;
        Ok(())
    }
}

async fn writer_loop(
    mut rx: mpsc::Receiver<OutboundMessage>,
    mut writer: tokio::net::tcp::OwnedWriteHalf,
    connection_id: u64,
    registry: ConnectionRegistry,
) {
    while let Some(message) = rx.recv().await {
        match serde_json::to_vec(&message) {
            Ok(mut bytes) => {
                bytes.push(b'\n');
                if let Err(err) = writer.write_all(&bytes).await {
                    warn!(%connection_id, error = %err, "failed to write to socket");
                    break;
                }
            }
            Err(err) => {
                warn!(%connection_id, error = %err, "serialize outbound message failed");
            }
        }
    }

    registry.unregister_connection(connection_id).await;
}

/// Ready dispatcher：消费时间轮 ready 队列并回调对应 route
pub fn spawn_ready_dispatcher(
    mut ready_rx: mpsc::Receiver<Arc<ScheduledTask>>,
    registry: ConnectionRegistry,
    storage: Arc<SledStorage>,
    wheel: Arc<TimerWheel>,
    retry_delay_ms: u64,
) {
    let retry_delay = Duration::from_millis(retry_delay_ms);
    tokio::spawn(async move {
        while let Some(task) = ready_rx.recv().await {
            let message = OutboundMessage::Delivery(DeliveryPayload {
                id: task.id,
                fire_at_epoch_ms: task.fire_at_epoch_ms,
                payload: task.payload.clone().to_vec().into(),
                compression: task.compression.clone(),
            });

            match registry.send_to_route(&task.route_key, message).await {
                Ok(_) => {
                    if let Err(err) = storage.remove_task(&task.id).await {
                        error!(task_id = %task.id, error = %err, "failed to remove persisted task");
                    }
                }
                Err(err) => {
                    warn!(task_id = %task.id, route = %task.route_key, error = %err, "delivery failed, retry later");
                    let retry = Arc::new(task.cloned_with_delay(retry_delay));
                    wheel.schedule_task(retry).await;
                }
            }
        }
    });
}

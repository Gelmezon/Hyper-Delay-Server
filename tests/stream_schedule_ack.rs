use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tempfile::TempDir;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tonic::transport::{Channel, Server};
use tonic::Request;

use hyper_delay_server::config::Config;
use hyper_delay_server::pb::delay;
use hyper_delay_server::persistence::SledStorage;
use hyper_delay_server::timer::TimerWheel;
use hyper_delay_server::transport::{ConnectionHub, DelaySchedulerService, spawn_ready_dispatcher};
use hyper_delay_server::types;

async fn start_server(bind: SocketAddr, data_dir: &str) {
    let cfg = Arc::new(Config {
        data_dir: data_dir.to_string(),
        bind_addr: bind.to_string(),
        tick_ms: 10,
        ready_channel_capacity: 1024,
        connection_write_channel: 128,
        retry_delay_ms: 200,
        max_delay_days: 30,
        max_delivery_retries: 5,
        allowed_hosts: vec!["127.0.0.1".into(), "::1".into()],
    });
    let storage = Arc::new(SledStorage::open(&cfg.data_dir).expect("open storage"));
    let (ready_tx, ready_rx) = mpsc::channel(cfg.ready_channel_capacity);
    let wheel = Arc::new(TimerWheel::new(Duration::from_millis(cfg.tick_ms), ready_tx));
    wheel.clone().start();

    let hub = Arc::new(ConnectionHub::new(wheel.clone(), storage.clone(), cfg.clone()));
    spawn_ready_dispatcher(ready_rx, hub.registry(), wheel.clone(), cfg.retry_delay_ms, cfg.max_delivery_retries, storage.clone());

    let svc = DelaySchedulerService::new(hub.clone(), cfg.clone());
    let server = Server::builder()
        .add_service(delay::delay_scheduler_server::DelaySchedulerServer::new(svc))
        .serve(bind);
    tokio::spawn(server);
}

async fn connect_client(bind: SocketAddr) -> delay::delay_scheduler_client::DelaySchedulerClient<Channel> {
    let endpoint = format!("http://{}", bind);
    delay::delay_scheduler_client::DelaySchedulerClient::connect(endpoint).await.unwrap()
}

#[tokio::test]
async fn test_schedule_delivery_and_ack() {
    let tmp = TempDir::new().unwrap();
    let bind: SocketAddr = "127.0.0.1:50070".parse().unwrap();
    start_server(bind, tmp.path().to_str().unwrap()).await;

    // Build client and open streaming
    let mut client = connect_client(bind).await;
    let (tx, rx) = tokio::sync::mpsc::channel::<delay::ClientMessage>(8);

    // spawn sender to forward mpsc to stream
    let outbound = async_stream::stream! {
        let mut rx = rx;
        while let Some(msg) = rx.recv().await {
            yield msg;
        }
    };

    let response = client.connect_stream(Request::new(outbound)).await.unwrap();
    let mut inbound = response.into_inner();

    // send register
    tx.send(delay::ClientMessage { message: Some(delay::client_message::Message::Register(delay::RegisterRequest { route_key: "test-route".into() })) }).await.unwrap();

    // send schedule for near future
    let fire_at = (types::now_epoch_ms() + 300) as u64;
    let schedule = delay::ScheduleRequest {
        id: String::new(),
        route_key: "test-route".into(),
        fire_at_epoch_ms: fire_at,
        payload: Bytes::from_static(b"hi").into(),
        compression: delay::Compression::None as i32,
    };
    tx.send(delay::ClientMessage { message: Some(delay::client_message::Message::Schedule(schedule)) }).await.unwrap();

    // expect ack then delivery
    let mut got_delivery = false;
    let mut delivered_id = String::new();

    let fut = async {
        while let Some(msg) = inbound.message().await.unwrap() {
            if let Some(delay::server_message::Message::Delivery(d)) = msg.message.clone() {
                got_delivery = true;
                delivered_id = d.id.clone();
                // ack back
                let ack = delay::ClientMessage { message: Some(delay::client_message::Message::Ack(delay::AckRequest { id: d.id })) };
                tx.send(ack).await.unwrap();
                break;
            }
        }
    };

    let _ = timeout(Duration::from_secs(3), fut).await;
    assert!(got_delivery, "did not receive delivery in time");
    assert!(!delivered_id.is_empty());
}

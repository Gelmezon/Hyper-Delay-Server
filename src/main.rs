// 程序入口：负责初始化依赖并启动所有后台任务
mod config;
mod persistence;
mod timer;
mod transport;
mod types;

use std::env;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use tokio::sync::mpsc;
use tracing::info;
use tracing_subscriber::EnvFilter;

use config::Config;
use persistence::SledStorage;
use timer::TimerWheel;
use types::PersistedTask;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let config_path = env::var("HYPER_DELAY_CONFIG").unwrap_or_else(|_| "config.json".into());
    let app_config = Arc::new(Config::load(&config_path).context("load config")?);

    // 初始化本地持久化与时间轮通道
    let storage = Arc::new(SledStorage::open(&app_config.data_dir).context("open storage")?);
    let (ready_tx, ready_rx) = mpsc::channel(app_config.ready_channel_capacity);
    let wheel = Arc::new(TimerWheel::new(
        Duration::from_millis(app_config.tick_ms),
        ready_tx,
    ));

    // 启动前先恢复历史任务，确保崩溃后能继续回调
    rehydrate_tasks(storage.clone(), wheel.clone()).await?;
    wheel.clone().start();

    // ConnectionHub 负责维护长连接、协议解析与回调分发
    let hub = Arc::new(transport::ConnectionHub::new(
        wheel.clone(),
        storage.clone(),
        app_config.clone(),
    ));
    transport::spawn_ready_dispatcher(
        ready_rx,
        hub.registry(),
        storage.clone(),
        wheel.clone(),
        app_config.retry_delay_ms,
    );

    hub.run(&app_config.bind_addr).await
}

async fn rehydrate_tasks(storage: Arc<SledStorage>, wheel: Arc<TimerWheel>) -> anyhow::Result<()> {
    let persisted = storage.load_tasks().await?;
    if persisted.is_empty() {
        return Ok(());
    }

    // 将落盘任务重新塞回时间轮，按原计划继续执行
    let tasks: Vec<_> = persisted
        .into_iter()
        .map(PersistedTask::into_scheduled_requeued)
        .collect();
    let count = tasks.len();
    wheel.schedule_many(tasks).await;
    info!(%count, "rehydrated tasks queued");
    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}

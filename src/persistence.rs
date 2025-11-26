use std::path::Path;

use anyhow::{Context, Result};
use sled::Db;
use tokio::task;
use uuid::Uuid;

use crate::types::{PersistedTask, ScheduledTask};

#[derive(Clone)]
pub struct SledStorage {
    db: Db,
}

impl SledStorage {
    /// 打开本地 sled 数据库，所有任务都持久化在这里
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let db = sled::open(path).context("open sled database")?;
        Ok(Self { db })
    }

    /// 将单个任务写入 WAL + B+Tree 并同步，保证崩溃可恢复
    pub async fn persist_task(&self, task: &ScheduledTask) -> Result<()> {
        let key = task.id.as_bytes().to_vec();
        let value = serde_json::to_vec(&task.to_persisted()).context("serialize persisted task")?;
        let db = self.db.clone();
        task::spawn_blocking(move || -> Result<()> {
            db.insert(key, value).context("sled insert")?;
            db.flush().context("sled flush")?;
            Ok(())
        })
        .await
        .context("persist task join")??;
        Ok(())
    }

    /// 回调成功后删除持久化记录，防止重复投递
    pub async fn remove_task(&self, id: &Uuid) -> Result<()> {
        let key = id.as_bytes().to_vec();
        let db = self.db.clone();
        task::spawn_blocking(move || -> Result<()> {
            db.remove(key).context("sled remove")?;
            db.flush().context("sled flush")?;
            Ok(())
        })
        .await
        .context("remove task join")??;
        Ok(())
    }

    /// 启动时扫描全部未完成任务，供时间轮批量恢复
    pub async fn load_tasks(&self) -> Result<Vec<PersistedTask>> {
        let db = self.db.clone();
        task::spawn_blocking(move || -> Result<Vec<PersistedTask>> {
            let mut tasks = Vec::new();
            for entry in db.iter() {
                let (_, val) = entry.context("iterate sled")?;
                let record: PersistedTask =
                    serde_json::from_slice(&val).context("deserialize task")?;
                tasks.push(record);
            }
            Ok(tasks)
        })
        .await
        .context("load tasks join")?
    }
}

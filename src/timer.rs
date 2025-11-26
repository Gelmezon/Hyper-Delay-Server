use std::{collections::BTreeMap, sync::Arc, time::Duration};

use tokio::sync::{RwLock, mpsc};

use crate::types::{ScheduledTask, now_epoch_ms};

/// 时间轮核心数据结构，BTreeMap 仅按触发时间排序，配合定时 tick 批量取出到期任务
pub struct TimerWheel {
    tick_interval: Duration,
    slots: RwLock<BTreeMap<u64, Vec<Arc<ScheduledTask>>>>,
    ready_tx: mpsc::Sender<Arc<ScheduledTask>>,
}

impl TimerWheel {
    pub fn new(tick_interval: Duration, ready_tx: mpsc::Sender<Arc<ScheduledTask>>) -> Self {
        Self {
            tick_interval,
            slots: RwLock::new(BTreeMap::new()),
            ready_tx,
        }
    }

    /// 将任务投递到指定时间槽
    pub async fn schedule_task(&self, task: Arc<ScheduledTask>) {
        let mut slots = self.slots.write().await;
        slots.entry(task.fire_at_epoch_ms).or_default().push(task);
    }

    /// 批量恢复或插入任务，减少锁竞争
    pub async fn schedule_many(&self, tasks: Vec<Arc<ScheduledTask>>) {
        if tasks.is_empty() {
            return;
        }
        let mut slots = self.slots.write().await;
        for task in tasks {
            slots.entry(task.fire_at_epoch_ms).or_default().push(task);
        }
    }

    /// 每个 tick 检查所有到期槽位并抛给 ready 通道
    async fn tick(&self) {
        let now = now_epoch_ms();
        let mut ready = Vec::new();
        {
            let mut slots = self.slots.write().await;
            let keys: Vec<u64> = slots.range(..=now).map(|(k, _)| *k).collect();
            for key in keys {
                if let Some(mut bucket) = slots.remove(&key) {
                    ready.append(&mut bucket);
                }
            }
        }

        for task in ready {
            if self.ready_tx.send(task).await.is_err() {
                break;
            }
        }
    }

    /// 长时间运行的后台任务，固定步长驱动 tick
    pub fn start(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(self.tick_interval);
            loop {
                interval.tick().await;
                self.tick().await;
            }
        });
    }
}

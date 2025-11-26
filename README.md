# Hyper Delay Server

> 基于 Rust + Tokio 的高性能时间轮服务，提供跨语言的延迟任务调度能力，支持长连接、持久化恢复、消息压缩与安全访问控制。

## 功能特性

- **多协程时间轮**：Tokio 驱动的轮式调度器，10ms 级别 tick，批量推进，任务触发延迟可控。
- **持久化恢复**：任务在写入时间轮前会落盘到 sled（WAL + B+Tree），服务重启后读取历史记录并重新入轮。
- **双工长连接**：每个接入方使用 TCP 长连接发送 `schedule` 请求并接收回调 `delivery`；支持显式 `register` 与自动 ACK。
- **安全控制**：`config.json` 中的 `allowed_hosts` 限定可访问 IP，默认仅允许本机；后续可扩展 token 鉴权。
- **压缩/格式**：payload 支持标记 `compression`（目前示例使用 `none`，可扩展到 LZ4/Zstd）。
- **语言无关**：任何语言只需按协议发送 JSON 即可；仓库内提供了 Java Netty 客户端示例。

## 目录结构

```
├── src/
│   ├── config.rs        # 配置模型与白名单校验
│   ├── persistence.rs   # sled 存储抽象（写入、删除、恢复）
│   ├── timer.rs         # 时间轮实现
│   ├── transport.rs     # TCP 服务、协议解析、连接管理
│   ├── types.rs         # 协议与内部数据结构
│   └── main.rs          # 程序入口，初始化所有组件
├── config.json          # 运行时配置
├── Cargo.toml
└── README.md
```

## 运行方式

```bash
# 1. 安装 Rust 1.87+（建议使用 rustup）
# 2. 修改 config.json（见下文）
# 3. 启动服务
env HYPER_DELAY_CONFIG=./config.json cargo run --release
# 或者使用默认 config.json，直接：
cargo run --release
```

### 配置（`config.json`）

| 字段 | 说明 | 默认值 |
| ---- | ---- | ------ |
| `data_dir` | sled 数据目录，保存任务持久化信息 | `data/db` |
| `bind_addr` | 服务监听地址（IP:端口）。建议生产环境绑内网地址 | `127.0.0.1:7000` |
| `tick_ms` | 时间轮 tick 间隔，过小影响 CPU，过大影响延迟 | `10` |
| `ready_channel_capacity` | 时间轮 ready 队列容量 | `2048` |
| `connection_write_channel` | 每条连接的发包队列容量 | `256` |
| `retry_delay_ms` | 发送失败后的重试延迟 | `1000` |
| `allowed_hosts` | 允许访问的 IP 列表；为空时默认 `["127.0.0.1","::1"]` | - |

在容器/多机部署时，可通过环境变量 `HYPER_DELAY_CONFIG=/path/to/config.json` 指向定制配置。

## 时间轮运行逻辑

1. **接收请求**：客户端通过 TCP 连接发送 `schedule` JSON。服务端解析后：
   - 校验时间（`fire_at`）与 payload。
   - 将任务转换为内部 `ScheduledTask`，附带 `route_key`（用于回调路由）。
2. **持久化**：`persistence::persist_task` 把任务写入 sled，只有写成功才视为接收成功。
3. **入轮**：`timer::TimerWheel` 按 `fire_at_epoch_ms` 放入 BTreeMap 槽位。后台 `tick` 每 `tick_ms` 触发，取出到期槽并把任务推入 `ready` 队列。
4. **回调投递**：`transport::spawn_ready_dispatcher` 消费 `ready` 队列，通过 `ConnectionRegistry` 找到对应连接并发送 `delivery`。
   - 投递成功 → 删除 sled 记录。
   - 投递失败（连接断开等）→ 任务克隆并延迟 `retry_delay_ms` 重新入轮，直到客户端重新注册。
5. **重启恢复**：`main::rehydrate_tasks` 启动时加载 sled 所有待执行任务，重新入轮（仍保留原 `route_key`）。客户端重连并重新 `register` 后即可继续接收。

## 协议说明（JSON 行分隔）

### Register
```json
{"kind":"register","route_key":"service-A"}
```
- 客户端连接建立后立即发送，用于标记回调路由。
- 服务端响应：`{"kind":"registered","route_key":"service-A"}`

### Schedule
```json
{
  "kind": "schedule",
  "id": "可选/UUID",
  "route_key": "service-A",
  "fire_at": 1732616400000,
  "compression": "none",
  "payload_base64": "..."
}
```
- `fire_at` 为 Unix epoch 毫秒。
- `route_key` 决定回调目标；未提供时服务端可回退为 `conn-<id>`。
- 返回：`{"kind":"ack","id":"...","fire_at":...}`

### Delivery & Ack
```json
{"kind":"delivery","id":"...","route_key":"service-A","fire_at":...,"payload_base64":"...","compression":"none"}
{"kind":"ack","id":"..."}
```
- 服务端发送 `delivery`，客户端消费后应发送 `ack`，服务端收到 ack 后删除持久化条目。

## 接入示例（Java / Netty）

```java
@Bean(destroyMethod = "close")
public DelayNettyClient delayClient() {
    return new DelayNettyClient("127.0.0.1", 7000, "my-service-1", ready -> {
        byte[] payload = ready.decodePayload();
        // TODO: 反序列化并执行业务逻辑
        log.info("task {} fireAt {}", ready.id, Instant.ofEpochMilli(ready.fireAt));
    });
}
```
- `DelayNettyClient` 位于 `com.example.delay`（见上一条回答中的代码）。
- 连接成功后自动注册 routeKey，`scheduleAt(epochMillis, payload)` 即可向服务端提交任务。

## 常见扩展

- **压缩**：在客户端压缩 payload（LZ4/Zstd）后 base64，`compression` 字段告诉服务端如何解压。
- **鉴权**：可在 Register/Schedule 请求添加 token，与服务端配置联动。
- **多副本部署**：通过上层负载均衡或一致性哈希决定 routeKey -> server 映射；或在服务端实现多节点队列。
- **监控**：增加 Prometheus 指标（入队数、ready 队列长度、重试次数等）。

## 开发调试

- `cargo fmt` / `cargo clippy` / `cargo test` 确保代码质量。
- 使用 `RUST_LOG=debug cargo run` 查看详细日志（依赖 `tracing` 设置）。
- sled 数据默认写在 `data/db`，可清空目录模拟空白启动。

---

欢迎根据业务需求继续扩展（如集群化、HTTP API、鉴权等）。如需更多示例或客户端 SDK，请提 Issues。
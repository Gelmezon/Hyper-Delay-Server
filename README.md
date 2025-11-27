# Hyper Delay Server

> 基于 Rust + Tokio 的高性能时间轮服务，提供跨语言的延迟任务调度能力，支持长连接、持久化恢复、消息压缩与安全访问控制。

## 功能特性

- **多协程时间轮**：Tokio 驱动的轮式调度器，10ms 级别 tick，批量推进，任务触发延迟可控。
- **持久化恢复**：任务在写入时间轮前会落盘到 sled（WAL + B+Tree），服务重启后读取历史记录并重新入轮。
- **双工长连接**：gRPC 双向流（`DelayScheduler.Connect`）保持 HTTP/2 连接，客户端发送注册、调度、ACK，一条连接即可推送/拉取事件。
- **安全控制**：`config.json` 中的 `allowed_hosts` 限定可访问 IP，默认仅允许本机；后续可扩展 token 鉴权。
- **压缩/格式**：payload 支持标记 `compression`（目前示例使用 `none`，可扩展到 LZ4/Zstd）。
- **语言无关**：任何语言只需按协议发送 JSON 即可；仓库内提供了 Java Netty 客户端示例。

## 目录结构

```
├── src/
│   ├── config.rs        # 配置模型与白名单校验
│   ├── persistence.rs   # sled 存储抽象（写入、删除、恢复）
│   ├── timer.rs         # 时间轮实现
│   ├── transport.rs     # gRPC 服务与连接管理
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

1. **接收请求**：客户端通过 gRPC `DelayScheduler.Connect` 长连接发送 `ClientMessage`（`Register`/`Schedule`）。服务端解析后：
  - 校验时间（`fire_at_epoch_ms`）与 payload。
  - 将任务转换为内部 `ScheduledTask`，附带 `route_key`（用于回调路由）。
2. **持久化**：`persistence::persist_task` 把任务写入 sled，只有写成功才视为接收成功。
3. **入轮**：`timer::TimerWheel` 按 `fire_at_epoch_ms` 放入 BTreeMap 槽位。后台 `tick` 每 `tick_ms` 触发，取出到期槽并把任务推入 `ready` 队列。
4. **回调投递**：`transport::spawn_ready_dispatcher` 消费 `ready` 队列，通过 `ConnectionRegistry` 找到对应连接并发送 `delivery`。
   - 投递成功 → 删除 sled 记录。
   - 投递失败（连接断开等）→ 任务克隆并延迟 `retry_delay_ms` 重新入轮，直到客户端重新注册。
5. **重启恢复**：`main::rehydrate_tasks` 启动时加载 sled 所有待执行任务，重新入轮（仍保留原 `route_key`）。客户端重连并重新 `register` 后即可继续接收。

## gRPC 协议（`proto/delay.proto`）

客户端与服务端通过 `DelayScheduler.Connect(stream ClientMessage)` 建立单个双向流：

- `ClientMessage.register`：连接建立后立刻发送，告诉服务端 `route_key`。
- `ServerMessage.registered`：服务端确认并完成路由绑定。
- `ClientMessage.schedule`：包含 `id`（可空）、`fire_at_epoch_ms`、payload bytes、`compression`、`route_key`。
- `ServerMessage.ack`：每个 schedule 后返回，携带分配的 `id` 与 `fire_at_epoch_ms`。
- `ServerMessage.delivery`：时间轮触发后推送，包含 payload、`compression` 与 `route_key`。
- `ClientMessage.ack`：客户端收到 delivery 后发送，用于清理 sled 记录。
- `ServerMessage.error`：解析/路由失败时返回，客户端可根据需要重试。 

压缩字段使用 proto 枚举（`COMPRESSION_NONE`/`COMPRESSION_LZ4`），由客户端在 schedule 前压缩和 base64 编码逻辑交由上层实现。protocol buffer 位置在 `proto/delay.proto`，可用 `protoc`/`tonic-build` 生成语言对应的 stub。

## 接入示例（gRPC）

### Java gRPC 客户端

```java
package com.example.delay;

import com.example.delay.proto.AckRequest;
import com.example.delay.proto.ClientMessage;
import com.example.delay.proto.Compression;
import com.example.delay.proto.DelaySchedulerGrpc;
import com.example.delay.proto.Delivery;
import com.example.delay.proto.RegisterRequest;
import com.example.delay.proto.ScheduleRequest;
import com.example.delay.proto.ServerMessage;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.io.Closeable;
import java.util.UUID;
import java.util.function.Consumer;

public class DelayGrpcClient implements Closeable {
  private final ManagedChannel channel;
  private final DelaySchedulerGrpc.DelaySchedulerStub stub;
  private final StreamObserver<ClientMessage> requestObserver;
  private final String routeKey;
  private final Consumer<Delivery> handler;

  public DelayGrpcClient(
      String host,
      int port,
      String routeKey,
      Consumer<Delivery> handler) {
    this.channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
    this.stub = DelaySchedulerGrpc.newStub(channel);
    this.routeKey = routeKey;
    this.handler = handler;

    this.requestObserver = stub.connect(new StreamObserver<ServerMessage>() {
      @Override
      public void onNext(ServerMessage serverMessage) {
        if (serverMessage.hasDelivery()) {
          Delivery delivery = serverMessage.getDelivery();
          handler.accept(delivery);
          requestObserver.onNext(ClientMessage.newBuilder()
              .setAck(AckRequest.newBuilder().setId(delivery.getId()).build())
              .build());
        }
      }

      @Override
      public void onError(Throwable t) {
        // 可加入重连逻辑
      }

      @Override
      public void onCompleted() {
        // 服务端主动关闭
      }
    });

    requestObserver.onNext(ClientMessage.newBuilder()
        .setRegister(RegisterRequest.newBuilder().setRouteKey(routeKey).build())
        .build());
  }

  public void scheduleAt(long fireAtEpochMs, byte[] payload) {
    ScheduleRequest request = ScheduleRequest.newBuilder()
        .setId(UUID.randomUUID().toString())
        .setRouteKey(routeKey)
        .setFireAtEpochMs(fireAtEpochMs)
        .setPayload(ByteString.copyFrom(payload))
        .setCompression(Compression.COMPRESSION_NONE)
        .build();
    requestObserver.onNext(ClientMessage.newBuilder().setSchedule(request).build());
  }

  @Override
  public void close() {
    requestObserver.onCompleted();
    channel.shutdownNow();
  }
}
```

- 负责：建立 gRPC 双向流、注册 `route_key`、处理 `delivery` 并发送 ACK、提供 `scheduleAt` 调度接口。

### Spring Bean 使用示例

```java
@Bean(destroyMethod = "close")
public DelayGrpcClient delayClient() {
  return new DelayGrpcClient("127.0.0.1", 7000, "my-service-1", delivery -> {
    byte[] payload = delivery.getPayload().toByteArray();
    // TODO: 反序列化并执行业务逻辑
    log.info("task {} fireAt {}", delivery.getId(), Instant.ofEpochMilli(delivery.getFireAtEpochMs()));
  });
}
```


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

## 日志含义

- `starting gRPC delay scheduler server`：服务启动并绑定监听地址（来自 `main.rs`）。
- `accept connection`：连接来源 IP 通过白名单校验，允许进入（`transport.rs`）。
- `reject connection: host not allowed`：来源 IP 不在 `allowed_hosts`，连接被拒绝（`transport.rs`）。
- `route registered`：客户端 `register` 成功，已绑定 `route_key`（`transport.rs`）。
- `task scheduled`：`schedule` 校验通过并持久化，等待到期（`transport.rs`）。
- `reject schedule: fire_at in the past`：提交的触发时间早于当前时间，被拒；客户端会收到 `ErrorResponse{code="INVALID_FIRE_AT"}`（`transport.rs`）。
- `reject schedule: fire_at exceeds max delay`：超过 `config.max_delay_days` 允许范围；客户端会收到 `ErrorResponse{code="EXCEEDS_MAX_DELAY"}`（`transport.rs`）。
- `delivery enqueued; awaiting client ack`：任务到期已投递到连接通道，等待客户端 ACK（`transport.rs`）。
- `delivery failed, will retry`：当前连接不可用或路由失败，任务将按 `retry_delay_ms` 重试（`transport.rs`）。
- `task acked; removed from storage`：收到客户端 ACK，持久化记录已清理（`transport.rs`）。
- `client stream error`：客户端流出现错误（网络中断、协议错误），连接关闭（`transport.rs`）。
- `stream closed`：客户端正常结束或错误后，连接注销（`transport.rs`）。

排查建议：
- 连接报 `UNAVAILABLE`：检查端口映射、`bind_addr` 是否 `0.0.0.0:7000`、`allowed_hosts` 是否包含来源 IP、云安全组/防火墙是否放行。
- 任务无回调：确认是否有 `route registered`，并观察是否出现 `delivery failed, will retry`。
- 任务被拒：根据 `ErrorResponse.code` 修正 `fire_at_epoch_ms` 或增大 `max_delay_days`。
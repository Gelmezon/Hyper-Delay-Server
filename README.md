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
{"kind":"delivery","id":"...","route_key":"service-A","fire_at":"...","payload_base64":"...","compression":"none"}
{"kind":"ack","id":"..."}
```
- 服务端发送 `delivery`，客户端消费后应发送 `ack`，服务端收到 ack 后删除持久化条目。

## 接入示例（Java / Netty）

### DelayNettyClient 工具类

```java
package com.example.delay;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.CharsetUtil;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.Base64;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.function.Consumer;

public class DelayNettyClient implements Closeable {
  private final String host;
  private final int port;
  private final String routeKey;
  private final Consumer<ReadyMessage> readyHandler;
  private final ObjectMapper mapper = new ObjectMapper();

  private final Bootstrap bootstrap;
  private final NioEventLoopGroup group = new NioEventLoopGroup(1);
  private volatile Channel channel;
  private final ScheduledExecutorService reconnectExec = Executors.newSingleThreadScheduledExecutor(r -> {
    Thread t = new Thread(r, "delay-client-reconnect");
    t.setDaemon(true);
    return t;
  });
  private final long initialReconnectMs;
  private volatile long currentBackoffMs;
  private final long maxBackoffMs = 30_000;
  private volatile boolean closed = false;

  public DelayNettyClient(String host, int port, String routeKey, Consumer<ReadyMessage> readyHandler) {
    this(host, port, routeKey, readyHandler, 2000);
  }

  public DelayNettyClient(String host, int port, String routeKey, Consumer<ReadyMessage> readyHandler, long initialReconnectMs) {
    this.host = host;
    this.port = port;
    this.routeKey = routeKey;
    this.readyHandler = readyHandler;
    this.initialReconnectMs = initialReconnectMs;
    this.currentBackoffMs = initialReconnectMs;

    bootstrap = new Bootstrap()
        .group(group)
        .channel(NioSocketChannel.class)
        .option(ChannelOption.SO_KEEPALIVE, true)
        .remoteAddress(new InetSocketAddress(host, port))
        .handler(new ChannelInitializer<SocketChannel>() {
          @Override
          protected void initChannel(SocketChannel ch) {
            ChannelPipeline p = ch.pipeline();
            p.addLast(new LineBasedFrameDecoder(64 * 1024));
            p.addLast(new StringDecoder(CharsetUtil.UTF_8));
            p.addLast(new StringEncoder(CharsetUtil.UTF_8));
            p.addLast(new DelayClientHandler());
          }
        });

    connect();
  }

  private void connect() {
    if (closed) {
      return;
    }
    bootstrap.connect().addListener((ChannelFutureListener) future -> {
      if (future.isSuccess()) {
        channel = future.channel();
        currentBackoffMs = initialReconnectMs;
      } else {
        scheduleReconnect();
      }
    });
  }

  private void scheduleReconnect() {
    if (closed) {
      return;
    }
    long delay = Math.min(currentBackoffMs, maxBackoffMs);
    reconnectExec.schedule(this::connect, delay, TimeUnit.MILLISECONDS);
    currentBackoffMs = Math.min(maxBackoffMs, currentBackoffMs * 2);
  }

  public void scheduleAt(long epochMillis, byte[] payload) {
    ScheduleMessage msg = new ScheduleMessage();
    msg.kind = "schedule";
    msg.routeKey = routeKey;
    msg.fireAt = epochMillis;
    msg.id = UUID.randomUUID().toString();
    msg.compression = "none";
    msg.payloadBase64 = Base64.getEncoder().encodeToString(payload);
    sendJson(msg);
  }

  private void sendJson(Object obj) {
    try {
      String s = mapper.writeValueAsString(obj) + "\n";
      Channel ch = channel;
      if (ch != null && ch.isActive()) {
        ch.writeAndFlush(s);
      }
    } catch (Exception ignored) {
    }
  }

  @Override
  public void close() {
    closed = true;
    try {
      if (channel != null) {
        channel.close().syncUninterruptibly();
      }
    } catch (Exception ignored) {
    }
    try {
      group.shutdownGracefully().syncUninterruptibly();
    } catch (Exception ignored) {
    }
    reconnectExec.shutdownNow();
  }

  private class DelayClientHandler extends SimpleChannelInboundHandler<String> {
    @Override
    public void channelActive(ChannelHandlerContext ctx) {
      RegisterMessage reg = new RegisterMessage("register", routeKey);
      try {
        String s = mapper.writeValueAsString(reg) + "\n";
        ctx.writeAndFlush(s);
      } catch (Exception ignored) {
      }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
      channel = null;
      if (!closed) {
        scheduleReconnect();
      }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String line) {
      try {
        GenericMessage gm = mapper.readValue(line, GenericMessage.class);
        if ("ready".equals(gm.kind)) {
          ReadyMessage ready = mapper.readValue(line, ReadyMessage.class);
          CompletableFuture.runAsync(() -> {
            try {
              readyHandler.accept(ready);
            } catch (Throwable ignored) {
            }
          });
          AckMessage ack = new AckMessage("ack", ready.id);
          sendJson(ack);
        }
      } catch (Exception ignored) {
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      ctx.close();
    }
  }

  private static class GenericMessage {
    public String kind;
  }

  private static class RegisterMessage {
    public final String kind;
    @JsonProperty("route_key")
    public final String routeKey;

    RegisterMessage(String kind, String routeKey) {
      this.kind = kind;
      this.routeKey = routeKey;
    }
  }

  private static class ScheduleMessage {
    public String kind;
    @JsonProperty("route_key")
    public String routeKey;
    @JsonProperty("fire_at")
    public long fireAt;
    public String id;
    public String compression;
    @JsonProperty("payload_base64")
    public String payloadBase64;
  }

  public static class ReadyMessage {
    public String kind;
    public String id;
    @JsonProperty("route_key")
    public String routeKey;
    @JsonProperty("fire_at")
    public long fireAt;
    @JsonProperty("payload_base64")
    public String payloadBase64;

    public byte[] decodePayload() {
      return Base64.getDecoder().decode(payloadBase64);
    }
  }

  private static class AckMessage {
    public String kind;
    public String id;

    AckMessage(String kind, String id) {
      this.kind = kind;
      this.id = id;
    }
  }
}
```

- 负责：自动重连、连接注册、schedule 发送、ready 回调与 ACK。
- 可继续扩展鉴权、心跳、payload 压缩等能力。

### Spring Bean 使用示例

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
- 连接成功后自动注册 `route_key`，调用 `scheduleAt(epochMillis, payload)` 即可提交任务。
- 在 Spring 中注册为单例 Bean，调用方注入后直接使用即可。

### Go 客户端示例

```go
package delayclient

import (
  "bufio"
  "context"
  "encoding/base64"
  "encoding/json"
  "errors"
  "net"
  "sync"
  "time"
)

type Client struct {
  addr      string
  routeKey  string
  handler   func(ReadyMessage)
  conn      net.Conn
  mu        sync.Mutex
  closeOnce sync.Once
}

type envelope struct {
  Kind string `json:"kind"`
}

type registerMessage struct {
  Kind     string `json:"kind"`
  RouteKey string `json:"route_key"`
}

type scheduleMessage struct {
  Kind          string `json:"kind"`
  RouteKey      string `json:"route_key"`
  FireAt        int64  `json:"fire_at"`
  ID            string `json:"id"`
  Compression   string `json:"compression"`
  PayloadBase64 string `json:"payload_base64"`
}

type ackMessage struct {
  Kind string `json:"kind"`
  ID   string `json:"id"`
}

type ReadyMessage struct {
  Kind          string `json:"kind"`
  ID            string `json:"id"`
  RouteKey      string `json:"route_key"`
  FireAt        int64  `json:"fire_at"`
  PayloadBase64 string `json:"payload_base64"`
}

func (r ReadyMessage) DecodePayload() ([]byte, error) {
  return base64.StdEncoding.DecodeString(r.PayloadBase64)
}

func New(addr, routeKey string, handler func(ReadyMessage)) (*Client, error) {
  conn, err := net.Dial("tcp", addr)
  if err != nil {
    return nil, err
  }
  c := &Client{
    addr:     addr,
    routeKey: routeKey,
    handler:  handler,
    conn:     conn,
  }
  if err := c.send(registerMessage{Kind: "register", RouteKey: routeKey}); err != nil {
    conn.Close()
    return nil, err
  }
  go c.readLoop()
  return c, nil
}

func (c *Client) Schedule(ctx context.Context, fireAt time.Time, payload []byte, id string) error {
  if id == "" {
    return errors.New("id required")
  }
  msg := scheduleMessage{
    Kind:          "schedule",
    RouteKey:      c.routeKey,
    FireAt:        fireAt.UnixMilli(),
    ID:            id,
    Compression:   "none",
    PayloadBase64: base64.StdEncoding.EncodeToString(payload),
  }
  return c.sendWithContext(ctx, msg)
}

func (c *Client) Close() error {
  var err error
  c.closeOnce.Do(func() {
    c.mu.Lock()
    defer c.mu.Unlock()
    if c.conn != nil {
      err = c.conn.Close()
    }
  })
  return err
}

func (c *Client) sendWithContext(ctx context.Context, msg any) error {
  done := make(chan error, 1)
  go func() {
    done <- c.send(msg)
  }()
  select {
  case <-ctx.Done():
    return ctx.Err()
  case err := <-done:
    return err
  }
}

func (c *Client) send(msg any) error {
  data, err := json.Marshal(msg)
  if err != nil {
    return err
  }
  data = append(data, '\n')
  c.mu.Lock()
  defer c.mu.Unlock()
  if c.conn == nil {
    return errors.New("connection closed")
  }
  _, err = c.conn.Write(data)
  return err
}

func (c *Client) readLoop() {
  scanner := bufio.NewScanner(c.conn)
  for scanner.Scan() {
    line := scanner.Bytes()
    var env envelope
    if err := json.Unmarshal(line, &env); err != nil {
      continue
    }
    if env.Kind != "delivery" {
      continue
    }
    var ready ReadyMessage
    if err := json.Unmarshal(line, &ready); err != nil {
      continue
    }
    go c.handler(ready)
    _ = c.send(ackMessage{Kind: "ack", ID: ready.ID})
  }
  c.Close()
}
```

```go
// 使用示例
func main() {
  client, err := delayclient.New("127.0.0.1:7000", "go-worker-1", func(msg delayclient.ReadyMessage) {
    payload, _ := msg.DecodePayload()
    log.Printf("fire %s payload=%s", msg.ID, string(payload))
  })
  if err != nil {
    log.Fatal(err)
  }
  defer client.Close()

  ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
  defer cancel()
  if err := client.Schedule(ctx, time.Now().Add(5*time.Second), []byte("hello"), "task-1"); err != nil {
    log.Fatal(err)
  }

  select {}
}
```
- Go 示例展示了最小化长连接客户端：启动后注册 routeKey、监听 delivery、发送 ack，并提供 `Schedule` 方法提交任务。
- 实际生产可在 `readLoop` 中加入自动重连、心跳和 payload 解密/解压等逻辑。

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
# agent-pair

**基于 WebSocket + yamux 的标签式 TCP 隧道。** 通过一台中转服务器，让多台远程机器同时访问内网服务——无需公网 IP。

---

## 工作原理

```
┌─────────────────┐         ┌──────────────────┐        ┌──────────────────────┐
│   内网机器      │         │     中转服务器    │        │     远程机器         │
│  (无公网 IP)    │         │    (公网中继)     │        │    (客户端)          │
└────────┬────────┘         └────────┬─────────┘        └───────────┬──────────┘
         │                           │                              │
         │── REGISTER "web" ────────→│                              │
         │    (WebSocket 会话)       │                              │
         │                           │←────── FORWARD "web" ───────│
         │                           │    (WebSocket 会话)          │
         │ bind(8080, "web")         │                              │ forward(9090, "web")
         │                           │                              │ 监听 :9090
         │                           │                              │   ↓ 接受连接
         │                           │◄─── yamux 流 ──────────────│   ctrl.open_stream()
         │← broker 打开 bind 流 ─────┤                            │
         │ connect(:8080)→bridge     │                             │ 客户端收到响应
         │                           │                              │
         │                           │←─── yamux 流 ──────────────│ Forward 2
         │←── broker 打开新流 ──────┤                            │
         │ connect(:8080)→bridge     │                             │ 所有 forward 共享 bind
```

**Broker** 运行在公网服务器上。**Bind** 连接 Broker 并注册标签，指向内网服务。**Forward** 使用相同标签连接，获得本地端口——所有流量通过每个 Agent 的一条 yamux 连接复用。

---

## 命令行

`agentd` 二进制基于 CLI 使用了 `AgentClient`。

```bash
# Broker（公网中转）— 只需 --server，不需要 bind/forward
cargo run --bin agentd --features cli

# 内网机器 — 注册标签 "web"，指向本机 :8080
cargo run --bin agentd --features cli -- --bind 8080:web

# 远程机器 — 监听 :9090，隧道连接到 "web" 服务
cargo run --bin agentd --features cli -- --forward 9090:web

# 在同一条连接上同时 bind 和 forward
cargo run --bin agentd --features cli -- --bind 3000:api --forward 9090:web
```

| 参数 | 说明 |
|------|------|
| `-s`, `--server <ADDR>` | Broker WebSocket 地址（默认 `127.0.0.1:7799`） |
| `--bind <PORT:LABEL>` | 注册标签并将传入流桥接到本地端口（可多次） |
| `--forward <PORT:LABEL>` | 监听本地端口并隧道连接到已注册标签（可多次） |

断连后 agent 每 3 秒自动重连。

---

## API

### Broker

```rust
use agent_pair::Broker;

Broker::listen("0.0.0.0:7799").await?;
```

### Bind — 服务所有者

```rust
use agent_pair::AgentClient;

let agent = AgentClient::connect("relay:7799").await?;

// 注册标签 "web"，指向本机 :8080
agent.bind(8080, "web").await?;
```

### Forward — 远程消费者

```rust
use agent_pair::AgentClient;

let agent = AgentClient::connect("relay:7799").await?;

// 监听 :9090，隧道连接到注册为 "web" 的服务
agent.forward(9090, "web").await?;
```

---

## 功能特性

- **标签路由** — 一个 Broker 可同时转发多个不同服务
- **WebSocket 传输** — 所有 Agent 连接通过 WebSocket 隧道，兼容标准 HTTP 代理
- **共享连接** — 单个 `AgentClient` 可以同时 bind 和 forward，并注册多个标签
- **多路转发** — 任意数量的 forward 可共享同一标签
- **断连恢复** — bind 断开后 Broker 清理脏条目，新 bind 可无缝重连
- **高并发** — 基于 yamux 多路复用；100 并发流 + 50 路 100KB 数据传输测试通过
- **无需公网 IP** — 只有 Broker 需要公网地址

---

## 协议

帧格式为 postcard 编码，通过 yamux 流传输，前缀 2 字节长度：

| 类型 | 载荷 | 方向 |
|------|------|------|
| `Register { label }` | 标签字符串 | Bind → Broker |
| `Data { label }` | 标签字符串 | Forward → Broker → Bind |

每个 yamux 流携带一个控制帧（Register 或 Data）。控制帧之后该流被桥接到目标 TCP 连接。

---

## 规则

| 规则 | 行为 |
|------|------|
| 先 bind 再 forward | forward 连接不存在的标签 → 流被关闭 |
| 每标签仅一个 bind | 重复 bind 返回错误 |
| 每标签可 N 个 forward | 每个 forward 是独立 TCP 连接到 broker |
| bind 断开 | 脏条目自动清理，forward 收到错误 |
| 标签作用域属于 Broker | 不同 Broker 的同名标签互相独立 |

---

## 测试

```bash
cargo test
```

22 个集成测试和单元测试，覆盖 bind/forward、并发流、数据完整性、断连重连、背压和错误处理。

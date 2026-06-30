[中文](README.zh.md)

# agent-pair

**Label-based TCP tunnel over WebSocket + yamux.** Expose a local service through a relay so
multiple remote machines can reach it — no public IP needed.

---

## How it works

```
┌─────────────────┐         ┌──────────────────┐        ┌──────────────────────┐
│  Bind machine   │         │     Broker       │        │  Forward machine(s)  │
│  (no public IP) │         │   (public relay)  │        │       (clients)      │
└────────┬────────┘         └────────┬─────────┘        └───────────┬──────────┘
         │                           │                              │
         │── REGISTER "web" ────────→│                              │
         │    (WebSocket session)    │                              │
         │                           │←────── FORWARD "web" ───────│
         │                           │    (WebSocket session)       │
         │ bind(8080, "web")         │                              │ forward(9090, "web")
         │                           │                              │ listen :9090
         │                           │                              │   ↓ accept
         │                           │◄─── yamux stream ──────────│   ctrl.open_stream()
         │← broker opens bind stream─┤                            │
         │ connect(:8080)→bridge     │                             │ client gets response
         │                           │                              │
         │                           │←─── yamux stream ──────────│ Forward 2
         │←── broker opens streams──┤                            │
         │ connect(:8080)→bridge     │                             │ all share bind
```

A **Broker** runs on a public server. A **bind** agent connects and registers a
label pointing at a local service. **Forward** agents connect with the same label
and get a local port — traffic is multiplexed over a single yamux session per
agent.

---

## CLI

The `agentd` binary wraps `AgentClient` for terminal usage.

```bash
# Broker (public relay) — only --server matters, no bind/forward needed
cargo run --bin agentd --features cli

# Service machine — register label "web" for local :8080
cargo run --bin agentd --features cli -- --bind 8080:web

# Remote machine — listen on :9090, tunnel to the "web" service
cargo run --bin agentd --features cli -- --forward 9090:web

# Combine bind and forward on the same connection
cargo run --bin agentd --features cli -- --bind 3000:api --forward 9090:web
```

| Option | Description |
|--------|-------------|
| `-s`, `--server <ADDR>` | Broker WebSocket address (default `127.0.0.1:7799`) |
| `--bind <PORT:LABEL>` | Register a label and bridge incoming streams to a local port (repeatable) |
| `--forward <PORT:LABEL>` | Listen on a local port and tunnel to a registered label (repeatable) |

On disconnect the agent retries every 3 s.

---

## API

### Broker

```rust
use agent_pair::Broker;

Broker::listen("0.0.0.0:7799").await?;
```

### Agent — bind (service owner)

```rust
use agent_pair::AgentClient;

let agent = AgentClient::connect("relay:7799").await?;

// Register label "web" pointing at local :8080
agent.bind(8080, "web").await?;
```

### Agent — forward (remote consumer)

```rust
use agent_pair::AgentClient;

let agent = AgentClient::connect("relay:7799").await?;

// Listen on :9090, tunnel to the service registered as "web"
agent.forward(9090, "web").await?;
```

---

## Features

- **Labels** — one broker can route many different services by name
- **WebSocket transport** — all agent connections tunnel through WebSocket, compatible with standard HTTP proxies
- **Shared connection** — a single `AgentClient` can `bind` and `forward` on the same
  connection, and can register multiple labels
- **Multiple forwards** — any number of forward agents can share one label
- **Resilient** — if the bind agent disconnects, the broker cleans up stale
  entries; new bind agents can re-register seamlessly
- **Concurrent** — built on yamux multiplexing; tested with 100 concurrent
  streams and 50 simultaneous 100 KB transfers
- **No public IP needed** — only the broker needs a public address

---

## Protocol

Frames are length-prefixed postcard-encoded messages over yamux streams:

| Type | Payload | Direction |
|------|---------|-----------|
| `Register { label }` | label string | Bind → Broker |
| `Data { label }` | label string | Forward → Broker → Bind |

Each yamux stream carries exactly one control frame (Register or Data). After
the control frame the stream is bridged to the target TCP connection.

---

## Rules

| Rule | Behaviour |
|------|-----------|
| Bind first | A forward stream with an unregistered label is dropped |
| One bind per label per broker | Duplicate `bind` returns an error |
| N forwards per label | Each forward is an independent TCP connection to the broker |
| Bind disconnects | Stale entries are cleaned up; forwards get errors |
| Labels are scoped per broker | The same label on different brokers is independent |

---

## Tests

```bash
cargo test
```

22 integration and unit tests covering bind/forward, concurrent streams, data
integrity, reconnection, backpressure, and error handling.

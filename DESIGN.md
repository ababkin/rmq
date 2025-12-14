# rmq - Simple RabbitMQ Client for Rust

A lightweight, robust RabbitMQ client library that follows lapin's patterns and handles connection failures gracefully.

## Design Principles

1. **One Connection, Many Channels** - Lapin connections are expensive, channels are cheap. We create channels as needed.
2. **No String Matching on Errors** - We rely on lapin's built-in error types and connection status.
3. **Automatic Reconnection** - Consumer loops automatically reconnect when the connection is lost.
4. **Simple API** - No complex abstractions, just thin wrappers around lapin.

## Key Differences from Old Design

### Old Design Issues
- ❌ Double mutex locks (deadlock hazard)
- ❌ String matching on errors (`"invalid channel state"`)
- ❌ Fixed 5-second reconnection delay
- ❌ Complex SafeChannel abstraction hiding errors
- ❌ Channel pooling (unnecessary with lapin)

### New Design
- ✅ Single Arc<Connection> shared across app
- ✅ Channels created on-demand (they're cheap!)
- ✅ Lapin's native error types
- ✅ Simple retry logic for transient failures
- ✅ Consumer auto-reconnect with exponential backoff

## Usage

### Setup Connection

```rust
use rmq::connection;

let conn = connection::connect("amqp://guest:guest@localhost:5672").await?;
```

### Publish Messages

```rust
use rmq::publish;

publish(&conn, "my_exchange", "routing.key", b"message payload").await?;
```

### Declare Queue

```rust
use rmq::declare_queue;

let queue = declare_queue(&conn, "my_queue").await?;
```

### Consume Messages

```rust
use rmq::{consume, Delivery, BasicAckOptions};
use std::sync::Arc;

let handler = Arc::new(|delivery: Delivery| {
    Box::pin(async move {
        // Process the message
        let payload = std::str::from_utf8(&delivery.data)?;
        println!("Got message: {}", payload);
        
        // Ack the message
        delivery.ack(BasicAckOptions::default()).await?;
        
        Ok(())
    })
});

// This will run forever, auto-reconnecting on connection loss
consume(&conn, "my_queue", 1, handler).await?;
```

## How It Handles Your Errors

### CONNECTION_FORCED (Graceful Shutdown)
```
CONNECTION_FORCED - broker forced connection closure with reason 'shutdown'
```
**Behavior:** Consumer automatically waits 1 second and reconnects. Connection is recreated automatically by lapin.

### Channel Errors
```
invalid channel state: Error
```
**Behavior:** We create a fresh channel for each operation. No state is cached.

### Network Timeouts
```
IOError(Os { code: 110, kind: TimedOut, message: "Connection timed out" })
```
**Behavior:** publish() retries up to 3 times with 100ms delay. Consumer reconnects automatically.

## Architecture

```
┌─────────────────────┐
│   Your Application  │
└──────────┬──────────┘
           │
           │ Arc<Connection>
           │
    ┌──────▼──────────────────┐
    │  lapin::Connection      │  ← Shared, long-lived
    │  (with auto-recovery)   │
    └──────┬──────────────────┘
           │
           │ create_channel() on each operation
           │
    ┌──────▼──────────────────┐
    │  lapin::Channel         │  ← Created as needed, not pooled
    │  (cheap, disposable)    │
    └─────────────────────────┘
```

## Testing

To test with a real RabbitMQ instance:

```bash
docker run -d -p 5672:5672 rabbitmq:3-management
cargo test -- --ignored
```

## Migration from Old Code

### Old Pattern
```rust
let sc = SafeChannel::new(url);
sc.ensure().await;
let channel = sc.get().await?;
```

### New Pattern
```rust
let conn = connection::connect(url).await?;
// Just use conn directly, channels are created as needed
```

### Old Consumer
```rust
consume_normal(&sc, queue_name, handler).await?;
```

### New Consumer
```rust
consume(&conn, queue_name, prefetch_count, handler).await?;
```

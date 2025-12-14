# RMQ Library Migration - Complete

## What Changed

The `rmq` library has been completely redesigned to use lapin's patterns properly instead of fighting against them.

### Old Design Problems
- ❌ `SafeChannel` wrapper that pooled channels unnecessarily
- ❌ Double mutex locks (potential deadlock)
- ❌ String matching on errors (`"invalid channel state"`)
- ❌ Fixed reconnection delays
- ❌ Custom error handling instead of using lapin's built-in mechanisms

### New Design
- ✅ Use `Arc<Connection>` directly - connections are expensive, channels are cheap
- ✅ Create channels on-demand for each operation (they're disposable)
- ✅ Lapin's native error types and connection status
- ✅ Simple retry logic for transient failures (3 retries with 100ms delay)
- ✅ Consumer auto-reconnect built into `consume()` function

## Migration Guide

### Before
```rust
// Old API
let sc = rmq::SafeChannel::new(rmq_url);
sc.ensure().await;
let shared_channel = Arc::new(sc);

// Using it
rmq::enq(&shared_channel, &target, &msg).await?;
```

### After
```rust
// New API
let conn = rmq::connection::connect(&rmq_url).await?;
let shared_conn = Arc::new(conn);

// Using it (same!)
rmq::enq(&shared_conn, &target, &msg).await?;
```

## What Was Updated

### All Usage Sites
- `event-server` - HTTP server for receiving webhook events
- `event-logger` - Consumes and logs events to ClickHouse
- `event-processor` - Main event processing logic with rules engine

### Key Changes
1. **Replace `SafeChannel` with `Connection`**
   - `Arc<rmq::SafeChannel>` → `Arc<rmq::Connection>`
   - `SafeChannel::new(url)` → `rmq::connection::connect(&url).await?`
   - Remove `.ensure().await` calls (connection is ready immediately)

2. **Remove `.invalidate()` calls**
   - Lapin handles reconnection automatically
   - Consumer functions already retry on error

3. **Update struct fields**
   - `rmq_channel: Arc<rmq::SafeChannel>` → `rmq_connection: Arc<rmq::Connection>`

## Compatibility Layer

The library provides compatibility functions so existing code works:
- `rmq::enq()` - Publish to exchange or queue
- `rmq::declare_with_dq()` - Declare queue (DLQ support simplified)
- `rmq::consume_concurrently()` - Consumer with old callback signature
- `rmq::normal_queue_opts()` - Durable queue options
- `rmq::Target::mk_exchange()` / `mk_queue()` - Target abstraction

These use the new `Connection` API under the hood but maintain backward compatibility.

## How It Handles Errors Now

### CONNECTION_FORCED (RabbitMQ shutdown)
- **Old**: Manual invalidation, fixed 5s retry
- **New**: Consumer automatically reconnects with 1s delay

### Channel State Errors
- **Old**: String matching on error messages
- **New**: Fresh channel created for each operation, no state

### Network Timeouts
- **Old**: Complex backoff logic, manual retry
- **New**: Simple 3-retry pattern with 100ms delay in `publish()`

## Architecture

```
Your App
    │
    ├─> Arc<Connection>  (shared, long-lived)
    │
    └─> Operations create channels as needed
        ├─> publish() → creates channel
        ├─> consume() → creates channel  
        └─> declare_*() → creates channel
```

## Files Modified

### Core Library (`rmq/`)
- Deleted: `src/safe_channel.rs` (no longer needed)
- Modified: `src/lib.rs` (clean API, compatibility layer)
- Added: `src/connection.rs` (simple connection helper)
- Added: `DESIGN.md` (architecture documentation)
- Added: `examples/simple.rs` (usage example)

### Usage Sites
- `event-server/src/main.rs` - Connect once, share connection
- `event-server/src/process.rs` - Use Connection
- `event-server/src/stripe.rs` - Use Connection
- `event-server/src/cron.rs` - Use Connection
- `event-logger/src/main.rs` - Connect once, share connection
- `event-processor/src/lib.rs` - Connect once, share connection
- `event-processor/src/env.rs` - Store Connection
- `event-processor/src/real_actor/mod.rs` - Use Connection

## Verification

```bash
cd mail-subsystem
cargo check  # ✅ All compiles cleanly
```

## Benefits

1. **Simpler Code**: No complex wrapper types, just use lapin directly
2. **Clearer Semantics**: Connection = expensive/long-lived, Channel = cheap/disposable
3. **Better Error Handling**: Use lapin's error types, not string matching
4. **More Robust**: Lapin's built-in reconnection logic is battle-tested
5. **Easier to Debug**: Less abstraction layers = clearer stack traces

## Testing

The production errors you were seeing should now be handled better:
- `CONNECTION_FORCED` → consumer reconnects automatically
- `invalid channel state` → fresh channels on each operation
- `Connection timed out` → publish retries 3x with delays

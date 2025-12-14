use anyhow::*;
use futures_util::stream::StreamExt;
use log::{error, warn, info};
use std::sync::Arc;
use std::time::Duration;
use std::result::Result as StdResult;

pub mod connection;

pub use lapin::{
    Connection,
    options::{QueueDeclareOptions, ExchangeDeclareOptions, BasicPublishOptions, BasicConsumeOptions, BasicAckOptions, BasicNackOptions, BasicQosOptions, QueueBindOptions},
    types::FieldTable,
    Queue,
    ExchangeKind,
    BasicProperties,
    message::Delivery,
    publisher_confirm::PublisherConfirm,
};

pub use connection::connect;

/// Publish a message with automatic retry on channel errors
pub async fn publish(
    connection: &Connection,
    exchange: &str,
    routing_key: &str,
    payload: &[u8],
) -> Result<PublisherConfirm> {
    const MAX_RETRIES: u32 = 3;
    let mut last_error = None;

    for attempt in 1..=MAX_RETRIES {
        // Get a fresh channel for each attempt
        match connection.create_channel().await {
            StdResult::Ok(channel) => {
                match channel.basic_publish(
                    exchange,
                    routing_key,
                    BasicPublishOptions::default(),
                    payload,
                    BasicProperties::default()
                        .with_content_type("application/json".into())
                        .with_delivery_mode(2), // persistent
                ).await {
                    StdResult::Ok(confirm) => return Ok(confirm),
                    StdResult::Err(e) => {
                        warn!("Publish attempt {}/{} failed: {:?}", attempt, MAX_RETRIES, e);
                        last_error = Some(e);
                        
                        if attempt < MAX_RETRIES {
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                    }
                }
            }
            StdResult::Err(e) => {
                warn!("Failed to create channel for publish attempt {}/{}: {:?}", attempt, MAX_RETRIES, e);
                last_error = Some(e);
                
                if attempt < MAX_RETRIES {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }

    Err(anyhow!(last_error.expect("Should have error after retries")))
}

/// Declare a durable queue
pub async fn declare_queue(
    connection: &Connection,
    name: &str,
) -> Result<Queue> {
    let channel = connection.create_channel().await?;
    
    let queue = channel.queue_declare(
        name,
        QueueDeclareOptions {
            durable: true,
            ..Default::default()
        },
        FieldTable::default(),
    ).await?;

    Ok(queue)
}

/// Declare a durable exchange
pub async fn declare_exchange(
    connection: &Connection,
    name: &str,
    kind: ExchangeKind,
) -> Result<()> {
    let channel = connection.create_channel().await?;
    
    channel.exchange_declare(
        name,
        kind,
        ExchangeDeclareOptions {
            durable: true,
            ..Default::default()
        },
        FieldTable::default(),
    ).await?;

    Ok(())
}

/// Bind queue to exchange
pub async fn bind_queue(
    connection: &Connection,
    queue_name: &str,
    exchange_name: &str,
    routing_key: &str,
) -> Result<()> {
    let channel = connection.create_channel().await?;
    
    channel.queue_bind(
        queue_name,
        exchange_name,
        routing_key,
        QueueBindOptions::default(),
        FieldTable::default(),
    ).await?;

    Ok(())
}

/// Consumer handler type - takes Delivery and processes it
/// Handler is responsible for acking/nacking the message
type MessageHandler = Arc<dyn Fn(Delivery) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>> + Send + Sync>;

/// Consume messages from a queue with automatic reconnection
/// This blocks forever, processing messages as they arrive
pub async fn consume(
    connection: &Connection,
    queue_name: &str,
    prefetch_count: u16,
    handler: MessageHandler,
) -> Result<()> {
    loop {
        match consume_once(connection, queue_name, prefetch_count, handler.clone()).await {
            StdResult::Ok(_) => {
                warn!("Consumer stream ended for queue '{}', reconnecting...", queue_name);
            }
            StdResult::Err(e) => {
                error!("Consumer error for queue '{}': {:?}, reconnecting...", queue_name, e);
            }
        }
        
        // Wait a bit before reconnecting
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Single consumption cycle - consumes until stream ends or error
async fn consume_once(
    connection: &Connection,
    queue_name: &str,
    prefetch_count: u16,
    handler: MessageHandler,
) -> Result<()> {
    // Create a fresh channel for this consumption cycle
    let channel = connection.create_channel().await?;
    
    // Set QoS
    channel.basic_qos(prefetch_count, BasicQosOptions { global: false }).await?;
    
    // Create consumer
    let mut consumer = channel.basic_consume(
        queue_name,
        "", // auto-generated consumer tag
        BasicConsumeOptions::default(),
        FieldTable::default(),
    ).await?;

    info!("Started consuming from queue '{}'", queue_name);

    // Process messages
    while let Some(delivery_result) = consumer.next().await {
        match delivery_result {
            StdResult::Ok(delivery) => {
                // Call handler
                if let StdResult::Err(e) = handler(delivery).await {
                    error!("Handler error: {:?}", e);
                }
            }
            StdResult::Err(e) => {
                error!("Error receiving delivery: {:?}", e);
                return Err(anyhow!(e));
            }
        }
    }

    Ok(())
}

/// Declare queue with dead-letter queue (returns same queue twice for now)
pub async fn declare_with_dq(
    connection: &Connection,
    name: &str,
    _opts: QueueDeclareOptions,
) -> Result<(Queue, Queue)> {
    let q = declare_queue(connection, name).await?;
    Ok((q.clone(), q))
}

/// Durable queue options
pub fn normal_queue_opts() -> QueueDeclareOptions {
    QueueDeclareOptions {
        durable: true,
        ..Default::default()
    }
}


/// Bind queue to exchange
pub async fn bind_queue_to_exchange(
    connection: &Connection,
    queue_name: &str,
    exchange_name: &str,
    routing_key: &str,
) -> Result<()> {
    bind_queue(connection, queue_name, exchange_name, routing_key).await
}

/// Target for publishing (exchange or queue)
pub enum Target {
    Exchange(String),
    Queue(String),
}

impl Target {
    pub fn mk_exchange(name: &str) -> Self {
        Target::Exchange(name.to_string())
    }

    pub fn mk_queue(name: &str) -> Self {
        Target::Queue(name.to_string())
    }
}

/// Publish message to exchange or queue
pub async fn enq(
    connection: &Connection,
    target: &Target,
    msg: &[u8],
) -> Result<PublisherConfirm> {
    match target {
        Target::Exchange(exchange) => publish(connection, exchange, "", msg).await,
        Target::Queue(queue) => publish(connection, "", queue, msg).await,
    }
}

/// Compatibility: Old consumer handler types
pub type DeliveryHandler = Box<
    dyn Fn(
        Delivery,
        Box<dyn FnOnce(Delivery) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>> + Send>
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>
    + Send
    + Sync
>;

pub type AckFn = Box<dyn FnOnce(Delivery) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>> + Send>;

/// Consume messages concurrently with old callback-style handler
pub async fn consume_concurrently(
    connection: &Connection,
    concurrency: usize,
    queue_name: &str,
    handle_delivery: DeliveryHandler,
) -> Result<()> {
    let handle_delivery = Arc::new(handle_delivery);
    let handler: MessageHandler = Arc::new(move |delivery: Delivery| {
        let ack_fn: AckFn = Box::new(|dlv| {
            Box::pin(async move {
                dlv.ack(BasicAckOptions::default()).await?;
                Ok(())
            })
        });
        let handle_delivery = Arc::clone(&handle_delivery);
        Box::pin(async move {
            handle_delivery(delivery, ack_fn).await
        }) as std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>
    });
    
    consume(connection, queue_name, concurrency as u16, handler).await
}

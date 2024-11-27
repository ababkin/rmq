use anyhow::*;
use futures_util::stream::StreamExt;
use futures_util::Future;
use std::pin::Pin;
use std::result::Result::Ok;
use std::sync::Arc; // Import Arc

pub mod safe_channel;
pub use safe_channel::SafeChannel;
use lapin::{
    message::Delivery,
    types::{FieldTable, ShortString},
    Queue,
    options::*, 
    publisher_confirm::PublisherConfirm, 
    BasicProperties, 
    Consumer,
    Channel, 
    ExchangeKind,
    protocol::basic::AMQPProperties,
};


pub enum Target {
    Exchange(String),
    Queue(String)
}

impl Target {
    pub fn mk_queue(name: &str) -> Self {
        Target::Queue(name.to_string())
    }

    pub fn mk_exchange(name: &str) -> Self {
        Target::Exchange(name.to_string())
    }
}

pub async fn create_consumer(sc: &SafeChannel, queue: &'static str, consumer_tag: &'static str) -> Result<Consumer, Error> {
    let channel = sc.get().await?;
    Ok(channel.basic_consume(
        queue,
        consumer_tag,
        BasicConsumeOptions::default(),
        FieldTable::default(),
    )
    .await?)
}

// pub async fn enq(sc: &SafeChannel, target: &Target, msg: &[u8], props: AMQPProperties) -> Result<PublisherConfirm, Error> {
pub async fn enq(sc: &SafeChannel, target: &Target, msg: &[u8]) -> Result<PublisherConfirm, Error> {
    let channel = sc.get().await?;

    let (exchange_name, queue_name) = match target {
        Target::Exchange(exchange_name) => (exchange_name.as_str(), ""),
        Target::Queue(queue_name) => ("", queue_name.as_str())
    };

    Ok(channel.basic_publish(
        exchange_name,
        queue_name,
        BasicPublishOptions::default(),
        &msg,
        // BasicProperties::default().with_content_type("application/json".into()).with_delivery_mode(2),
        // props
        BasicProperties::default()
            .with_content_type("application/json".into())
            .with_delivery_mode(2),
    ).await?)
}

pub type AckFn = Box<dyn FnOnce(Delivery) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send>> + Send>;
pub type DeliveryHandler = Box<dyn Fn(Delivery, AckFn) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send>> + Send>;

pub async fn consume_normal(sc: &SafeChannel, queue_name: &str, handle_delivery: DeliveryHandler) -> Result<(), Error> {
    let channel = sc.get().await?;

    // Setting prefetch count to 1 to ensure that only one message is processed at a time
    let options = BasicQosOptions {
        global: false,      // Apply setting per consumer, not to the entire channel
    };
    channel.basic_qos(1, options).await?;

    // info!("will consume");
    let mut consumer = channel
        .basic_consume(
            queue_name,
            "",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;
    // info!(state=?conn.status().state());

    while let Some(delivery_result) = consumer.next().await {
        // info!(message=?delivery, "received message");
        if let Ok(delivery) = delivery_result {

            let ack: AckFn = Box::new(|dlv| {
                Box::pin(async move {
                    dlv.ack(BasicAckOptions::default()).await?;
                    Ok(())
                })
            });

            handle_delivery(delivery, ack).await?
        }
    };
    Ok(())
}

pub async fn consume_concurrently(sc: &SafeChannel, concurrency: usize, queue_name: &str, handle_delivery: DeliveryHandler) -> Result<(), Error> {
    let channel = sc.get().await?;

    // Setting prefetch count to match the concurrency limit
    let options = BasicQosOptions {
        global: false,      // Apply setting per consumer, not to the entire channel
    };
    channel.basic_qos(concurrency as u16, options).await?;

    let consumer = channel
        .basic_consume(
            queue_name,
            "",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;

    let handle_delivery = Arc::new(handle_delivery);

    // Process messages concurrently up to the specified limit
    consumer.for_each_concurrent(concurrency, |delivery_result| {
        let handle_delivery_clone = Arc::clone(&handle_delivery); // Clone the Arc for each task
        async move {
            if let Ok(delivery) = delivery_result {
                let ack: AckFn = Box::new(|dlv| {
                    Box::pin(async move {
                        dlv.ack(BasicAckOptions::default()).await?;
                        Ok(())
                    })
                });

                // Use cloned handle_delivery in each async task
                if let Err(e) = handle_delivery_clone(delivery, ack).await {
                    eprintln!("Error handling message: {:?}", e);
                }
            }
        }
    }).await;

    Ok(())
}

pub async fn declare_queue(sc: &SafeChannel, name: &str, opts: QueueDeclareOptions) -> Result<Queue, Error> {
    let chan = sc.get().await?;

    let queue = chan.queue_declare(
        name,
        opts,
        FieldTable::default(),
    ).await?;

    Ok(queue)
}

pub async fn declare_with_dq(
    sc: &SafeChannel,
    name: &str,
    opts: QueueDeclareOptions,
) -> Result<(Queue, Queue), Error> {
    let chan = sc.get().await?;

    // Declare the dead-letter queue
    let dead_queue_name = format!("{}_dq", name);
    let dead_queue = chan
        .queue_declare(
            &dead_queue_name,
            QueueDeclareOptions {
                durable: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await?;

    // Add dead-letter arguments to the main queue
    let mut arguments = FieldTable::default();
    arguments.insert(
        ShortString::from("x-dead-letter-exchange"),
        lapin::types::AMQPValue::ShortString("".into()), // Default exchange
    );
    arguments.insert(
        ShortString::from("x-dead-letter-routing-key"),
        lapin::types::AMQPValue::ShortString(dead_queue_name.into()),
    );
    arguments.insert(
        ShortString::from("x-max-delivery-attempts"),
        lapin::types::AMQPValue::LongInt(5),
    );

    // Declare the main queue with dead-letter setup
    let main_queue = chan
        .queue_declare(
            name,
            opts,
            arguments,
        )
        .await?;

    Ok((main_queue, dead_queue))
}

pub async fn declare_exchange(sc: &SafeChannel, name: &str, kind: ExchangeKind, opts: ExchangeDeclareOptions) -> Result<(), Error> {
    let chan = sc.get().await?;

    chan.exchange_declare(
        name,
        kind,
        opts,
        FieldTable::default(),
    ).await?;

    Ok(())
}

// Bind a queue to an exchange with a routing key
pub async fn bind_queue_to_exchange(sc: &SafeChannel, queue_name: &str, exchange_name: &str, routing_key: &str) -> Result<(), Error> {
    let chan = sc.get().await?;

    chan.queue_bind(
        queue_name,
        exchange_name,
        routing_key,
        QueueBindOptions::default(),
        FieldTable::default(),
    ).await?;

    Ok(())
}

// Queue declare options with defaults
pub fn normal_queue_opts() -> QueueDeclareOptions {
    QueueDeclareOptions {
        durable: true,  // Queue survives broker restarts
        ..QueueDeclareOptions::default()
    }
}

// Exchange declare options with defaults
pub fn normal_exchange_opts() -> ExchangeDeclareOptions {
    ExchangeDeclareOptions {
        durable: true,  // Exchange survives broker restarts
        ..ExchangeDeclareOptions::default()
    }
}
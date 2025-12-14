use rmq::{connection, publish, declare_queue, consume, Delivery, BasicAckOptions};
use std::sync::Arc;
use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    // 1. Connect to RabbitMQ
    let conn = connection::connect("amqp://guest:guest@localhost:5672").await?;
    
    // 2. Declare a queue
    let queue_name = "example_queue";
    declare_queue(&conn, queue_name).await?;
    
    // 3. Publish some messages
    for i in 1..=5 {
        let msg = format!("Message {}", i);
        publish(&conn, "", queue_name, msg.as_bytes()).await?;
        println!("Published: {}", msg);
    }
    
    // 4. Consume messages
    let handler = Arc::new(|delivery: Delivery| {
        Box::pin(async move {
            let payload = std::str::from_utf8(&delivery.data)?;
            println!("Received: {}", payload);
            
            // Ack the message
            delivery.ack(BasicAckOptions::default()).await?;
            
            Ok::<(), anyhow::Error>(())
        }) as std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>
    });
    
    println!("Starting consumer...");
    consume(&conn, queue_name, 1, handler).await?;
    
    Ok(())
}

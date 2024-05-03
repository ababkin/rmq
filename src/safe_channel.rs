use std::env;
use tokio::sync::Mutex;
use lapin::{ Channel, Connection, ConnectionProperties };
use anyhow::{ Result, Error };
use tracing::{error, info, debug, warn, Level};


#[derive(Debug)]
pub struct SafeChannel(Mutex<Option<Channel>>);

impl SafeChannel {
    pub fn new() -> Self {
        SafeChannel(Mutex::new(None))
    }

    // pub async fn ensure(&self) {
    //     loop {
    //         debug!("Trying to open RabbitMQ channel...");
    //         match create().await {
    //             Ok(chan) => {
    //                 let mut locked = self.0.lock().await;
    //                 *locked = Some(chan);
    //                 debug!("RabbitMQ channel is established.");
    //                 break;
    //             },
    //             Err(e) => {
    //                 error!("Failed to create channel: {:?}", e);
    //                 tokio::time::sleep(tokio::time::Duration::from_secs(1)).await; // Wait before retrying
    //             }
    //         }
    //     }
    // }

    pub async fn ensure(&self) {
        let mut locked = self.0.lock().await;
        if locked.is_none() {
            debug!("Trying to open RabbitMQ channel...");
            match create().await {
                Ok(chan) => {
                    *locked = Some(chan);
                    debug!("RabbitMQ channel is established.");
                },
                Err(e) => {
                    error!("Failed to create channel: {:?}", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await; // Wait before retrying
                }
            }
        }
    }

    pub async fn invalidate(&self) -> () {
        let mut lock = self.0.lock().await;
        *lock = None;
    }

    pub async fn get(&self) -> Result<Channel, Error> {
        {
            let lock = self.0.lock().await;
            if let Some(channel) = &*lock {
                return Ok(channel.clone());  // Clone the channel before returning
            }
        }
        warn!("RabbitMQ channel lost, attempting to reconnect...");
        self.ensure().await;
        Box::pin(self.get()).await  // Use Box::pin to handle recursion
    }

}

async fn create() -> Result<Channel, lapin::Error> {
    // let addr = env::var("AMQP_ADDR").unwrap_or_else(|_| "amqp://127.0.0.1:5672/%2f".into());
    let amqp_addr = env::var("STACKHERO_RABBITMQ_AMQP_URL_TLS").expect("STACKHERO_RABBITMQ_AMQP_URL_TLS must be set");

    let conn = Connection::connect(&amqp_addr, ConnectionProperties::default()).await?;
    conn.create_channel().await
}


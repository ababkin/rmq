use std::env;
use tokio::sync::Mutex;
use lapin::{ Channel, Connection, ConnectionProperties };
use anyhow::{ Result, Error };
use tracing::{error, info, debug, warn, Level};


#[derive(Debug)]
pub struct SafeChannel {
    channel: Mutex<Option<Channel>>,
    url: String,
}

impl SafeChannel {
    pub fn new(url: String) -> Self {
        SafeChannel{ 
            channel: Mutex::new(None),
            url
        }
    }

    pub async fn ensure(&self) {
        loop {
            debug!("Trying to open RabbitMQ channel...");
            match create(&self.url).await {
                Ok(chan) => {
                    let mut locked = self.channel.lock().await;
                    *locked = Some(chan);
                    debug!("RabbitMQ channel is established.");
                    break;
                },
                Err(e) => {
                    error!("Failed to create channel: {:?}", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await; // Wait before retrying
                }
            }
        }
    }

    pub async fn invalidate(&self) -> () {
        let mut lock = self.channel.lock().await;
        *lock = None;
    }

    pub async fn get(&self) -> Result<Channel, Error> {
        loop {
            {
                let lock = self.channel.lock().await;
                if let Some(channel) = &*lock {
                    return Ok(channel.clone());  // Clone the channel before returning
                }
            }
            warn!("RabbitMQ channel lost, attempting to reconnect...");
            self.ensure().await;
            // tokio::time::sleep(tokio::time::Duration::from_secs(1)).await; // Wait before retrying
        }
    }

}

async fn create(url: &str) -> Result<Channel, lapin::Error> {
    let conn = Connection::connect(url, ConnectionProperties::default()).await?;
    conn.create_channel().await
}


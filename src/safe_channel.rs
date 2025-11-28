use std::env;
use tokio::sync::Mutex;
use lapin::{ Channel, Connection, ConnectionProperties };
use anyhow::{ Result, Error };
use log::{error, info, debug, warn};


#[derive(Debug)]
pub struct SafeChannel {
    channel: Mutex<Option<Channel>>,
    url: String,
    connection: Mutex<Option<Connection>>,
}

impl SafeChannel {
    pub fn new(url: String) -> Self {
        SafeChannel{ 
            channel: Mutex::new(None),
            connection: Mutex::new(None),
            url
        }
    }

    pub async fn ensure(&self) {
        loop {
            debug!("Trying to open RabbitMQ connection and channel...");
            match create_connection_and_channel(&self.url).await {
                Ok((conn, chan)) => {
                    {
                        let mut conn_locked = self.connection.lock().await;
                        *conn_locked = Some(conn);
                    }
                    
                    {
                        let mut chan_locked = self.channel.lock().await;
                        *chan_locked = Some(chan);
                    }
                    
                    debug!("RabbitMQ connection and channel are established.");
                    break;
                },
                Err(e) => {
                    error!("Failed to create connection or channel: {:?}", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await; // Wait before retrying
                }
            }
        }
    }

    pub async fn invalidate(&self) -> () {
        debug!("Invalidating RabbitMQ channel and connection");
        {
            let mut chan_lock = self.channel.lock().await;
            *chan_lock = None;
        }
        
        {
            let mut conn_lock = self.connection.lock().await;
            if let Some(conn) = conn_lock.take() {
                // Close the connection gracefully if possible
                if let Err(e) = conn.close(200, "Reconnecting").await {
                    error!("Error while closing connection: {:?}", e);
                }
            }
        }
    }

    pub async fn get(&self) -> Result<Channel, Error> {
        loop {
            {
                let lock = self.channel.lock().await;
                if let Some(channel) = &*lock {
                    // Check if channel is still in a good state
                    if channel.status().connected() {
                        return Ok(channel.clone());  // Clone the channel before returning
                    }
                    // If we get here, the channel exists but is not connected
                    drop(lock); // Drop the lock before calling invalidate
                    warn!("RabbitMQ channel is not connected, invalidating...");
                    self.invalidate().await;
                } else {
                    // No channel, drop lock and call ensure
                    drop(lock);
                    warn!("RabbitMQ channel not initialized, attempting to connect...");
                    self.ensure().await;
                }
            }
        }
    }
}

async fn create_connection_and_channel(url: &str) -> Result<(Connection, Channel), lapin::Error> {
    let conn = Connection::connect(url, ConnectionProperties::default()).await?;
    let channel = conn.create_channel().await?;
    Ok((conn, channel))
}


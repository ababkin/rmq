use lapin::{Connection, ConnectionProperties};
use log::{info, error, warn};
use std::sync::Arc;
use tokio::sync::RwLock;
use std::time::Duration;

/// Create a connection to RabbitMQ
pub async fn connect(url: &str) -> Result<Connection, lapin::Error> {
    info!("Connecting to RabbitMQ at {}", url);
    
    let properties = ConnectionProperties::default()
        .with_connection_name("rmq-connection".into());
    
    let connection = Connection::connect(url, properties).await?;
    
    connection.on_error(|error| {
        error!("RabbitMQ connection error: {:?}", error);
    });
    
    info!("Connected to RabbitMQ successfully");
    Ok(connection)
}

/// A resilient connection wrapper that automatically reconnects when the connection fails
pub struct ResilientConnection {
    url: String,
    inner: Arc<RwLock<Option<Connection>>>,
}

impl ResilientConnection {
    /// Create a new resilient connection
    pub async fn new(url: String) -> Result<Self, lapin::Error> {
        let connection = connect(&url).await?;
        Ok(Self {
            url,
            inner: Arc::new(RwLock::new(Some(connection))),
        })
    }

    /// Check if the current connection is valid and reconnect if necessary
    async fn ensure_connected(&self) -> Result<(), lapin::Error> {
        // First check if we need to reconnect
        {
            let conn = self.inner.read().await;
            if let Some(c) = conn.as_ref() {
                if c.status().connected() {
                    return Ok(());
                }
            }
        }

        // Need to reconnect
        let mut conn_write = self.inner.write().await;
        
        // Double-check in case another task already reconnected
        if let Some(c) = conn_write.as_ref() {
            if c.status().connected() {
                return Ok(());
            }
        }

        warn!("Connection lost, reconnecting to RabbitMQ...");
        
        // Try to reconnect with exponential backoff
        const MAX_RETRIES: u32 = 5;
        let mut retry_delay = Duration::from_millis(100);
        
        for attempt in 1..=MAX_RETRIES {
            match connect(&self.url).await {
                Ok(new_conn) => {
                    info!("Successfully reconnected to RabbitMQ on attempt {}", attempt);
                    *conn_write = Some(new_conn);
                    return Ok(());
                }
                Err(e) => {
                    error!("Failed to reconnect on attempt {}/{}: {:?}", attempt, MAX_RETRIES, e);
                    if attempt < MAX_RETRIES {
                        // Release lock during sleep
                        drop(conn_write);
                        tokio::time::sleep(retry_delay).await;
                        retry_delay = std::cmp::min(retry_delay * 2, Duration::from_secs(5));
                        // Reacquire lock
                        conn_write = self.inner.write().await;
                    } else {
                        *conn_write = None;
                        return Err(e);
                    }
                }
            }
        }
        
        unreachable!("Should have returned from loop");
    }

    /// Create a channel, reconnecting if necessary
    pub async fn create_channel(&self) -> Result<lapin::Channel, lapin::Error> {
        const MAX_RETRIES: u32 = 3;
        let mut retry_delay = Duration::from_millis(100);
        
        for attempt in 1..=MAX_RETRIES {
            // Ensure we're connected
            self.ensure_connected().await?;
            
            // Try to create a channel
            let result = {
                let conn = self.inner.read().await;
                if let Some(c) = conn.as_ref() {
                    c.create_channel().await
                } else {
                    return Err(lapin::Error::InvalidConnectionState(
                        lapin::ConnectionState::Error
                    ));
                }
            };
            
            match result {
                Ok(channel) => return Ok(channel),
                Err(e) => {
                    warn!("Failed to create channel on attempt {}/{}: {:?}", attempt, MAX_RETRIES, e);
                    
                    // Mark connection as invalid and retry
                    {
                        let mut conn = self.inner.write().await;
                        *conn = None;
                    }
                    
                    if attempt < MAX_RETRIES {
                        tokio::time::sleep(retry_delay).await;
                        retry_delay = std::cmp::min(retry_delay * 2, Duration::from_secs(2));
                    } else {
                        return Err(e);
                    }
                }
            }
        }
        
        unreachable!("Should have returned from loop");
    }
}

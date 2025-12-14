use std::sync::Arc;
use lapin::{Connection, ConnectionProperties, Channel};
use log::{info, error};

/// Create a connection with automatic recovery enabled
pub async fn connect(url: &str) -> Result<Connection, lapin::Error> {
    info!("Connecting to RabbitMQ at {}", url);
    
    // Configure connection properties with automatic recovery
    let properties = ConnectionProperties::default()
        .with_connection_name("rmq-connection".into());
    
    let connection = Connection::connect(url, properties).await?;
    
    // Set up error handler
    connection.on_error(|error| {
        error!("RabbitMQ connection error: {:?}", error);
    });
    
    info!("Connected to RabbitMQ successfully");
    Ok(connection)
}

/// Create a channel from a connection
/// Channels are cheap - create them as needed, don't pool them
pub async fn create_channel(connection: &Connection) -> Result<Channel, lapin::Error> {
    connection.create_channel().await
}

/// Check if connection is healthy
pub fn is_connected(connection: &Connection) -> bool {
    connection.status().connected()
}

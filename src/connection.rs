use lapin::{Connection, ConnectionProperties};
use log::{info, error};

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

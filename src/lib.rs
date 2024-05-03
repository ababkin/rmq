use lapin::{
    options::*, publisher_confirm::PublisherConfirm, types::FieldTable, BasicProperties, 
    Consumer, message::Delivery
};
use anyhow::{*, Result, Error};

pub mod safe_channel;
use safe_channel::*;


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

pub async fn publish(sc: &SafeChannel, q: &str, payload: &[u8]) 
    -> Result<PublisherConfirm, Error> {
    let channel = sc.get().await?;
    Ok(channel
        .basic_publish(
            "",
            q,
            BasicPublishOptions::default(),
            payload,
            BasicProperties::default(),
        )
        .await?)
}

pub async fn ack(sc: &SafeChannel, delivery: &Delivery) -> Result<(), Error> {
    let channel = sc.get().await?;
    let delivery_tag = delivery.delivery_tag;
    Ok(channel.basic_ack(
        delivery_tag, BasicAckOptions::default()
    ).await?)
}
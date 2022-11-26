#[macro_use]
extern crate log;

use std::collections::HashMap;
use futures::stream::StreamExt;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
pub struct Sensor {
    id: String,
    location: String
}

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    env_logger::init();

    let mut sensors = HashMap::<String, Sensor>::new();

    let mut handles = vec![];

    let client = async_nats::connect("nats://localhost:4222").await?;

    // spawn a task - listen for PROPERTY_CHANGED events
    handles.push(tokio::task::spawn({
        let client = client.clone();
        async move {
            let mut subscriber = client.subscribe("*.event".to_string()).await?;

            println!("Awaiting events");
            while let Some(message) = subscriber.next().await {
                if message.payload == "PROPERTY_CHANGED" {
                    info!("got message from: {}", message.subject);

                    // get the sensor id from the subject
                    let mut tokens_iter = message.subject.split(".");
                    let sensor = tokens_iter.next().unwrap();

                    info!("sensor {} property changed", sensor);

                    // get the new location from the sensor
                    let response = client
                        .request(format!("{}.get", sensor), "location".into())
                        .await?;
                    
                    info!("get response: {:#?}", response);

                    let location = String::from_utf8_lossy(&response.payload.to_vec()).to_string();

                    sensors.insert(sensor.to_string(), Sensor{
                        id: sensor.to_string(),
                        location: String::from(&location)
                    });

                    // store the location in memory and on disk for when the sensor asks for it below
                    std::fs::write(
                        "/tmp/test.json",
                        serde_json::to_string_pretty(&sensors).unwrap(),
                    )?;
                }
            }

            Ok::<(), async_nats::Error>(())
        }
    }));

    // spawn a task that processes get location requests
    handles.push(tokio::task::spawn({
        let client = client.clone();
        async move {
            let mut subscriber = client.subscribe("000.get.location".to_string()).await?;

            println!("Awaiting get requests");
            while let Some(message) = subscriber.next().await {
                if message.payload == "380" {
                    info!("got message from: {}", message.subject);

                    // get the sensor id from the subject
                    let mut tokens_iter = message.subject.split(".");
                    let sensor = tokens_iter.next().unwrap();

                    info!("sensor {} property changed", sensor);

                    // get the new location from the sensor
                    // store the location in memory and on disk for when the sensor asks for it below
                }
            }

            Ok::<(), async_nats::Error>(())
        }
    }));

    futures::future::join_all(handles).await;

    Ok(())
}

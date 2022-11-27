#[macro_use]
extern crate log;

use futures::stream::StreamExt;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Sensor {
    id: String,
    location: String,
}

#[tokio::main]
async fn main() -> Result<(), async_nats::Error> {
    env_logger::init();

    let mut handles = vec![];
    let client = async_nats::connect("nats://localhost:4222").await?;

    // let mut sensors = HashMap::<String, Sensor>::new();
    // let mut shared_sensors = Arc::new(sensors);

    // how to use hashmap across threads/tasks with Arc and Mutex

    // initialize the map within an Arc (for sharing) and a Mutex (for synchronization)
    //https://stackoverflow.com/questions/39045636/how-do-i-have-one-thread-that-deletes-from-a-hashmap-and-another-that-inserts
    let sensors: Arc<Mutex<HashMap<String, Sensor>>> = Arc::new(Mutex::new(HashMap::new())); 

    // clone the Arc so it can be owned jointly by multiple threads
    let sensors_events = sensors.clone(); 
    let sensors_get = sensors.clone();

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

                    // get the sensors hashmap from mutex
                    sensors_events.lock().unwrap().insert(
                        sensor.to_string(),
                        Sensor {
                            id: sensor.to_string(),
                            location: String::from(&location),
                        },
                    );
                    
                    // store the location in memory and on disk for when the sensor asks for it below
                    std::fs::write(
                        "/tmp/test.json",
                        serde_json::to_string_pretty(&*sensors_events.lock().unwrap()).unwrap()
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
                // use the message payload to get the location
                let sensor_id = String::from_utf8_lossy(&message.payload.to_vec()).to_string();

                info!("requesting location for {}", sensor_id);

                // get the requested sensor location from sensors
                // TODO if let/ match in case we haven;t seen sensor and location is not found
                let sensor = sensors_get.lock().unwrap().get(&sensor_id).unwrap().clone();

                info!("location for sensor {:#?} is {:#?}", sensor, sensor.location);

                // reply to request with location
                // TODO check for errors
                client.publish(message.reply.unwrap(), sensor.location.into()).await.unwrap();
            }

            Ok::<(), async_nats::Error>(())
        }
    }));

    futures::future::join_all(handles).await;

    Ok(())
}

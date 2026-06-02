/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
mod handler;
use handler::IggyMqtt5Handler;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::info;
use secrecy::ExposeSecret;
use async_trait::async_trait;
use flowsdk::mqtt_client::{
    MqttClientOptions, TokioAsyncClientConfig, TokioAsyncMqttClient,
};
use iggy_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Sink, TopicMetadata, sink_connector,
};
use secrecy::SecretString;
use serde::{Deserialize, Serialize};

sink_connector!(MqttSink);

pub struct MqttSink {
    pub id: u32,
    client: Option<TokioAsyncMqttClient>,
    config: MqttSinkConfig,
    messages_sent: AtomicU64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MqttSinkConfig {
    url: String,
    client_id: String,
    keep_alive: u16,
    topic: String,
    username: Option<String>,
    #[serde(serialize_with = "iggy_common::serde_secret::serialize_optional_secret")]
    password: Option<SecretString>,
    reconnect: bool,
    qos: Option<u8>,
    retain: Option<bool>,
}

impl fmt::Debug for MqttSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MqttSink")
            .field("client", &self.client.as_ref().map(|_| "<connected>"))
            .finish()
    }
}

#[async_trait]
impl Sink for MqttSink {
    async fn open(&mut self) -> Result<(), Error> {
        info!("Opening MQTT sink connector with ID: {}.", self.id,);
        self.create_client().await?;
        self.client
            .as_ref()
            .expect("Client should be present")
            .connect()
            .await
            .map_err(|e| Error::InitError(format!("Client failed to connect: {e}")))?;

        Ok(())
    }
    async fn consume(
        &self,
        _topic_metadata: &TopicMetadata,
        _messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        for message in messages {
            let bytes = match message.payload.try_to_bytes() {
                Ok(result) => result,
                Err(error) => {Error::InitError(format!("Failed to serialize payload: {error}"));
                                continue;}
            };
            let slice = bytes.as_slice();
            let _sent = match self.client
            .as_ref()
            .expect("Client should be present")
            .publish(&self.config.topic, slice, self.config.qos.unwrap_or(2), self.config.retain.unwrap_or(false))
            .await {
                Ok(_sent) => self.messages_sent.fetch_add(1, Ordering::Relaxed),
                Err(error) => {Error::InitError(format!("Failed to send message: {error}"));
                                continue;}
            };
        }
        info!("MQTT Sink: sent {:?} messages", self.messages_sent);
        Ok(())
    }
    async fn close(&mut self) -> Result<(), Error> {
        self.client
            .as_ref()
            .expect("Client should be present")
            .disconnect()
            .await
            .map_err(|e| Error::InitError(format!("Client failed to send disconnect: {e}")))?;
        tokio::time::sleep(Duration::from_secs(1)).await;
        self.client
            .take()
            .expect("Client should be present")
            .shutdown()
            .await
            .map_err(|e| Error::InitError(format!("Client failed to send shutdown: {e}")))?;
        Ok(())
    }
}

impl MqttSink {
    pub fn new(id: u32, config: MqttSinkConfig) -> Self {
        MqttSink { id, client: None, config, messages_sent: AtomicU64::new(0) }
    }
    async fn create_client(&mut self) -> Result<(), Error> {
        let options;

        if self.config.password.is_none() && self.config.username.is_none() {
            options = MqttClientOptions::builder()
            .peer(&self.config.url)
            .client_id(&self.config.client_id)
            .keep_alive(self.config.keep_alive)
            .build();
        } else {
            options = MqttClientOptions::builder()
            .peer(&self.config.url)
            .username(self.config.username.as_ref().unwrap())
            .password(self.config.password.as_ref().unwrap().expose_secret())
            .client_id(&self.config.client_id)
            .keep_alive(self.config.keep_alive)
            .build();
        }
        let context = Arc::new(Mutex::new(None::<u16>));
        // Create event handler
        let event_handler = Box::new(IggyMqtt5Handler::new(
            "IggyMqttAsyncClient",
            context.clone(),
        ));

        let client =
            TokioAsyncMqttClient::new(options, event_handler, TokioAsyncClientConfig::default())
                .await
                .map_err(|e| Error::InitError(format!("Failed to create client: {e}")))?;
        client
            .connect_sync()
            .await
            .map_err(|e| Error::InitError(format!("Client failed to connect: {e}")))?;
        self.client = Some(client);
        Ok(())
    }
}

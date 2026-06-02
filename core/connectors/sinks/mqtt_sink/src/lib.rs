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
use async_trait::async_trait;
use flowsdk::mqtt_client::{MqttClientOptions, TokioAsyncClientConfig, TokioAsyncMqttClient};
use handler::IggyMqtt5Handler;
use iggy_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Sink, TopicMetadata, sink_connector,
};
use secrecy::ExposeSecret;
use secrecy::SecretString;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::info;

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
                Err(error) => {
                    Error::InitError(format!("Failed to serialize payload: {error}"));
                    continue;
                }
            };
            let slice = bytes.as_slice();
            let _sent = match self
                .client
                .as_ref()
                .expect("Client should be present")
                .publish(
                    &self.config.topic,
                    slice,
                    self.config.qos.unwrap_or(2),
                    self.config.retain.unwrap_or(false),
                )
                .await
            {
                Ok(_sent) => self.messages_sent.fetch_add(1, Ordering::Relaxed),
                Err(error) => {
                    Error::InitError(format!("Failed to send message: {error}"));
                    continue;
                }
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
        MqttSink {
            id,
            client: None,
            config,
            messages_sent: AtomicU64::new(0),
        }
    }
    async fn create_client(&mut self) -> Result<(), Error> {
        let options = if self.config.password.is_none() && self.config.username.is_none() {
            MqttClientOptions::builder()
                .peer(&self.config.url)
                .client_id(&self.config.client_id)
                .keep_alive(self.config.keep_alive)
                .build()
        } else {
            MqttClientOptions::builder()
                .peer(&self.config.url)
                .username(self.config.username.as_ref().unwrap())
                .password(self.config.password.as_ref().unwrap().expose_secret())
                .client_id(&self.config.client_id)
                .keep_alive(self.config.keep_alive)
                .build()
        };
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

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_connector_sdk::Payload;

    fn simd_json_from_str(s: &str) -> simd_json::OwnedValue {
        let mut bytes = s.as_bytes().to_vec();
        simd_json::serde::from_slice(&mut bytes).expect("Failed to build JSON value")
    }

    fn make_config() -> MqttSinkConfig {
        MqttSinkConfig {
            url: "127.0.0.1:1883".to_string(),
            client_id: "iggy_mqtt_sink".to_string(),
            keep_alive: 60,
            topic: "iggy/events".to_string(),
            username: Some("iggy".to_string()),
            password: Some(SecretString::from("iggy_password")),
            reconnect: true,
            qos: Some(1),
            retain: Some(true),
        }
    }

    #[test]
    fn given_full_toml_with_auth_should_parse_all_fields() {
        let toml = r#"
            url = "172.18.0.2:1883"
            client_id = "iggy_mqtt_sink"
            keep_alive = 60
            topic = "iggy/events"
            username = "iggy"
            password = "iggy_password"
            reconnect = true
            qos = 0
            retain = false
        "#;

        let config: MqttSinkConfig = toml::from_str(toml).expect("Failed to parse config");

        assert_eq!(config.url, "172.18.0.2:1883");
        assert_eq!(config.client_id, "iggy_mqtt_sink");
        assert_eq!(config.keep_alive, 60);
        assert_eq!(config.topic, "iggy/events");
        assert_eq!(config.username.as_deref(), Some("iggy"));
        assert_eq!(
            config.password.as_ref().map(|p| p.expose_secret()),
            Some("iggy_password")
        );
        assert!(config.reconnect);
        assert_eq!(config.qos, Some(0));
        assert_eq!(config.retain, Some(false));
    }

    #[test]
    fn given_minimal_toml_should_leave_optional_fields_none() {
        let toml = r#"
            url = "127.0.0.1:1883"
            client_id = "iggy_mqtt_sink"
            keep_alive = 30
            topic = "iggy/events"
            reconnect = false
        "#;

        let config: MqttSinkConfig = toml::from_str(toml).expect("Failed to parse config");

        assert!(config.username.is_none());
        assert!(config.password.is_none());
        assert!(config.qos.is_none());
        assert!(config.retain.is_none());
    }

    #[test]
    fn given_new_sink_should_initialize_without_client() {
        let sink = MqttSink::new(7, make_config());

        assert_eq!(sink.id, 7);
        assert!(sink.client.is_none());
        assert_eq!(sink.messages_sent.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn given_no_qos_should_default_to_2() {
        let mut config = make_config();
        config.qos = None;
        assert_eq!(config.qos.unwrap_or(2), 2);
    }

    #[test]
    fn given_no_retain_should_default_to_false() {
        let mut config = make_config();
        config.retain = None;
        assert!(!config.retain.unwrap_or(false));
    }

    #[test]
    fn given_configured_qos_and_retain_should_use_them() {
        let config = make_config();
        assert_eq!(config.qos.unwrap_or(2), 1);
        assert!(config.retain.unwrap_or(false));
    }

    #[test]
    fn given_raw_payload_should_serialize_to_same_bytes() {
        let payload = Payload::Raw(b"hello world".to_vec());
        assert_eq!(payload.try_to_bytes().unwrap(), b"hello world");
    }

    #[test]
    fn given_text_payload_should_serialize_to_utf8_bytes() {
        let payload = Payload::Text("hello mqtt".to_string());
        assert_eq!(payload.try_to_bytes().unwrap(), "hello mqtt".as_bytes());
    }

    #[test]
    fn given_json_payload_should_serialize_to_json_bytes() {
        let payload = Payload::Json(simd_json_from_str(r#"{"key":1}"#));

        let bytes = payload.try_to_bytes().expect("Failed to serialize JSON");
        let roundtrip: serde_json::Value =
            serde_json::from_slice(&bytes).expect("Bytes are not valid JSON");
        assert_eq!(roundtrip, serde_json::json!({"key": 1}));
    }
}

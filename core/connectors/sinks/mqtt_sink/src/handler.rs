// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use flowsdk::mqtt_client::client::{
    ConnectionResult, PingResult, PublishResult, SubscribeResult, UnsubscribeResult,
};
use flowsdk::mqtt_client::{
    MqttClientError,
    TokioMqttEventHandler,
};
use flowsdk::mqtt_serde::mqttv5::publishv5::MqttPublish;
use std::sync::{Arc, Mutex};

pub struct IggyMqtt5Handler {
    name: String,
    context: Arc<Mutex<Option<u16>>>,
}

impl IggyMqtt5Handler {
    pub fn new(name: &str, context: Arc<Mutex<Option<u16>>>) -> Self {
        IggyMqtt5Handler {
            name: name.to_string(),
            context,
        }
    }

    pub fn update_last_acked_packet_id(&mut self, packet_id: u16) {
        if let Ok(mut ctx) = self.context.lock() {
            println!(
                "[{}] Updating last acknowledged packet ID to {}",
                self.name, packet_id
            );
            *ctx = Some(packet_id);
        }
    }
}

#[async_trait::async_trait]
impl TokioMqttEventHandler for IggyMqtt5Handler {
    async fn on_connected(&mut self, result: &ConnectionResult) {
        if result.is_success() {
            println!(
                "[{}] Connected successfully! Session present: {}",
                self.name, result.session_present
            );
            if let Some(properties) = &result.properties {
                println!("[{}] Broker properties: {:?}", self.name, properties);
            }
        } else {
            println!(
                "[{}] Connection failed: {} (code: {})",
                self.name,
                result.reason_description(),
                result.reason_code
            );
        }
    }

    async fn on_disconnected(&mut self, reason: Option<u8>) {
        match reason {
            Some(code) => println!("[{}] Disconnected (reason code: {})", self.name, code),
            None => println!("[{}] Disconnected (connection lost)", self.name),
        }
    }

    async fn on_published(&mut self, result: &PublishResult) {
        if let Some(packet_id) = result.packet_id {
            self.update_last_acked_packet_id(packet_id);
        }
        if result.is_success() {
            println!(
                "[{}] Message published successfully (QoS: {}, ID: {:?})",
                self.name, result.qos, result.packet_id
            );
        } else {
            println!(
                "[{}] Publish failed: {} (code: {:?})",
                self.name,
                result.reason_description(),
                result.reason_code
            );
        }
    }

    async fn on_subscribed(&mut self, result: &SubscribeResult) {
        self.update_last_acked_packet_id(result.packet_id);
        if result.is_success() {
            println!(
                "[{}] Subscribed successfully! ({} subscriptions)",
                self.name,
                result.successful_subscriptions()
            );
        } else {
            println!(
                "[{}] Subscription failed: {:?}",
                self.name, result.reason_codes
            );
        }
    }

    async fn on_unsubscribed(&mut self, result: &UnsubscribeResult) {
        self.update_last_acked_packet_id(result.packet_id);
        println!(
            "[{}] Unsubscribe result for packet ID {:?}",
            self.name, result.packet_id
        );
        if result.is_success() {
            println!("[{}] Unsubscribed successfully!", self.name);
        } else {
            println!(
                "[{}] Unsubscribe failed: {:?}",
                self.name, result.reason_codes
            );
        }
    }

    async fn on_message_received(&mut self, publish: &MqttPublish) {
        let payload_str = String::from_utf8_lossy(&publish.payload);
        println!(
            "[{}] Message received on '{}': {}",
            self.name, publish.topic_name, payload_str
        );
        println!(
            "QoS: {}, Retain: {}, Packet ID: {:?}",
            publish.qos, publish.retain, publish.packet_id
        );
    }

    async fn on_ping_response(&mut self, result: &PingResult) {
        if result.success {
            println!("[{}] Ping response received", self.name);
        } else {
            println!("[{}] Ping failed", self.name);
        }
    }

    async fn on_error(&mut self, error: &MqttClientError) {
        println!("[{}] Error: {}", self.name, error.user_message());
    }

    async fn on_connection_lost(&mut self) {
        println!(
            "[{}] Connection lost! Attempting to reconnect...",
            self.name
        );
    }

    async fn on_reconnect_attempt(&mut self, attempt: u32) {
        println!("[{}] Reconnection attempt #{}", self.name, attempt);
    }

    async fn on_pending_operations_cleared(&mut self) {
        println!("[{}] Pending operations cleared", self.name);
    }
}

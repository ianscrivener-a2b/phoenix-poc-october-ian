import dotenv from 'dotenv';
import mqtt from 'mqtt';
import fs from 'fs';
import { randomUUID } from 'crypto';

dotenv.config();

const mqttConfig = {
  broker: process.env.mqtt_broker,
  username: process.env.mqtt_username,
  password: process.env.mqtt_password,
  clientId: randomUUID(),
  ca: fs.readFileSync(process.env.mqtt_ca_filepath),
  keepAliveInterval: parseInt(process.env.mqtt_keep_alive_sec) * 1000,
  keepAliveTopic: process.env.mqtt_keep_alive_topic
};

let mqttClient;

// Initialize MQTT client
mqttClient = mqtt.connect(mqttConfig.broker, {
  username: mqttConfig.username,
  password: mqttConfig.password,
  clientId: mqttConfig.clientId,
  ca: mqttConfig.ca,
  rejectUnauthorized: false
});

// MQTT client event handlers
mqttClient.on('connect', () => {
  console.log('Connected to MQTT broker');
  sendKeepAlive();
  setInterval(sendKeepAlive, mqttConfig.keepAliveInterval);
});

function sendKeepAlive() {
  mqttClient.publish(mqttConfig.keepAliveTopic, 'null', { qos: 1 });
  console.log(`Keep-alive message sent to topic: ${mqttConfig.keepAliveTopic}`);
}

mqttClient.on('error', (error) => {
  console.error('MQTT error:', error);
});

mqttClient.on('close', () => {
  console.log('MQTT connection closed');
});

// Graceful shutdown
process.on('SIGINT', () => {
  console.log('Closing MQTT connection...');
  mqttClient.end();
  process.exit();
});

console.log('MQTT Keep-Alive process started');
console.log(`Keep-alive interval: ${mqttConfig.keepAliveInterval / 1000} seconds`);
console.log('Press Ctrl+C to exit');
// File: mqtt-client.mjs

import dotenv from 'dotenv';
import mqtt from 'mqtt';
import fs from 'fs';
import { randomUUID } from 'crypto';


// Load environment variables
dotenv.config();

// Configuration from .env
const mqttConfig = {
  broker: process.env.mqtt_broker,
  username: process.env.mqtt_username,
  password: process.env.mqtt_password,
  clientId: randomUUID(),
  ca: fs.readFileSync(process.env.mqtt_ca_filepath),
  keepAliveInterval: parseInt(process.env.mqtt_keep_alive_sec) * 1000, // Convert to milliseconds
  topic: process.env.mqtt_topic,
  qos: parseInt(process.env.mqtt_qos),
  keepAliveTopic: process.env.mqtt_keep_alive_topic
};

let mqttClient;

// Create MQTT client
mqttClient = mqtt.connect(mqttConfig.broker, {
  username: mqttConfig.username,
  password: mqttConfig.password,
  clientId: mqttConfig.clientId,
  ca: mqttConfig.ca,
  rejectUnauthorized: false // Add this line to accept self-signed certificates if necessary
});

const keepAliveFn = function() {
  mqttClient.publish(mqttConfig.keepAliveTopic, 'null', { qos: mqttConfig.qos }, (err) => {
    if (!err) {
      console.log(`Keep-alive message sent to ${mqttConfig.keepAliveTopic}`);
    } else {
      console.error('Error sending keep-alive message:', err);
    }
  });
};

// Handle MQTT connection
mqttClient.on('connect', () => {
  console.log('Connected to MQTT broker');
  
  keepAliveFn()

  mqttClient.subscribe(mqttConfig.topic, { qos: mqttConfig.qos }, (err) => {
    if (!err) {
      console.log(`Subscribed to topic: ${mqttConfig.topic}`);
    } else {
      console.error('Subscription error:', err);
    }
  });

  // Set up keep-alive message
  setInterval(keepAliveFn, mqttConfig.keepAliveInterval);
});

// Handle incoming MQTT messages
mqttClient.on('message', (topic, message) => {
  console.log(`Received message on topic ${topic}: ${message.toString()}`);
});

// Handle MQTT errors
mqttClient.on('error', (error) => {
  console.error('MQTT error:', error);
});

// Add more event listeners for debugging
mqttClient.on('reconnect', () => {
  console.log('Attempting to reconnect to MQTT broker');
});

mqttClient.on('close', () => {
  console.log('Connection to MQTT broker closed');
});

mqttClient.on('offline', () => {
  console.log('MQTT client is offline');
});

// Clean up on exit
process.on('SIGINT', () => {
  console.log('Closing connection...');
  mqttClient.end();
  process.exit();
});

// Log the MQTT configuration (excluding sensitive data)
console.log('MQTT Configuration:', {
  broker: mqttConfig.broker,
  clientId: mqttConfig.clientId,
  topic: mqttConfig.topic,
  qos: mqttConfig.qos,
  keepAliveTopic: mqttConfig.keepAliveTopic,
  keepAliveInterval: mqttConfig.keepAliveInterval
});
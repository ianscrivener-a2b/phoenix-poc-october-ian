// File: mqtt-client.mjs

import dotenv from 'dotenv';
import mqtt from 'mqtt';
import fs from 'fs';

// Load environment variables
dotenv.config();

// Configuration from .env
const mqttConfig = {
  broker: process.env.mqtt_broker,
  username: process.env.mqtt_username,
  password: process.env.mqtt_password,
  clientId: process.env.mqtt_clientId,
  ca: fs.readFileSync(process.env.mqtt_ca_filepath),
  keepAliveInterval: parseInt(process.env.mqtt_keep_alive_sec) * 1000, // Convert to milliseconds
  topic: process.env.mqtt_topic,
  qos: parseInt(process.env.mqtt_qos),
  keepAliveTopic: process.env.mqtt_keep_alive_topic
};

let mqttClient;

// Object to store cumulative message counts for each topic
const topicCounts = {};

// Variable to store the start time of the application
const startTime = new Date();

// Create MQTT client
mqttClient = mqtt.connect(mqttConfig.broker, {
  username: mqttConfig.username,
  password: mqttConfig.password,
  clientId: mqttConfig.clientId,
  ca: mqttConfig.ca,
  rejectUnauthorized: false
});

const keepAliveFn = function() {
  mqttClient.publish(mqttConfig.keepAliveTopic, 'null', { qos: mqttConfig.qos }, (err) => {
    if (err) {
      console.error('Error sending keep-alive message:', err);
    }
  });
};

// Function to update the cumulative message count for a topic
const updateTopicCount = (topic) => {
  topic = topic.replaceAll("/", "_");  
  if (!topicCounts[topic]) {
    topicCounts[topic] = 0;
  }
  topicCounts[topic]++;
};

// Function to output summary of all topic message counts
const outputSummary = () => {
  const currentTime = new Date();
  const runningTime = (currentTime - startTime) / 1000; // in seconds

  console.log('\n--- Cumulative Topic Message Count Summary ---');
  console.log(`Running time: ${runningTime.toFixed(2)} seconds`);
  for (const [topic, count] of Object.entries(topicCounts)) {
    console.log(`${count} --- ${topic}`);
  }
  console.log('Topic Count', Object.keys(topicCounts).length);
  console.log('-----------------------------------------------\n');
};

// Handle MQTT connection
mqttClient.on('connect', () => {
  console.log('Connected to MQTT broker');
  keepAliveFn()
  mqttClient.subscribe(mqttConfig.topic, { qos: mqttConfig.qos }, (err) => {
    if (err) {
      console.error('Subscription error:', err);
    } else {
      console.log(`Subscribed to topic: ${mqttConfig.topic}`);
    }
  });

  // Set up keep-alive message
  setInterval(keepAliveFn, mqttConfig.keepAliveInterval);

  // Set up periodic summary output
  setInterval(outputSummary, 15000); // 15000 ms = 15 seconds
});

// Handle incoming MQTT messages
mqttClient.on('message', (topic, message) => {
  updateTopicCount(topic);
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
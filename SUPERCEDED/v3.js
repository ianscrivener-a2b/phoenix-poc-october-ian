// File: mqtt-to-eventhub.mjs

import dotenv from 'dotenv';
import mqtt from 'mqtt';
import fs from 'fs';
import { EventHubProducerClient } from '@azure/event-hubs';

dotenv.config();

const mqttConfig = {
  broker: process.env.mqtt_broker,
  username: process.env.mqtt_username,
  password: process.env.mqtt_password,
  clientId: process.env.mqtt_clientId,
  ca: fs.readFileSync(process.env.mqtt_ca_filepath),
  keepAliveInterval: parseInt(process.env.mqtt_keep_alive_sec) * 1000,
  topic: process.env.mqtt_topic,
  qos: parseInt(process.env.mqtt_qos),
  keepAliveTopic: process.env.mqtt_keep_alive_topic
};

const eventHubConfig = {
  connectionString: process.env.eventhub_conn_str,
  eventHubName: process.env.eventhub_name
};

let mqttClient;
let eventHubClient;

// Counters for I/O summary
let messageReceived = 0;
let messageSent = 0;
let lastSummaryTime = Date.now();

mqttClient = mqtt.connect(mqttConfig.broker, {
  username: mqttConfig.username,
  password: mqttConfig.password,
  clientId: mqttConfig.clientId,
  ca: mqttConfig.ca,
  rejectUnauthorized: false
});

eventHubClient = new EventHubProducerClient(eventHubConfig.connectionString, eventHubConfig.eventHubName);

mqttClient.on('connect', () => {
  console.log('Connected to MQTT broker');
  mqttClient.subscribe(mqttConfig.topic, { qos: mqttConfig.qos }, (err) => {
    if (!err) {
      console.log(`Subscribed to topic: ${mqttConfig.topic}`);
      sendKeepAlive();
    } else {
      console.error('Subscription error:', err);
    }
  });

  setInterval(sendKeepAlive, mqttConfig.keepAliveInterval);
  setInterval(printIOSummary, 15000); // Print I/O summary every 15 seconds
});

function sendKeepAlive() {
  mqttClient.publish(mqttConfig.keepAliveTopic, 'null', { qos: mqttConfig.qos });
}

mqttClient.on('message', async (topic, message) => {
  messageReceived++;
  
  try {
    const eventDataBatch = await eventHubClient.createBatch();
    topic = topic.split("/").slice(2).join("/")
    const eventData = {
      body: {
        topic: topic,
        message: message.toString()
      }
    };
    // console.log(eventData)
    
    if (!eventDataBatch.tryAdd(eventData)) {
      throw new Error("Event data couldn't be added to the batch");
    }
    
    await eventHubClient.sendBatch(eventDataBatch);
    messageSent++;
  } catch (error) {
    console.error('Error sending message to Azure Event Hub:', error);
  }
});

function printIOSummary() {
  const now = Date.now();
  const elapsedSeconds = (now - lastSummaryTime) / 1000;
  const receivedPerSecond = messageReceived / elapsedSeconds;
  const sentPerSecond = messageSent / elapsedSeconds;

  console.log(`I/O Summary (last ${elapsedSeconds.toFixed(2)} seconds):`);
  console.log(`  Messages Received: ${messageReceived} (${receivedPerSecond.toFixed(2)}/s)`);
  console.log(`  Messages Sent to Event Hub: ${messageSent} (${sentPerSecond.toFixed(2)}/s)`);

  // Reset counters
  messageReceived = 0;
  messageSent = 0;
  lastSummaryTime = now;
}

mqttClient.on('error', (error) => {
  console.error('MQTT error:', error);
});

process.on('SIGINT', async () => {
  console.log('Closing connections...');
  mqttClient.end();
  try {
    await eventHubClient.close();
    console.log('Event Hub connection closed');
  } catch (err) {
    console.error('Error closing Event Hub connection:', err);
  }
  process.exit();
});

console.log('MQTT to Azure Event Hub bridge started');
console.log(`Subscribed to MQTT topic: ${mqttConfig.topic}`);
console.log(`Sending data to Event Hub: ${eventHubConfig.eventHubName}`);
console.log('Press Ctrl+C to exit');
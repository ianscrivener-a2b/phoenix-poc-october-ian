// File: mqtt-to-eventhub.mjs

import dotenv from 'dotenv';
import mqtt from 'mqtt';
import fs from 'fs';
import path from 'path';
import { randomUUID } from 'crypto';
import { EventHubProducerClient } from '@azure/event-hubs';
import { createClient } from '@clickhouse/client'
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc.js';
import timezone from 'dayjs/plugin/timezone.js';

dotenv.config();
dayjs.extend(utc);
dayjs.extend(timezone);

// #####################
const mqttConfig = {
  broker: process.env.mqtt_broker,
  username: process.env.mqtt_username,
  password: process.env.mqtt_password,
  clientId: randomUUID(),
  ca: fs.readFileSync(process.env.mqtt_ca_filepath),
  keepAliveInterval: parseInt(process.env.mqtt_keep_alive_sec) * 1000,
  topic: process.env.mqtt_topic,
  qos: parseInt(process.env.mqtt_qos),
  keepAliveTopic: process.env.mqtt_keep_alive_topic
};

// #####################
const eventHubConfig = {
  connectionString: process.env.eventhub_conn_str,
  eventHubName: process.env.eventhub_name
};

// #####################
const logConfig = {
  enabled: process.env.log_raw_enabled === 'true',
  filepath: process.env.log_raw_filepath,
  maxRows: parseInt(process.env.log_raw_max_rows)
};

// #####################
const clickhouseConfig = {
  url: process.env.clickhouse_host,
  // username: process.env.clickhouse_user,
  // password: process.env.clickhouse_password,
  compression: { response: false, request: false},
  database: process.env.clickhouse_database, 
};

// #####################
let mqttClient;
let eventHubClient;
let clickhouseClient;
let logStream;

let logRowCount             = 0;
let currentLogFilepath;             // current log file path

let batchBuffer             = [];                                                 // in memory buffer for messages to be saved to Clickhouse DB
let batchBufferMaxSize      = process.env.clickhouse_batch_size || 10000;         // max size of the buffer
let clickhouse_table_name   = process.env.clickhouse_table_name || 'mqtt';        // table name in clickhouse
// let clickhouse_database     = process.env.clickhouse_database || 'default';       // database name in clickhouse

let lastSummaryTime         = Date.now();
let messageReceived         = 0;                                                  // Counters for I/O summary
let messageSent             = 0;                                                  // Counters for I/O summary


// #####################
// Initialize MQTT client
mqttClient = mqtt.connect(mqttConfig.broker, {
  username: mqttConfig.username,
  password: mqttConfig.password,
  clientId: mqttConfig.clientId,
  ca: mqttConfig.ca,
  rejectUnauthorized: false
});

// #####################
// Initialize ClickHouse client
try {
  clickhouseClient = createClient(clickhouseConfig);
  console.log('Connected to ClickHouse');
}
catch (error) {
  console.error('Error connecting to ClickHouse:', error);
}

// #####################
// Initialize eventHub client
// eventHubClient = new EventHubProducerClient(eventHubConfig.connectionString, eventHubConfig.eventHubName);


// #####################
// Initialize log stream if logging is enabled
if (logConfig.enabled) {
  initLogStream();
}

// #####################
// Get Sydney date time
function getSydneyDateTime() {
  return dayjs(new Date()).tz('Australia/Sydney').format('YYYY-MM-DD HH:mm:ss');
}

// #####################
// create raw data logging stream
function initLogStream() {
  const timestamp = new Date().toISOString().replace(/:/g, '-');
  const logFilename = `raw_data_${timestamp}.jsonl`;
  currentLogFilepath = path.join(logConfig.filepath, logFilename);
  logStream = fs.createWriteStream(currentLogFilepath, { flags: 'a' });
  logRowCount = 0;
  console.log(`Logging raw data to: ${currentLogFilepath}`);
}

// #####################
// Log raw data to file
function logRawData(data) {
  if (!logConfig.enabled) return;

  logStream.write(JSON.stringify(data) + '\n');
  logRowCount++;

  // roll logs if max rows reached
  if (logRowCount >= logConfig.maxRows) {
    logStream.end();
    initLogStream();
    // do clickHouseInsert
  }
}

// #####################
// Save data to Clickhouse
async function clickHouseInsert() {
  try { clickhouseClient.insert({
      table: clickhouse_table_name,
      columns: ['d', 't', 'm'],
      values: batchBuffer,
      format: 'JSONEachRow'
    });
    console.log(`Inserted ${batchBuffer.length} rows into ClickHouse`);
    batchBuffer = [];
  }
  catch (error) {
    console.error('Error inserting data into ClickHouse:', error);
  }
}

// ##################
// MQTT client event handlers
mqttClient.on('connect', () => {
  console.log('Connected to MQTT broker');
  mqttClient.subscribe(mqttConfig.topic, { qos: mqttConfig.qos }, (err) => {
    if (!err) {
      console.log(`Subscribed to topic: ${mqttConfig.topic}`);
      sendKeepAlive();
    }
    else {
      console.error('Subscription error:', err);
    }
  });

  setInterval(sendKeepAlive, mqttConfig.keepAliveInterval);
  setInterval(printIOSummary, 15000); // Print I/O summary every 15 seconds
});

function sendKeepAlive() {
  mqttClient.publish(mqttConfig.keepAliveTopic, 'null', { qos: mqttConfig.qos });
}

mqttClient.on('error', (error) => {
  console.error('MQTT error:', error);
});

mqttClient.on('close', () => {
  console.log('MQTT connection closed');
});

mqttClient.on('message', async (topic, message) => {
  messageReceived++;
  
  try {
    // &&&&&&&&&&&&&&&&&&&&&&&&&
    // (1) prep data
    let parsedMessage;

    topic = topic.split("/").slice(2).join("/")
    try {
      parsedMessage = JSON.parse(message.toString());
    }
    catch (error) {
      console.error('Error parsing message JSON:', error);
      parsedMessage = message.toString();  // Use the original message if parsing fails
    }
    const eventData = {
      d: getSydneyDateTime(),
      t: topic,
      m: parsedMessage.value
    };
    
    // &&&&&&&&&&&&&&&&&&&&&&&&&
    // (2) Log raw data
    logRawData(eventData);
    
    // &&&&&&&&&&&&&&&&&&&&&&&&&
    // (3) send to eventhub
    // const eventDataBatch = await eventHubClient.createBatch();
    // if (!eventDataBatch.tryAdd({ body: eventData })) {
    //   throw new Error("Event data couldn't be added to the batch");
    // }
    // await eventHubClient.sendBatch(eventDataBatch);

    // // &&&&&&&&&&&&&&&&&&&&&&&&&
    // (4) save to Clickhouse 
    batchBuffer.push(eventData);
    if (batchBuffer.length >= batchBufferMaxSize) {
      await clickHouseInsert(batchBuffer);
    }
    
    messageSent++;
  }
  catch (error) {
    console.error('Error sending message to Azure Event Hub:', error);
  }
});

// ##################
// Print I/O summary
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

// ##################
// Graceful shutdown
process.on('SIGINT', async () => {
  console.log('Closing connections...');
  await clickHouseInsert(batchBuffer);
  mqttClient.end();
  try {
    // await eventHubClient.close();
    console.log('Event Hub connection closed');
    await clickhouseClient.close();
    console.log('ClickHouse connection closed');
  }
  catch (err) {
    console.error('Error closing connections:', err);
  }
  if (logStream) {
    logStream.end();
  }
  process.exit();
});

// ##################
// Print startup information
console.log('MQTT to Azure Event Hub bridge started');
console.log(`Subscribed to MQTT topic: ${mqttConfig.topic}`);
console.log(`Sending data to Event Hub: ${eventHubConfig.eventHubName}`);
if (logConfig.enabled) {
  console.log(`Raw data logging enabled. Max rows per file: ${logConfig.maxRows}`);
}
console.log('Press Ctrl+C to exit');
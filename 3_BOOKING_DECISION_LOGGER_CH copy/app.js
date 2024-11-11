import { EventHubConsumerClient } from "@azure/event-hubs";
import { Buffer } from "buffer"
import { createClient } from '@clickhouse/client'
import { config } from 'dotenv';
import { fileURLToPath } from 'url';
import { dirname, resolve } from 'path';


// #####################
// Load the .env file
config({ path: resolve(dirname(fileURLToPath(import.meta.url)), '.env') });
// console.log(process.env);

const clickhouse_column_array = ["tenantId","fleetId","jobTime","jobType","customerId","bookingRef","destination","origin","correlationId"];


let clickhouseClient;


let batchBuffer = [];
let batchBufferMaxSize      = process.env.clickhouse_batch_size || 3;         // max size of the buffer

const verbose               = process.env.verbose === 'true';
let clickhouse_table_name   = process.env.clickhouse_table_name || 'mqtt';        // table name in clickhouse
let clickhouse_enabled      = process.env.clickhouse_enabled === 'true'; 

// #####################
const clickhouseConfig = {
    url:            process.env.clickhouse_host,
    database:       process.env.clickhouse_database, 
    compression:    { response: false, request: false},
};

// #####################
const eventHubConfig = {
    connectionString:   process.env.eventhub_conn_str,
    eventHubName:   process.env.eventHubName
};

  

// #####################
// instantiate the ClickHouse client
const eventHubClient = new EventHubConsumerClient(
    EventHubConsumerClient.defaultConsumerGroupName, 
    eventHubConfig.connectionString,
    eventHubConfig.eventHubName
);


// #####################
// Initialize ClickHouse client
if(clickhouse_enabled){
    try {
      clickhouseClient = createClient(clickhouseConfig);
      console.log('Connected to ClickHouse... ');
    }
    catch (error) {
      console.error('Error connecting to ClickHouse:', error);
    }
  }

// #####################
// Save data to Clickhouse
async function clickHouseInsert() {
    console.log(`ClickHouse Fn`);
    if(clickhouse_enabled){
      try { 
        clickhouseClient.insert({
          table: clickhouse_table_name,
          columns: clickhouse_column_array,
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
  }

// #####################
// function to process events
const processEvents = async (events, context) => {
    for (const event of events) {

        let x = Buffer.from(event.body.data_base64, 'base64').toString('utf-8');
        x = JSON.parse(x);
        x.event_hub = {};
        x.event_hub.timestamp = event.body.time;     
        x.event_hub.source = event.body.source;
        x.event_hub.id = event.body.id;
        x.event_hub.subject = event.body.subject;
        x.event_hub.datacontenttype = event.body.datacontenttype;
        x.event_hub.type = event.body.type;

        if(verbose){         
            console.log(x);
        }

        // &&&&&&&&&&&&&&&&&&&&&&&&&
        // save to Clickhouse 
        if(clickhouse_enabled){
            batchBuffer.push(x);
            console.log(`Buffer size: ${batchBuffer.length}`);
            if (batchBuffer.length >= batchBufferMaxSize) {
                await clickHouseInsert(batchBuffer);
            }
        }
    }
};

// #####################
// function to process errors
const processError = async (err, context) => {
    console.error(`Error: ${err.message}`);
};


// #####################
// subscribe to the event hub
const subscription = eventHubClient.subscribe({
    processEvents,
    processError
});



// ##################
// Graceful shutdown
process.on('SIGINT', async () => {
    console.log('Closing connections...');
    
    await eventHubClient.close();

    if(clickhouse_enabled){
      try { 
        await clickHouseInsert(batchBuffer);
        await clickhouseClient.close();
      }
      catch (error) {
        console.error('Error closing ClickHouse:', error);
      }
    }

    process.exit();
});


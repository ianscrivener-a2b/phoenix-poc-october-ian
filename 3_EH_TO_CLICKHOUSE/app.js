// Step 1: Install the package
// npm install @azure/event-hubs

// Step 2: Import the required modules
import { EventHubConsumerClient } from "@azure/event-hubs";

// Step 3: Define connection details
const connectionString  = "Endpoint=sb://arb1-ehnamesp346.servicebus.windows.net/;SharedAccessKeyName=ehbookingrx_read;SharedAccessKey=L6+QJ2fDlidhNbEcXsk4i6z96xL9bOl06+AEhNlhR3E=;EntityPath=ehbookingrx";
const eventHubName      = "ehbookingrx";
const consumerGroup = EventHubConsumerClient.defaultConsumerGroupName;

// Step 4: Create a consumer client
const consumerClient = new EventHubConsumerClient(consumerGroup, connectionString, eventHubName);

// Step 5: Define the event processing logic
const processEvents = async (events, context) => {
    for (const event of events) {
        console.log(`Received event: ${JSON.stringify(event.body)}`);
    }
};

const processError = async (err, context) => {
    console.error(`Error: ${err.message}`);
};

// Step 6: Start the consumer client
const subscription = consumerClient.subscribe({
    processEvents,
    processError
});

// To stop the subscription, you can call:
// await subscription.close();
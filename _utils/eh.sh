az eventhubs eventhub receive \
    --eventhub-name <your-eventhub-name> \
    --namespace-name <your-namespace-name> \
    --resource-group <your-resource-group> \
    --consumer-group \$Default \
    --partition-id 0 \
    --starting-sequence-number -1

    
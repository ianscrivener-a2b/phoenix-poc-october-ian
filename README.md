# POC MQTT to Event Hub ... and more


### MQTT data source (broker)
An MQTT data feed of the electrical systems on Ian's boat - is emitting around 1000 messages per minute. The temetry is sent via an embedded Linux device (Victron Cerbo GX) to a Cloud based MQTT broker

### `.env` file for auth secrets
See `.env.sample`


### basic_mqtt_client.js
1. connect to MQTT broker with user, pass & certificate
2. issue a keepalive every 60 secs
3. subscribe to the designated topic and recaive all raw data
4. verbosely log all MQTT messages to console

### app.js
(1-3 per *basic_mqtt_client.js* above)
1. connect to MQTT broker with user, pass & certificate
2. issue a keepalive every 60 secs
3. subscribe to the designated topic and recaive all raw data
4. send data to Azure Event Hub
5. periodically (15 seconds) display MQTT & Event Hub message totals 

---
### Running the app

**Simple**
`node app.js`

**In a Docker Container - interactive**
```
docker run -it --rm \
  --name phx_poc_matt_eventhub \
  -v "$PWD":/usr/src/app \
  -w /usr/src/app \
  -e NPM_CONFIG_LOGLEVEL=info \
  node:current-alpine3.20 \
  node basic_mqtt_client.js
```

**In a Docker Container - daemonised**
```
docker run -d --rm \
  --name phx_poc_matt_eventhub \
  -v "$PWD":/usr/src/app \
  -w /usr/src/app \
  -e NPM_CONFIG_LOGLEVEL=info \
  node:current-alpine3.20 \
  node basic_mqtt_client.js
```
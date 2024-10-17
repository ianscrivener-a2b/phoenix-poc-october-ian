curl -X 'POST' \
  'https://jobapieg001devae-ekb8djhwfjdabveh.australiaeast-01.azurewebsites.net/booking' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -H 'api-shared-key: Kn57ida0SUQ6ooWXuf6bF8JIsQdrNrqrV0lmXSGZmWy03qFZrrpG0H4nZM00gaTo' \
  -d '{
  "tenantId": "happycabs",
  "fleetId": "fleet001",
  "jobTime": "string",
  "jobType": "string",
  "customerId": "string",
  "bookingRef": "string",
  "destination": "dddddstring",
  "origin": "ddddd",
  "correlationId": "string"
}'
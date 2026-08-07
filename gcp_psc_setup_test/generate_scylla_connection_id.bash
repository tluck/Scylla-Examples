#!/usr/bin/env bash

# Fetch node IDs from ScyllaDB REST API (adjust endpoint/port as needed)
host_ids=($(curl -s http://localhost:10000/storage_service/host_id/ | jq -r '.[].value'))

# Verify we got all node IDs
echo "Node IDs: ${host_ids[@]}"

# endpoint="10.138.0.150"  # PSC endpoint IP in app subnet
endpoint="endpoint.cluster-1.scylladb.com"  # PSC endpoint IP in app subnet
connection_id="1"  # Arbitrary connection ID for this set of routes
curl -X POST \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json' \
  -d "[\
    {\
      \"connection_id\": \"$connection_id\",\
      \"host_id\": \"${host_ids[0]}\",\
      \"address\": \"$endpoint\",\
      \"port\": 9001,\
      \"tls_port\": 9101\
    },\
    {\
      \"connection_id\": \"$connection_id\",\
      \"host_id\": \"${host_ids[1]}\",\
      \"address\": \"$endpoint\",\
      \"port\": 9002,\
      \"tls_port\": 9102\
    },\
    {\
      \"connection_id\": \"$connection_id\",\
      \"host_id\": \"${host_ids[2]}\",\
      \"address\": \"$endpoint\",\
      \"port\": 9003,\
      \"tls_port\": 9103\
    }\
  ]" \
  http://localhost:10000/v2/client-routes

curl -sX GET http://localhost:10000/v2/client-routes | jq -c '.[] | {connection_id: .connection_id, host_id: .host_id, address: .address}'

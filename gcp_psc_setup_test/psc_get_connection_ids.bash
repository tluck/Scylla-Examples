#!/bin/bash

CLUSTER_ID="${1}"
if [[ $1 == '' ]]; then
echo Usage: $0 cluster_id
exit 1
fi

cx cql -c $CLUSTER_ID --reason connection_id '-e select * from system.client_routes'
cx cql -c $CLUSTER_ID --reason connection_id '-e select connection_id, address, port from system.client_routes'


#!/bin/bash
set -e
script_name="$(basename "${BASH_SOURCE[0]}")"

log_file="scylla_loader_key_value_1_$(date "+%Y-%m-%d_%H%M%S").log"

timeout=60  # Maximum wait time in seconds
interval=2  # Interval between checks in seconds
elapsed=0

source gcp_init.conf
nodes=${NODE_LIST:-scylla-client}
dc=${DC:-dc1}
username=${USERNAME:-cassandra}
password=${PASSWORD:-cassandra}

cassandra-stress user profile=key_value.yaml cl=LOCAL_QUORUM n=1000000000 'ops(insert=1)' \
    -mode native cql3 user=${username}  password=${password} \
    -rate threads=500  -log interval=10 -node ${nodes[0]} | tee $log_file

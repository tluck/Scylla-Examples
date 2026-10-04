#!/bin/bash
set -e

log_file="stresstest_load-factor-1-key_value_1_$(date "+%Y-%m-%d_%H%M%S").log"

timeout=60  # Maximum wait time in seconds
interval=2  # Interval between checks in seconds
elapsed=0

# Connection settings come from the environment. Generate them for a ScyllaDB
# Cloud cluster with ../sc_api/gen_cluster_env.bash, then either source the
# file first or point ENV_FILE at it:
#   (cd ../sc_api && ./gen_cluster_env.bash -c <cluster_id>)
#   ENV_FILE=../sc_api/cluster-<cluster_id>.env ./run-stress.sh
[[ -n ${ENV_FILE:-} ]] && source "$ENV_FILE"
nodes=${CONTACT_POINTS:-scylla-client}
dc=${DC:-dc1}
username=${USERNAME:-cassandra}
password=${PASSWORD:-cassandra}

cassandra-stress user profile=key_value.yaml cl=LOCAL_QUORUM duration=60m 'ops(insert=1,select=1)' \
  -mode native cql3 user=${username} password=${password} \
  -rate threads=750 fixed=100000/s -log interval=10 -node ${nodes[0]} | tee "$log_file" 

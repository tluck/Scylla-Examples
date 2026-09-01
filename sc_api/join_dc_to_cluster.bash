#!/usr/bin/env bash

# Join a DC that has been added (BOOTSTRAP_COMPLETE) to its cluster.
# The dc_id is reported by add_dc_to_cluster.bash.

if [[ "$1" == '' ]] || [[ "$2" == '' ]];then
    echo "Usage: $0 cluster_id dc_id"
    exit 1
fi

CLUSTER_ID=$1
DC_ID=$2

if [[ ! $CLUSTER_ID =~ ^[0-9]+$ ]] || [[ ! $DC_ID =~ ^[0-9]+$ ]];then
    echo "error: cluster_id and dc_id must be numeric" >&2
    echo "Usage: $0 cluster_id dc_id"
    exit 1
fi

cx sc cluster dc join \
  --cluster-id "$CLUSTER_ID" \
  --cluster-dc-id "$DC_ID"

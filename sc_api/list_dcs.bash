#!/usr/bin/env bash

# List the data centers of a ScyllaDB Cloud cluster.
#
# The DC "name" is the Scylla DC name used in NetworkTopologyStrategy,
# e.g. GCE_US_WEST_1. Only joined DCs show up here -- a DC that has been
# added but not yet joined is not in this list.

API_BASE_URL="https://api.cloud.scylladb.com"
API_TOKEN=${SC_TOKEN}
accountId=${SC_ACCOUNT}

if [[ "$1" == '' ]] || [[ "$1" == -h ]] || [[ "$1" == --help ]];then
    echo "Usage: $0 cluster_id [-j]"
    echo "  -j   raw JSON instead of the table"
    exit 1
fi

clusterId=$1
shift

CLUSTER=$(curl -s -X GET "${API_BASE_URL}/account/${accountId}/cluster/${clusterId}" \
  -H "Authorization: Bearer ${API_TOKEN}") || exit 1

if [[ $(jq -r 'has("data")' <<<"$CLUSTER") != true ]];then
    echo "error: unexpected response for cluster ${clusterId}" >&2
    jq . <<<"$CLUSTER" >&2
    exit 1
fi

if [[ "$1" == -j ]];then
    jq '.data.cluster.dataCenters // []' <<<"$CLUSTER"
    exit 0
fi

# Header outside the column(1) pipe, so it does not widen the table columns.
jq -r '.data.cluster |
  "cluster: \(.id) \(.clusterName) (\(.status), RF \(.replicationFactor))\n"
  ' <<<"$CLUSTER"

# dataCenters is null until the cluster leaves QUEUED, hence "// []".
jq -r '.data.cluster |
  "ID\tNAME\tREGION\tSTATUS\tCIDR",
  ((.dataCenters // [])[] | "\(.id)\t\(.name)\t\(.regionId)\t\(.status)\t\(.cidrBlock)")
  ' <<<"$CLUSTER" | column -t -s $'\t'

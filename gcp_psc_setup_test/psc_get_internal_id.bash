#!/usr/bin/env bash

# Print the internal ID of the cluster's PSC connection(s), one per line.
# Optional connection_name selects one connection (e.g. psc-tls-54742 from psc_connection_setup.bash).
#
# usage: psc_get_internal_id.bash [cluster_id] [connection_name]

script_dir=$(cd "$(dirname "$0")" && pwd)
source "${script_dir}/psc_common.bash"

cluster_id=${1:-${default_cluster_id}}
connection_name=${2:-}

psc_connections ${cluster_id} "${connection_name}" | jq -r .id

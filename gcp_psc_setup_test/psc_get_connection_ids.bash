#!/usr/bin/env bash

# Show the client routes of the cluster's PSC connections: the connection summary from the REST API, then the
# per-node ports from system.client_routes (port = plain, tls_port = TLS). The client route ID is what cqlsh
# (--client-route) and the drivers need; it is not the internal connection ID.
#
# usage: psc_get_connection_ids.bash [cluster_id]

script_dir=$(cd "$(dirname "$0")" && pwd)
source "${script_dir}/psc_common.bash"

cluster_id=${1:-${default_cluster_id}}

psc_connections ${cluster_id} | jq -r '[.id, .name, .status, "tls=\(.data.tls)", .data.address,
    .data.clientRoutesConnectionId] | @tsv'

echo
${cx_timeout} cx cql -c ${cluster_id} --reason connection_id \
    '-e select connection_id, address, port, tls_port from system.client_routes' 2>/dev/null | tr -d '\r' \
  || echo "WARNING: cx cql failed or timed out; per-node ports not shown" >&2

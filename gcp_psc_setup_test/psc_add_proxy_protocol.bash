#!/usr/bin/env bash

# Give the cluster both proxy protocol v2 listeners: plain (29042) and TLS (29142, needed by a PSC connection
# created with tls:true). set-and-restart does a rolling restart, so skip it when both are already set.

#
# usage: psc_add_proxy_protocol.bash [cluster_id] [interval]   (interval 0s for a test cluster with no load)
#
# There is no REST call for this; it needs scylla-cloud (cx). For a new cluster, set both ports in the create
# request instead (../sc_api/scylladb_cloud_cli.py create --psc) and skip the restart.

cluster_id=${1:-54742}
interval=${2:-600s} # 10 minutes between node restarts

get_port() {
  timeout 120 scylla-cloud cluster config get --cluster-id ${cluster_id} $1 2>/dev/null \
    | tr -d '\r' | sed -n 's/^value: *"\{0,1\}\([0-9]*\)"\{0,1\}$/\1/p'
}

pp2_port=$(get_port native_transport_port_proxy_protocol)
pp2_ssl_port=$(get_port native_transport_port_ssl_proxy_protocol)
echo "cluster ${cluster_id}: native_transport_port_proxy_protocol=${pp2_port:-<unset>}" \
  "native_transport_port_ssl_proxy_protocol=${pp2_ssl_port:-<unset>}"

if [[ ${pp2_port} == 29042 && ${pp2_ssl_port} == 29142 ]]; then
  echo "both PPv2 listeners already set, nothing to do"
  exit 0
fi

scylla-cloud cluster config set-and-restart --cluster-id ${cluster_id} --interval ${interval} --yes \
    -p native_transport_port_ssl_proxy_protocol=29142 -p native_transport_port_proxy_protocol=29042

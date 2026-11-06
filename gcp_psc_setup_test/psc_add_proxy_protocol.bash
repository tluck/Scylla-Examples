#!/usr/bin/env bash

# Give the cluster the proxy protocol v2 listener(s) its PSC connections need: plain 29042 (tls_mode false),
# TLS 29142 (tls_mode true) or both. set-and-restart does a rolling restart, so it only runs when a listener is
# missing, and only after every setting was read successfully: a failed or timed-out read stops the script
# instead of being taken for "not set".
#
# usage: psc_add_proxy_protocol.bash [cluster_id] [interval] [tls_mode true|false|both]
#   interval 0s for a test cluster with no load
#
# A listener is there when either its plain key or its shard-aware key has the port: clusters created by
# ScyllaDB Cloud with PSC support come with native_shard_aware_transport_port[_ssl]_proxy_protocol 29042/29142,
# clusters given the listeners later (this script, older clusters) with native_transport_port[_ssl]_proxy_protocol.
# Setting the plain key on a cluster that has the shard-aware one would bind the same port twice.
#
# There is no REST call for this; it needs scylla-cloud (cx). For a new cluster, set both ports in the create
# request instead (../sc_api/scylladb_cloud_cli.py create --psc) and skip the restart.

script_dir=$(cd "$(dirname "$0")" && pwd)
source "${script_dir}/psc_common.bash"

cluster_id=${1:-${default_cluster_id}}
interval=${2:-${default_interval}}
tls_mode=${3:-${default_tls_mode}}
tls_list=$(tls_values ${tls_mode}) || exit 1

# listener: the port, then the keys that can provide it (the first is the one set when it is missing)
listeners=()
for tls in ${tls_list}; do
  if [[ ${tls} == true ]]; then
    listeners+=("$(ppv2_port true) native_transport_port_ssl_proxy_protocol native_shard_aware_transport_port_ssl_proxy_protocol")
  else
    listeners+=("$(ppv2_port false) native_transport_port_proxy_protocol native_shard_aware_transport_port_proxy_protocol")
  fi
done

# prints "<value> <status>" when set, "unset" when the cluster has no value; returns 1 when the read failed
get_setting() {
  local out rc
  # pipefail: rc is scylla-cloud's (or timeout's 124), not tr's
  out=$(set -o pipefail; ${cx_timeout} scylla-cloud cluster config get --cluster-id ${cluster_id} $1 </dev/null 2>&1 | tr -d '\r')
  rc=$?
  if [[ ${rc} == 0 ]]; then
    echo "$(sed -n 's/^value: *"\{0,1\}\([0-9]*\)"\{0,1\}$/\1/p' <<< "${out}") $(sed -n 's/^status: *//p' <<< "${out}")"
  elif grep -q -e 'value not set' -e 'record not found' <<< "${out}"; then
    echo unset
  else
    echo "ERROR: could not read $1 (exit ${rc}$([[ ${rc} == 124 ]] && echo ', timed out')):" >&2
    grep -v '^{"level' <<< "${out}" | tail -3 >&2
    return 1
  fi
}

missing=()
for listener in "${listeners[@]}"; do
  read -r port keys <<< "${listener}"
  found=""
  for key in ${keys}; do
    setting=$(get_setting ${key}) || exit 1
    read -r value status <<< "${setting}"
    echo "cluster ${cluster_id}: ${key}=${value}${status:+ (${status})}"
    if [[ ${value} == "${port}" && -z ${found} ]]; then
      found=${key}
      [[ ${status} == ACTIVE ]] || echo "  set, but not ACTIVE yet: a restart may still be in progress"
    fi
  done
  [[ -n ${found} ]] || missing+=(-p "${keys%% *}=${port}")
done

if [[ ${#missing[@]} == 0 ]]; then
  echo "PPv2 listener(s) for tls_mode ${tls_mode} already set, nothing to do"
  exit 0
fi

echo "setting ${missing[*]} with a rolling restart (interval ${interval})"
scylla-cloud cluster config set-and-restart --cluster-id ${cluster_id} --interval ${interval} --yes "${missing[@]}"

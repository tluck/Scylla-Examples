#!/usr/bin/env bash

# Give the cluster both proxy protocol v2 listeners: plain (29042) and TLS (29142, needed by a PSC connection
# created with tls:true). set-and-restart does a rolling restart, so it only runs when a listener is missing,
# and only after both settings were read successfully: a failed or timed-out read stops the script instead of
# being taken for "not set".
#
# usage: psc_add_proxy_protocol.bash [cluster_id] [interval]   (interval 0s for a test cluster with no load)
#
# There is no REST call for this; it needs scylla-cloud (cx). For a new cluster, set both ports in the create
# request instead (../sc_api/scylladb_cloud_cli.py create --psc) and skip the restart.

cluster_id=${1:-54742}
interval=${2:-600s} # 10 minutes between node restarts

wanted_port() {
  case $1 in
    native_transport_port_proxy_protocol) echo 29042 ;;
    native_transport_port_ssl_proxy_protocol) echo 29142 ;;
  esac
}

# prints "<value> <status>" when set, "unset" when the cluster has no value; returns 1 when the read failed
get_setting() {
  local out rc
  # pipefail: rc is scylla-cloud's (or timeout's 124), not tr's
  out=$(set -o pipefail; timeout 120 scylla-cloud cluster config get --cluster-id ${cluster_id} $1 2>&1 | tr -d '\r')
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
for key in native_transport_port_proxy_protocol native_transport_port_ssl_proxy_protocol; do
  setting=$(get_setting ${key}) || exit 1
  read -r value status <<< "${setting}"
  echo "cluster ${cluster_id}: ${key}=${value}${status:+ (${status})}"
  if [[ ${value} == "$(wanted_port ${key})" ]]; then
    [[ ${status} == ACTIVE ]] || echo "  set, but not ACTIVE yet: a restart may still be in progress"
  else
    missing+=(-p "${key}=$(wanted_port ${key})")
  fi
done

if [[ ${#missing[@]} == 0 ]]; then
  echo "both PPv2 listeners already set, nothing to do"
  exit 0
fi

echo "setting ${missing[*]} with a rolling restart (interval ${interval})"
scylla-cloud cluster config set-and-restart --cluster-id ${cluster_id} --interval ${interval} --yes "${missing[@]}"

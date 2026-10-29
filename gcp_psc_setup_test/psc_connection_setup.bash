#!/usr/bin/env bash

# Create a PSC connection on the cluster through the public REST API and wait until it is ACTIVE.
# The REST API is used instead of `scylla-cloud cluster connection create` because the CLI run locally (cx sc)
# cannot evaluate the enable_private_connectivity_tls Flipt flag ("authentication/authorization error").
#
# A connection is all-TLS or all-plain, and tls / proxyProtocolV2 cannot be changed after the create.
# A TLS connection needs client-to-node encryption on the cluster and the TLS PPv2 listener (29142, see
# psc_add_proxy_protocol.bash). Every connection needs its own /29 NAT subnet; the next free one in
# 10.0.201.0/24 is picked unless one is given.
#
# usage: psc_connection_setup.bash [cluster_id] [tls true|false] [consumer_project] [connection_name] [nat_subnet]

script_dir=$(cd "$(dirname "$0")" && pwd)
source "${script_dir}/psc_common.bash"

cluster_id=${1:-${default_cluster_id}}
tls=${2:-true}
consumer_project=${3:-"cx-sa-lab"}
if [[ ${tls} == true ]]; then
  connection_name=${4:-"psc-tls-${cluster_id}"}
else
  connection_name=${4:-"psc-${cluster_id}"}
fi
nat_subnet=${5:-}

existing=$(psc_connections ${cluster_id})
if jq -e --arg name "${connection_name}" 'select(.name==$name)' <<< "${existing}" >/dev/null; then
  echo "ERROR: cluster ${cluster_id} already has a connection named ${connection_name}" >&2
  exit 1
fi

if [[ -z ${nat_subnet} ]]; then
  used=$(jq -r '.data.natSubnetCidr // empty' <<< "${existing}")
  for offset in $(seq 0 8 248); do
    if ! grep -qx "10.0.201.${offset}/29" <<< "${used}"; then
      nat_subnet="10.0.201.${offset}/29"
      break
    fi
  done
fi

dc_id=$(api GET "/cluster/${cluster_id}" | jq -r '.data.cluster.dc.id // empty')
if [[ -z ${dc_id} ]]; then
  echo "ERROR: could not read the datacenter ID of cluster ${cluster_id}" >&2
  exit 1
fi

body=$(jq -n --arg name "${connection_name}" --argjson dc ${dc_id} --argjson tls ${tls} \
    --arg nat "${nat_subnet}" --arg project "${consumer_project}" '{
  name: $name,
  clusterDcid: $dc,
  type: "GCP_PRIVATE_SERVICE_CONNECT",
  cidrList: ["0.0.0.0/0"],
  data: {
    tls: $tls,
    proxyProtocolV2: true,
    connectionPreference: "ACCEPT_MANUAL",
    natSubnetCidr: $nat,
    consumerProjects: [{projectId: $project}]
  }
}')
echo "${body}"

response=$(api POST "/cluster/${cluster_id}/network/vpc/connection" -d "${body}")
connection_id=$(jq -r '.data.connectionId // empty' <<< "${response}")
if [[ -z ${connection_id} ]]; then
  echo "ERROR: create refused: ${response}" >&2
  exit 1
fi
echo "connection ${connection_id} (${connection_name}) requested, waiting for ACTIVE"

for i in $(seq 1 60); do
  status=$(api GET "/cluster/${cluster_id}/network/vpc/connection/${connection_id}" \
    | jq -r '"\(.data.status) \(.data.stage) \(.data.stageMessage)"')
  echo "$(date +%T) ${status}"
  case ${status} in
    ACTIVE*) break ;;
    *ERROR*|*FAIL*) exit 1 ;;
  esac
  sleep 30
done
if [[ ${status} != ACTIVE* ]]; then
  echo "ERROR: connection ${connection_id} is not ACTIVE after 30 minutes" >&2
  exit 1
fi

api GET "/cluster/${cluster_id}/network/vpc/connection/${connection_id}" | jq .data

# Shared by the psc_*.bash scripts (source it, do not run it).
#
# The public REST API is used wherever it has the data: cx / scylla-cloud run remotely, can hang, and cannot
# evaluate Flipt flags locally. cx is still needed for what REST does not expose (TLS name, PPv2 config,
# system.client_routes ports, the generated terraform); wrap those calls in cx_timeout.

api_base="https://api.cloud.scylladb.com"
default_cluster_id=54793
cx_timeout="timeout 120"

if [[ -z ${SC_TOKEN} || -z ${SC_ACCOUNT} ]]; then
  echo "ERROR: export SC_TOKEN and SC_ACCOUNT first" >&2
  exit 1
fi

api() {
  local method=$1 path=$2
  shift 2
  curl -s -X ${method} "${api_base}/account/${SC_ACCOUNT}${path}" \
    -H "Authorization: Bearer ${SC_TOKEN}" -H "Content-Type: application/json" "$@"
}

# PSC connections of a cluster, one compact JSON object per line (REST shape: id, name, status, data.tls,
# data.address, data.serviceAttachmentId, data.clientRoutesConnectionId, data.natSubnetCidr).
# Optional name selects one connection
psc_connections() {
  local cluster_id=$1 name=${2:-}
  api GET "/cluster/${cluster_id}/network/vpc/connection" \
    | jq -c --arg name "${name}" '.data.connections[]?
        | select(.type=="GCP_PRIVATE_SERVICE_CONNECT")
        | select($name=="" or .name==$name)'
}

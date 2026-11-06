# Shared by the psc_*.bash scripts (source it, do not run it).
#
# The public REST API is used wherever it has the data: cx / scylla-cloud run remotely, can hang, and cannot
# evaluate Flipt flags locally. cx is still needed for what REST does not expose (TLS name, PPv2 config,
# system.client_routes ports, the generated terraform); wrap those calls in cx_timeout.

api_base="https://api.cloud.scylladb.com"
default_cluster_id="55024"
default_consumer_project="cx-sa-lab"
default_region="us-west1"
default_tls_mode="false"
default_interval="600s"          # between rolling node restarts when the PPv2 listeners are added; 0s for an idle test cluster
default_consumer_network="default" # consumer VPC network; its subnet in the region has the same name
default_nat_range="10.0.201"      # PSC NAT subnets: the next free /29 in <range>.0/24

# Give every timed cx call </dev/null: cx always runs ssh -t, and under timeout (its own process group) ssh
# stops on SIGTTOU when it resets a terminal on stdin, so it hangs until the timeout kills it.
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

#!/usr/bin/env bash

# Write the sample-app env files for connecting straight to the nodes (SCYLLA_MODE=direct), e.g. over VPC
# peering: sample_apps/direct-<cluster_id>.env (plain) and, when the cluster has client-to-node encryption,
# sample_apps/direct-tls-<cluster_id>.env. The PSC counterparts come from psc_generate_client.bash.
# Contact points are the node DNS names of the cluster's DC; with private broadcast they resolve to the node
# private IPs, so they only work from a network that reaches those IPs (peering, VPN).
#
# usage: direct_env.bash [cluster_id]

script_dir=$(cd "$(dirname "$0")" && pwd)
source "${script_dir}/psc_common.bash"

cluster_id=${1:-${default_cluster_id}}

cluster=$(api GET "/cluster/${cluster_id}")
dc_id=$(jq -r '.data.cluster.dc.id // empty' <<< "${cluster}")
dc_name=$(jq -r '.data.cluster.dc.name // empty' <<< "${cluster}")
encryption=$(jq -r '.data.cluster.encryptionMode // empty' <<< "${cluster}")
if [[ -z ${dc_id} ]]; then
  echo "ERROR: could not read cluster ${cluster_id}" >&2
  exit 1
fi

contact_points=$(api GET "/cluster/${cluster_id}/nodes" \
  | jq -r --argjson dc ${dc_id} '[.data.nodes[]? | select(.dcId==$dc and .status=="ACTIVE") | .dns] | join(",")')
if [[ -z ${contact_points} ]]; then
  echo "ERROR: no ACTIVE nodes found in DC ${dc_name} of cluster ${cluster_id}" >&2
  exit 1
fi

write_env() {
  local env_file=$1 tls=$2
  {
    echo "# Direct connection (VPC peering) to cluster ${cluster_id}, DC ${dc_name}, written by direct_env.bash"
    echo "SCYLLA_MODE=direct"
    echo "SCYLLA_CONTACT_POINTS=${contact_points}"
    echo "SCYLLA_DC=${dc_name}"
    if [[ ${tls} == true ]]; then
      # relative to this file
      echo "SCYLLA_CA_FILE=../$(basename "${ca_file}")"
      echo "SCYLLA_TLS_NAME=${tls_name}"
    fi
  } > "${env_file}"
  echo "Wrote ${env_file}"
}

write_env "${script_dir}/sample_apps/direct-${cluster_id}.env" false

# TLS needs client-to-node encryption on the cluster (encryptionMode "both" or similar), the CA and the TLS name
if [[ -n ${encryption} && ${encryption} != none && ${encryption} != NONE ]]; then
  ca_file="${script_dir}/ca-${cluster_id}.pem"
  api GET "/cluster/${cluster_id}/certificate" | jq -r '.data.content // empty' > "${ca_file}"
  tls_name=$(${cx_timeout} scylla-cloud cluster describe --cluster-id ${cluster_id} 2>/dev/null \
    | tr -d '\r' | sed -n 's/^tlsname: *"\{0,1\}\([^"]*\)"\{0,1\}$/\1/p' | head -1)
  if [[ ! -s ${ca_file} ]]; then
    echo "WARNING: could not fetch the CA certificate; no TLS env file written" >&2
    rm -f "${ca_file}"
  else
    [[ -n ${tls_name} ]] || echo "WARNING: TLS name not found (cx unavailable?); TLS will check the CA only" >&2
    write_env "${script_dir}/sample_apps/direct-tls-${cluster_id}.env" true
  fi
fi

echo
echo "In sample_apps/, from a VM that reaches the node private IPs (export SCYLLA_PASSWORD first):"
for env in direct-${cluster_id}.env direct-tls-${cluster_id}.env; do
  [[ -f ${script_dir}/sample_apps/${env} ]] && echo "  PSC_ENV=${env} ./run_sample_java.bash --pool-size 16 --pool-wait 15"
done

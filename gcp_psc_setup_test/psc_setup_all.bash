#!/usr/bin/env bash

# End to end PSC setup for one cluster, for the connection type(s) of tls_mode (true = TLS, false = plain,
# both = one of each; default_tls_mode in psc_common.bash):
#   1. the PPv2 listener(s) those connections need (psc_add_proxy_protocol.bash; no restart when already set)
#   2. a PSC connection per type (psc_connection_setup.bash; a type that already has a connection on the
#      cluster is skipped, so a UI-created plain connection counts)
#   3. the consumer endpoint and the client examples for those connections (psc_generate_client.bash)
# Every step skips what already exists, so the script can be re-run after a failure.
#
# usage: psc_setup_all.bash [cluster_id] [interval] [consumer_project] [region] [network] [tls_mode true|false|both]
#   interval: time between node restarts when the PPv2 listeners are added (0s for a test cluster with no load)

script_dir=$(cd "$(dirname "$0")" && pwd)
source "${script_dir}/psc_common.bash"

cluster_id=${1:-${default_cluster_id}}
interval=${2:-${default_interval}} # 10 minutes between node restarts for a cluster under load; 0s for idle/new
consumer_project=${3:-${default_consumer_project}}
region=${4:-${default_region}}
consumer_network=${5:-${default_consumer_network}}
tls_mode=${6:-${default_tls_mode}}
tls_list=$(tls_values ${tls_mode}) || exit 1

echo "=== Step 1 of 3 - check or set up the PPv2 listener(s) for tls_mode ${tls_mode} on cluster ${cluster_id} ==="
"${script_dir}/psc_add_proxy_protocol.bash" ${cluster_id} ${interval} ${tls_mode} || exit 1

for tls in ${tls_list}; do
  echo
  echo "=== Step 2 of 3 - create the PSC connection, TLS=${tls} ==="
  existing=$(psc_connections ${cluster_id} | jq -r --argjson tls ${tls} 'select(.data.tls==$tls) | "\(.id) \(.name) \(.status)"')
  if [[ -n ${existing} ]]; then
    echo "exists: ${existing}"
    continue
  fi
  "${script_dir}/psc_connection_setup.bash" ${cluster_id} ${tls} ${consumer_project} || exit 1
done

# the cluster's PSC connections of the selected type(s), one JSON object per line
selected_connections() {
  psc_connections ${cluster_id} | jq -c --arg tls "${tls_list}" 'select(.data.tls|tostring|IN($tls|split(" ")[]))'
}

echo
echo "=== Step 3 of 3 - create the consumer endpoint(s) in ${consumer_project} ${region} ==="
names=$(selected_connections | jq -r .name)
if [[ -z ${names} ]]; then
  echo "ERROR: cluster ${cluster_id} has no PSC connection for tls_mode ${tls_mode}" >&2
  exit 1
fi
for name in ${names}; do
  "${script_dir}/psc_generate_client.bash" ${cluster_id} ${name} ${consumer_project} ${region} ${consumer_network} || exit 1
done

# psc_generate_client.bash wrote one sample_apps env file per ACTIVE connection; list them with the run commands
echo
echo "=== Sample apps for cluster ${cluster_id} ==="
selected_connections | while read -r connection; do
  name=$(jq -r .name <<< "${connection}")
  env_file="${script_dir}/sample_apps/psc-${name#psc-}.env"
  [[ -f ${env_file} ]] || continue
  echo
  echo "$(basename "${env_file}")  ($([[ $(jq -r .data.tls <<< "${connection}") == true ]] && echo TLS || echo non-TLS), connection $(jq -r .id <<< "${connection}"))"
  sed -n 's/^SCYLLA_/  SCYLLA_/p' "${env_file}"
done
echo
echo "Copy sample_apps/ (with the env files, bin/ and java-psc/target/psc-sample.jar) and ca-${cluster_id}.pem to"
echo "the client VM, then in sample_apps/:"
echo "  export SCYLLA_PASSWORD='...'   # API cluster/connect?clusterId=${cluster_id}, .data.credentials.password"
echo "  PSC_ENV=<env file> ./run_sample_python.bash   (or run_sample_go.bash, run_sample_java.bash, run_benchmark_psc.bash)"

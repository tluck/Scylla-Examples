#!/usr/bin/env bash

# End to end PSC setup for one cluster, TLS and non-TLS:
#   1. both PPv2 listeners on the cluster (psc_add_proxy_protocol.bash; no restart when already set)
#   2. one TLS and one plain PSC connection (psc_connection_setup.bash; a type that already has a connection
#      on the cluster is skipped, so a UI-created plain connection counts)
#   3. the consumer endpoint for every PSC connection and the client examples (psc_generate_client.bash)
# Every step skips what already exists, so the script can be re-run after a failure.
#
# usage: psc_setup_all.bash [cluster_id] [consumer_project] [region] [interval]
#   interval: time between node restarts when the PPv2 listeners are added (0s for a test cluster with no load)

script_dir=$(cd "$(dirname "$0")" && pwd)
source "${script_dir}/psc_common.bash"

cluster_id=${1:-${default_cluster_id}}
consumer_project=${2:-"cx-sa-lab"}
region=${3:-"us-west1"}
interval=${4:-600s}

echo "=== 1/3 PPv2 listeners on cluster ${cluster_id} ==="
"${script_dir}/psc_add_proxy_protocol.bash" ${cluster_id} ${interval} || exit 1

for tls in true false; do
  echo
  echo "=== 2/3 PSC connection, TLS=${tls} ==="
  existing=$(psc_connections ${cluster_id} | jq -r --argjson tls ${tls} 'select(.data.tls==$tls) | "\(.id) \(.name) \(.status)"')
  if [[ -n ${existing} ]]; then
    echo "exists: ${existing}"
    continue
  fi
  "${script_dir}/psc_connection_setup.bash" ${cluster_id} ${tls} ${consumer_project} || exit 1
done

echo
echo "=== 3/3 consumer endpoints in ${consumer_project} ${region} ==="
"${script_dir}/psc_generate_client.bash" ${cluster_id} "" ${consumer_project} ${region} || exit 1

# psc_generate_client.bash wrote one sample-apps env file per ACTIVE connection; list them with the run commands
echo
echo "=== Sample apps for cluster ${cluster_id} ==="
psc_connections ${cluster_id} | while read -r connection; do
  name=$(jq -r .name <<< "${connection}")
  env_file="${script_dir}/sample-apps/psc-${name#psc-}.env"
  [[ -f ${env_file} ]] || continue
  echo
  echo "$(basename "${env_file}")  ($([[ $(jq -r .data.tls <<< "${connection}") == true ]] && echo TLS || echo non-TLS), connection $(jq -r .id <<< "${connection}"))"
  sed -n 's/^SCYLLA_/  SCYLLA_/p' "${env_file}"
done
echo
echo "Copy sample-apps/ (with the env files, bin/ and java-psc/target/psc-sample.jar) and ca-${cluster_id}.pem to"
echo "the client VM, then in sample-apps/:"
echo "  export SCYLLA_PASSWORD='...'   # API cluster/connect?clusterId=${cluster_id}, .data.credentials.password"
echo "  PSC_ENV=<env file> ./run_sample_python.bash   (or run_sample_go.bash, run_sample_java.bash, run_benchmark_psc.bash)"

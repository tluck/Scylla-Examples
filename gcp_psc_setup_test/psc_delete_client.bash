#!/usr/bin/env bash

# Remove the consumer side of a PSC connection created by psc_generate_client.bash: A record, private DNS zone,
# forwarding rule (endpoint) and static IP. Deleting the connection in ScyllaDB Cloud does not remove these.
# With --connection it then also deletes the PSC connection itself through the REST API.
# Delete connections before you delete the cluster.
#
# usage: psc_delete_client.bash [cluster_id] connection_name [consumer_project] [region] [--connection]

script_dir=$(cd "$(dirname "$0")" && pwd)
source "${script_dir}/psc_common.bash"

delete_connection=false
args=()
for arg in "$@"; do
  if [[ ${arg} == --connection ]]; then delete_connection=true; else args+=("${arg}"); fi
done

cluster_id=${args[0]:-${default_cluster_id}}
connection_name=${args[1]:-}
consumer_project=${args[2]:-"cx-sa-lab"}
region=${args[3]:-"us-west1"}

if [[ -z ${connection_name} ]]; then
  echo "usage: $0 [cluster_id] connection_name [consumer_project] [region] [--connection]" >&2
  exit 1
fi

connection=$(psc_connections ${cluster_id} "${connection_name}")
if [[ -z ${connection} ]]; then
  echo "ERROR: no PSC connection named ${connection_name} found for cluster ${cluster_id}" >&2
  exit 1
fi
connection_id=$(jq -r .id <<< "${connection}")
domain=$(jq -r .data.address <<< "${connection}")
name_prefix=${domain%%.*}

echo "cluster ${cluster_id}, connection ${connection_id} (${connection_name}): ${domain}"
what="the consumer endpoint"
[[ ${delete_connection} == true ]] && what="${what} and the connection"
read -r -p "Delete ${what}? [y/N] " answer
[[ ${answer} == y ]] || exit 1

gcloud dns record-sets delete "${domain}." --type A \
    --project ${consumer_project} --zone ${name_prefix}-psc-dns --quiet
gcloud dns managed-zones delete ${name_prefix}-psc-dns --project ${consumer_project} --quiet
gcloud compute forwarding-rules delete ${name_prefix}-psc-ep \
    --project ${consumer_project} --region ${region} --quiet
gcloud compute addresses delete ${name_prefix}-psc-ip \
    --project ${consumer_project} --region ${region} --quiet

if [[ ${delete_connection} == true ]]; then
  api DELETE "/cluster/${cluster_id}/network/vpc/connection/${connection_id}" | jq -c .
  echo "delete of connection ${connection_id} requested; it is deleted in the background"
fi

#!/bin/bash

# Consumer side of the cluster's PSC connection(s): generate terraform, create the endpoint with gcloud
# (static IP, forwarding rule, private DNS zone + A record), write sample_apps/psc-<name>.env for the sample
# apps, and print the connect examples.
# Without connection_name every PSC connection of the cluster is handled; each gets its own psc_<id>/ directory
# (the generated files declare the same variables/provider, so they cannot share one terraform directory).
#
# usage: psc_generate_client.bash [cluster_id] [connection_name] [consumer_project] [region] [network] [subnet]

script_dir=$(cd "$(dirname "$0")" && pwd)
source "${script_dir}/psc_common.bash"

cluster_id=${1:-${default_cluster_id}}
connection_name=${2:-} # e.g. psc1 or psc-tls-54742; empty = all PSC connections
consumer_project=${3:-"cx-sa-lab"}
region=${4:-"us-west1"}
consumer_network=${5:-"projects/${consumer_project}/global/networks/default"}
consumer_subnet=${6:-"projects/${consumer_project}/regions/${region}/subnetworks/default"}

# connections come from the REST API: it has everything the endpoint needs (service attachment, domain,
# client route ID, tls), so only the optional terraform and the TLS name depend on cx
connections=$(psc_connections ${cluster_id} "${connection_name}")
if [[ -z ${connections} ]]; then
  echo "ERROR: no PSC connection${connection_name:+ named ${connection_name}} found for cluster ${cluster_id}" >&2
  exit 1
fi

# --- cluster-wide values, shared by all connections ---

# CA cert of the cluster (needed by TLS clients) from the public API: .data.content (the wiki's .Content is wrong)
ca_file="${script_dir}/ca-${cluster_id}.pem"
api GET "/cluster/${cluster_id}/certificate" | jq -r '.data.content // empty' > "${ca_file}"
if [[ ! -s ${ca_file} ]]; then
  echo "WARNING: could not fetch CA certificate" >&2
  rm -f "${ca_file}"
fi

# TLS name is not in the public API; cx describe has it. It stays empty until the first TLS connection
# re-issues the node certificates with the name. Only needed when there is a TLS connection
tls_name=""
if jq -e 'select(.data.tls==true)' <<< "${connections}" >/dev/null; then
  tls_name=$(${cx_timeout} scylla-cloud cluster describe --cluster-id ${cluster_id} 2>/dev/null \
    | tr -d '\r' | sed -n 's/^tlsname: *"\{0,1\}\([^"]*\)"\{0,1\}$/\1/p' | head -1)
fi

# local datacenter for the sample apps (the Java driver requires it)
dc_name=$(api GET "/cluster/${cluster_id}" | jq -r '.data.cluster.dc.name // empty')

# per-connection settings for the sample apps (sample_apps/psc_env.bash); no password, git-ignored (*.env)
write_sample_env() {
  local env_file=$1 domain=$2 client_route=$3 tls=$4
  {
    echo "# PSC connection ${name} (${connection_id}) of cluster ${cluster_id}, written by psc_generate_client.bash"
    echo "SCYLLA_MODE=psc"
    echo "SCYLLA_PSC_DNS=${domain}"
    echo "SCYLLA_PSC_CONN_ID=${client_route}"
    echo "SCYLLA_PSC_PORT=9000"
    echo "SCYLLA_DC=${dc_name}"
    if [[ ${tls} == "true" ]]; then
      # relative to this file
      echo "SCYLLA_CA_FILE=../$(basename "${ca_file}")"
      echo "SCYLLA_TLS_NAME=${tls_name}"
    fi
  } > "${env_file}"
  echo "Wrote ${env_file}"
}

# --- per connection ---

generate_terraform() {
  local connection_id=$1 tf_file=$2

  # output has ssh/log noise around (and JSON log lines inside) the terraform; strip CRs, keep from "terraform {"
  # on, drop JSON log lines and the ssh close message
  ${cx_timeout} sc dev network psc client-setup --connection-ids ${connection_id} 2>/dev/null \
    | tr -d '\r' \
    | sed -n '/^terraform {/,$p' \
    | grep -v -e '^{"level":' -e '^Shared connection' > "${tf_file}"

  if [[ ! -s ${tf_file} ]]; then
    echo "WARNING: client-setup produced no terraform for connection ${connection_id} (cx failed or timed out)" >&2
    rm -f "${tf_file}"
    return 1
  fi

  # the generated variables have no values (only placeholders in the descriptions); add defaults so the file
  # applies as-is. endpoints keys must match the keys in locals.connections (e.g. "scylla_psc_nr68713_d8f82611")
  local endpoint_keys
  endpoint_keys=$(sed -n '/^locals {/,/^}/p' "${tf_file}" | sed -n 's/^    "\([^"]*\)" = {$/\1/p')
  awk -v project="${consumer_project}" -v network="${consumer_network}" \
      -v region="${region}" -v subnet="${consumer_subnet}" -v keys="${endpoint_keys}" '
    /^variable "/ { var = $2; gsub(/"/, "", var) }
    { print }
    var == "project_id" && /^  type *= string$/ { printf "  default     = \"%s\"\n", project; var = "" }
    var == "network"    && /^  type *= string$/ { printf "  default     = \"%s\"\n", network; var = "" }
    var == "endpoints"  && /^  }\)\)$/ {
      print "  default = {"
      n = split(keys, k, "\n")
      for (i = 1; i <= n; i++) if (k[i] != "") {
        printf "    \"%s\" = {\n      region     = \"%s\"\n      subnetwork = \"%s\"\n    }\n", k[i], region, subnet
      }
      print "  }"
      var = ""
    }
  ' "${tf_file}" > "${tf_file}.tmp" && mv "${tf_file}.tmp" "${tf_file}"
  echo "Wrote ${tf_file}"
}

# manual alternative to the terraform (mirrors psc_<id>.tf): static IP, PSC endpoint, private DNS zone + A record.
# terraform var.project_id -> consumer_project, var.network -> consumer_network,
# var.endpoints[].region/subnetwork -> region/consumer_subnet; locals.connections come from the connection's Data.
# Each step skips creation when the resource already exists (e.g. created by terraform or a previous run)
create_endpoint() {
  local name_prefix=$1 service_attachment_id=$2 domain=$3

  if gcloud compute addresses describe ${name_prefix}-psc-ip \
      --project ${consumer_project} --region ${region} >/dev/null 2>&1; then
    echo "exists: address ${name_prefix}-psc-ip"
  else
    gcloud compute addresses create ${name_prefix}-psc-ip \
        --project ${consumer_project} --region ${region} --subnet ${consumer_subnet}
  fi

  if gcloud compute forwarding-rules describe ${name_prefix}-psc-ep \
      --project ${consumer_project} --region ${region} >/dev/null 2>&1; then
    echo "exists: forwarding rule ${name_prefix}-psc-ep"
  else
    gcloud compute forwarding-rules create ${name_prefix}-psc-ep \
        --project ${consumer_project} --region ${region} --network ${consumer_network} \
        --address ${name_prefix}-psc-ip \
        --target-service-attachment ${service_attachment_id} \
        --allow-psc-global-access
  fi

  endpoint_ip=$(gcloud compute addresses describe ${name_prefix}-psc-ip \
      --project ${consumer_project} --region ${region} --format='value(address)')

  if gcloud dns managed-zones describe ${name_prefix}-psc-dns \
      --project ${consumer_project} >/dev/null 2>&1; then
    echo "exists: DNS zone ${name_prefix}-psc-dns"
  else
    gcloud dns managed-zones create ${name_prefix}-psc-dns \
        --project ${consumer_project} --dns-name "${domain}." --visibility private \
        --networks ${consumer_network} --description "PSC endpoint for ${domain}"
  fi

  if gcloud dns record-sets describe "${domain}." --type A \
      --project ${consumer_project} --zone ${name_prefix}-psc-dns >/dev/null 2>&1; then
    echo "exists: A record ${domain}"
  else
    gcloud dns record-sets create "${domain}." \
        --project ${consumer_project} --zone ${name_prefix}-psc-dns \
        --type A --ttl 300 --rrdatas ${endpoint_ip}
  fi
}

# fd 3, not stdin: sc (ssh) and gcloud inside the loop would otherwise read the remaining connections
while read -r connection <&3; do
  connection_id=$(jq -r .id <<< "${connection}")
  name=$(jq -r .name <<< "${connection}")
  status=$(jq -r .status <<< "${connection}")
  tls=$(jq -r .data.tls <<< "${connection}")
  service_attachment_id=$(jq -r .data.serviceAttachmentId <<< "${connection}")
  domain=$(jq -r .data.address <<< "${connection}")
  client_route=$(jq -r .data.clientRoutesConnectionId <<< "${connection}")
  # the resource name prefix is the first label of the PSC domain (also the service attachment name minus -sa)
  name_prefix=${domain%%.*}

  echo
  echo "=== cluster ${cluster_id}, PSC connection ${connection_id} (${name}), TLS=${tls}, ${status} ==="
  if [[ ${status} != ACTIVE ]]; then
    echo "skipped: connection is not ACTIVE yet"
    continue
  fi

  # the terraform is a by-product for reference; the endpoint below does not depend on it
  mkdir -p "${script_dir}/psc_${connection_id}"
  tf_file="${script_dir}/psc_${connection_id}/psc_${connection_id}.tf"
  generate_terraform ${connection_id} "${tf_file}"

  create_endpoint ${name_prefix} ${service_attachment_id} ${domain}
  # with ACCEPT_MANUAL the endpoint stays PENDING until the consumer project is on the connection's accept list
  psc_status=$(gcloud compute forwarding-rules describe ${name_prefix}-psc-ep \
      --project ${consumer_project} --region ${region} --format='value(pscConnectionStatus)')
  echo "PSC endpoint ${endpoint_ip} -> ${domain} (${psc_status})"

  # ports: 9000-9002 discovery (any node), 9003+ one node each (system.client_routes port / tls_port).
  # A connection is all-TLS or all-plain: plain clients cannot use a TLS connection's ports and vice versa,
  # so only the examples that match this connection are printed (cqlsh and the Python sample app)
  echo
  echo "Ports: 9000-9002 discovery, 9003+ one node each (psc_get_connection_ids.bash ${cluster_id})"
  echo "Client route ID: ${client_route}"
  env_name="psc-${name#psc-}.env"
  write_sample_env "${script_dir}/sample_apps/${env_name}" ${domain} ${client_route} ${tls}
  echo
  echo "Sample apps (in sample_apps/, export SCYLLA_PASSWORD first):"
  for run in run_sample_go.bash run_sample_python.bash run_sample_java.bash run_benchmark_psc.bash; do
    echo "  PSC_ENV=${env_name} ./${run}"
  done
  echo
  if [[ ${tls} == "true" ]]; then
    echo "TLS name (driver ServerName / host verification): ${tls_name:-<not set yet or cx unavailable>}"
    echo "CA certificate: ${ca_file} (copy it to the client VM, into the directory you run cqlsh or the sample app from)"
    echo
    echo "Check the handshake and the certificate names:"
    echo "  openssl s_client -connect ${domain}:9000 -CAfile $(basename "${ca_file}") </dev/null 2>/dev/null \\"
    echo "    | openssl x509 -noout -subject -ext subjectAltName"
    echo
    echo "Example (TLS), cqlsh:"
    # cqlsh has no ServerName/SNI option and the PSC domain is not the TLS name, so validate the CA chain only;
    # drivers should set ServerName to the TLS name and keep host verification on
    echo "  SSL_CERTFILE=$(basename "${ca_file}") SSL_VALIDATE=true SSL_CHECK_HOSTNAME=false \\"
    echo "    cqlsh --ssl -u \$USERNAME -p \$PASSWORD --client-route ${client_route} ${domain} 9000"
  else
    echo "Example (non-TLS), cqlsh:"
    echo "  cqlsh -u \$USERNAME -p \$PASSWORD --client-route ${client_route} ${domain} 9000"
  fi
done 3<<< "${connections}"

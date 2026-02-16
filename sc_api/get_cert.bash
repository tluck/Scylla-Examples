#!/usr/bin/env bash

# ScyllaDB Cloud API example: Create a cluster using cURL

# Configuration
API_BASE_URL="https://api.cloud.scylladb.com"
# From environment variables
API_TOKEN=${SC_TOKEN}  # Replace with your actual API token
accountId=${SC_ACCOUNT} # Replace with your actual account ID

if [[ $1 == -h ]] || [[ $1 == --help ]]; then
  printf 'Usage:\n  %s -l\n  %s -c <clusterId>\n' "$0" "$0"
  exit
fi  

if [[ ${1} == "-l" ]]; then
printf "Listing clusters for account %s:\n" "$accountId"  
printf "   ID Cluster Name\n"
  curl -s -X GET "${API_BASE_URL}/account/${accountId}/clusters" \
    -H "Authorization: Bearer ${API_TOKEN}" | jq -r '.data.clusters[] | "\(.id) \(.clusterName)"'
  exit
fi

if [[ ${1} == "-c" ]]; then
  shift
  clusterId=$1
  printf "\nGetting cluster info for cluster %s:\n" "$clusterId"
  curl -s -X GET "${API_BASE_URL}/account/${accountId}/cluster/${clusterId}" \
    -H "Authorization: Bearer ${API_TOKEN}" | jq
  printf "\nGetting rootCA certificate for account %s:\n" "$accountId"
  curl -s -X GET "${API_BASE_URL}/account/${accountId}/certificate" \
    -H "Authorization: Bearer ${API_TOKEN}" | jq -r .data.content > ca-${accountId}.crt
  cat ca-${accountId}.crt
  printf "Saved rootCA certificate to ca-%s.crt\n\n" "$accountId"
  exit 0
fi  

printf 'Usage:\n  %s -l\n  %s -c <clusterId>\n' "$0" "$0"
exit 0

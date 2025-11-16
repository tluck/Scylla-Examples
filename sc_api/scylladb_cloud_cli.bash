#!/usr/bin/env bash

# ScyllaDB Cloud API example: Create a cluster using cURL

# Configuration
API_BASE_URL="https://api.cloud.scylladb.com"
# From environment variables
API_TOKEN=${SC_TOKEN}  # Replace with your actual API token
accountId=${SC_ACCOUNT} # Replace with your actual account ID

if [[ $1 == "gcp" ]]; then
  instanceType="n2-highmem-2" # "e2-micro" # id=40
  localDiskCount=1 # (use 0 for e2-micro)
  mode=${2:-"xcloud"} # "standard" or "xcloud"
  cloudProviderId=2
  region="us-west1"
  name="tjl-gcp-$instanceType-$mode"
  name="${name//./-}"
else
  instanceType="t3.micro" #instanceType="i4i.large" # id=62
  instanceType="i4i.large" # id=62
  mode=${2:-"xcloud"} # "standard" or "xcloud"
  cloudProviderId=1
  region="us-west-2"
  name="tjl-aws-$instanceType-$mode"
  name="${name//./-}"
fi  
  owner="Account" # BYOA
  cidr="172.30.0.0/24"

if [[ $1 == -h ]] || [[ $1 == --help ]]; then
  printf 'Usage:\n  %s -l\n  %s -s <clusterId>\n  %s -d <clusterId> <clusterName>\n  %s gcp|aws [xcloud|standard]\n' "$0" "$0" "$0" "$0"
  exit
fi  

if [[ ${1} == "-l" ]]; then
printf "Listing clusters for account %s:\n" "$accountId"  
printf "   ID Cluster Name\n"
  curl -s -X GET "${API_BASE_URL}/account/${accountId}/clusters" \
    -H "Authorization: Bearer ${API_TOKEN}" | jq -r '.data.clusters[] | "\(.id) \(.clusterName)"'
  exit
fi

if [[ ${1} == "-s" ]]; then
  shift
  clusterId=$1
  curl -s -X GET "${API_BASE_URL}/account/${accountId}/cluster/${clusterId}" \
    -H "Authorization: Bearer ${API_TOKEN}" | jq
  exit
fi  

if [[ ${1} == "-d" ]]; then
  shift
  clusterId=$1
  name=$2
  printf "Deleting cluster %s (%s)\n" "$name" "$clusterId"
  read -p "Are you sure? (y/N) " confirm
  if [[ "$confirm" != "y" ]]; then
    echo "Aborting."
    exit 1
  fi
(curl -s -X POST "${API_BASE_URL}/account/${accountId}/cluster/${clusterId}/delete" \
  -H "Authorization: Bearer ${API_TOKEN}" \
  -H "Content-Type: application/json" \
  -d@- <<EOF
{ "clusterName": "${name}" }
EOF
) | jq
exit
fi

cloudCredentialId=$( curl -s \
  -X GET "${API_BASE_URL}/account/${accountId}/cloud-account" \
  -H "Authorization: Bearer ${API_TOKEN}" | jq -r --arg owner "$owner" --argjson cpId "$cloudProviderId" '.data[] | select(.owner == $owner and .cloudProviderId == $cpId) | .id' )
printf "Cloud Credential ID: %s\n" "$cloudCredentialId"

regionId=$(curl -s \
  -X GET "${API_BASE_URL}/deployment/cloud-provider/${cloudProviderId}/regions" \
  -H "Authorization: Bearer ${API_TOKEN}" | jq -r --arg region "$region" '.data.regions[] | select(.externalId == $region ) | .id' )
printf "Region ID: %s\n" "$regionId"

if [[ $1 == "gcp" ]]; then
instanceId=$(curl -s \
  -X GET "${API_BASE_URL}/deployment/cloud-provider/${cloudProviderId}/region/${regionId}" \
  -H "Authorization: Bearer ${API_TOKEN}" | jq -r --arg inst "$instanceType" --argjson lDC "$localDiskCount" '.data.instances[] | select(.externalId == $inst and .localDiskCount == $lDC) | .id')
else
instanceId=$(curl -s \
  -X GET "${API_BASE_URL}/deployment/cloud-provider/${cloudProviderId}/region/${regionId}" \
  -H "Authorization: Bearer ${API_TOKEN}" | jq -r --arg inst "$instanceType" '.data.instances[] | select(.externalId == $inst) | .id')
fi
printf "Instance ID: %s\n" "$instanceId"

base_json=$(jq -n \
  --argjson accountCredentialId "$cloudCredentialId" \
  --arg broadcastType "PRIVATE" \
  --arg cidr "$cidr" \
  --argjson rackCIDRSize 26 \
  --argjson cloudProviderId "$cloudProviderId" \
  --argjson regionId "$regionId" \
  --arg clusterName "$name" \
  --argjson replicationFactor 3 \
  --arg scyllaVersion "2025.3.3" \
  --arg userApiInterface "CQL" \
  --arg tablets "enforced" \
  --argjson freeTier false \
  --arg mode "$mode" \
  --argjson instanceId "$instanceId" '
  {
    accountCredentialId: $accountCredentialId,
    broadcastType: $broadcastType,
    cidrBlock: $cidr,
    rackCIDRSize: $rackCIDRSize,
    cloudProviderId: $cloudProviderId,
    regionId: $regionId,
    clusterName: $clusterName,
    replicationFactor: $replicationFactor,
    scyllaVersion: $scyllaVersion,
    userApiInterface: $userApiInterface,
    tablets: $tablets,
    freeTier: $freeTier
  }
')

# Conditionally add or remove numberOfNodes
if [[ "$mode" == "standard" ]]; then
  base_json=$(echo "$base_json" | jq --argjson instanceId "${instanceId}" '. + {numberOfNodes: 3} + {instanceId: $instanceId}' )
  else
  base_json=$(echo "$base_json" | jq --argjson instanceId "${instanceId}" '. + {scaling: {mode: "xcloud", instanceTypeIDs: [$instanceId], policies: {storage: {min: 0, targetUtilization: 0.8}, vcpu: {min: 0}}}}')
fi

echo $base_json | jq
( curl -s -X POST "${API_BASE_URL}/account/${accountId}/cluster" \
  -H "Authorization: Bearer ${API_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "$base_json"
) | jq | tee scylladb_cloud_cli_${name}.log

exit 0

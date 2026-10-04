#!/usr/bin/env bash

# ScyllaDB Cloud API example: list, show, delete and create clusters using cURL.
# Mirrors the options of scylladb_cloud_cli.py (scale and monitor are Python only).

# Configuration
API_BASE_URL="https://api.cloud.scylladb.com"
# From environment variables
API_TOKEN=${SC_TOKEN}  # Replace with your actual API token
accountId=${SC_ACCOUNT} # Replace with your actual account ID

default_cidr="172.30.1.0/24"
default_instance_gcp="n2-highmem-2"
default_instance_aws="i8g.large"
default_family_gcp="n2-highmem"
default_family_aws="i8g"
default_region_gcp="us-west1"
default_region_aws="us-west-2"

usage() {
  cat <<EOF
Usage:
  $0 list
  $0 show -c CLUSTER_ID
  $0 delete -c CLUSTER_ID CLUSTER_NAME
  $0 create -p gcp|aws [options]

create options:
  -p, --cloud gcp|aws           Cloud provider (required)
  -m, --mode xcloud|standard    Deployment mode (default: xcloud)
  -o, --owner byoa|scylla       Whose cloud account hosts the cluster: 'byoa'
                                (owner=Account) or 'scylla' (owner=Scylla) (default: byoa)
  -l, --name NAME               Cluster name (overrides default naming)
  -v, --vcpu N                  Initial total vCPU minimum for the xcloud scaling policy
                                (xcloud only, default: 0); on its own it scales within the
                                cloud's default family ($default_family_gcp / $default_family_aws)
  -t, --tib N                   Initial total storage minimum in TiB, sent as storage.min
                                in GiB (xcloud only, default: 0)
  -F, --instance-family [FAM]   Instance family to scale within instead of an instance type;
                                bare -F uses the cloud's default family; requires --vcpu
  -T, --instance-type TYPE      Instance type (default: $default_instance_gcp / $default_instance_aws)
  -d, --disk N                  Local disk count (GCP only)
  -n, --nodes N                 Number of nodes (standard mode, default: 3)
  -r, --region REGION           Region (default: $default_region_gcp / $default_region_aws)
  -i, --cidr CIDR               CIDR block for the cluster VPC (default: $default_cidr)
  -f, --replication N           Replication factor (default: 3)
  -s, --scylla-version VER      Scylla version (default: latest)
  -e, --encryption [KEY_ID]     Encryption at rest: bare -e uses a ScyllaDB-managed key;
                                -e KEY_ID uses a customer-managed key (BYOK), e.g. key-deadbeef
                                (default: not sent, so the account default applies)
EOF
}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

lower() {
  echo "$1" | tr '[:upper:]' '[:lower:]'
}

# The API reports rejections as {"error": "..."} under an HTTP 200, so check the
# body as well as the status.
api() {
  local method=$1 url=$2 data=$3 body
  if [[ -n "$data" ]]; then
    body=$(curl -s -f -X "$method" "$url" \
      -H "Authorization: Bearer ${API_TOKEN}" \
      -H "Content-Type: application/json" \
      -d "$data") || die "$method $url failed"
  else
    body=$(curl -s -f -X "$method" "$url" \
      -H "Authorization: Bearer ${API_TOKEN}") || die "$method $url failed"
  fi
  if [[ -n "$(echo "$body" | jq -r '.error // empty' 2>/dev/null)" ]]; then
    die "$method $url -> error $(echo "$body" | jq -r '.error')"
  fi
  echo "$body"
}

handle_list() {
  printf "Listing clusters for account %s:\n" "$accountId"
  printf "   ID Cluster Name\n"
  api GET "${API_BASE_URL}/account/${accountId}/clusters" | jq -r '.data.clusters[] | "\(.id) \(.clusterName)"'
}

handle_show() {
  [[ -n "$clusterId" ]] || die "Cluster ID required for show"
  api GET "${API_BASE_URL}/account/${accountId}/cluster/${clusterId}" | jq
}

handle_delete() {
  [[ -n "$clusterId" && -n "$clusterName" ]] || die "Cluster ID and name required for delete"
  read -p "Deleting cluster ${clusterName} (${clusterId}). Are you sure? (y/N) " confirm
  if [[ "$(lower "$confirm")" != "y" ]]; then
    echo "Aborting."
    exit 1
  fi
  api POST "${API_BASE_URL}/account/${accountId}/cluster/${clusterId}/delete" \
    "$(jq -n --arg name "$clusterName" '{clusterName: $name}')" | jq
}

handle_create() {
  [[ -n "$cloud" ]] || die "--cloud (-p) is required for create"
  [[ "$cloud" == "gcp" || "$cloud" == "aws" ]] || die "--cloud must be gcp or aws"
  [[ "$mode" == "xcloud" || "$mode" == "standard" ]] || die "--mode must be xcloud or standard"
  [[ "$owner_arg" == "byoa" || "$owner_arg" == "scylla" ]] || die "--owner must be byoa or scylla"

  if [[ "$cloud" == "gcp" ]]; then
    default_family=$default_family_gcp
  else
    default_family=$default_family_aws
  fi
  # A bare -F takes the default family for the cloud.
  family=$instance_family
  if [[ -n "$explicit_family" && -z "$family" ]]; then
    family=$default_family
  fi

  if [[ "$mode" != "xcloud" && ( -n "$vcpu" || -n "$tib" || -n "$explicit_family" ) ]]; then
    die "--vcpu/--tib/--instance-family set the xcloud scaling policy and cannot be used with --mode standard"
  fi
  if [[ -n "$explicit_family" && -n "$custom_instance" ]]; then
    die "--instance-family and --instance-type are mutually exclusive"
  fi
  if [[ -n "$encryption" && ! "$encryption" =~ ^key-[a-zA-Z0-9]+$ ]]; then
    die "invalid --encryption key ID '$encryption'; expected a portal key ID such as key-deadbeef"
  fi
  if [[ -n "$explicit_family" && -z "$vcpu" ]]; then
    die "--instance-family requires --vcpu; without a vCPU minimum the API has nothing to size the family against"
  fi

  # A vCPU minimum with nothing pinning a specific instance (--instance-type or
  # its GCP --disk count) scales within the cloud's default family.
  if [[ -z "$family" && -n "$vcpu" && -z "$custom_instance" && -z "$custom_disks" ]]; then
    family=$default_family
    echo "No --instance-type given; scaling within default instance family '$family'"
  fi

  # choose defaults per cloud
  instanceType=""
  localDiskCount=""
  if [[ "$cloud" == "gcp" ]]; then
    if [[ -z "$family" ]]; then
      instanceType=${custom_instance:-$default_instance_gcp}
      localDiskCount=${custom_disks:-1}
    fi
    region=${region:-$default_region_gcp}
    if [[ -n "$name" ]]; then
      :
    elif [[ -n "$family" ]]; then
      name="tjl-gcp-$family"
    else
      name="tjl-gcp-$instanceType-$localDiskCount"
    fi
    cloudProviderId=2
  else
    if [[ -z "$family" ]]; then
      instanceType=${custom_instance:-$default_instance_aws}
      localDiskCount=$custom_disks # For AWS, keep empty if not specified
    fi
    region=${region:-$default_region_aws}
    if [[ -n "$name" ]]; then
      :
    elif [[ -n "$family" ]]; then
      name="tjl-aws-$family"
    else
      name="tjl-aws-$instanceType"
    fi
    cloudProviderId=1
  fi

  name="${name//./-}"
  if [[ "$owner_arg" == "scylla" ]]; then owner="Scylla"; else owner="Account"; fi
  cidr=${cidr:-$default_cidr}
  replication=${replication:-3}

  if [[ -n "$family" ]]; then
    echo "Creating cluster '$name' with instance family: $family"
  elif [[ -n "$localDiskCount" ]]; then
    echo "Creating cluster '$name' with instance type: $instanceType and $localDiskCount disks"
  else
    echo "Creating cluster '$name' with instance type: $instanceType"
  fi

  # Get cloudCredentialId (highest id if several match owner + cloud provider)
  cloudCredentialId=$(api GET "${API_BASE_URL}/account/${accountId}/cloud-account" |
    jq -r --arg owner "$owner" --argjson cpId "$cloudProviderId" \
      '[.data[] | select(.owner == $owner and .cloudProviderId == $cpId) | .id] | max // empty')
  printf "Cloud Credential ID: %s (owner: %s)\n" "$cloudCredentialId" "$owner"

  # Get regionId
  regionId=$(api GET "${API_BASE_URL}/deployment/cloud-provider/${cloudProviderId}/regions" |
    jq -r --arg region "$region" '.data.regions[] | select(.externalId == $region) | .id' | head -1)
  printf "Region ID: %s\n" "$regionId"
  [[ -n "$regionId" ]] || die "Could not find region '$region'"

  # Get instanceId (or validate the family, which is sent by name, not by ID)
  instances=$(api GET "${API_BASE_URL}/deployment/cloud-provider/${cloudProviderId}/region/${regionId}")
  instanceId=""
  if [[ -n "$family" ]]; then
    if ! echo "$instances" | jq -e --arg fam "$family" 'any(.data.instances[]; .instanceFamily == $fam)' >/dev/null; then
      echo "ERROR: Could not find instance family '$family' in $region"
      echo
      echo "Available instance families:"
      echo "$instances" | jq -r '[.data.instances[].instanceFamily | select(. != null and . != "")] | unique[] | "  \(.)"'
      exit 1
    fi
  elif [[ "$cloud" == "gcp" && -n "$localDiskCount" ]]; then
    instanceId=$(echo "$instances" | jq -r --arg inst "$instanceType" --argjson lDC "$localDiskCount" \
      '.data.instances[] | select(.externalId == $inst and .localDiskCount == $lDC) | .id' | head -1)
  else
    instanceId=$(echo "$instances" | jq -r --arg inst "$instanceType" \
      '.data.instances[] | select(.externalId == $inst) | .id' | head -1)
  fi

  if [[ -z "$family" && -z "$instanceId" ]]; then
    if [[ -n "$localDiskCount" ]]; then
      echo "ERROR: Could not find instance type '$instanceType' with $localDiskCount disks"
    else
      echo "ERROR: Could not find instance type '$instanceType'"
    fi
    echo
    echo "Available instances:"
    if [[ "$cloud" == "gcp" ]]; then
      echo "$instances" | jq -r '.data.instances[] | "  \(.externalId) (disks: \(.localDiskCount))"'
    else
      echo "$instances" | jq -r '.data.instances[] | "  \(.externalId)"'
    fi
    exit 1
  fi

  [[ -n "$family" ]] || printf "Instance ID: %s\n" "$instanceId"

  # Build payload
  base_json=$(jq -n \
    --argjson accountCredentialId "$cloudCredentialId" \
    --arg cidr "$cidr" \
    --argjson cloudProviderId "$cloudProviderId" \
    --argjson regionId "$regionId" \
    --arg clusterName "$name" \
    --argjson replicationFactor "$replication" '
    {
      accountCredentialId: $accountCredentialId,
      broadcastType: "PRIVATE",
      cidrBlock: $cidr,
      rackCIDRSize: 26,
      cloudProviderId: $cloudProviderId,
      regionId: $regionId,
      clusterName: $clusterName,
      replicationFactor: $replicationFactor,
      userApiInterface: "CQL",
      tablets: "enforced",
      freeTier: false
    }
  ')

  if [[ -n "$scylla_version" ]]; then
    base_json=$(echo "$base_json" | jq --arg v "$scylla_version" '. + {scyllaVersion: $v}')
  fi

  # Encryption at rest: "scylla-<cloud>" for a ScyllaDB-managed key, or "<cloud>" with a keyId for BYOK
  if [[ -n "$explicit_encryption" && -z "$encryption" ]]; then
    base_json=$(echo "$base_json" | jq --arg provider "scylla-$cloud" '. + {encryptionAtRest: {provider: $provider}}')
  elif [[ -n "$encryption" ]]; then
    base_json=$(echo "$base_json" | jq --arg provider "$cloud" --arg keyId "$encryption" '. + {encryptionAtRest: {provider: $provider, keyId: $keyId}}')
  fi

  if [[ "$mode" == "standard" ]]; then
    base_json=$(echo "$base_json" | jq --argjson nodes "${nodes:-3}" --argjson instanceId "$instanceId" \
      '. + {numberOfNodes: $nodes, instanceId: $instanceId}')
  else
    # The API docs say GB, but the values are GiB, so a TiB argument converts with 1024.
    storage_min=$(( ${tib:-0} * 1024 ))
    base_json=$(echo "$base_json" | jq --argjson storage "$storage_min" --argjson vcpu "${vcpu:-0}" \
      '. + {scaling: {mode: "xcloud", policies: {storage: {min: $storage, targetUtilization: 0.8}, vcpu: {min: $vcpu}}}}')
    # The API requires exactly one of instanceTypeIDs or instanceFamilies.
    if [[ -n "$family" ]]; then
      base_json=$(echo "$base_json" | jq --arg fam "$family" '.scaling.instanceFamilies = [$fam]')
    else
      base_json=$(echo "$base_json" | jq --argjson instanceId "$instanceId" '.scaling.instanceTypeIDs = [$instanceId]')
    fi
  fi

  echo "$base_json" | jq
  api POST "${API_BASE_URL}/account/${accountId}/cluster" "$base_json" | jq | tee "scylladb_cloud_cli_${name}.log"
}

# Value for an option that requires one
need_value() {
  [[ $# -ge 2 && -n "$2" ]] || die "option $1 requires a value"
}

# Parse the command, then its options
command=$1
[[ $# -gt 0 ]] && shift
mode="xcloud"
owner_arg="byoa"

while [[ $# -gt 0 ]]; do
  case $1 in
    -h|--help) usage; exit 0 ;;
    -c|--cluster) need_value "$@"; clusterId=$2; shift ;;
    -p|--cloud) need_value "$@"; cloud=$(lower "$2"); shift ;;
    -m|--mode) need_value "$@"; mode=$(lower "$2"); shift ;;
    -o|--owner) need_value "$@"; owner_arg=$(lower "$2"); shift ;;
    -l|--name) need_value "$@"; name=$2; shift ;;
    -v|--vcpu) need_value "$@"; vcpu=$2; shift ;;
    -t|--tib) need_value "$@"; tib=$2; shift ;;
    -T|--instance-type) need_value "$@"; custom_instance=$2; shift ;;
    -d|--disk) need_value "$@"; custom_disks=$2; shift ;;
    -n|--nodes) need_value "$@"; nodes=$2; shift ;;
    -r|--region) need_value "$@"; region=$(lower "$2"); shift ;;
    -i|--cidr) need_value "$@"; cidr=$2; shift ;;
    -f|--replication) need_value "$@"; replication=$2; shift ;;
    -s|--scylla-version) need_value "$@"; scylla_version=$2; shift ;;
    # -F and -e take an optional value, used only when the next word is not an option
    -F|--instance-family)
      explicit_family=1
      if [[ $# -ge 2 && "$2" != -* ]]; then instance_family=$2; shift; fi ;;
    -e|--encryption)
      explicit_encryption=1
      if [[ $# -ge 2 && "$2" != -* ]]; then encryption=$2; shift; fi ;;
    -*) usage; die "unknown option $1" ;;
    *)
      # delete takes the cluster name as a positional argument
      [[ "$command" == "delete" && -z "$clusterName" ]] || { usage; die "unexpected argument $1"; }
      clusterName=$1 ;;
  esac
  shift
done

for n in vcpu tib custom_disks nodes replication; do
  [[ -z "${!n}" || "${!n}" =~ ^[0-9]+$ ]] || die "$n must be an integer, got '${!n}'"
done

case $command in
  list) handle_list ;;
  show) handle_show ;;
  delete) handle_delete ;;
  create) handle_create ;;
  -h|--help) usage ;;
  scale|monitor) die "'$command' is only available in scylladb_cloud_cli.py" ;;
  *) usage; exit 1 ;;
esac

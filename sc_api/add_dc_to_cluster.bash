#!/usr/bin/env bash

# Add a DC to an existing ScyllaDB Cloud cluster, then join it.
#
# The join runs only when the add succeeded and the DC reached
# BOOTSTRAP_COMPLETE; DC_JOIN=0 stops after the add, leaving the DC to be
# joined later with join_dc_to_cluster.bash.
#
# The request body is the template file (default add-dc-gcp.json) with the
# variables below overlaid on it. Override any of them in the environment:
#   DC_REGION_NAME=us-east1 DC_CIDR=172.31.2.0/24 ./add_dc_to_cluster.bash 136566
#
# DC_REGION_NAME drives both the region id and the availability zones; set
# DC_REGION / DC_AZS directly to override what the region table gives you.

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)

ACCOUNT_ID=$SC_ACCOUNT
USER_ID=$SC_USER

# ---- template variables ----------------------------------------------------
DC_REGION_NAME=${DC_REGION_NAME:-us-central1}       # GCP region name; sets DC_REGION and DC_AZS below
DC_CIDR=${DC_CIDR:-172.31.0.0/24}                   # CidrBlock (must not overlap the existing DC)
DC_RF=${DC_RF:-3}                                   # ReplicationFactor
DC_INSTANCE_FAMILIES=${DC_INSTANCE_FAMILIES:-n2-highmem}  # comma-separated
DC_CREDENTIAL_ID=${DC_CREDENTIAL_ID:-21946}         # AccountCredentialID
DC_NODES=${DC_NODES:-0}                             # Nodes: 0 = let scaling decide
DC_VCPU_MIN=${DC_VCPU_MIN:-6}                       # Scaling.Policies.VCPU.Min
DC_JOIN=${DC_JOIN:-1}                               # 1 = join the DC after a successful add
                                                    #   0 = add only, join separately
# ----------------------------------------------------------------------------

# GCP US regions (CloudProvider 2): name -> CloudProviderRegion id + zones.
# us-west2 / us-west3 are not offered; note us-east1 has no "-a" zone.
# Refresh the ids for any provider (1 = AWS, 2 = GCP) with:
#   curl -s "https://api.cloud.scylladb.com/deployment/cloud-provider/2/regions" \
#     -H "Authorization: Bearer ${SC_TOKEN}" \
#     | jq -r '.data.regions[] | "\(.id)\t\(.externalId)"' | sort -n
# DC_NAME_BASE is the region's dcName from that same endpoint -- the Scylla DC
# name used in NetworkTopologyStrategy.
case $DC_REGION_NAME in
  us-central1) REGION_ID=29; ZONES=a,b,c; DC_NAME_BASE=GCE_US_CENTRAL_1 ;;
  us-east1)    REGION_ID=30; ZONES=b,c,d; DC_NAME_BASE=GCE_US_EAST_1 ;;
  us-east4)    REGION_ID=31; ZONES=a,b,c; DC_NAME_BASE=GCE_US_EAST_4 ;;
  us-west1)    REGION_ID=32; ZONES=a,b,c; DC_NAME_BASE=GCE_US_WEST_1 ;;
  us-west4)    REGION_ID=35; ZONES=a,b,c; DC_NAME_BASE=GCE_US_WEST_4 ;;
  us-south1)   REGION_ID=49; ZONES=a,b,c; DC_NAME_BASE=GCE_US_SOUTH_1 ;;
  *) echo "Unknown DC_REGION_NAME: $DC_REGION_NAME (set DC_REGION and DC_AZS explicitly)" >&2
     REGION_ID=; ZONES=; DC_NAME_BASE= ;;
esac

# CloudProviderRegion, and AvailabilityZoneIDsOverride as <region>-<zone>.
# Override either explicitly to bypass the table above.
DC_REGION=${DC_REGION:-$REGION_ID}
if [[ -z ${DC_AZS:-} && -n $ZONES ]]; then
  DC_AZS=$(echo "$ZONES" | tr ',' '\n' | sed "s/^/${DC_REGION_NAME}-/" | paste -sd, -)
fi
DC_AZS=${DC_AZS:-}                                  # empty = leave the template's value alone

if [[ -z $DC_REGION ]]; then
  echo "error: no region id; set DC_REGION explicitly" >&2
  exit 1
fi

if [[ "$1" == '' ]];then
    echo "Usage: $0 cluster_id [request_id | body.json]"
    exit 1
else
    CLUSTER_ID=$1
    shift
fi

if [[ $1 =~ ^[0-9]+$ ]];then
REQUEST_ID=$1

CX_ARGS=(
  --cluster-id "$CLUSTER_ID"
  --account-id "$ACCOUNT_ID"
  --user-id "$USER_ID"
  --cluster-request-id "$REQUEST_ID"
)

else

BODY_FILE=${1:-$SCRIPT_DIR/add-dc-gcp.json}
if [[ ! -f $BODY_FILE ]];then
    echo "No such file: $BODY_FILE"
    exit 1
fi

BODY=$(jq -c \
  --arg     cidr       "$DC_CIDR" \
  --argjson region     "$DC_REGION" \
  --argjson rf         "$DC_RF" \
  --arg     families   "$DC_INSTANCE_FAMILIES" \
  --argjson credential "$DC_CREDENTIAL_ID" \
  --argjson nodes      "$DC_NODES" \
  --argjson vcpumin    "$DC_VCPU_MIN" \
  --arg     azs        "$DC_AZS" \
  '
    def csv: split(",") | map(gsub("^\\s+|\\s+$"; "")) | map(select(length > 0));

    .CidrBlock                 = $cidr
  | .CloudProviderRegion       = $region
  | .ReplicationFactor         = $rf
  | .Scaling.InstanceFamilies  = ($families | csv)
  | .AccountCredentialID       = $credential
  | .Nodes                     = $nodes
  | .Scaling.Policies.VCPU.Min = $vcpumin
  | if $azs == "" then . else .AvailabilityZoneIDsOverride = ($azs | csv) end
  ' "$BODY_FILE") || exit 1

CX_ARGS=(
  --cluster-id "$CLUSTER_ID"
  --account-id "$ACCOUNT_ID"
  --user-id "$USER_ID"
  --body "$BODY"
)

fi

# Run it, keeping the (very chatty) output in a log so the ids can be pulled
# back out of it. DC_LOG=path to keep the log somewhere permanent.
LOG=${DC_LOG:-$(mktemp -t add-dc)}
cx sc cluster dc add-dc "${CX_ARGS[@]}" 2>&1 | tee "$LOG"
CX_STATUS=${PIPESTATUS[0]}

# The last human-readable line looks like:
#   Cluster 52447, DC 51538 has status BOOTSTRAP_COMPLETE and is ready for command '...'
DC_ID=$(grep -oE 'DC [0-9]+ has status' "$LOG" | tail -1 | grep -oE '[0-9]+')
DC_STATUS=$(grep -oE 'DC [0-9]+ has status [A-Z_]+' "$LOG" | tail -1 | awk '{print $NF}')
REQUEST_ID=$(grep -oE '"siren\.cluster_request\.id":[0-9]+' "$LOG" | tail -1 | grep -oE '[0-9]+$')

# Scylla DC name: $DC_NAME_BASE from the region table, with _2 appended when
# the cluster already has a DC in that region. Prefer what the log reports;
# fall back to the table. The suffixed form shows up in resource names, so
# look for it explicitly.
DC_NAME=$(grep -oE '"siren\.dc\.name":"[A-Z0-9_]+"' "$LOG" | tail -1 | cut -d'"' -f4)
DC_NAME=${DC_NAME:-$DC_NAME_BASE}
DC_NAME_SUFFIXED=$(grep -oE "${DC_NAME_BASE}_[0-9]+" "$LOG" | sort -u | tail -1)

printf '\n----------------------------------------------------------------\n'
printf 'cluster: %s\n' "$CLUSTER_ID"
printf 'region:  %s (id %s)  cidr %s\n' "$DC_REGION_NAME" "$DC_REGION" "$DC_CIDR"
printf 'request: %s\n' "${REQUEST_ID:-?}"
printf 'dc:      %s %s %s\n' "${DC_ID:-?}" "${DC_NAME:-?}" "${DC_STATUS:+($DC_STATUS)}"
if [[ -n $DC_NAME_SUFFIXED ]]; then
  printf '         (cluster already had a DC in %s; the name may be %s --\n' \
    "$DC_REGION_NAME" "$DC_NAME_SUFFIXED"
  printf '          confirm with ./list_dcs.bash %s after the join)\n' "$CLUSTER_ID"
fi
printf 'log:     %s\n' "$LOG"

if [[ -z $DC_ID ]]; then
  printf '\nno DC id found in the output (add-dc exited %s)\n' "$CX_STATUS" >&2
  exit "${CX_STATUS:-1}"
fi

# ---- join ------------------------------------------------------------------
# Only worth attempting when the add actually succeeded: cx must have exited 0
# and the DC must have reached BOOTSTRAP_COMPLETE, which is the state
# 'cluster dc join' validates for.
if [[ $DC_JOIN != 1 ]]; then
  printf '\nskipping join (DC_JOIN=%s)\n' "$DC_JOIN"
  printf 'next:    %s/join_dc_to_cluster.bash %s %s\n' "$SCRIPT_DIR" "$CLUSTER_ID" "$DC_ID"
  exit "$CX_STATUS"
fi

if [[ $CX_STATUS != 0 ]]; then
  printf '\nnot joining: add-dc exited %s\n' "$CX_STATUS" >&2
  exit "$CX_STATUS"
fi

if [[ $DC_STATUS != BOOTSTRAP_COMPLETE ]]; then
  printf '\nnot joining: DC %s is %s, expected BOOTSTRAP_COMPLETE\n' \
    "$DC_ID" "${DC_STATUS:-unknown}" >&2
  exit 1
fi

printf '\njoining DC %s to cluster %s ...\n\n' "$DC_ID" "$CLUSTER_ID"

JOIN_LOG=$LOG.join
cx sc cluster dc join \
  --cluster-id "$CLUSTER_ID" \
  --cluster-dc-id "$DC_ID" 2>&1 | tee "$JOIN_LOG"
JOIN_STATUS=${PIPESTATUS[0]}

printf '\n----------------------------------------------------------------\n'
if [[ $JOIN_STATUS == 0 ]]; then
  printf 'joined:  DC %s %s -> cluster %s\n' "$DC_ID" "$DC_NAME" "$CLUSTER_ID"
else
  printf 'JOIN FAILED (exit %s) -- the DC exists but is not joined.\n' "$JOIN_STATUS" >&2
  grep -m1 '^Error:' "$JOIN_LOG" >&2
  printf 'retry:   %s/join_dc_to_cluster.bash %s %s\n' "$SCRIPT_DIR" "$CLUSTER_ID" "$DC_ID" >&2
fi
printf 'log:     %s\n' "$JOIN_LOG"
printf 'verify:  %s/list_dcs.bash %s\n' "$SCRIPT_DIR" "$CLUSTER_ID"

exit "$JOIN_STATUS"

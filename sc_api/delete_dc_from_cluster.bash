#!/usr/bin/env bash

# Delete a DC from a ScyllaDB Cloud cluster.
#
#   ./delete_dc_from_cluster.bash <cluster_id> <dc_id> [dc_name]
#
# The DC name is looked up from the cluster when not given, which only works
# for a joined DC (see list_dcs.bash) -- pass it explicitly otherwise, e.g. a
# DC that was added but never joined, or a second DC in a region whose name
# carries a _2 suffix.
#
# This is destructive: -y (or DC_YES=1) skips the confirmation prompt.

API_BASE_URL="https://api.cloud.scylladb.com"
API_TOKEN=${SC_TOKEN}
accountId=${SC_ACCOUNT}

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
YES=${DC_YES:-0}

usage() {
    echo "Usage: $0 cluster_id dc_id [dc_name] [-y]"
    echo "  -y   do not prompt for confirmation"
    exit 1
}

[[ "$1" == -h || "$1" == --help ]] && usage

CLUSTER_ID=
DC_ID=
DC_NAME=
for arg in "$@"; do
    case $arg in
        -y|--yes) YES=1 ;;
        *) if   [[ -z $CLUSTER_ID ]]; then CLUSTER_ID=$arg
           elif [[ -z $DC_ID      ]]; then DC_ID=$arg
           elif [[ -z $DC_NAME    ]]; then DC_NAME=$arg
           else echo "Unexpected argument: $arg" >&2; usage
           fi ;;
    esac
done

[[ -n $CLUSTER_ID && -n $DC_ID ]] || usage

if [[ ! $CLUSTER_ID =~ ^[0-9]+$ ]] || [[ ! $DC_ID =~ ^[0-9]+$ ]];then
    echo "error: cluster_id and dc_id must be numeric" >&2
    usage
fi

# Look the DC up in the cluster: gives us the name when it was not passed, and
# shows what is about to be deleted.
CLUSTER=$(curl -s -X GET "${API_BASE_URL}/account/${accountId}/cluster/${CLUSTER_ID}" \
  -H "Authorization: Bearer ${API_TOKEN}")

DC_JSON=$(jq -c --argjson id "$DC_ID" \
  '(.data.cluster.dataCenters // [])[] | select(.id == $id)' <<<"$CLUSTER")

if [[ -n $DC_JSON ]];then
    FOUND_NAME=$(jq -r .name <<<"$DC_JSON")
    DC_STATUS=$(jq -r .status <<<"$DC_JSON")
    DC_CIDR=$(jq -r .cidrBlock <<<"$DC_JSON")
    if [[ -z $DC_NAME ]];then
        DC_NAME=$FOUND_NAME
    elif [[ $DC_NAME != "$FOUND_NAME" ]];then
        echo "warning: given name '$DC_NAME' but cluster reports '$FOUND_NAME'" >&2
    fi
else
    DC_STATUS="(not listed on the cluster)"
    DC_CIDR="?"
    if [[ -z $DC_NAME ]];then
        echo "error: DC $DC_ID is not listed on cluster $CLUSTER_ID, so its name" >&2
        echo "       cannot be looked up -- pass the dc_name explicitly." >&2
        echo "       ${SCRIPT_DIR}/list_dcs.bash $CLUSTER_ID" >&2
        exit 1
    fi
fi

CLUSTER_NAME=$(jq -r '.data.cluster.clusterName // "?"' <<<"$CLUSTER")
DC_COUNT=$(jq '(.data.cluster.dataCenters // []) | length' <<<"$CLUSTER")

printf 'About to DELETE a data center:\n\n'
printf '  cluster: %s %s\n' "$CLUSTER_ID" "$CLUSTER_NAME"
printf '  dc:      %s %s\n' "$DC_ID" "$DC_NAME"
printf '  status:  %s\n' "$DC_STATUS"
printf '  cidr:    %s\n' "$DC_CIDR"
printf '  cluster currently has %s joined DC(s)\n\n' "$DC_COUNT"

if [[ $DC_COUNT == 1 && -n $DC_JSON ]];then
    printf 'NOTE: this is the only joined DC on the cluster.\n\n'
fi

if [[ $YES != 1 ]];then
    if [[ ! -t 0 ]];then
        echo "error: not a terminal; re-run with -y to confirm" >&2
        exit 1
    fi
    read -r -p "Type the DC id ($DC_ID) to confirm: " reply
    if [[ $reply != "$DC_ID" ]];then
        echo "aborted"
        exit 1
    fi
fi

printf "Running: sc cluster manager pause-all-jobs --cluster-id %s --reason \"DC Removal\" \n" $CLUSTER_ID
sc cluster manager pause-all-jobs --cluster-id $CLUSTER_ID --reason "DC Removal"

cx sc cluster dc delete \
  --cluster-id "$CLUSTER_ID" \
  --dc-name "$DC_NAME" \
  --cluster-dc-id "$DC_ID"

if [[ $? == 0 ]]; then
    printf "Successful dc removal \n"
    printf "Running: sc cluster manager resume-all-jobs --cluster-id %s \n", $CLUSTER_ID
    sc cluster manager resume-all-jobs --cluster-id $CLUSTER_ID
else
    printf "Error detected not running manager job resume \n"
fi

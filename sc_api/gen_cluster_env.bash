#!/usr/bin/env bash

# Generate a shell env file for connecting to a ScyllaDB Cloud cluster DC.
#
#   ./gen_cluster_env.bash -c <cluster_id> [dc_name|dc_id] [-o outfile]
#   ./gen_cluster_env.bash <cluster_id> [dc_name|dc_id] [-o outfile]
#
# Emits USERNAME / PASSWORD / DC / CONTACT_POINTS, e.g.
#
#   export USERNAME='scylla'
#   export PASSWORD='...'
#   export DC='GCE_US_WEST_1'
#   export CONTACT_POINTS='node-0.gce-us-west-1.<hash>.clusters.scylla.cloud,node-1...'
#
# Source it with: . cluster-<id>.env
#
# CONTACT_POINTS is comma-separated -- the -s/--hosts format every sample app
# takes, and what they fall back to when -s is not given. (A bash array cannot
# be exported, so a list would not reach a child process.) For an array in an
# interactive shell: IFS=, read -ra NODES <<<"$CONTACT_POINTS"
#
# Everything comes from the ScyllaDB Cloud REST API:
#
#   GET /account/<acct>/cluster/<id>                 -> dataCenters[].name, .id
#   GET /account/<acct>/cluster/<id>/nodes           -> nodes[].dns, .dcId
#   GET /account/<acct>/cluster/connect?clusterId=<id>
#                                                    -> credentials.username,
#                                                       .password
#   The DC name is read, not derived from the node hostname: a second DC in
#   the same region is named GCE_US_CENTRAL_1_2, which no hostname transform
#   would produce. The connect endpoint is what the portal's Connect tab and
#   the Terraform scylladbcloud_cql_auth data source use; it returns the
#   customer CQL user, not the internal role 'cx describe cluster' shows.
#
# Needs SC_TOKEN and SC_ACCOUNT. The file holds a password, so it is written
# mode 600.

API_BASE_URL="https://api.cloud.scylladb.com"
API_TOKEN=${SC_TOKEN}
accountId=${SC_ACCOUNT}

usage() {
    echo "Usage: $0 -c cluster_id [dc_name|dc_id] [-o outfile]"
    echo "       $0 cluster_id [dc_name|dc_id] [-o outfile]"
    echo "  -c, --cluster  cluster ID (or give it as the first positional argument)"
    echo "  dc             defaults to the lowest DC id on the cluster"
    echo "  -o             output file (default: cluster-<cluster_id>.env)"
    exit 1
}

[[ "$1" == '' ]] && usage

CLUSTER_ID=
DC_WANTED=
OUTFILE=

# Positional arguments are cluster_id then dc, or just dc when -c gives the cluster
POSITIONAL=()
while (($# > 0)); do
    case $1 in
        -h|--help) usage ;;
        -c|--cluster) [[ -n $2 ]] || { echo "error: $1 requires a cluster ID" >&2; usage; }
                      CLUSTER_ID=$2; shift 2 ;;
        -o) [[ -n $2 ]] || { echo "error: -o requires a file name" >&2; usage; }
            OUTFILE=$2; shift 2 ;;
        -*) echo "Unknown option: $1" >&2; usage ;;
        *)  POSITIONAL+=("$1"); shift ;;
    esac
done

[[ -z $CLUSTER_ID ]] && { CLUSTER_ID=${POSITIONAL[0]}; POSITIONAL=("${POSITIONAL[@]:1}"); }
DC_WANTED=${POSITIONAL[0]}
((${#POSITIONAL[@]} > 1)) && { echo "Unexpected argument: ${POSITIONAL[1]}" >&2; usage; }

[[ -n $CLUSTER_ID ]] || { echo "error: cluster_id is required" >&2; usage; }

[[ $CLUSTER_ID =~ ^[0-9]+$ ]] || { echo "error: cluster_id must be numeric" >&2; usage; }
[[ -n $API_TOKEN  ]] || { echo "error: SC_TOKEN is not set" >&2; exit 1; }
[[ -n $accountId  ]] || { echo "error: SC_ACCOUNT is not set" >&2; exit 1; }
OUTFILE=${OUTFILE:-cluster-${CLUSTER_ID}.env}

# ---- REST: cluster + nodes -------------------------------------------------
CLUSTER=$(curl -s -X GET "${API_BASE_URL}/account/${accountId}/cluster/${CLUSTER_ID}" \
  -H "Authorization: Bearer ${API_TOKEN}")
NODES_JSON=$(curl -s -X GET "${API_BASE_URL}/account/${accountId}/cluster/${CLUSTER_ID}/nodes" \
  -H "Authorization: Bearer ${API_TOKEN}")

if [[ $(jq -r '.data.cluster.id // empty' <<<"$CLUSTER") != "$CLUSTER_ID" ]];then
    echo "error: could not read cluster ${CLUSTER_ID}" >&2
    jq -c . <<<"$CLUSTER" >&2
    exit 1
fi

CLUSTER_NAME=$(jq -r '.data.cluster.clusterName // "?"' <<<"$CLUSTER")

# Pick the DC by name, by id, or default to the lowest DC id -- numerically,
# which is the cluster's original DC. The API returns them in neither id nor
# name order, so sort explicitly.
DC_JSON=$(jq -c --arg want "$DC_WANTED" '
  (.data.cluster.dataCenters // []) as $dcs
  | if $want == "" then ($dcs | sort_by(.id) | .[0])
    else ($dcs[] | select((.name == $want) or ((.id|tostring) == $want)))
    end' <<<"$CLUSTER")

if [[ -z $DC_JSON || $DC_JSON == null ]];then
    echo "error: no such DC '${DC_WANTED}' on cluster ${CLUSTER_ID}" >&2
    echo "available:" >&2
    jq -r '(.data.cluster.dataCenters // [])[] | "  \(.id)  \(.name)"' <<<"$CLUSTER" >&2
    exit 1
fi

DC_ID=$(jq -r .id <<<"$DC_JSON")
DC_NAME=$(jq -r .name <<<"$DC_JSON")
DC_STATUS=$(jq -r .status <<<"$DC_JSON")

mapfile -t NODE_DNS < <(jq -r --argjson dc "$DC_ID" \
  '(.data.nodes // [])[] | select(.dcId == $dc and .dns != null) | .dns' <<<"$NODES_JSON" | sort)

if ((${#NODE_DNS[@]} == 0));then
    echo "error: no node DNS names for DC ${DC_ID} (${DC_NAME}, ${DC_STATUS})" >&2
    echo "       nodes may still be provisioning, or the cluster has no DNS" >&2
    exit 1
fi

# ---- REST: the CQL credentials ---------------------------------------------
CONNECT=$(curl -s -X GET "${API_BASE_URL}/account/${accountId}/cluster/connect?clusterId=${CLUSTER_ID}" \
  -H "Authorization: Bearer ${API_TOKEN}")

USERNAME=$(jq -r '.data.credentials.username // empty' <<<"$CONNECT")
PASSWORD=$(jq -r '.data.credentials.password // empty' <<<"$CONNECT")

if [[ -z $USERNAME || -z $PASSWORD ]];then
    echo "error: could not read the CQL username/password from" >&2
    echo "       GET /account/${accountId}/cluster/connect?clusterId=${CLUSTER_ID}" >&2
    jq -c 'del(.data.credentials)' <<<"$CONNECT" >&2
    exit 1
fi

# Single-quote for the shell: ' becomes '\''
q() { printf "'%s'" "${1//\'/\'\\\'\'}"; }

{
  printf '# %s -- cluster %s (%s), DC %s (%s)\n' \
    "$OUTFILE" "$CLUSTER_ID" "$CLUSTER_NAME" "$DC_NAME" "$DC_ID"
  printf '# generated by %s\n' "$(basename "$0")"
  printf 'export USERNAME=%s\n' "$(q "$USERNAME")"
  printf 'export PASSWORD=%s\n' "$(q "$PASSWORD")"
  printf 'export DC=%s\n' "$(q "$DC_NAME")"
  printf 'export CONTACT_POINTS=%s\n' "$(q "$(IFS=,; echo "${NODE_DNS[*]}")")"
} > "$OUTFILE"

chmod 600 "$OUTFILE"

printf 'wrote %s\n' "$OUTFILE"
printf '  cluster: %s %s  dc: %s (%s, %s)  user: %s  nodes: %s\n' \
  "$CLUSTER_ID" "$CLUSTER_NAME" "$DC_NAME" "$DC_ID" "$DC_STATUS" "$USERNAME" "${#NODE_DNS[@]}"
printf '  source it with: . %s\n' "$OUTFILE"

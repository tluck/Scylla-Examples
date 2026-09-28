#!/usr/bin/env bash
#
# clusters.sh - build the four ScyllaDB X Cloud permutations with Terraform:
#
#     aws-scylla   AWS, ScyllaDB Cloud's own account
#     aws-byoa     AWS, your own account (BYOA)
#     gcp-scylla   GCP, ScyllaDB Cloud's own project
#     gcp-byoa     GCP, your own project (BYOA)
#
# Each permutation gets its own Terraform workspace, so the four states stay
# independent and can be applied or destroyed individually.
#
# Usage:
#   ./clusters.sh <action> [-p aws|gcp] [-o byoa|scylla] [options]
#
# Actions: plan | apply | destroy | output | status
#
# Selection (flags match scylladb_cloud_cli.py; omit an axis to mean "both"):
#   -p, --cloud aws|gcp      restrict to one cloud        (default: both)
#   -o, --owner byoa|scylla  restrict to one account type (default: both)
#
#   ./clusters.sh apply                     # all four permutations
#   ./clusters.sh apply -p aws              # both AWS permutations
#   ./clusters.sh apply -o byoa             # both BYOA permutations
#   ./clusters.sh apply -p aws -o byoa      # exactly one
#   ./clusters.sh status                    # what each workspace tracks
#   ./clusters.sh output -p gcp -o scylla   # one cluster's outputs
#   ./clusters.sh destroy                   # tear all four down
#
# Overrides (also match the CLI; otherwise taken from config.env):
#   -l, --name PREFIX        cluster name prefix
#   -r, --region REGION      region for the selected cloud
#   -v, --vcpu N             xcloud vCPU minimum
#   -t, --tib N              xcloud storage minimum, in TiB
#   -F, --instance-family F  instance family to scale within
#   -i, --cidr CIDR          cluster VPC CIDR (only with a single target)
#
# Other:
#   -S, --serial             run one at a time instead of in parallel
#   -y, --yes                skip the confirmation prompt
#   -h, --help               this text
#
# Configuration comes from the environment. See config.env.example.

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"
ROOT=$PWD
LOGDIR=${LOGDIR:-$ROOT/.logs}

ALL_TARGETS=(aws-scylla aws-byoa gcp-scylla gcp-byoa)

# ---------------------------------------------------------------- configuration

# Load config.env if present, so values can live in a file instead of the shell.
# shellcheck disable=SC1091
[ -f "$ROOT/config.env" ] && . "$ROOT/config.env"

: "${SCYLLADB_CLOUD_TOKEN:=${SC_TOKEN:-}}"
export SCYLLADB_CLOUD_TOKEN

# Cluster naming and placement
CLUSTER_PREFIX=${CLUSTER_PREFIX:-tf-demo}
AWS_REGION=${AWS_REGION:-us-east-1}
GCP_REGION=${GCP_REGION:-us-central1}
GCP_PROJECT_ID=${GCP_PROJECT_ID:-}

# BYOA credential IDs. Find them with:
#   curl -s -H "Authorization: Bearer $SCYLLADB_CLOUD_TOKEN" \
#     https://api.cloud.scylladb.com/account/<accountId>/cloud-account
AWS_BYOA_ID=${AWS_BYOA_ID:-}
GCP_BYOA_ID=${GCP_BYOA_ID:-}

# X Cloud autoscaling policy
AWS_INSTANCE_FAMILY=${AWS_INSTANCE_FAMILY:-i8g}
GCP_INSTANCE_FAMILY=${GCP_INSTANCE_FAMILY:-n2-highmem}
STORAGE_MIN_GB=${STORAGE_MIN_GB:-500}
STORAGE_TARGET_UTIL=${STORAGE_TARGET_UTIL:-0.8}
VCPU_MIN=${VCPU_MIN:-6}

# AWS client networking. A route table holds only one route per prefix, so the
# two AWS clusters need distinct CIDRs when peering to the same client network.
AWS_CONNECTION_TYPE=${AWS_CONNECTION_TYPE:-vpc_peering}   # vpc_peering|transit_gateway|none
AWS_USE_DEFAULT_VPC=${AWS_USE_DEFAULT_VPC:-true}
AWS_SCYLLA_CIDR=${AWS_SCYLLA_CIDR:-172.28.0.0/24}
AWS_BYOA_CIDR=${AWS_BYOA_CIDR:-172.27.0.0/24}
GCP_SCYLLA_CIDR=${GCP_SCYLLA_CIDR:-172.28.0.0/24}
GCP_BYOA_CIDR=${GCP_BYOA_CIDR:-172.27.0.0/24}

# ---------------------------------------------------------------------- helpers

RED=$'\033[31m'; GRN=$'\033[32m'; YLW=$'\033[33m'; BLD=$'\033[1m'; RST=$'\033[0m'
[ -t 1 ] || { RED=; GRN=; YLW=; BLD=; RST=; }

info() { printf '%s==>%s %s\n' "$BLD" "$RST" "$*"; }
warn() { printf '%s==> %s%s\n' "$YLW" "$*" "$RST" >&2; }
die()  { printf '%serror:%s %s\n' "$RED" "$RST" "$*" >&2; exit 1; }

usage() { sed -n '2,/^set -euo/p' "$0" | sed 's/^# \{0,1\}//; $d'; exit "${1:-0}"; }

dir_for()  { case $1 in aws-*) echo aws ;; gcp-*) echo gcp ;; esac; }
kind_for() { case $1 in *-scylla) echo scylla ;; *-byoa) echo byoa ;; esac; }

# Write a JSON tfvars file for one permutation and echo its path.
#
# A generated file is used rather than a list of -var flags for two reasons:
# an explicit -var-file outranks the auto-loaded terraform.tfvars (so a stale
# byoa_id there cannot leak into the ScyllaDB-owned permutations), and JSON can
# express a real null, which "-var byoa_id=null" cannot - that passes the
# string "null" and fails to convert to a number.
vars_file_for() {
  local target=$1 kind dir out
  kind=$(kind_for "$target"); dir=$(dir_for "$target")
  out="$LOGDIR/$target.tfvars.json"

  local byoa=null cidr family region extra=""
  case $target in
    aws-scylla) cidr=$AWS_SCYLLA_CIDR ;;
    aws-byoa)   cidr=$AWS_BYOA_CIDR; byoa=$AWS_BYOA_ID ;;
    gcp-scylla) cidr=$GCP_SCYLLA_CIDR ;;
    gcp-byoa)   cidr=$GCP_BYOA_CIDR; byoa=$GCP_BYOA_ID ;;
  esac

  if [ "$dir" = aws ]; then
    region=$AWS_REGION; family=$AWS_INSTANCE_FAMILY
    extra="\"connection_type\": \"$AWS_CONNECTION_TYPE\","
    [ "$AWS_CONNECTION_TYPE" = vpc_peering ] &&
      extra="$extra \"use_default_vpc\": $AWS_USE_DEFAULT_VPC,"
  else
    region=$GCP_REGION; family=$GCP_INSTANCE_FAMILY
    extra="\"gcp_project_id\": \"$GCP_PROJECT_ID\","
  fi

  cat >"$out" <<JSON
{
  "cluster_name": "$CLUSTER_PREFIX-$target",
  "region": "$region",
  "byoa_id": $byoa,
  "cluster_vpc_cidr": "$cidr",
  $extra
  "xcloud_instance_families": ["$family"],
  "xcloud_storage_min_gb": $STORAGE_MIN_GB,
  "xcloud_storage_target_utilization": $STORAGE_TARGET_UTIL,
  "xcloud_vcpu_min": $VCPU_MIN
}
JSON
  echo "$out"
}

preflight() {
  command -v terraform >/dev/null || die "terraform not found in PATH"
  [ -n "$SCYLLADB_CLOUD_TOKEN" ] ||
    die "SCYLLADB_CLOUD_TOKEN is not set. Export it, or 'source ./env.sh', or put it in config.env"

  local t
  for t in "$@"; do
    case $t in
      *-byoa)
        local var=AWS_BYOA_ID; [ "$(dir_for "$t")" = gcp ] && var=GCP_BYOA_ID
        [ -n "${!var}" ] || die "$t needs $var set (see config.env.example)"
        ;;
    esac
    [ "$(dir_for "$t")" = gcp ] && [ -z "$GCP_PROJECT_ID" ] &&
      die "$t needs GCP_PROJECT_ID set"
  done
  return 0
}

# terraform init once per directory
init_dir() {
  local d=$1
  [ -d "$ROOT/$d/.terraform" ] || { info "terraform init ($d)"; terraform -chdir="$ROOT/$d" init -input=false >/dev/null; }
}

run_one() {
  local action=$1 target=$2
  local d; d=$(dir_for "$target")
  local log="$LOGDIR/$target.$action.log"
  local extra=""; [ "$action" = apply ] || [ "$action" = destroy ] && extra="-auto-approve"

  # Workspaces isolate state. TF_WORKSPACE is used instead of
  # 'terraform workspace select' so parallel runs in one directory do not
  # race on the shared .terraform/environment file.
  terraform -chdir="$ROOT/$d" workspace new "$target" >/dev/null 2>&1 || true

  local vf; vf=$(vars_file_for "$target")

  # shellcheck disable=SC2086
  if TF_WORKSPACE=$target terraform -chdir="$ROOT/$d" "$action" \
       -no-color -input=false $extra -var-file="$vf" >"$log" 2>&1; then
    printf '%s  ok%s      %-12s %s\n' "$GRN" "$RST" "$target" "$(summarise "$log")"
  else
    printf '%s  FAILED%s  %-12s see %s\n' "$RED" "$RST" "$target" "$log"
    grep -E '^(Error|│ Error)' "$log" | head -3 | sed 's/^/           /'
    return 1
  fi
}

summarise() {
  grep -oE '(Apply|Destroy|Plan): [0-9].*' "$1" | tail -1 || echo done
}

# ------------------------------------------------------------------------- main

ACTION=""; SERIAL=false; ASSUME_YES=false
OPT_CLOUD=""; OPT_OWNER=""; OPT_CIDR=""

need_val() { [ -n "${2:-}" ] || die "$1 requires a value"; }

while [ $# -gt 0 ]; do
  case $1 in
    -h|--help)   usage 0 ;;
    -S|--serial) SERIAL=true ;;
    -y|--yes)    ASSUME_YES=true ;;
    -p|--cloud)  need_val "$1" "${2:-}"; OPT_CLOUD=$(printf '%s' "$2" | tr 'A-Z' 'a-z'); shift ;;
    -o|--owner)  need_val "$1" "${2:-}"; OPT_OWNER=$(printf '%s' "$2" | tr 'A-Z' 'a-z'); shift ;;
    -l|--name)   need_val "$1" "${2:-}"; CLUSTER_PREFIX=$2; shift ;;
    -r|--region) need_val "$1" "${2:-}"; OPT_REGION=$2; shift ;;
    -v|--vcpu)   need_val "$1" "${2:-}"; VCPU_MIN=$2; shift ;;
    -t|--tib)    need_val "$1" "${2:-}"; STORAGE_MIN_GB=$(( $2 * 1024 )); shift ;;
    -F|--instance-family) need_val "$1" "${2:-}"; OPT_FAMILY=$2; shift ;;
    -i|--cidr)   need_val "$1" "${2:-}"; OPT_CIDR=$2; shift ;;
    plan|apply|destroy|output|status) ACTION=$1 ;;
    -*) die "unknown option: $1 (try --help)" ;;
    *)  die "unknown argument: $1 (try --help)" ;;
  esac
  shift
done

case $OPT_CLOUD in ""|aws|gcp) ;; *) die "-p must be aws or gcp, got '$OPT_CLOUD'" ;; esac
case $OPT_OWNER in ""|byoa|scylla) ;; *) die "-o must be byoa or scylla, got '$OPT_OWNER'" ;; esac

# Build the target set from the two axes; an empty axis means "both".
TARGETS=()
for t in "${ALL_TARGETS[@]}"; do
  [ -n "$OPT_CLOUD" ] && [ "$(dir_for  "$t")" != "$OPT_CLOUD" ] && continue
  [ -n "$OPT_OWNER" ] && [ "$(kind_for "$t")" != "$OPT_OWNER" ] && continue
  TARGETS+=("$t")
done
[ ${#TARGETS[@]} -gt 0 ] || die "no permutation matches -p '$OPT_CLOUD' -o '$OPT_OWNER'"

# A single CIDR cannot sensibly apply to several clusters that peer into the
# same network - one route table holds one route per prefix.
[ -n "$OPT_CIDR" ] && [ ${#TARGETS[@]} -gt 1 ] &&
  die "-i/--cidr needs a single target; narrow it with -p and -o"

# Region/family/cidr overrides apply to whichever cloud is selected.
if [ -n "${OPT_REGION:-}" ]; then AWS_REGION=$OPT_REGION; GCP_REGION=$OPT_REGION; fi
if [ -n "${OPT_FAMILY:-}" ]; then AWS_INSTANCE_FAMILY=$OPT_FAMILY; GCP_INSTANCE_FAMILY=$OPT_FAMILY; fi
if [ -n "$OPT_CIDR" ]; then
  case ${TARGETS[0]} in
    aws-scylla) AWS_SCYLLA_CIDR=$OPT_CIDR ;; aws-byoa) AWS_BYOA_CIDR=$OPT_CIDR ;;
    gcp-scylla) GCP_SCYLLA_CIDR=$OPT_CIDR ;; gcp-byoa) GCP_BYOA_CIDR=$OPT_CIDR ;;
  esac
fi

[ -n "$ACTION" ] || usage 1
mkdir -p "$LOGDIR"

if [ "$ACTION" = status ]; then
  for t in "${TARGETS[@]}"; do
    d=$(dir_for "$t")
    n=$(TF_WORKSPACE=$t terraform -chdir="$ROOT/$d" state list 2>/dev/null | wc -l | tr -d ' ')
    printf '  %-12s %s resource(s)\n' "$t" "$n"
  done
  exit 0
fi

if [ "$ACTION" = output ]; then
  for t in "${TARGETS[@]}"; do
    d=$(dir_for "$t")
    printf '%s=== %s%s\n' "$BLD" "$t" "$RST"
    TF_WORKSPACE=$t terraform -chdir="$ROOT/$d" output 2>/dev/null || echo "  (no state)"
  done
  exit 0
fi

preflight "${TARGETS[@]}"

if [ "$ACTION" != plan ] && [ "$ASSUME_YES" = false ]; then
  printf '%s%s%s these clusters: %s\n' "$BLD" "$ACTION" "$RST" "${TARGETS[*]}"
  [ "$ACTION" = destroy ] && warn "this permanently deletes the clusters and their data"
  read -r -p "continue? [y/N] " reply
  case $reply in [yY]*) ;; *) die "aborted" ;; esac
fi

for d in $(for t in "${TARGETS[@]}"; do dir_for "$t"; done | sort -u); do init_dir "$d"; done

info "$ACTION: ${TARGETS[*]}   (logs in $LOGDIR)"
START=$SECONDS
rc=0

if [ "$SERIAL" = true ]; then
  for t in "${TARGETS[@]}"; do run_one "$ACTION" "$t" || rc=1; done
else
  pids=()
  for t in "${TARGETS[@]}"; do run_one "$ACTION" "$t" & pids+=("$!"); done
  for p in "${pids[@]}"; do wait "$p" || rc=1; done
fi

info "finished in $((SECONDS - START))s"
[ "$rc" -eq 0 ] || die "one or more targets failed"

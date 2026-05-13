#!/usr/bin/env bash
set -euo pipefail

# ScyllaDB Cloud API: create firewall allowed rules (one POST per CIDR).
# Ref: https://cloud.docs.scylladb.com/stable/api.html#tag/Account-Cluster-Network/operation/createFirewallAllowedRule
# OpenAPI: POST .../network/firewall/allowed  body: { "ipAddress": "<CIDR or IP>" }

API_BASE_URL="https://api.cloud.scylladb.com"
API_TOKEN="${SC_TOKEN:-}"
accountId="${SC_ACCOUNT:-}"

usage() {
  printf 'Usage: SC_TOKEN=... SC_ACCOUNT=... %s <clusterId> [cidr ...]\n' "$(basename "$0")" >&2
  printf '  POSTs one firewall allowed rule per CIDR to network/firewall/allowed.\n' >&2
  printf '  With no cidrs after clusterId, uses defaults (two rules).\n' >&2
  exit "${1:-0}"
}

[[ "${1:-}" == -h ]] || [[ "${1:-}" == --help ]] && usage 0

[[ -n "$API_TOKEN" ]] || { echo "error: SC_TOKEN is not set" >&2; exit 1; }
[[ -n "$accountId" ]] || { echo "error: SC_ACCOUNT is not set" >&2; exit 1; }

clusterId="${1:-}"
[[ -n "$clusterId" ]] || { echo "error: cluster id required (first argument)" >&2; usage 1; }
shift

if (($# > 0)); then
  cidrs=("$@")
else
  cidrs=("10.138.0.0/20")
fi

url="${API_BASE_URL}/account/${accountId}/cluster/${clusterId}/network/firewall/allowed"
body_file=$(mktemp)
trap 'rm -f "$body_file"' EXIT

for cidr in "${cidrs[@]}"; do
  printf 'Setting allowed CIDR: %s\n' "$cidr"
  payload=$(jq -n --arg ip "$cidr" '{ipAddress: $ip}')
  http_code=$(curl -sS -o "$body_file" -w '%{http_code}' -X POST "$url" \
    -H "Authorization: Bearer ${API_TOKEN}" \
    -H 'Content-Type: application/json' \
    -d "$payload")
  body=$(cat "$body_file")

  if [[ "$http_code" != 2* ]]; then
    printf 'error: HTTP %s for %s\n' "$http_code" "$cidr" >&2
    [[ -n "$body" ]] && printf '%s\n' "$body" >&2
    exit 1
  fi

  if [[ -z "$body" ]]; then
    printf 'ok: %s (HTTP %s, empty body)\n' "$cidr" "$http_code"
    continue
  fi

  if jq -e . >/dev/null 2>&1 <<<"$body"; then
    printf 'ok: %s\n' "$cidr"
    jq . <<<"$body"
  else
    printf 'warning: HTTP %s for %s but response is not JSON:\n%s\n' "$http_code" "$cidr" "$body" >&2
    exit 1
  fi
done

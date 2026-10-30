# Shared settings for the sample apps (run_sample_go.bash, run_sample_python.bash, run_sample_java.bash,
# run_benchmark_psc.bash). Sourced by those scripts, do not run it.
#
# Two connection models, picked by SCYLLA_MODE (or inferred: a client route ID means psc):
#   psc     through a GCP Private Service Connect endpoint, with client routes
#   direct  straight to the nodes, e.g. over VPC peering (no client routes, shard-aware ports reachable)
#
# Per-connection values come from an env file, so one file per connection switches every sample at once:
#   PSC_ENV=psc-tls-54793.env ./run_sample_java.bash     # explicit file
#   ./run_sample_python.bash                             # ./psc.env if it exists
# psc_generate_client.bash writes psc-<connection_name>.env for each PSC connection and direct_env.bash writes
# direct-<cluster_id>.env / direct-tls-<cluster_id>.env (no password). Values already set in the environment
# win over the file.
#
# Credentials come from the environment - never put them in a version-controlled file:
#   export SCYLLA_PASSWORD='...'
#   or: source ../../sc_api/cluster-<id>.env       # USERNAME/PASSWORD/DC are used as fallbacks
#
# Variables (all samples read the same names):
#   SCYLLA_MODE           psc | direct (default: psc when SCYLLA_PSC_CONN_ID is set, else direct)
#   SCYLLA_PSC_DNS        psc: PSC endpoint DNS name (required)
#   SCYLLA_PSC_CONN_ID    psc: client route ID(s), comma separated (required)
#   SCYLLA_CONTACT_POINTS direct: node addresses, comma separated (default CONTACT_POINTS)
#   SCYLLA_PORT           psc: discovery port 9000-9002 (default 9000, or SCYLLA_PSC_PORT)
#                         direct: CQL port; each sample picks its default (9042/9142, Java 19042/19142)
#   SCYLLA_USER           default USERNAME, then scylla
#   SCYLLA_PASSWORD       default PASSWORD (required)
#   SCYLLA_DC             local datacenter, default DC (required by Java, optional elsewhere)
#   SCYLLA_CA_FILE        cluster CA (PEM); set = TLS. Only for a TLS PSC connection
#   SCYLLA_TLS_NAME       cluster TLS name: SNI, and verified by Go and Java (Python: SNI only)

sample_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

psc_env_file=${PSC_ENV:-${sample_dir}/psc.env}
if [[ -f ${psc_env_file} ]]; then
  # keep values that are already set in the environment, but say so: a stale export is easy to miss
  while IFS='=' read -r key value; do
    [[ ${key} =~ ^[A-Z_][A-Z0-9_]*$ ]] || continue
    value=${value%\"}; value=${value#\"}; value=${value%\'}; value=${value#\'}
    if [[ -z ${!key:-} ]]; then
      export "${key}=${value}"
    elif [[ ${!key} != "${value}" ]]; then
      echo "note: ${key}=${!key} from the environment overrides ${value} in ${psc_env_file##*/}" \
        "(unset ${key} to use the file)" >&2
    fi
  done < <(sed -e 's/^export //' -e 's/[[:space:]]*#.*$//' "${psc_env_file}")
  # a relative CA path is relative to the env file
  if [[ -n ${SCYLLA_CA_FILE:-} && ${SCYLLA_CA_FILE} != /* ]]; then
    SCYLLA_CA_FILE="$(cd "$(dirname "${psc_env_file}")" && pwd)/${SCYLLA_CA_FILE}"
  fi
elif [[ -n ${PSC_ENV:-} ]]; then
  echo "ERROR: PSC_ENV=${PSC_ENV} not found" >&2
  exit 1
fi

export SCYLLA_USER="${SCYLLA_USER:-${USERNAME:-scylla}}"
export SCYLLA_PASSWORD="${SCYLLA_PASSWORD:-${PASSWORD:-}}"
export SCYLLA_DC="${SCYLLA_DC:-${DC:-}}"
export SCYLLA_CA_FILE="${SCYLLA_CA_FILE:-}"
export SCYLLA_TLS_NAME="${SCYLLA_TLS_NAME:-}"
: "${SCYLLA_PASSWORD:?set SCYLLA_PASSWORD (see psc_env.bash)}"

if [[ -z ${SCYLLA_MODE:-} ]]; then
  [[ -n ${SCYLLA_PSC_CONN_ID:-} ]] && SCYLLA_MODE=psc || SCYLLA_MODE=direct
fi
export SCYLLA_MODE
case ${SCYLLA_MODE} in
  psc)
    : "${SCYLLA_PSC_DNS:?set SCYLLA_PSC_DNS or use a PSC_ENV file (see psc_env.bash)}"
    : "${SCYLLA_PSC_CONN_ID:?set SCYLLA_PSC_CONN_ID or use a PSC_ENV file (see psc_env.bash)}"
    export SCYLLA_PSC_DNS SCYLLA_PSC_CONN_ID
    export SCYLLA_PORT="${SCYLLA_PORT:-${SCYLLA_PSC_PORT:-9000}}"
    target="PSC ${SCYLLA_PSC_DNS}:${SCYLLA_PORT} route ${SCYLLA_PSC_CONN_ID}"
    ;;
  direct)
    export SCYLLA_CONTACT_POINTS="${SCYLLA_CONTACT_POINTS:-${CONTACT_POINTS:-}}"
    : "${SCYLLA_CONTACT_POINTS:?set SCYLLA_CONTACT_POINTS or use a direct env file (see psc_env.bash)}"
    # the connection IDs must not leak into direct mode: the samples take a set route ID as psc
    unset SCYLLA_PSC_DNS SCYLLA_PSC_CONN_ID
    export SCYLLA_PORT="${SCYLLA_PORT:-}"
    target="direct ${SCYLLA_CONTACT_POINTS}${SCYLLA_PORT:+ port ${SCYLLA_PORT}}"
    ;;
  *)
    echo "ERROR: SCYLLA_MODE must be psc or direct, not ${SCYLLA_MODE}" >&2
    exit 1
    ;;
esac

if [[ -n ${SCYLLA_CA_FILE} && ! -f ${SCYLLA_CA_FILE} ]]; then
  echo "ERROR: SCYLLA_CA_FILE ${SCYLLA_CA_FILE} not found" >&2
  exit 1
fi

echo "${target}${SCYLLA_DC:+ dc ${SCYLLA_DC}}" \
  "${SCYLLA_CA_FILE:+TLS (CA ${SCYLLA_CA_FILE}${SCYLLA_TLS_NAME:+, name ${SCYLLA_TLS_NAME}})}" \
  "${psc_env_file:+$([[ -f ${psc_env_file} ]] && echo "[${psc_env_file##*/}]")}" >&2

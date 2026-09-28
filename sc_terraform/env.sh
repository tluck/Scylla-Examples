# Source this before running terraform:  source ./env.sh
#
# The ScyllaDB Cloud provider reads the API token from SCYLLADB_CLOUD_TOKEN.
# If it is already exported, this is a no-op. Otherwise it falls back to
# SC_TOKEN, which is a common name to keep the token in ~/.bash_profile.
#
# Get a token: https://cloud.docs.scylladb.com/stable/api-docs/api-get-started.html

if [ -z "$SCYLLADB_CLOUD_TOKEN" ]; then
  if [ -n "$SC_TOKEN" ]; then
    export SCYLLADB_CLOUD_TOKEN="$SC_TOKEN"
  else
    echo "env.sh: set SCYLLADB_CLOUD_TOKEN (or SC_TOKEN) to your ScyllaDB Cloud API token" >&2
    return 1 2>/dev/null || exit 1
  fi
fi

echo "SCYLLADB_CLOUD_TOKEN set (${#SCYLLADB_CLOUD_TOKEN} chars)"

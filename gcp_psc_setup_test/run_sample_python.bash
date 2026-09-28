# Source or run with bash:  source ./run_sample_python.bash  |  bash run_sample_python.bash
#
# Credentials come from the environment - never hardcode them here, this file
# is version controlled. Export them first, e.g. from a cluster env file:
#
#   source ../sc_api/cluster-<id>.env     # sets USERNAME/PASSWORD
#   export SCYLLA_USER="$USERNAME" SCYLLA_PASSWORD="$PASSWORD"
#
# or set them directly:
#
#   export SCYLLA_PASSWORD='...'

export SCYLLA_PSC_DNS="${SCYLLA_PSC_DNS:-scylla-psc-nr67594-df67f06f.clusters.scylla.cloud}"
export SCYLLA_PSC_CONN_ID="${SCYLLA_PSC_CONN_ID:-dfb43f71-ee86-51a8-b18d-0b59023aef17}"

export SCYLLA_USER="${SCYLLA_USER:-scylla}"
: "${SCYLLA_PASSWORD:?set SCYLLA_PASSWORD before running (see the comments at the top of this file)}"
export SCYLLA_PASSWORD
export SCYLLA_DC="${SCYLLA_DC:-GCE_US_WEST_1}"

python3 sample_python_psc.py "$@"

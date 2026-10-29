#!/usr/bin/env bash
#
# Runs sample_python_psc.py. Settings and credentials: see psc_env.bash.
#
#   PSC_ENV=psc-<connection_name>.env ./run_sample_python.bash [options]

set -euo pipefail
cd "$(dirname "$0")"
source ./psc_env.bash

python3 sample_python_psc.py "$@"

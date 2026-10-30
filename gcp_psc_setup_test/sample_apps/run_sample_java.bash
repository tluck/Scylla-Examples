#!/usr/bin/env bash
#
# Runs the Java sample (java-psc/, driver 4.19.2.x) with its per-node shard-awareness check.
# Settings and credentials: see psc_env.bash. SCYLLA_DC is required here.
#
#   PSC_ENV=psc-<connection_name>.env ./run_sample_java.bash [options]
#   ... ./run_sample_java.bash --shard-aware false        # compare without shard-aware connections
#
# Build the jar first (needs Maven and JDK 11+; the VM only needs a JRE 11+), then rsync it:
#   (cd java-psc && mvn -q package)       # -> java-psc/target/psc-sample.jar

set -euo pipefail
cd "$(dirname "$0")"
source ./psc_env.bash

jar=java-psc/target/psc-sample.jar
[[ -f ${jar} ]] || { echo "${jar} not found - build it with: (cd java-psc && mvn -q package)" >&2; exit 1; }
java -jar "${jar}" "$@"

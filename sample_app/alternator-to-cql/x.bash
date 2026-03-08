#!/usr/bin/env bash

# Docker
#ALTERNATOR_HOST=172.39.0.2 
#ALTERNATOR_PORT=9998 
#ALTERNATOR_HTTPS_PORT=9999 

# xcloud
set -x
export INTEGRATION_TESTS=true 
export ALTERNATOR_HOST=localhost
export ALTERNATOR_PORT=8000
export ALTERNATOR_HTTPS_PORT=8043 
export ALTERNATOR_CA_CERT_PATH=~/k8s/sample_app/alternator-to-cql/test/scylla/ca-929.crt

export CQL_USERNAME=cassandra 
export CQL_PASSWORD=cassandra 
export ALTERNATOR_ACCESS_KEY=cassandra 
export ALTERNATOR_SECRET_KEY='$(ALTERNATOR_SECRET)'
export ALTERNATOR_DATACENTER=dc1

mvn test -Dtest="**/*IT" -DfailIfNoTests=false -Dsurefire.timeout=300 

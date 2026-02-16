#!/usr/bin/env bash 
#
connection_id=${1:-1}
set -x
scylla-bench -workload uniform -mode mixed -nodes endpoint.cluster-1.scylladb.com:9001 -username scylla -password lbjH51uGVMI9cLZ -replication-factor 3 -concurrency 512 -duration 300s -client-routes-connection-ids ${connection_id}

#!/bin/bash

password=$PASSWORD
nodelist=( "node-0.gce-us-west-1.b6f1f6846b4178bfdbc2.clusters.scylla.cloud" "node-1.gce-us-west-1.b6f1f6846b4178bfdbc2.clusters.scylla.cloud" "node-2.gce-us-west-1.b6f1f6846b4178bfdbc2.clusters.scylla.cloud")

time ./stream_parquet_to_scylladb.py -u scylla -p ${password} -s ${nodelist[$@]} $@



#!/usr/bin/env bash

for n in {0..2}
do
printf "scylla-dc1-rack1-%s\n" $n
kubectl -n scylla-dc1 exec -it scylla-dc1-rack1-${n} -c scylla -- bash -c "nodetool flush; nodetool compact; sleep 10; nodetool clearsnapshot; du -sh /var/lib/scylla/data/moloco*|sort -n" 
done

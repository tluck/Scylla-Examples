#!/usr/bin/env bash

nodes=(34.187.208.134 35.197.44.79 34.11.248.152)

for n in ${nodes[@]}
do
ssh -i ~/.ssh/gce $n $@
done

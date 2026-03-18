#!/usr/bin/env bash

# gcloud compute instances list --format='table(name,zone,status,networkInterfaces.network:label=VPC,tags.list())'
gcloud compute instances list \
  --format="table(
    name,
    zone.basename(),
    status,
    networkInterfaces[].network.basename():label=VPC,
    tags.list():label=TAGS
  )"

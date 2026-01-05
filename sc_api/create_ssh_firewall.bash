#!/usr/bin/env bash 

if [[ $# -ne 2 ]]; then
  echo "Usage: $0 <vpc> <tag>"
  exit 1
fi

vpc=$1
tag=$2

gcloud compute firewall-rules create allow-ssh-$$ \
  --network=$vpc \
  --direction=INGRESS \
  --priority=1000 \
  --action=ALLOW \
  --rules=tcp:22 \
  --source-ranges=0.0.0.0/0 \
  --target-tags=$tag \
  --description="Allow SSH access to instances with tag $tag"

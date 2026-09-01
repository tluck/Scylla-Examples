#!/bin/bash -x

CLUSTER_NAME=${1:-"tjl-gcp-n2-highmem-2-1"}
REGION_ID='65505' # GCP Oregon region_id # can be found in the backoffice
SUBNET_CIDR='10.0.201.0/29' # default CIDR for the PSC backend in the cluster VPC

ACCOUNT_ID="${SC_ACCOUNT}" # user account_id
USER_ID='122833' # this can also be obtained in the backoffice after the cluster is built

cx sc dev network psc create --user-id $USER_ID --account-id $ACCOUNT_ID --network-region-id $REGION_ID --name $CLUSTER_NAME # --nat-subnet-cidr $SUBNET_CIDR

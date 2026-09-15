#!/bin/bash -x

CLUSTER_NAME=${1:-"tjl-gcp-n2-highmem-2-1"}
REGION_ID=${2:-65505} # GCP Oregon region_id # can be found in the backoffice
ACCOUNT_ID=${3:-${SC_ACCOUNT}} # user account_id
USER_ID=${4:-122833} # this can also be obtained in the backoffice after the cluster is built
SUBNET_CIDR='10.0.201.0/29' # default CIDR for the PSC backend in the cluster VPC

cx sc dev network psc create --user-id $USER_ID --account-id $ACCOUNT_ID --network-region-id $REGION_ID --name $CLUSTER_NAME # --nat-subnet-cidr $SUBNET_CIDR

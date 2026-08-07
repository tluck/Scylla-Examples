#!/bin/bash -x

USER_ID='122833'
ACCOUNT_ID=$SC_ACCOUNT
CLUSTER_NAME='meijer'
REGION_ID='65035' # GCP Oregon
SUBNET_CDIR='10.0.201.0/29'

cx sc dev network psc create --user-id $USER_ID --account-id $ACCOUNT_ID --network-region-id $REGION_ID --name $CLUSTER_NAME # --nat-subnet-cidr 10.0.202.0/29

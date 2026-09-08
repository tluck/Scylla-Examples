#!/bin/bash -x 

connection_id=${1:-759} # from psc_connection_setup.bash

cx sc dev network psc client-setup --connection-ids ${connection_id} | grep -v "Shared connect"| grep -vi "level" > psc_${connection_id}.tf

#terraform init && terraform apply


#!/bin/bash -x 

connection_id=759 # from psc_connection_setup.bash

cx sc dev network psc client-setup --connection-ids ${connection_id} | grep -v Connect| grep -v "level" > psc_${connection_id}.tf

#terraform init && terraform apply


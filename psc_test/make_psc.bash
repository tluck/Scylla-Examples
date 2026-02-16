#!/bin/bash

[[ $1 == "-d" ]] && cleanup=true || cleanup=false
[[ $1 == "-c" ]] && CLUSTER_ID=$2 || CLUSTER_ID="45344"

set -euo pipefail
set -x

# === CONFIGURATION ===
PROJECT="cx-sa-lab"
APP_PROJECT="${PROJECT}" # in case the app is in a different project, update this variable
# VPCs for App and ScyllaDB (update if different)
REGION="us-west1"

# Network CIDRs (for reference)
ENDPOINT_IP="10.138.0.150"    # Static IP for PSC endpoint in app subnet
# APP_CIDR="10.138.0.0/24"      # App subnet  
# SDB_CIDR="172.31.0.0/24"      # Scylla VPC subnet
PSC_CIDR="172.31.1.0/24"      # PSC NAT subnet

# === VPC names (update if different)
# SDB_VPC="np-66093-bc554453-0e5f-4e48-8311-dfdce05898d7"
APP_VPC="default"
CLUSTER_ID="45344"
# === NODES ===
NODE_a='scylla-cloud-45344-node-3-48cfe68e3619bfe02cfc'
NODE_b='scylla-cloud-45344-node-4-48cfe68e3619bfe02cfc'
NODE_c='scylla-cloud-45344-node-5-48cfe68e3619bfe02cfc'
# === RESOURCE NAMES ===
SERVICE_NAME="psc-${CLUSTER_ID}"
APP_NAME="psc-${APP_VPC}"

# Ports for port mapping (client ports on Scylla nodes mapped to these ports on PSC)
PORTMAP_CQL=(9001 9002 9003)
PORTMAP_TLS=(9101 9102 9103)

set +x
# Get network and subnet of the nodes (assuming all nodes are in the same network/subnet)
NODE_NETWORK=$(gcloud compute instances describe ${NODE_a} \
    --zone="${REGION}-a" --format='value(networkInterfaces[0].network)' --quiet)
NODE_SUBNET=$(gcloud compute instances describe ${NODE_a} \
    --zone="${REGION}-a" --format='value(networkInterfaces[0].subnetwork)' --quiet)
# NODE_NETWORK="projects/${PROJECT}/global/networks/${SDB_VPC}"
# NODE_SUBNET="projects/${PROJECT}/regions/${REGION}/subnetworks/${NODE_SUBNET}"

SERVICE_ATTACHMENT_NAME="${SERVICE_NAME}"
BACKEND_NAME="${SERVICE_NAME}"
SDB_FORWARDING_RULE_NAME="${SERVICE_NAME}"
NEG_NAME="${SERVICE_NAME}"
NEG="projects/${PROJECT}/regions/${REGION}/networkEndpointGroups/${NEG_NAME}"
APP_FORWARDING_RULE_NAME="${APP_NAME}"

# === CLEANUP (delete in correct dependency order) ===

if [[ $cleanup == true ]]; then
printf "\nCleaning up existing resources...\n"
gcloud compute service-attachments delete     ${SERVICE_ATTACHMENT_NAME} --region="${REGION}" --quiet || true 
gcloud compute forwarding-rules delete        ${SERVICE_ATTACHMENT_NAME} --region="${REGION}" --quiet || true
gcloud compute backend-services delete        ${BACKEND_NAME} --region="${REGION}" --quiet || true
gcloud compute network-endpoint-groups delete ${NEG_NAME} --region="${REGION}" --quiet || true
gcloud compute networks subnets delete        ${SERVICE_NAME} --region="${REGION}" --quiet || true
# Cleanup consumer side
gcloud compute forwarding-rules delete        ${APP_FORWARDING_RULE_NAME} --region="${REGION}" --quiet || true
gcloud compute addresses delete               ${APP_NAME} --region="${REGION}" --quiet || true
exit
fi

# === CREATE ===

# Create a Network Endpoint Group (NEG) and add each node
printf "\n=== 1.a Creating network endpoint groups ===\n"
# 1. Create regional portmap NEG (empty initially)
gcloud compute network-endpoint-groups create "${NEG_NAME}" \
    --region="${REGION}" \
    --network-endpoint-type="GCE_VM_IP_PORTMAP" \
    --network="${NODE_NETWORK}" \
    --subnet="${NODE_SUBNET}" \
    --project="${PROJECT}" || true

# Add 3 network endpoints with port mapping
gcloud compute network-endpoint-groups update "${NEG_NAME}" \
    --region="${REGION}" \
    --add-endpoint="instance=projects/${PROJECT}/zones/${REGION}-a/instances/${NODE_a},port=9042,client-destination-port=${PORTMAP_CQL[0]}" \
    --add-endpoint="instance=projects/${PROJECT}/zones/${REGION}-b/instances/${NODE_b},port=9042,client-destination-port=${PORTMAP_CQL[1]}" \
    --add-endpoint="instance=projects/${PROJECT}/zones/${REGION}-c/instances/${NODE_c},port=9042,client-destination-port=${PORTMAP_CQL[2]}" \
    --add-endpoint="instance=projects/${PROJECT}/zones/${REGION}-a/instances/${NODE_a},port=9142,client-destination-port=${PORTMAP_TLS[0]}" \
    --add-endpoint="instance=projects/${PROJECT}/zones/${REGION}-b/instances/${NODE_b},port=9142,client-destination-port=${PORTMAP_TLS[1]}" \
    --add-endpoint="instance=projects/${PROJECT}/zones/${REGION}-c/instances/${NODE_c},port=9142,client-destination-port=${PORTMAP_TLS[2]}" \
    --project="${PROJECT}" || true

# === PRODUCER SIDE (ScyllaDB VPC) ===
printf "\n=== 1.b CREATE PSC NAT SUBNET (ScyllaDB VPC) ===\n"
gcloud compute networks subnets create ${SERVICE_NAME} \
    --region="${REGION}" \
    --purpose=PRIVATE_SERVICE_CONNECT \
    --range="${PSC_CIDR}" \
    --network="${NODE_NETWORK}" \
    --project="${PROJECT}" || true

PSC_SUBNET=$(gcloud compute networks subnets describe ${SERVICE_NAME} \
    --region="${REGION}" --project="${PROJECT}" --format='value(selfLink)')

printf "\n=== 2. CREATE/VERIFY BACKEND SERVICE ===\n"
# 2.a Create empty backend service matching your spec
gcloud compute backend-services create ${BACKEND_NAME} \
    --project="${PROJECT}" \
    --region="${REGION}" \
    --load-balancing-scheme=INTERNAL \
    --protocol=TCP \
    --network="${NODE_NETWORK}" \
    --session-affinity=NONE \
    --no-enable-logging || true
    # failoverPolicy is empty, so no flags needed
    # networkPassThroughLbTrafficPolicy.zonalAffinity.spillover uses default

# 2.b Create the backend with the NEG created above, using CONNECTION mode for port mapping
gcloud compute backend-services add-backend ${BACKEND_NAME} \
    --project="${PROJECT}" \
    --region="${REGION}" \
    --network-endpoint-group="${NEG}" \
    --network-endpoint-group-region="${REGION}" \
    --balancing-mode=CONNECTION \
    --no-failover || true

printf "\n=== 3. CREATE FORWARDING RULE ===\n"
# SUBNET=$(gcloud compute networks subnets list \
#     --regions="${REGION}" \
#     --network=${SDB_VPC} \
#     --format="value(name)" --limit 1)
# printf "%s\n" "SUBNET: $SUBNET" 

gcloud compute forwarding-rules create ${SDB_FORWARDING_RULE_NAME} \
    --project="${PROJECT}" \
    --region="${REGION}" \
    --load-balancing-scheme=INTERNAL \
    --network-tier=PREMIUM \
    --network="${NODE_NETWORK}" \
    --subnet="${NODE_SUBNET}" \
    --ip-protocol=TCP \
    --ports=ALL \
    --backend-service=${BACKEND_NAME} || true

FR_URL=$(gcloud compute forwarding-rules describe ${SDB_FORWARDING_RULE_NAME} \
    --region="${REGION}" \
    --project="${PROJECT}" --format='value(selfLink)')

printf "\n✅ PSC Forwarding Rule: $FR_URL\n"
# --------------------------------------------------------
printf "\n=== 4. CREATE SERVICE ATTACHMENT ===\n"
gcloud compute service-attachments create ${SERVICE_ATTACHMENT_NAME} \
    --region="${REGION}" \
    --target-service="${FR_URL}" \
    --connection-preference=ACCEPT_AUTOMATIC \
    --nat-subnets="${PSC_SUBNET}" \
    --project="${PROJECT}" || true

printf "\n✅ PSC attachment: projects/${PROJECT}/regions/${REGION}/serviceAttachments/${SERVICE_ATTACHMENT_NAME}\n"

# === CONSUMER SIDE (App VPC) ===
printf "\n=== 5. CONSUMER ENDPOINT (App VPC) ===\n"
# Reserve IP in app subnet
gcloud compute addresses create ${APP_NAME} \
    --region=${REGION} \
    --subnet="default" \
    --addresses=${ENDPOINT_IP} \
    --project=${APP_PROJECT} || true

# Create forwarding rule for PSC endpoint in app VPC that points to the service attachment
gcloud compute forwarding-rules create ${APP_FORWARDING_RULE_NAME} \
    --region="${REGION}" \
    --network="https://www.googleapis.com/compute/v1/projects/${APP_PROJECT}/global/networks/${APP_VPC}" \
    --subnet="default" \
    --target-service-attachment="projects/${PROJECT}/regions/${REGION}/serviceAttachments/${SERVICE_ATTACHMENT_NAME}" \
    --address=${APP_NAME} \
    --project="${APP_PROJECT}" || true

APP_PSC_IP=$(gcloud compute forwarding-rules describe ${APP_FORWARDING_RULE_NAME} \
    --region="${REGION}" \
    --project="${APP_PROJECT}" --format='value(IPAddress)')

printf "\n✅ APPLICATION CONSUMER PSC ENDPOINT: ${APP_PSC_IP}\n"
printf   "✅ Test: nc -zv ${APP_PSC_IP} ${PORTMAP_CQL[0]}\n"
printf   "✅ Test: cqlsh ${APP_PSC_IP} ${PORTMAP_CQL[0]}\n"

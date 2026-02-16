#!/usr/bin/env bash 

set -x

REGION="us-west1"
ZONE_a="${REGION}-a"
ZONE_b="${REGION}-b" 
ZONE_c="${REGION}-c"

NODE_a='scylla-cloud-45344-node-3-48cfe68e3619bfe02cfc'
NODE_b='scylla-cloud-45344-node-4-48cfe68e3619bfe02cfc'
NODE_c='scylla-cloud-45344-node-5-48cfe68e3619bfe02cfc'

NODE_NETWORK=$(gcloud compute instances describe ${NODE_a} \
    --zone="${ZONE_a}" --format='value(networkInterfaces[0].network)' --quiet)
NODE_SUBNET=$(gcloud compute instances describe ${NODE_a} \
    --zone="${ZONE_a}" --format='value(networkInterfaces[0].subnetwork)' --quiet)

echo "NETWORK: $NODE_NETWORK"
echo "SUBNET: $NODE_SUBNET" 

# === CLEANUP (delete in correct dependency order) ===
printf "Cleaning up existing resources...\n"
gcloud compute forwarding-rules delete scylla-ilb --region="${REGION}" --quiet || true

gcloud compute backend-services remove-backend scylla-backend-service \
    --instance-group=scylla-a --instance-group-zone="${ZONE_a}" --region="${REGION}" || true
gcloud compute backend-services remove-backend scylla-backend-service \
    --instance-group=scylla-b --instance-group-zone="${ZONE_b}" --region="${REGION}" || true
gcloud compute backend-services remove-backend scylla-backend-service \
    --instance-group=scylla-c --instance-group-zone="${ZONE_c}" --region="${REGION}" || true

gcloud compute backend-services delete scylla-backend-service --region="${REGION}" --quiet
gcloud compute health-checks delete scylla-health-check --region="${REGION}" --quiet

gcloud compute instance-groups unmanaged delete scylla-a --zone="${ZONE_a}" --quiet
gcloud compute instance-groups unmanaged delete scylla-b --zone="${ZONE_b}" --quiet
gcloud compute instance-groups unmanaged delete scylla-c --zone="${ZONE_c}" --quiet

# === RECREATE ===
printf "\nRecreating resources...\n"
gcloud compute instance-groups unmanaged create scylla-a --zone="${ZONE_a}"
gcloud compute instance-groups unmanaged create scylla-b --zone="${ZONE_b}"
gcloud compute instance-groups unmanaged create scylla-c --zone="${ZONE_c}"

gcloud compute instance-groups unmanaged add-instances scylla-a --instances="${NODE_a}" --zone="${ZONE_a}"
gcloud compute instance-groups unmanaged add-instances scylla-b --instances="${NODE_b}" --zone="${ZONE_b}"
gcloud compute instance-groups unmanaged add-instances scylla-c --instances="${NODE_c}" --zone="${ZONE_c}"

gcloud compute health-checks create tcp scylla-health-check --port=9042 --region="${REGION}"

gcloud compute backend-services create scylla-backend-service \
    --protocol=TCP \
    --health-checks="https://www.googleapis.com/compute/v1/projects/cx-sa-lab/regions/${REGION}/healthChecks/scylla-health-check" \
    --region="${REGION}" \
    --load-balancing-scheme=internal \
    --network="${NODE_NETWORK}"

gcloud compute backend-services add-backend scylla-backend-service \
    --instance-group=scylla-a --instance-group-zone="${ZONE_a}" --region="${REGION}"
gcloud compute backend-services add-backend scylla-backend-service \
    --instance-group=scylla-b --instance-group-zone="${ZONE_b}" --region="${REGION}"
gcloud compute backend-services add-backend scylla-backend-service \
    --instance-group=scylla-c --instance-group-zone="${ZONE_c}" --region="${REGION}"

gcloud compute forwarding-rules create scylla-ilb \
    --load-balancing-scheme=internal \
    --network="${NODE_NETWORK}" \
    --subnet="${NODE_SUBNET}" \
    --region="${REGION}" \
    --ports=9042 \
    --backend-service=scylla-backend-service

IP_NAME=$(gcloud compute forwarding-rules describe scylla-ilb --region=us-west1 --format="value(IPAddress)")
echo "IP_NAME: $IP_NAME"

echo "=== SUCCESS ==="
echo "Test with: cqlsh $IP_NAME 9042"
echo "Check health: gcloud compute backend-services get-health scylla-backend-service --region=${REGION}"

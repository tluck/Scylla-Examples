#!/usr/bin/env python3

import os
import sys
import json
import requests

API_BASE_URL = "https://api.cloud.scylladb.com"
API_TOKEN = os.getenv('SC_TOKEN')
accountId = os.getenv('SC_ACCOUNT')

def get_headers():
    return {
        "Authorization": f"Bearer {API_TOKEN}",
        "Content-Type": "application/json"
    }

def api_get(url):
    resp = requests.get(url, headers=get_headers())
    resp.raise_for_status()
    return resp.json()

def api_post(url, data):
    resp = requests.post(url, headers=get_headers(), data=json.dumps(data))
    resp.raise_for_status()
    return resp.json()

def print_usage():
    print(f'Usage:\n  {sys.argv[0]} -l\n  {sys.argv[0]} -s <clusterId>\n  {sys.argv[0]} -d <clusterId> <clusterName>\n  {sys.argv[0]} gcp|aws [xcloud|standard]')

def main():
    if len(sys.argv) < 2 or sys.argv[1] in ['-h', '--help']:
        print_usage()
        sys.exit(0)

    cmd = sys.argv[1]

    if cmd == "-l":
        print(f"Listing clusters for account {accountId}:\n   ID Cluster Name")
        clusters = api_get(f"{API_BASE_URL}/account/{accountId}/clusters")
        for cl in clusters.get('data', {}).get('clusters', []):
            print(f"{cl.get('id')} {cl.get('clusterName')}")
        sys.exit(0)

    elif cmd == "-s":
        if len(sys.argv) < 3:
            print("Cluster ID required")
            sys.exit(1)
        clusterId = sys.argv[2]
        cluster = api_get(f"{API_BASE_URL}/account/{accountId}/cluster/{clusterId}")
        print(json.dumps(cluster, indent=2))
        sys.exit(0)

    elif cmd == "-d":
        if len(sys.argv) < 4:
            print("Cluster ID and name required")
            sys.exit(1)
        clusterId = sys.argv[2]
        name = sys.argv[3]
        confirm = input(f"Deleting cluster {name} ({clusterId}). Are you sure? (y/N) ")
        if confirm.lower() != "y":
            print("Aborting.")
            sys.exit(1)
        data = {"clusterName": name}
        resp = api_post(f"{API_BASE_URL}/account/{accountId}/cluster/{clusterId}/delete", data)
        print(json.dumps(resp, indent=2))
        sys.exit(0)

    # Default behavior: create cluster for gcp or aws
    cloud = sys.argv[1]
    mode = sys.argv[2] if len(sys.argv) > 2 else "xcloud"

    if cloud == "gcp":
        instanceType = "n2-highmem-8" 
        name = f"tjl-gcp-{instanceType}"
        name=name.replace('.', '-')
        localDiskCount = 1
        cloudProviderId = 2
        region = "us-west1"
    else:
        instanceType = "i4i.large" # "t3.micro"
        name = f"tjl-aws-{instanceType}"
        name=name.replace('.', '-')
        localDiskCount = None
        cloudProviderId = 1
        region = "us-west-2"

    owner = "Account"
    cidr = "172.29.0.0/24"

    # Get cloudCredentialId
    cloud_accounts = api_get(f"{API_BASE_URL}/account/{accountId}/cloud-account")
    cloudCredentialId = next(
        (x['id'] for x in cloud_accounts.get('data', []) if x.get('owner') == owner and x.get('cloudProviderId') == cloudProviderId),
        None
    )
    print(f"Cloud Credential ID: {cloudCredentialId}")

    # Get regionId
    regions = api_get(f"{API_BASE_URL}/deployment/cloud-provider/{cloudProviderId}/regions")
    regionId = next(
        (r['id'] for r in regions.get('data', {}).get('regions', []) if r.get('externalId') == region),
        None
    )
    print(f"Region ID: {regionId}")

    # Get instanceId
    instances = api_get(f"{API_BASE_URL}/deployment/cloud-provider/{cloudProviderId}/region/{regionId}")
    if cloud == "gcp":
        instanceId = next(
            (i['id'] for i in instances.get('data', {}).get('instances', [])
             if i.get('externalId') == instanceType and i.get('localDiskCount') == localDiskCount),
            None
        )
    else:
        instanceId = next(
            (i['id'] for i in instances.get('data', {}).get('instances', []) if i.get('externalId') == instanceType),
            None
        )
    print(f"Instance ID: {instanceId}")

    # Build payload
    base_json = {
        "accountCredentialId": cloudCredentialId,
        "broadcastType": "PRIVATE",
        "cidrBlock": cidr,
        "rackCIDRSize": 26,
        "cloudProviderId": cloudProviderId,
        "regionId": regionId,
        "clusterName": name,
        "replicationFactor": 3,
        "scyllaVersion": "2025.3.3",
        "userApiInterface": "CQL",
        "tablets": "enforced",
        "freeTier": False
    }

    if mode == "standard":
        base_json.update({
            "numberOfNodes": 3,
            "instanceId": instanceId
        })
    else:
        base_json["scaling"] = {
            "mode": "xcloud",
            "instanceTypeIDs": [instanceId],
            "policies": {
                "storage": {"min": 0, "targetUtilization": 0.8},
                "vcpu": {"min": 0}
            }
        }

    print(json.dumps(base_json, indent=2))

    # Create cluster
    response = api_post(f"{API_BASE_URL}/account/{accountId}/cluster", base_json)
    print(json.dumps(response, indent=2))

if __name__ == "__main__":
    main()

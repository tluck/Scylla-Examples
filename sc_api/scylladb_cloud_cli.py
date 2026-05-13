#!/usr/bin/env python3

import os
import sys
import json
import requests
import argparse

API_BASE_URL = "https://api.cloud.scylladb.com"
API_TOKEN = os.getenv('SC_TOKEN')
accountId = os.getenv('SC_ACCOUNT')
default_version="2026.1.3"
default_cidr = "172.30.0.0/24"
default_instance_gcp = "n2-highmem-2"
default_instance_aws = "i8g.large"
default_region_gcp = "us-west1"
default_region_aws = "us-west-2"

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

def build_parser():
    parser = argparse.ArgumentParser(
        description="Scylla Cloud helper script"
    )

    subparsers = parser.add_subparsers(dest="command")

    # list clusters
    subparsers.add_parser("list", help="List clusters")

    # show cluster
    p_show = subparsers.add_parser("show", help="Show cluster JSON")
    p_show.add_argument("-c", "--cluster", metavar="CLUSTER_ID",
                        help="Show cluster details by ID")

    # delete cluster
    p_delete = subparsers.add_parser("delete", help="Delete a cluster")
    p_delete.add_argument(
        "-x", "--delete",
        metavar="CLUSTER_ID",
        help="Cluster ID to delete"
    )
    p_delete.add_argument(
        "cluster_name",
        nargs="?",
        help="Cluster name"
    )

    # create cluster
    p_create = subparsers.add_parser("create", help="Create a cluster")

    p_create.add_argument(
        "-c", "--cloud",
        choices=["gcp", "aws"],
        required=True,
        help="Cloud provider"
    )
    p_create.add_argument(
        "-m", "--mode",
        choices=["xcloud", "standard"],
        default="xcloud",
        help="Deployment mode (default: xcloud)"
    )
    p_create.add_argument(
        "-l", "--name",
        help="Cluster name (or prefix; overrides default naming)"
    )
    p_create.add_argument(
        "-t", "--instance-type",
        help=f"Instance type (e.g. default {default_instance_gcp}, {default_instance_aws})"
    )
    p_create.add_argument(
        "-d", "--disk",
        type=int,
        help="Local disk count (GCP only)"
    )
    p_create.add_argument(
        "-n", "--nodes",
        type=int,
        help="Number of nodes (for standard mode, default: 3)"
    )
    p_create.add_argument(
        "-r", "--region",
        help=f"Region (default: {default_region_gcp} for GCP, {default_region_aws} for AWS)"
    )
    p_create.add_argument(
        "-i", "--cidr",
        help=f"CIDR block for the cluster VPC (default: {default_cidr})"
    )
    p_create.add_argument(
        "-f", "--replication",
        type=int,
        help=f"Replication factor (default: {default_version})"
    )
    p_create.add_argument(
        "-s", "--scylla-version",
        help=f"Scylla version (default: {default_version})"
    )

    return parser


def handle_list():
    print(f"Listing clusters for account {accountId}:\n   ID Cluster Name")
    clusters = api_get(f"{API_BASE_URL}/account/{accountId}/clusters")
    for cl in clusters.get('data', {}).get('clusters', []):
        print(f"{cl.get('id')} {cl.get('clusterName')}")

def handle_show(cluster_id):
    if not cluster_id:
        print("Cluster ID required for show")
        sys.exit(1)
    cluster = api_get(f"{API_BASE_URL}/account/{accountId}/cluster/{cluster_id}")
    print(json.dumps(cluster, indent=2))

def handle_delete(cluster_id, name):
    if not cluster_id or not name:
        print("Cluster ID and name required for delete")
        sys.exit(1)

    confirm = input(f"Deleting cluster {name} ({cluster_id}). Are you sure? (y/N) ")
    if confirm.lower() != "y":
        print("Aborting.")
        sys.exit(1)

    data = {"clusterName": name}
    resp = api_post(f"{API_BASE_URL}/account/{accountId}/cluster/{cluster_id}/delete", data)
    print(json.dumps(resp, indent=2))

def handle_create(args):
    cloud = args.cloud
    mode = args.mode

    # instance type and disk from options
    custom_instance = args.instance_type
    custom_disks = args.disk

    # choose defaults per cloud
    if cloud == "gcp":
        instanceType = custom_instance if custom_instance else default_instance_gcp
        localDiskCount = custom_disks if custom_disks is not None else 1
        region = args.region if args.region else default_region_gcp
        # name construction: use provided name if given, otherwise default pattern
        if args.name:
            name = args.name
        else:
            name = f"tjl-gcp-{instanceType}-{localDiskCount}"
        cloudProviderId = 2
    else:
        instanceType = custom_instance if custom_instance else default_instance_aws
        localDiskCount = custom_disks  # For AWS, keep None if not specified
        region = args.region if args.region else default_region_aws
        if args.name:
            name = args.name
        else:
            name = f"tjl-aws-{instanceType}"
        cloudProviderId = 1

    name = name.replace('.', '-')
    owner = "Account"
    cidr = args.cidr if args.cidr else default_cidr
    replication = args.replication if args.replication else 3
    scylla_version = args.scylla_version if args.scylla_version else default_version

    print(f"Creating cluster '{name}' with instance type: {instanceType}", end="")
    if localDiskCount is not None:
        print(f" and {localDiskCount} disks")
    else:
        print()

    # Get cloudCredentialId (highest id if several match owner + cloud provider)
    cloud_accounts = api_get(f"{API_BASE_URL}/account/{accountId}/cloud-account")
    matches = [
        x for x in cloud_accounts.get('data', [])
        if x.get('owner') == owner and x.get('cloudProviderId') == cloudProviderId
    ]
    cloudCredentialId = max((x['id'] for x in matches), default=None)
    print(f"Cloud Credential ID: {cloudCredentialId}")

    # Get regionId
    regions = api_get(f"{API_BASE_URL}/deployment/cloud-provider/{cloudProviderId}/regions")
    regionId = next(
        (r['id'] for r in regions.get('data', {}).get('regions', [])
         if r.get('externalId') == region),
        None
    )
    print(f"Region ID: {regionId}")

    # Get instanceId
    instances = api_get(f"{API_BASE_URL}/deployment/cloud-provider/{cloudProviderId}/region/{regionId}")
    if cloud == "gcp" and localDiskCount is not None:
        instanceId = next(
            (i['id'] for i in instances.get('data', {}).get('instances', [])
             if i.get('externalId') == instanceType and i.get('localDiskCount') == localDiskCount),
            None
        )
    else:
        instanceId = next(
            (i['id'] for i in instances.get('data', {}).get('instances', [])
             if i.get('externalId') == instanceType),
            None
        )

    if instanceId is None:
        print(f"ERROR: Could not find instance type '{instanceType}'", end="")
        if localDiskCount is not None:
            print(f" with {localDiskCount} disks")
        else:
            print()
        print("\nAvailable instances:")
        for i in instances.get('data', {}).get('instances', []):
            if cloud == "gcp":
                print(f"  {i.get('externalId')} (disks: {i.get('localDiskCount')})")
            else:
                print(f"  {i.get('externalId')}")
        sys.exit(1)

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
        "replicationFactor": replication,
        "scyllaVersion": scylla_version,
        "userApiInterface": "CQL",
        "tablets": "enforced",
        "freeTier": False
    }

    if mode == "standard":
        numberOfNodes = args.nodes if args.nodes else 3
        base_json.update({
            "numberOfNodes": numberOfNodes,
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


def main():
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "list":
        handle_list()
    elif args.command == "show":
        handle_show(args.cluster)
    elif args.command == "delete":
        handle_delete(args.delete, args.cluster_name)
    elif args.command == "create":
        handle_create(args)
    else:
        parser.print_help()
        sys.exit(1)

if __name__ == "__main__":
    main()

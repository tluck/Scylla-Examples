#!/usr/bin/env python3

import os
import sys
import json
import time
import requests
import argparse

API_BASE_URL = "https://api.cloud.scylladb.com"
API_TOKEN = os.getenv('SC_TOKEN')
accountId = os.getenv('SC_ACCOUNT')
default_version="2026.1.7"
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

def api_put(url, data):
    resp = requests.put(url, headers=get_headers(), data=json.dumps(data))
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

    # scale (resize) an xcloud cluster to a vCPU minimum
    p_scale = subparsers.add_parser(
        "scale",
        help="Scale in/out (resize) an xcloud cluster to a vCPU minimum and monitor progress"
    )
    p_scale.add_argument(
        "-c", "--cluster",
        metavar="CLUSTER_ID",
        required=True,
        help="Cluster ID to scale"
    )
    p_scale.add_argument(
        "-v", "--vcpu",
        type=int,
        required=True,
        help="Minimum total vCPU count to scale to"
    )
    p_scale.add_argument(
        "--interval",
        type=int,
        default=15,
        help="Polling interval in seconds while monitoring (default: 15)"
    )
    p_scale.add_argument(
        "--timeout",
        type=int,
        default=3600,
        help="Maximum time in seconds to monitor progress (default: 3600)"
    )
    p_scale.add_argument(
        "--no-monitor",
        action="store_true",
        help="Submit the resize but do not wait/monitor progress"
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


# Request statuses that mean a cluster request is no longer running
DONE_STATUSES = {"COMPLETED", "FAILED", "CANCELLED", "ERROR"}
# Request types produced by a scaling policy change / resize
RESIZE_TYPES = {"AUTOSCALING", "RESIZE_CLUSTER", "RESIZE_CLUSTER_V3", "RESIZE"}

def get_cluster(cluster_id):
    resp = api_get(f"{API_BASE_URL}/account/{accountId}/cluster/{cluster_id}")
    return resp.get('data', {}).get('cluster', {})

def list_requests(cluster_id):
    resp = api_get(f"{API_BASE_URL}/account/{accountId}/cluster/{cluster_id}/request")
    return resp.get('data', []) or []

def get_nodes(cluster_id):
    resp = api_get(f"{API_BASE_URL}/account/{accountId}/cluster/{cluster_id}/nodes")
    return resp.get('data', {}).get('nodes') or resp.get('data') or []

def node_summary(nodes):
    # Tally nodes by lifecycle status + topology state, e.g.
    # "6 nodes: 4 ACTIVE/NORMAL, 2 BOOTSTRAPPING/JOINING"
    from collections import Counter
    tally = Counter(
        f"{n.get('status', '?')}/{n.get('state', '?')}" for n in nodes
    )
    breakdown = ", ".join(f"{cnt} {label}" for label, cnt in sorted(tally.items()))
    return f"{len(nodes)} nodes: {breakdown}"

def request_done(req):
    return req.get('status') in DONE_STATUSES or req.get('progressPercent', 0) >= 100

def handle_scale(args):
    cluster_id = args.cluster
    target_vcpu = args.vcpu

    cluster = get_cluster(cluster_id)
    if not cluster:
        print(f"ERROR: Cluster {cluster_id} not found")
        sys.exit(1)

    if cluster.get('scalingMode') != 'xcloud':
        print(f"ERROR: Cluster {cluster_id} is '{cluster.get('scalingMode')}' mode; "
              "scale-to-vCPU is only supported for xcloud clusters")
        sys.exit(1)

    dc = cluster.get('dc') or (cluster.get('dataCenters') or [{}])[0]
    dc_id = dc.get('id')
    if not dc_id:
        print(f"ERROR: Could not determine data center for cluster {cluster_id}")
        sys.exit(1)

    # Preserve the existing scaling policy, changing only the vCPU minimum.
    scaling = dc.get('scaling', {}) or {}
    policies = scaling.get('policies', {}) or {}
    storage = policies.get('storage', {}) or {"min": 0, "targetUtilization": 0.8}
    current_min = (policies.get('vcpu', {}) or {}).get('min')

    new_scaling = {
        "mode": cluster.get('scalingMode', 'xcloud'),
        "instanceTypeIDs": scaling.get('instanceTypeIDs', []),
        "policies": {
            "storage": storage,
            "vcpu": {"min": target_vcpu}
        }
    }

    print(f"Scaling cluster '{cluster.get('clusterName')}' ({cluster_id}) "
          f"vCPU minimum: {current_min} -> {target_vcpu}")
    print(json.dumps(new_scaling, indent=2))

    # Snapshot existing request IDs so we can detect the ones this change triggers.
    before_ids = {r.get('id') for r in list_requests(cluster_id)}

    resp = api_put(
        f"{API_BASE_URL}/account/{accountId}/cluster/{cluster_id}/dc/{dc_id}/scaling",
        new_scaling
    )
    print(json.dumps(resp, indent=2))

    if args.no_monitor:
        print("Resize submitted. Skipping monitoring (--no-monitor).")
        return

    monitor_scale(cluster_id, before_ids, args.interval, args.timeout)

def monitor_scale(cluster_id, before_ids, interval, timeout):
    print("\nMonitoring resize progress (Ctrl-C to stop watching)...")
    start = time.time()
    deadline = start + timeout
    seen_new = False

    while time.time() < deadline:
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        requests_now = list_requests(cluster_id)
        # Only consider requests created by this scale operation.
        new_reqs = [
            r for r in requests_now
            if r.get('id') not in before_ids
            and r.get('requestType') in RESIZE_TYPES
        ]

        if new_reqs:
            seen_new = True
            nodes = get_nodes(cluster_id)
            print(f"  {ts} {node_summary(nodes)}")
            for n in sorted(nodes, key=lambda x: x.get('id', 0)):
                print(f"      node {n.get('id')} [{n.get('rackName', '?')}]: "
                      f"{n.get('status', '?')}/{n.get('state', '?')}")
            for r in sorted(new_reqs, key=lambda x: x.get('id', 0)):
                pct = r.get('progressPercent', 0)
                desc = r.get('progressDescription') or r.get('status', '')
                err = r.get('userFriendlyError')
                line = f"  {ts} [{r.get('requestType')}] {r.get('status')} {pct}% - {desc}"
                if err:
                    line += f" (error: {err})"
                print(line)

            failed = [r for r in new_reqs if r.get('status') in ("FAILED", "ERROR", "CANCELLED")]
            if failed:
                print("Resize did not complete successfully.")
                sys.exit(1)

            if all(request_done(r) for r in new_reqs):
                cluster = get_cluster(cluster_id)
                vcpu_min = (((cluster.get('dc') or {}).get('scaling') or {})
                            .get('policies', {}).get('vcpu', {}).get('min'))
                elapsed = int(time.time() - start)
                print(f"Resize complete in {elapsed // 60}m{elapsed % 60}s. "
                      f"Cluster status: {cluster.get('status')}, vCPU minimum: {vcpu_min}, "
                      f"{node_summary(get_nodes(cluster_id))}")
                return
        elif not seen_new:
            print(f"  {ts} Waiting for resize request to be created...")

        print("-" * 40)
        time.sleep(interval)

    print(f"Timed out after {timeout}s waiting for resize to complete.")
    sys.exit(1)


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
    elif args.command == "scale":
        handle_scale(args)
    else:
        parser.print_help()
        sys.exit(1)

if __name__ == "__main__":
    main()

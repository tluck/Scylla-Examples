#!/usr/bin/env python3

import os
import sys
import json
import time
import requests
import argparse
from datetime import datetime, timezone

API_BASE_URL = "https://api.cloud.scylladb.com"
API_TOKEN = os.getenv('SC_TOKEN')
accountId = os.getenv('SC_ACCOUNT')
default_version="2026.2.5"
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

class ApiError(Exception):
    pass

def api_result(resp):
    # The API reports rejections as {"error": "..."} under an HTTP 200, so the
    # status code alone is not enough to tell a submit succeeded.
    resp.raise_for_status()
    body = resp.json()
    if isinstance(body, dict) and body.get('error'):
        raise ApiError(f"{resp.request.method} {resp.url} -> error {body['error']}")
    return body

def api_get(url):
    return api_result(requests.get(url, headers=get_headers()))

def api_post(url, data):
    return api_result(requests.post(url, headers=get_headers(), data=json.dumps(data)))

def api_put(url, data):
    return api_result(requests.put(url, headers=get_headers(), data=json.dumps(data)))

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
        type=str.lower,
        choices=["gcp", "aws"],
        required=True,
        help="Cloud provider"
    )
    p_create.add_argument(
        "-m", "--mode",
        type=str.lower,
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
        type=str.lower,
        help=f"Region (default: {default_region_gcp} for GCP, {default_region_aws} for AWS)"
    )
    p_create.add_argument(
        "-i", "--cidr",
        help=f"CIDR block for the cluster VPC (default: {default_cidr})"
    )
    p_create.add_argument(
        "-f", "--replication",
        type=int,
        help=f"Replication factor (default: 3)"
    )
    p_create.add_argument(
        "-s", "--scylla-version",
        help=f"Scylla version (default: {default_version})"
    )
    p_create.add_argument(
        "--interval",
        type=int,
        default=20,
        help="Polling interval in seconds while monitoring (default: 20)"
    )
    p_create.add_argument(
        "--timeout",
        type=int,
        default=3600,
        help="Maximum time in seconds to monitor progress (default: 3600)"
    )
    p_create.add_argument(
        "--no-monitor",
        action="store_true",
        help="Submit the create but do not wait/monitor progress"
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
        "-t", "--tib",
        type=int,
        help="Minimum total storage in TiB to scale to, sent as storage.min "
             "in GiB (default: keep current)"
    )
    p_scale_dir = p_scale.add_mutually_exclusive_group()
    p_scale_dir.add_argument(
        "--vertical",
        dest="direction",
        action="store_const",
        const="vertical",
        help="Send the cluster's instance family instead of its exact instance "
             "type(s), letting the API pick the best size within that family, "
             "which may replace the current nodes"
    )
    p_scale_dir.add_argument(
        "--horizontal",
        dest="direction",
        action="store_const",
        const="horizontal",
        help="Pin the cluster's current instance type(s) in the request, so the "
             "resize adds more of the same nodes instead of replacing them "
             "(default)"
    )
    p_scale.set_defaults(direction="horizontal")
    p_scale.add_argument(
        "--interval",
        type=int,
        default=20,
        help="Polling interval in seconds while monitoring (default: 20)"
    )
    p_scale.add_argument(
        "--timeout",
        type=int,
        default=3600,
        help="Maximum time in seconds to monitor progress (default: 3600)"
    )
    p_scale.add_argument(
        "--resize-wait",
        type=int,
        default=RESIZE_WAIT_SECONDS,
        help="How long in seconds to wait for the resize request after the scaling "
             "update completes before concluding nothing needs to move "
             f"(default: {RESIZE_WAIT_SECONDS})"
    )
    p_scale.add_argument(
        "--no-monitor",
        action="store_true",
        help="Submit the resize but do not wait/monitor progress"
    )

    # monitor an existing cluster's in-progress requests
    p_monitor = subparsers.add_parser(
        "monitor",
        help="Monitor in-progress requests (create, resize, ...) on an existing cluster"
    )
    p_monitor.add_argument(
        "-c", "--cluster",
        metavar="CLUSTER_ID",
        required=True,
        help="Cluster ID to monitor"
    )
    p_monitor.add_argument(
        "--interval",
        type=int,
        default=20,
        help="Polling interval in seconds while monitoring (default: 20)"
    )
    p_monitor.add_argument(
        "--timeout",
        type=int,
        default=3600,
        help="Maximum time in seconds to monitor progress (default: 3600)"
    )
    p_monitor.add_argument(
        "--all",
        action="store_true",
        help="Follow every unfinished request, including background work "
             "(INSTALL_MANAGER, health checks) and stalled ones"
    )
    p_monitor.add_argument(
        "--stale-minutes",
        type=int,
        default=10,
        help="Ignore requests queued with no progress for longer than this "
             "(default: 10; ignored with --all)"
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
    cloud = args.cloud.lower()
    mode = args.mode.lower()

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

    if args.no_monitor:
        print("Create submitted. Skipping monitoring (--no-monitor).")
        return

    request_id = (response.get('data') or {}).get('requestId')
    monitor_create(name, request_id, args.interval, args.timeout)


# Request statuses that mean a cluster request is no longer running
DONE_STATUSES = {"COMPLETED", "FAILED", "CANCELLED", "ERROR"}
# The scaling PUT lands as its own request, which completes within a minute or
# two. The resize it triggers shows up as a separate request shortly after, so a
# scale is only finished once that second request is done.
SCALING_TYPES = {"UPDATE_DC_SCALING"}
# Request types that do the actual node add/remove work
RESIZE_TYPES = {"AUTOSCALING", "RESIZE_CLUSTER", "RESIZE_CLUSTER_V3", "RESIZE"}
# How long to keep waiting for the resize after the scaling update completes.
# Some UPDATE_DC_SCALING inputs are a no-op (the requested vCPU minimum needs no
# node change), so no resize is ever created - stop watching instead of hanging.
RESIZE_WAIT_SECONDS = 120
# Request types produced by a cluster create
CREATE_TYPES = {"CREATE_CLUSTER", "CREATE_CLUSTER_V2", "CREATE_CLUSTER_V3", "CREATE"}
# Request types produced by a data center add/remove
DC_TYPES = {"ADD_DC", "DELETE_DC", "ADD_DATACENTER", "DELETE_DATACENTER"}
# User-facing operations the monitor command follows by default. Everything else
# (INSTALL_MANAGER, *_CHECK, ROTATE_*, ...) is background work that runs on its
# own schedule; use --all to follow those too.
MONITOR_TYPES = CREATE_TYPES | SCALING_TYPES | RESIZE_TYPES | DC_TYPES

def get_cluster(cluster_id):
    resp = api_get(f"{API_BASE_URL}/account/{accountId}/cluster/{cluster_id}")
    return resp.get('data', {}).get('cluster', {})

def find_cluster_id(name):
    resp = api_get(f"{API_BASE_URL}/account/{accountId}/clusters")
    return next(
        (cl.get('id') for cl in resp.get('data', {}).get('clusters', [])
         if cl.get('clusterName') == name),
        None
    )

def list_requests(cluster_id):
    resp = api_get(f"{API_BASE_URL}/account/{accountId}/cluster/{cluster_id}/request")
    return resp.get('data', []) or []

def get_nodes(cluster_id):
    resp = api_get(f"{API_BASE_URL}/account/{accountId}/cluster/{cluster_id}/nodes")
    data = resp.get('data')
    # Normally {"data": {"nodes": [...]}}; tolerate a bare list. An empty node
    # list is legitimate while a cluster is still provisioning.
    nodes = data.get('nodes') if isinstance(data, dict) else data
    return [n for n in nodes if isinstance(n, dict)] if isinstance(nodes, list) else []

def node_summary(nodes):
    # Tally nodes by lifecycle status + topology state, e.g.
    # "6 nodes: 4 ACTIVE/NORMAL, 2 BOOTSTRAPPING/JOINING"
    from collections import Counter
    if not nodes:
        return "no nodes yet"
    tally = Counter(
        f"{n.get('status', '?')}/{n.get('state', '?')}" for n in nodes
    )
    breakdown = ", ".join(f"{cnt} {label}" for label, cnt in sorted(tally.items()))
    return f"{len(nodes)} nodes: {breakdown}"

def request_done(req):
    return req.get('status') in DONE_STATUSES or req.get('progressPercent', 0) >= 100

def request_age(req, now):
    # Seconds since the request was created, or None if the timestamp is unusable.
    created = req.get('createdAt')
    try:
        ts = datetime.strptime(created, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return None
    return (now - ts).total_seconds()

def request_stalled(req, now, stale_seconds):
    # Clusters keep standing requests that sit QUEUED at 0% forever (background
    # health checks, a stuck autoscaling entry). Anything queued with no progress
    # for this long is not something to wait on.
    age = request_age(req, now)
    return (req.get('status') == "QUEUED"
            and req.get('progressPercent', 0) == 0
            and age is not None and age > stale_seconds)

def instance_families(dc, instance_ids):
    # Map the DC's instance type IDs onto their families ("n2-highmem" for
    # n2-highmem-2), which is what a vertical scale sends instead of the IDs.
    catalog = api_get(f"{API_BASE_URL}/deployment/cloud-provider/"
                      f"{dc.get('cloudProviderId')}/region/{dc.get('regionId')}")
    families = {i.get('id'): i.get('instanceFamily')
                for i in catalog.get('data', {}).get('instances', [])}
    return sorted({families[i] for i in instance_ids if families.get(i)})

def handle_scale(args):
    cluster_id = args.cluster
    target_vcpu = args.vcpu
    target_tib = args.tib

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

    # Preserve the existing scaling policy, changing only the vCPU/storage minimums.
    scaling = dc.get('scaling', {}) or {}
    policies = scaling.get('policies', {}) or {}
    storage = dict(policies.get('storage', {}) or {"min": 0, "targetUtilization": 0.8})
    current_min = (policies.get('vcpu', {}) or {}).get('min')
    current_storage_min = storage.get('min')

    # The API docs say GB, but the values are GiB: they are compared against the
    # instance totalStorage field, and a GCP local SSD reported there as 375 is a
    # 375 GiB disk. So a TiB argument converts with 1024, not 1000.
    target_gib = target_tib * 1024 if target_tib is not None else None
    if target_gib is not None:
        storage['min'] = target_gib

    new_scaling = {
        "mode": cluster.get('scalingMode', 'xcloud'),
        "policies": {
            "storage": storage,
            "vcpu": {"min": target_vcpu}
        }
    }

    # The API requires exactly one of instanceTypeIDs or instanceFamilies (error
    # 040737 if you send neither). Horizontal pins the exact type(s) the cluster
    # already runs, so a resize adds more of the same. Vertical names the family
    # instead, leaving the API free to pick a different size within it.
    instance_ids = scaling.get('instanceTypeIDs') or sorted(
        {n['instanceId'] for n in get_nodes(cluster_id) if n.get('instanceId')}
    )
    if not instance_ids:
        print(f"ERROR: Could not determine the current instance type for "
              f"cluster {cluster_id}")
        sys.exit(1)

    if args.direction == "horizontal":
        new_scaling["instanceTypeIDs"] = instance_ids
    else:
        families = instance_families(dc, instance_ids)
        if not families:
            print(f"ERROR: Could not resolve an instance family for instance "
                  f"type(s) {instance_ids}; use --horizontal to pin them instead")
            sys.exit(1)
        new_scaling["instanceFamilies"] = families

    print(f"Scaling cluster '{cluster.get('clusterName')}' ({cluster_id}) "
          f"vCPU minimum: {current_min} -> {target_vcpu} ({args.direction})")
    if target_gib is not None:
        print(f"Storage minimum (GiB): {current_storage_min} -> {target_gib} "
              f"({target_tib} TiB)")
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

    monitor_scale(cluster_id, before_ids, args.interval, args.timeout, args.resize_wait)

def all_done(reqs):
    return bool(reqs) and all(request_done(r) for r in reqs)

def monitor_cluster(cluster_id, interval, timeout, match, label,
                    wait_for_request=True, extra_summary=None, start=None,
                    complete_when=all_done, waiting_for=None, follow_up_grace=600):
    """Poll a cluster until the requests matching `match` are done.

    `match` selects the requests this monitor cares about; `label` names the
    operation in progress/completion messages. With wait_for_request=False an
    empty match set means "nothing to wait for" rather than "not created yet".
    `extra_summary(cluster)` may return a string appended to the final line.
    `complete_when(reqs)` decides when to stop: the default is "everything
    tracked is done", but a scale also has to wait for the follow-on resize
    request to appear, which is what `waiting_for` names in the meantime. A policy
    update does not always trigger one - some UPDATE_DC_SCALING inputs are a no-op,
    because the requested vCPU minimum needs no node change - so that wait gives up
    after `follow_up_grace` seconds instead of holding on until `timeout`.
    """
    waiting_for = waiting_for or f"{label.lower()} request"
    if start is None:
        start = time.time()
    deadline = start + timeout
    seen = False
    settled_at = None

    while time.time() < deadline:
        ts = time.strftime("%Y-%m-%d %H:%M:%S")

        try:
            cluster = get_cluster(cluster_id)
            nodes = get_nodes(cluster_id)
            reqs = [r for r in list_requests(cluster_id) if match(r)]
        except requests.HTTPError as e:
            print(f"  {ts} Cluster not queryable yet ({e})")
            print("-" * 40)
            time.sleep(interval)
            continue

        print(f"  {ts} cluster status: {cluster.get('status')}, {node_summary(nodes)}")
        for n in sorted(nodes, key=lambda x: x.get('id', 0)):
            print(f"      node {n.get('id')} [{n.get('rackName', '?')}]: "
                  f"{n.get('status', '?')}/{n.get('state', '?')}")
        for r in sorted(reqs, key=lambda x: x.get('id', 0)):
            pct = r.get('progressPercent', 0)
            desc = r.get('progressDescription') or r.get('status', '')
            err = r.get('userFriendlyError')
            line = f"  {ts} [{r.get('requestType')}] {r.get('status')} {pct}% - {desc}"
            if err:
                line += f" (error: {err})"
            print(line)

        failed = [r for r in reqs if r.get('status') in ("FAILED", "ERROR", "CANCELLED")]
        if failed:
            print(f"{label} did not complete successfully.")
            sys.exit(1)

        if reqs:
            seen = True

        # Done when the completion rule is satisfied, or when nothing is left to
        # track (the requests we were following dropped out of the match set).
        # Everything tracked is finished, but the completion rule wants a request
        # that has not appeared - start the clock on how long we hold out for it.
        if reqs and all_done(reqs) and not complete_when(reqs):
            settled_at = settled_at or time.time()
        elif not reqs and not seen:
            # Nothing has been created at all - bound this wait too, otherwise a
            # submit the API quietly declined holds the loop until `timeout`.
            settled_at = settled_at or start
        else:
            settled_at = None

        outcome = None
        if reqs and complete_when(reqs):
            outcome = f"{label} complete in"
        elif not reqs and (seen or not wait_for_request):
            outcome = f"{label}: nothing left to follow after"
        elif settled_at and time.time() - settled_at > follow_up_grace:
            if seen:
                outcome = (f"{label}: request completed but no {waiting_for} appeared "
                           f"within {follow_up_grace}s - nothing to resize. Finished after")
            else:
                print(f"ERROR: no {waiting_for} was created within {follow_up_grace}s "
                      f"- the submit may have been declined by the API")
                sys.exit(1)

        if outcome:
            elapsed = int(time.time() - start)
            # Re-read: cluster status trails the request, so the value fetched at
            # the top of this poll is usually one step behind by now.
            cluster = get_cluster(cluster_id)
            extra = f"{extra_summary(cluster)}, " if extra_summary else ""
            print(f"{outcome} {elapsed // 60}m{elapsed % 60}s. "
                  f"Cluster {cluster_id} status: {cluster.get('status')}, {extra}"
                  f"{node_summary(get_nodes(cluster_id))}")
            return

        # Either nothing has been created yet, or everything tracked so far is
        # done but the operation still needs a follow-on request.
        if not reqs or all_done(reqs):
            print(f"  {ts} Waiting for {waiting_for} to be created...")

        print("-" * 40)
        time.sleep(interval)

    print(f"Timed out after {timeout}s waiting for {label.lower()} to complete.")
    sys.exit(1)

def monitor_scale(cluster_id, before_ids, interval, timeout,
                  resize_wait=RESIZE_WAIT_SECONDS):
    print("\nMonitoring resize progress (Ctrl-C to stop watching)...")

    def vcpu_min(cluster):
        min_ = (((cluster.get('dc') or {}).get('scaling') or {})
                .get('policies', {}).get('vcpu', {}).get('min'))
        return f"vCPU minimum: {min_}"

    # Only consider requests created by this scale operation: first the scaling
    # policy update, then the resize it triggers a few minutes later.
    monitor_cluster(
        cluster_id, interval, timeout,
        match=lambda r: (r.get('id') not in before_ids
                         and r.get('requestType') in SCALING_TYPES | RESIZE_TYPES),
        label="Resize",
        extra_summary=vcpu_min,
        complete_when=resize_done,
        waiting_for="resize request",
        follow_up_grace=resize_wait
    )

def resize_done(reqs):
    # The scaling update finishing is not the end of the scale - hold out for the
    # resize request, otherwise we would report success before any node moves.
    resizes = [r for r in reqs if r.get('requestType') in RESIZE_TYPES]
    return bool(resizes) and all_done(reqs)

def monitor_create(cluster_name, request_id, interval, timeout):
    print("\nMonitoring cluster creation (Ctrl-C to stop watching)...")
    start = time.time()
    deadline = start + timeout

    # The create response carries only a request ID; the cluster itself shows up
    # in the account listing a moment later.
    cluster_id = None
    while cluster_id is None and time.time() < deadline:
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        cluster_id = find_cluster_id(cluster_name)
        if cluster_id is None:
            print(f"  {ts} Waiting for cluster '{cluster_name}' to appear...")
            print("-" * 40)
            time.sleep(interval)

    if cluster_id is None:
        print(f"Timed out after {timeout}s waiting for cluster '{cluster_name}' to appear.")
        sys.exit(1)
    print(f"  {time.strftime('%Y-%m-%d %H:%M:%S')} Cluster '{cluster_name}' has ID {cluster_id}")

    # Match the request this create produced; fall back to any create-type
    # request for the cluster if the ID was not returned.
    monitor_cluster(
        cluster_id, interval, timeout,
        match=lambda r: (r.get('id') == request_id
                         or r.get('requestType') in CREATE_TYPES),
        label="Create",
        start=start
    )

def handle_monitor(args):
    cluster_id = args.cluster
    cluster = get_cluster(cluster_id)
    if not cluster:
        print(f"ERROR: Cluster {cluster_id} not found")
        sys.exit(1)

    now = datetime.now(timezone.utc)
    stale_seconds = args.stale_minutes * 60
    pending = [r for r in list_requests(cluster_id) if not request_done(r)]
    if args.all:
        active = pending
    else:
        active = [
            r for r in pending
            if r.get('requestType') in MONITOR_TYPES
            and not request_stalled(r, now, stale_seconds)
        ]

    skipped = [r for r in pending if r not in active]
    if skipped:
        skipped_types = ", ".join(sorted({r.get('requestType', '?') for r in skipped}))
        print(f"Ignoring {len(skipped)} background/stalled request(s) "
              f"[{skipped_types}] (use --all to include them)")

    if not active:
        print(f"No requests in progress for cluster '{cluster.get('clusterName')}' "
              f"({cluster_id}). Status: {cluster.get('status')}, "
              f"{node_summary(get_nodes(cluster_id))}")
        return

    types = ", ".join(sorted({r.get('requestType', '?') for r in active}))
    print(f"Monitoring cluster '{cluster.get('clusterName')}' ({cluster_id}): "
          f"{len(active)} request(s) in progress [{types}] (Ctrl-C to stop watching)...")

    # Follow the requests that were already running when we attached, plus any
    # resize that shows up later (a scaling update in flight triggers one).
    # Staleness is re-checked every poll, so a request that never leaves QUEUED
    # drops out instead of holding the monitor until --timeout.
    active_ids = {r.get('id') for r in active}
    known_ids = {r.get('id') for r in pending} | {r.get('id') for r in list_requests(cluster_id)}

    def still_tracking(req):
        tracked = (req.get('id') in active_ids
                   or (req.get('requestType') in RESIZE_TYPES
                       and req.get('id') not in known_ids))
        if not tracked:
            return False
        return args.all or not request_stalled(req, datetime.now(timezone.utc), stale_seconds)

    def monitor_complete(reqs):
        # Attaching mid-scale means the resize has not been created yet.
        if any(r.get('requestType') in SCALING_TYPES for r in reqs):
            return resize_done(reqs)
        return all_done(reqs)

    monitor_cluster(
        cluster_id, args.interval, args.timeout,
        match=still_tracking,
        label="Cluster operation",
        wait_for_request=False,
        complete_when=monitor_complete,
        waiting_for="follow-on resize request",
        follow_up_grace=RESIZE_WAIT_SECONDS
    )


def main():
    # Keep progress streaming when stdout is a pipe or file; Python block-buffers
    # those by default, which hides monitoring output until the process exits.
    sys.stdout.reconfigure(line_buffering=True)

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
    elif args.command == "monitor":
        handle_monitor(args)
    else:
        parser.print_help()
        sys.exit(1)

if __name__ == "__main__":
    try:
        main()
    except ApiError as e:
        print(f"ERROR: {e}")
        sys.exit(1)

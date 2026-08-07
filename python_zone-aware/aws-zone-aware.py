#!/usr/bin/env python3

import argparse
from json import load
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.concurrent import execute_concurrent_with_args
from cassandra import ConsistencyLevel
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy, WhiteListRoundRobinPolicy, RoundRobinPolicy, HostFilterPolicy, DefaultLoadBalancingPolicy
from cassandra.auth import PlainTextAuthProvider
from cassandra.connection import UnixSocketEndPoint
from cassandra.query import SimpleStatement, ordered_dict_factory, TraceUnavailable
from ssl import SSLContext, TLSVersion, CERT_REQUIRED, PROTOCOL_TLS_CLIENT

parser = argparse.ArgumentParser(description='Connect to ScyllaDB with AZ awareness.')
parser.add_argument('-n', '--node-index', type=int, default=1, help='Index of the node and AZ to connect to (0, 1, or 2).')
parser.add_argument('-p', '--policy-index', type=int, default=1, help='Index of the policy to use (0, 1, or 2).')
args = parser.parse_args()
n = args.node_index

# ScyllaDB Cloud connection details (from your cluster console)
azs = [
       "usw2-az2", 
       "usw2-az1", 
       "usw2-az3"
]  # Example AZs in AWS US West 2   
nodes = [ 
       "node-0.aws-us-west-2.6081f40a1bb45d3c6daa.clusters.scylla.cloud",
       "node-1.aws-us-west-2.6081f40a1bb45d3c6daa.clusters.scylla.cloud",
       "node-2.aws-us-west-2.6081f40a1bb45d3c6daa.clusters.scylla.cloud", 
]
CONTACT_POINTS = [
    nodes[n],  # AWS US West 2 AZ1
]
USERNAME = "scylla"  
PASSWORD = "DZL9wN5PAfGl2Eh"
KEYSPACE = "mykeyspace"
TABLE = "mytable"
DC="AWS_US_WEST_2"  # Your region as DC name in ScyllaDB

# Zone-aware load balancing policy
# Prioritizes replicas in local AZ ('us-west-2a'), then other AZs in same DC
# Policy 0: TokenAwarePolicy with DCAwareRoundRobinPolicy (local DC, no remote hosts)
# Policy 1: TokenAwarePolicy with WhiteListRoundRobinPolicy (only contact points)
load_balancing_policy = [None] * 2
dc_aware_policy = DCAwareRoundRobinPolicy(local_dc=DC, used_hosts_per_remote_dc=0)
whitelist_policy = WhiteListRoundRobinPolicy(CONTACT_POINTS)
load_balancing_policy[0] = TokenAwarePolicy(dc_aware_policy)
load_balancing_policy[1] = TokenAwarePolicy(whitelist_policy)

profile = ExecutionProfile(
    load_balancing_policy=load_balancing_policy[args.policy_index],
)

# Connect with AZ awareness
cluster = Cluster(
    contact_points=CONTACT_POINTS,
    execution_profiles={EXEC_PROFILE_DEFAULT: profile},
    protocol_version=4,
    connect_timeout=30,
    control_connection_timeout=30,
    auth_provider=PlainTextAuthProvider(username=USERNAME, password=PASSWORD)
    # Optional: connection pooling tweaks for AWS
)

session = cluster.connect(KEYSPACE)
print(f"✅ Connected from AZ: {azs[n]} to ScyllaDB (DC=us-west-2, 3 AZs)")

# Test: insert + query (replicas span 3 AZs, driver prefers local)
try:
    result = session.execute(f"""
        INSERT INTO {KEYSPACE}.{TABLE} (id, ssn, message) 
        VALUES (0, '290-123-4567', 'Hello from my AZ')
    """)
    rows = session.execute(f"SELECT * FROM {KEYSPACE}.{TABLE} where id=0")
    for row in rows:
        print(f"   ID: {row.id}, Value: {row.message}, AZ-preferred read")

    print(f'✅ Inserted data with AZ-aware policy')
except Exception as e:
    print(f'❌ Error inserting data: {e}')

# Query

# Show topology awareness
locals = session.execute("SELECT key , host_id, data_center, rack, listen_address FROM system.local;")
print("\n📍 Cluster AZs (racks) visible to driver:")
for local in locals:
    print(f"   Peer: {local.key}, host_id: {local.host_id}, DC: {local.data_center}, Rack(AZ): {local.rack}, preferred IP: {local.listen_address}")

# Show topology awareness
peers = session.execute("SELECT peer, host_id, data_center, rack, preferred_ip from system.peers;")
print("\n📍 Cluster AZs (racks) visible to driver:")
for peer in peers:
    print(f"  Peer: {peer.peer}, host_id: {peer.host_id}, DC: {peer.data_center}, Rack(AZ): {peer.rack}, preferred IP: {peer.preferred_ip}")

cluster.shutdown()

#!/usr/bin/env python3
import os
import uuid

from cassandra import ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider
from cassandra.client_routes import ClientRouteProxy
from cassandra.cluster import (
    EXEC_PROFILE_DEFAULT,
    ClientRoutesConfig,
    Cluster,
    ExecutionProfile,
)
from cassandra.policies import DCAwareRoundRobinPolicy, RoundRobinPolicy

# PSC: set SCYLLA_PSC_DNS and SCYLLA_PSC_CONN_ID, or replace the placeholders below.
PSC_DNS = os.environ.get("SCYLLA_PSC_DNS", "<placeholder, insert your PSC endpoint DNS name here>")
PSC_CONN_ID = os.environ.get("SCYLLA_PSC_CONN_ID", "<placeholder, insert your PSC connection_id here>")
PSC_PORT = 9000

SCYLLA_USER = os.environ.get("SCYLLA_USER", "<placeholder, insert your user id here>")
SCYLLA_PASSWORD = os.environ.get("SCYLLA_PASSWORD", "<placeholder, insert your password here>")
SCYLLA_DC = os.environ.get("SCYLLA_DC", "")

KEYSPACE = "test_psc"
TABLE = "users"


def build_cluster():
    """Build the PSC-routed Cluster. The caller owns its lifetime."""
    # execution profile: load balancing, consistency and timeouts for every query
    profile = ExecutionProfile(
        load_balancing_policy=DCAwareRoundRobinPolicy(local_dc=SCYLLA_DC) if SCYLLA_DC else RoundRobinPolicy(),
        consistency_level=ConsistencyLevel.LOCAL_QUORUM if SCYLLA_DC else ConsistencyLevel.QUORUM,
        request_timeout=15,
    )
    return Cluster(
        contact_points=[PSC_DNS],
        port=PSC_PORT,
        auth_provider=PlainTextAuthProvider(SCYLLA_USER, SCYLLA_PASSWORD),
        execution_profiles={EXEC_PROFILE_DEFAULT: profile},
        client_routes_config=ClientRoutesConfig(
            proxies=[ClientRouteProxy(conn_id) for conn_id in PSC_CONN_ID.split(",")]
        ),
        protocol_version=4,
    )


def create_schema(session, ks, table):
    print(f"Creating keyspace {ks} and table {table}...")
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {ks}
        WITH replication = {{'class': 'org.apache.cassandra.locator.NetworkTopologyStrategy', 'replication_factor': '3'}}
    """)
    session.set_keyspace(ks)
    session.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            user_id uuid PRIMARY KEY,
            name text,
            email text
        )
    """)


def drop_schema(session, ks, table):
    print("Dropping table and keyspace...")
    session.execute(f"DROP TABLE IF EXISTS {ks}.{table}")
    session.execute(f"DROP KEYSPACE IF EXISTS {ks}")


def run_example(session, table):
    """Run the CRUD example against an already-connected session."""
    # Prepared statements: parsed once server-side, then reused with bound values.
    insert_stmt = session.prepare(f"INSERT INTO {table} (user_id, name, email) VALUES (?, ?, ?)")
    select_stmt = session.prepare(f"SELECT * FROM {table} WHERE user_id = ?")
    update_stmt = session.prepare(f"UPDATE {table} SET email = ? WHERE user_id = ?")
    delete_stmt = session.prepare(f"DELETE FROM {table} WHERE user_id = ?")

    # --- CREATE (Insert) ---
    user_id = uuid.uuid4()
    print(f"Inserting user: {user_id}")
    session.execute(insert_stmt, [user_id, "Alice", "alice@example.com"])

    # --- READ ---
    print("Reading user...")
    row = session.execute(select_stmt, [user_id]).one()
    if row:
        print(f"Found: {row.name} ({row.email})")

    # --- UPDATE ---
    print("Updating user email...")
    session.execute(update_stmt, ["alice_new@example.com", user_id])

    # Verify Update
    updated_row = session.execute(select_stmt, [user_id]).one()
    print(f"New email: {updated_row.email}")

    # --- DELETE (Row) ---
    print("Deleting user record...")
    session.execute(delete_stmt, [user_id])

    # Verify Deletion
    check = session.execute(select_stmt, [user_id]).one()
    print(f"User exists after delete? {check is not None}")


def main():
    print(f"Connection to Endpoint: {PSC_DNS} on port {PSC_PORT} using connection_id {PSC_CONN_ID}")
    # `with` shuts the cluster (and its sessions) down on every exit path.
    with build_cluster() as cluster:
        session = cluster.connect()
        create_schema(session, KEYSPACE, TABLE)
        try:
            run_example(session, TABLE)
        finally:
            drop_schema(session, KEYSPACE, TABLE)
    print("Connection closed.")


if __name__ == "__main__":
    main()

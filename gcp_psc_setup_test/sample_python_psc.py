#!/usr/bin/env python3
import argparse
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

# Every setting can come from a command line option or, if the option is
# omitted, from the matching environment variable shown in the help text.
DEFAULT_PSC_DNS = "<placeholder, insert your PSC endpoint DNS name here>"
DEFAULT_PSC_CONN_ID = "<placeholder, insert your PSC connection_id here>"
DEFAULT_PSC_PORT = 9000
DEFAULT_USER = "<placeholder, insert your user id here>"
DEFAULT_PASSWORD = "<placeholder, insert your password here>"
DEFAULT_KEYSPACE = "test_psc"
DEFAULT_TABLE = "users"


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="ScyllaDB Cloud PSC connectivity sample: connects over a Private "
                    "Service Connect endpoint and runs a small CRUD example.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--psc-dns", default=os.environ.get("SCYLLA_PSC_DNS", DEFAULT_PSC_DNS),
        help="PSC endpoint DNS name [env: SCYLLA_PSC_DNS]",
    )
    parser.add_argument(
        "--psc-conn-id", default=os.environ.get("SCYLLA_PSC_CONN_ID", DEFAULT_PSC_CONN_ID),
        help="PSC connection_id; comma separated for several proxies [env: SCYLLA_PSC_CONN_ID]",
    )
    parser.add_argument(
        "--port", type=int, default=int(os.environ.get("SCYLLA_PSC_PORT", DEFAULT_PSC_PORT)),
        help="CQL port on the PSC endpoint [env: SCYLLA_PSC_PORT]",
    )
    parser.add_argument(
        "--user", default=os.environ.get("SCYLLA_USER", DEFAULT_USER),
        help="ScyllaDB user [env: SCYLLA_USER]",
    )
    parser.add_argument(
        "--password", default=os.environ.get("SCYLLA_PASSWORD", DEFAULT_PASSWORD),
        help="ScyllaDB password [env: SCYLLA_PASSWORD] "
             "(default: %(default).0shidden)",
    )
    parser.add_argument(
        "--dc", default=os.environ.get("SCYLLA_DC", ""),
        help="Local datacenter; enables DC-aware routing and LOCAL_QUORUM [env: SCYLLA_DC]",
    )
    parser.add_argument(
        "--keyspace", default=os.environ.get("SCYLLA_KEYSPACE", DEFAULT_KEYSPACE),
        help="Keyspace to create and use [env: SCYLLA_KEYSPACE]",
    )
    parser.add_argument(
        "--table", default=os.environ.get("SCYLLA_TABLE", DEFAULT_TABLE),
        help="Table to create and use [env: SCYLLA_TABLE]",
    )
    parser.add_argument(
        "--replication-factor", type=int,
        default=int(os.environ.get("SCYLLA_REPLICATION_FACTOR", 3)),
        help="Replication factor for the created keyspace [env: SCYLLA_REPLICATION_FACTOR]",
    )
    parser.add_argument(
        "--request-timeout", type=float,
        default=float(os.environ.get("SCYLLA_REQUEST_TIMEOUT", 15)),
        help="Per-request timeout in seconds [env: SCYLLA_REQUEST_TIMEOUT]",
    )
    parser.add_argument(
        "--protocol-version", type=int,
        default=int(os.environ.get("SCYLLA_PROTOCOL_VERSION", 4)),
        help="CQL native protocol version [env: SCYLLA_PROTOCOL_VERSION]",
    )
    parser.add_argument(
        "--keep-schema", action="store_true",
        default=os.environ.get("SCYLLA_KEEP_SCHEMA", "").lower() in ("1", "true", "yes"),
        help="Leave the keyspace and table in place instead of dropping them "
             "at the end [env: SCYLLA_KEEP_SCHEMA]",
    )
    return parser.parse_args(argv)


def build_cluster(args):
    """Build the PSC-routed Cluster. The caller owns its lifetime."""
    # execution profile: load balancing, consistency and timeouts for every query
    profile = ExecutionProfile(
        load_balancing_policy=DCAwareRoundRobinPolicy(local_dc=args.dc) if args.dc else RoundRobinPolicy(),
        consistency_level=ConsistencyLevel.LOCAL_QUORUM if args.dc else ConsistencyLevel.QUORUM,
        request_timeout=args.request_timeout,
    )
    return Cluster(
        contact_points=[args.psc_dns],
        port=args.port,
        auth_provider=PlainTextAuthProvider(args.user, args.password),
        execution_profiles={EXEC_PROFILE_DEFAULT: profile},
        client_routes_config=ClientRoutesConfig(
            proxies=[ClientRouteProxy(conn_id) for conn_id in args.psc_conn_id.split(",")]
        ),
        protocol_version=args.protocol_version,
    )


def create_schema(session, ks, table, replication_factor):
    print(f"Creating keyspace {ks} and table {table}...")
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {ks}
        WITH replication = {{'class': 'org.apache.cassandra.locator.NetworkTopologyStrategy', 'replication_factor': '{replication_factor}'}}
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


def main(argv=None):
    args = parse_args(argv)
    print(f"Connection to Endpoint: {args.psc_dns} on port {args.port} "
          f"using connection_id {args.psc_conn_id}")
    # `with` shuts the cluster (and its sessions) down on every exit path.
    with build_cluster(args) as cluster:
        session = cluster.connect()
        create_schema(session, args.keyspace, args.table, args.replication_factor)
        try:
            run_example(session, args.table)
        finally:
            if args.keep_schema:
                print(f"Keeping keyspace {args.keyspace} and table {args.table}.")
            else:
                drop_schema(session, args.keyspace, args.table)
    print("Connection closed.")


if __name__ == "__main__":
    main()

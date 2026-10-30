#!/usr/bin/env python3
import argparse
import os
import ssl
import time
import uuid

import cassandra.connection
from cassandra import ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider
from cassandra.client_routes import ClientRouteProxy
from cassandra.cluster import (
    EXEC_PROFILE_DEFAULT,
    ClientRoutesConfig,
    Cluster,
    ExecutionProfile,
    ShardAwareOptions,
)
from cassandra.policies import DCAwareRoundRobinPolicy, RoundRobinPolicy

# Every setting can come from a command line option or, if the option is
# omitted, from the matching environment variable shown in the help text.
# Two connection models: psc (PSC endpoint + client routes) or direct (node addresses, e.g. over VPC peering).
DEFAULT_PSC_PORT = 9000
DEFAULT_USER = "<placeholder, insert your user id here>"
DEFAULT_PASSWORD = "<placeholder, insert your password here>"
DEFAULT_KEYSPACE = "test_psc"
DEFAULT_TABLE = "users"

# Local ports the driver binds for shard-aware connections (its default 49152-65535 overlaps the OS ephemeral
# range). Below 32768, a port in this range is one the driver chose for a shard.
SHARD_PORT_LOW = 10000
SHARD_PORT_HIGH = 20000


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="ScyllaDB Cloud connectivity sample: connects over a Private Service Connect endpoint "
                    "(psc) or straight to the nodes, e.g. over VPC peering (direct), checks shard-awareness "
                    "and runs a small CRUD example.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--mode", choices=("psc", "direct"), default=os.environ.get("SCYLLA_MODE") or None,
        help="psc or direct (default: psc when a connection_id is given, else direct) [env: SCYLLA_MODE]",
    )
    parser.add_argument(
        "--psc-dns", default=os.environ.get("SCYLLA_PSC_DNS", ""),
        help="psc: PSC endpoint DNS name [env: SCYLLA_PSC_DNS]",
    )
    parser.add_argument(
        "--psc-conn-id", default=os.environ.get("SCYLLA_PSC_CONN_ID", ""),
        help="psc: PSC connection_id; comma separated for several proxies [env: SCYLLA_PSC_CONN_ID]",
    )
    parser.add_argument(
        "--contact-points", default=os.environ.get("SCYLLA_CONTACT_POINTS", ""),
        help="direct: node addresses, comma separated [env: SCYLLA_CONTACT_POINTS]",
    )
    parser.add_argument(
        "--port", type=int,
        default=int(os.environ.get("SCYLLA_PORT") or os.environ.get("SCYLLA_PSC_PORT") or 0) or None,
        help="CQL port (default: psc 9000, direct 9042 or 9142 with TLS) [env: SCYLLA_PORT]",
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
        "--ca-file", default=os.environ.get("SCYLLA_CA_FILE", ""),
        help="Cluster CA certificate (PEM); enables TLS, required by a TLS PSC connection "
             "[env: SCYLLA_CA_FILE]",
    )
    parser.add_argument(
        "--tls-name", default=os.environ.get("SCYLLA_TLS_NAME", ""),
        help="Cluster TLS name, sent as SNI. The driver cannot verify it while client routes "
             "are on, so only the CA chain is checked [env: SCYLLA_TLS_NAME]",
    )
    parser.add_argument(
        "--shard-aware", choices=("true", "false"),
        default=os.environ.get("SCYLLA_SHARD_AWARE", "true").lower(),
        help="Advanced shard awareness: bind a source port per shard, which the node can only honour if "
             "PPv2 carries the source port through PSC [env: SCYLLA_SHARD_AWARE]",
    )
    parser.add_argument(
        "--pool-wait", type=float, default=float(os.environ.get("SCYLLA_POOL_WAIT", 5)),
        help="Seconds to let the connection pools fill before the shard check [env: SCYLLA_POOL_WAIT]",
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
    args = parser.parse_args(argv)
    if args.mode is None:
        args.mode = "psc" if args.psc_conn_id else "direct"
    if args.mode == "psc" and not (args.psc_dns and args.psc_conn_id):
        parser.error("psc mode needs --psc-dns and --psc-conn-id")
    if args.mode == "direct" and not args.contact_points:
        parser.error("direct mode needs --contact-points")
    if args.port is None:
        args.port = DEFAULT_PSC_PORT if args.mode == "psc" else 9142 if args.ca_file else 9042
    return args


def build_cluster(args):
    """Build the Cluster for the chosen connection model. The caller owns its lifetime."""
    # execution profile: load balancing, consistency and timeouts for every query
    profile = ExecutionProfile(
        load_balancing_policy=DCAwareRoundRobinPolicy(local_dc=args.dc) if args.dc else RoundRobinPolicy(),
        consistency_level=ConsistencyLevel.LOCAL_QUORUM if args.dc else ConsistencyLevel.QUORUM,
        request_timeout=args.request_timeout,
    )
    shard_aware = args.shard_aware == "true"
    tls = {}
    if args.ca_file:
        # Verify the CA chain and send the cluster TLS name as SNI; the node certificates carry it. The driver
        # (3.29) refuses check_hostname together with client_routes_config, so only direct mode verifies the
        # name itself (gocql and the Java sample verify it in both modes)
        ssl_context = ssl.create_default_context(cafile=args.ca_file)
        ssl_context.check_hostname = args.mode == "direct" and bool(args.tls_name)
        tls["ssl_context"] = ssl_context
        if args.tls_name:
            tls["ssl_options"] = {"server_hostname": args.tls_name}

    if args.mode == "psc":
        # advanced_shard_awareness: the driver binds a source port with port % shards == wanted shard and still
        # connects to the client_routes port (tls_port for TLS), not to the advertised 19042/19142
        routing = {
            "contact_points": [args.psc_dns],
            "client_routes_config": ClientRoutesConfig(
                proxies=[ClientRouteProxy(conn_id) for conn_id in args.psc_conn_id.split(",")],
                advanced_shard_awareness=shard_aware,
            ),
        }
    else:
        # direct: the driver dials the node's shard-aware port (19042, or 19142 with TLS) from a source port
        # chosen for the shard, the normal ScyllaDB setup
        routing = {
            "contact_points": [cp.strip() for cp in args.contact_points.split(",")],
            "shard_aware_options": ShardAwareOptions(disable_shardaware_port=not shard_aware),
        }
    return Cluster(
        port=args.port,
        auth_provider=PlainTextAuthProvider(args.user, args.password),
        execution_profiles={EXEC_PROFILE_DEFAULT: profile},
        protocol_version=args.protocol_version,
        **routing,
        **tls,
    )


def _local_addr(conn):
    """(ip, port) of a driver connection's socket, or None."""
    sock = getattr(conn, "_socket", None)
    try:
        return sock.getsockname()[:2] if sock else None
    except OSError:
        return None


def check_shard_awareness(cluster, session, mode):
    """
    For every node: shard count and this session's connections as the node sees them in system.clients
    (source address and port, shard, TLS). Our connections are found by their local (ip, port), read from the
    driver's own pools. A shard-requested connection (source port the driver bound for a shard) that lands on
    port % shards == shard_id shows that the node got the driver's source port through PSC (PPv2).
    """
    ours = {}
    for pool in session.get_pools():
        for conn in list(pool._connections.values()) + list(pool._excess_connections):
            addr = _local_addr(conn)
            if addr:
                ours[addr] = "pool"
    control = getattr(cluster.control_connection, "_connection", None)
    addr = _local_addr(control) if control else None
    if addr:
        ours[addr] = "control"

    print("\n=== Shard-awareness check (system.clients on each node) ===")
    chosen = chosen_ok = arbitrary = arbitrary_ok = 0
    for host in cluster.metadata.all_hosts():
        info = host.sharding_info
        shards = info.shards_count if info else 0
        print(f"\nnode {host.host_id}  dc {host.datacenter}  rack {host.rack}")
        print(f"  shards {shards}, advertised shard-aware ports "
              f"{info.shard_aware_port if info else '-'} / {info.shard_aware_port_ssl if info else '-'} (ssl)"
              + (" - not used by client routes" if mode == "psc" else " - dialed directly"))
        if not shards:
            print("  no sharding info (no connection to this node yet)")
            continue
        try:
            rows = session.execute("SELECT * FROM system.clients", host=host)
        except Exception as exc:
            print(f"  could not read system.clients: {exc}")
            continue
        node_chosen = node_chosen_ok = node_arb = node_arb_ok = 0
        reached = set()
        for row in rows:
            kind = ours.get((str(row.address), row.port))
            if kind is None:
                continue
            ok = row.port % shards == row.shard_id
            requested = SHARD_PORT_LOW <= row.port < SHARD_PORT_HIGH
            if requested:
                node_chosen += 1
                node_chosen_ok += ok
            else:
                node_arb += 1
                node_arb_ok += ok
            reached.add(row.shard_id)
            tls = getattr(row, "ssl_enabled", None)
            print(f"  {row.address}:{row.port:<6} shard {row.shard_id:2d}  "
                  f"{'port%shards ok' if ok else f'expected {row.port % shards}':14s} "
                  f"{'shard-req' if requested else 'arbitrary':9s} {kind:7s}"
                  + ("" if tls is None else f"  TLS {getattr(row, 'ssl_protocol', '')}" if tls else "  plain"))
        chosen += node_chosen
        chosen_ok += node_chosen_ok
        arbitrary += node_arb
        arbitrary_ok += node_arb_ok
        print(f"  => shard-requested {node_chosen_ok}/{node_chosen} on the requested shard, "
              f"arbitrary {node_arb_ok}/{node_arb} match, {len(reached)}/{shards} shards have a connection")

    print(f"\nArbitrary-port connections matching port % shards: {arbitrary_ok}/{arbitrary} "
          "(chance alone: about 1/shards)")
    if chosen == 0:
        print("RESULT: no shard-requested connections found (--shard-aware false, pool not filled yet, or the "
              "first connection already reached every shard); try --pool-wait 15")
    elif chosen < 10:
        # the driver asks only for the shards the first connection missed, and a node that hands out shards in
        # turn gets those right too, so one connection per shard cannot tell the two apart
        print(f"RESULT: inconclusive: {chosen_ok}/{chosen} shard-requested connections on the requested shard is "
              "too small a sample (the Python driver keeps one connection per shard, and a node that hands out "
              "shards in turn gives the same result); use the Java sample with --pool-size 16")
    elif chosen_ok == chosen:
        print(f"RESULT: shard-aware routing works end to end: {chosen_ok}/{chosen} shard-requested connections "
              "landed on the requested shard")
    else:
        print(f"RESULT: {chosen_ok}/{chosen} shard-requested connections on the requested shard (chance alone: "
              "about 1/shards): the node does not place connections by the driver's source port. Requests are "
              "still routed to the right shard's connection, but filling every shard needs extra connection "
              "attempts")
    print()


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
    target = (f"PSC endpoint {args.psc_dns} on port {args.port} using connection_id {args.psc_conn_id}"
              if args.mode == "psc" else f"nodes {args.contact_points} (direct) on port {args.port}")
    print(f"Connection to {target}"
          + (f", TLS (CA {args.ca_file}, {'verified' if args.mode == 'direct' and args.tls_name else 'SNI'} "
             f"{args.tls_name or '<none>'})" if args.ca_file else "")
          + f", shard-aware {args.shard_aware}")
    # `with` shuts the cluster (and its sessions) down on every exit path.
    # module-level generator, read when a shard-aware connection binds its source port
    cassandra.connection.DefaultShardAwarePortGenerator.start_port = SHARD_PORT_LOW
    cassandra.connection.DefaultShardAwarePortGenerator.end_port = SHARD_PORT_HIGH
    with build_cluster(args) as cluster:
        session = cluster.connect()
        print(f"Waiting {args.pool_wait:g} s for the connection pools to fill...")
        time.sleep(args.pool_wait)
        check_shard_awareness(cluster, session, args.mode)
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

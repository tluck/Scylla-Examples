import os
import uuid

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import ClientRoutesConfig, Cluster
from cassandra.client_routes import ClientRouteProxy

# PSC: set SCYLLA_PSC_DNS and SCYLLA_PSC_CONN_ID, or replace the placeholders below.
PSC_DNS = os.environ.get("SCYLLA_PSC_DNS", "<placeholder, insert your PSC DNS here>")
PSC_CONN_ID = os.environ.get("SCYLLA_PSC_CONN_ID", "<placeholder, insert your PSC connection id here>")

SCYLLA_USER = os.environ.get("SCYLLA_USER", "<placeholder, insert your user id here>")
SCYLLA_PASSWORD = os.environ.get("SCYLLA_PASSWORD", "<placeholder, insert your password here>")
SCYLLA_DC = os.environ.get("SCYLLA_DC", "")

# Alias so test code can use: cls.cluster = TestCluster(...); cls.session = cls.cluster.connect()
# TestCluster = Cluster

cluster = Cluster(
    contact_points=[PSC_DNS],
    port=9000,
    auth_provider=PlainTextAuthProvider(SCYLLA_USER, SCYLLA_PASSWORD),
    load_balancing_policy=DCAwareRoundRobinPolicy(local_dc=SCYLLA_DC) if SCYLLA_DC else RoundRobinPolicy(),
    client_routes_config=ClientRoutesConfig(
        proxies=[ClientRouteProxy(conn_id) for conn_id in PSC_CONN_ID.split(',')]
    ),
)

session = cluster.connect()

def run_example():
    try:
        # --- SCHEMA CREATION ---
        print("Creating keyspace and table...")
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS test_ks 
            WITH replication = {'class': 'org.apache.cassandra.locator.NetworkTopologyStrategy', 'replication_factor': '3'}
        """)
        session.set_keyspace('test_ks')
        
        session.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id uuid PRIMARY KEY,
                name text,
                email text
            )
        """)

        # --- CREATE (Insert) ---
        user_id = uuid.uuid4()
        print(f"Inserting user: {user_id}")
        insert_stmt = session.prepare("INSERT INTO users (user_id, name, email) VALUES (?, ?, ?)")
        session.execute(insert_stmt, [user_id, "Alice", "alice@example.com"])

        # --- READ ---
        print("Reading user...")
        row = session.execute("SELECT * FROM users WHERE user_id = %s", [user_id]).one()
        if row:
            print(f"Found: {row.name} ({row.email})")

        # --- UPDATE ---
        print("Updating user email...")
        session.execute("UPDATE users SET email = %s WHERE user_id = %s", ["alice_new@example.com", user_id])
        
        # Verify Update
        updated_row = session.execute("SELECT email FROM users WHERE user_id = %s", [user_id]).one()
        print(f"New email: {updated_row.email}")

        # --- DELETE (Row) ---
        print("Deleting user record...")
        session.execute("DELETE FROM users WHERE user_id = %s", [user_id])
        
        # Verify Deletion
        check = session.execute("SELECT * FROM users WHERE user_id = %s", [user_id]).one()
        print(f"User exists after delete? {check is not None}")

        # --- CLEANUP (Delete Table/Keyspace) ---
        print("Dropping table and keyspace...")
        session.execute("DROP TABLE users")
        session.execute("DROP KEYSPACE test_ks")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        cluster.shutdown()
        print("Connection closed.")

if __name__ == "__main__":
    run_example()


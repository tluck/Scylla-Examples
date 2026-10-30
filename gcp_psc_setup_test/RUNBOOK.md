# GCP Private Service Connect (PSC) runbook

How to set up a PSC connection to a ScyllaDB Cloud cluster on GCP and connect from a client VM in the consumer
project, for both plain CQL and TLS.

Source for the TLS steps: the wiki page
[How-to PSC TLS PPv2](https://scylladb.atlassian.net/wiki/spaces/RND/pages/493191169/How-to+PSC+TLS+PPv2).

Defaults used by the scripts: consumer project `cx-sa-lab`, region `us-west1`, network/subnet `default`.

## Key facts

- **A PSC connection is either all-TLS or all-plain.** `tls` and `proxyProtocolV2` are set when the connection
  is created. Changing them later fails with "Immutable field cannot be changed": delete the connection and
  create a new one instead. To use both, create two connections on the same cluster.
- Each connection gets its own service attachment, DNS name (`scylla-psc-nr<N>-<hash>.clusters.scylla.cloud`) and
  port set:
  - `9000-9002`: discovery ports, one of three nodes each. Point the client here.
  - `9003+`: one node each, as listed in `system.client_routes` (`port` for plain, `tls_port` for TLS).
  - On a TLS connection, **all** ports are TLS-only. A plain client cannot use them, and a TLS client cannot
    use a plain connection's ports.
- Clients must use **client routes**: the connection's `ClientRoutesConnectionID` (the same value as
  `system.client_routes.connection_id`) is passed to cqlsh (`--client-route`) or to the driver. The internal
  connection ID (for example `822`) is a different value, used only by the tooling.
- The cluster serves PSC through proxy protocol v2 (PPv2) listeners: `29042` for plain and `29142` for TLS.
  Clusters get only `29042` by default.

## Prerequisites

Account (both flags are off by default):

- `enable_private_service_connect_gcp`: needed for any PSC connection
- `enable_private_connectivity_tls`: needed for TLS connections

In staging and production, Flipt reads the flags from the `siren-flipt` repository: add the account to the
flag's segment with a pull request. In the lab, change them in the Flipt web UI.

Cluster (check on the Backoffice cluster page):

- Provider is GCP, in the same region as the consumer subnet
- Cluster Peering is `PRIVATE`
- for TLS: Cluster Encryption mode is `encrypted` or `both`
- for TLS: the TLS PPv2 listener `29142` is set (step 1)

Workstation:

- `gcloud` authenticated with rights in the consumer project (compute addresses, forwarding rules, Cloud DNS)
- `SC_TOKEN` and `SC_ACCOUNT` exported. The scripts use the public REST API for connections, the CA and
  cleanup (shared helpers in `psc_common.bash`).
- `scylla-cloud`, `sc` and `cx` for what REST does not expose: the PPv2 config (step 1), the TLS name, the
  per-node ports in `system.client_routes`, and the generated terraform. These calls are time-limited and,
  except for step 1, optional.
- `jq`, `curl`, `timeout` (on macOS: `brew install coreutils`)

Client VM: a VM in the consumer network with a cqlsh that supports `--client-route`, and the sample-app
binaries if you want them (see [Sample apps](#sample-apps)).

## Quick path: everything at once

`psc_setup_all.bash` runs steps 1-3 for both connection types, then prints the connect examples:

```bash
./psc_setup_all.bash [cluster_id] [consumer_project] [region] [interval]   # e.g. 54742 cx-sa-lab us-west1 0s
```

It does each of the following only if it isn't already done, so it is safe to re-run:

1. Adds the PPv2 listeners. This is a rolling restart, with `interval` between nodes.
2. Creates one TLS connection and one plain connection. If the cluster already has a connection of a type,
   including one created in the UI, it skips that type.
3. Creates the consumer endpoint for every PSC connection.

The sections below explain each step.

## 1. Give the cluster the TLS PPv2 listener (TLS only, once per cluster)

Skip this step for a plain-only setup.

**New cluster:** set both ports in the create request so no restart is needed later. Use
`../sc_api/scylladb_cloud_cli.py create -p gcp --psc ...`, which adds:

```json
"ProxyProtocolV2": {
  "native_transport_port_proxy_protocol": 29042,
  "native_transport_port_ssl_proxy_protocol": 29142
}
```

Swagger does not list `ProxyProtocolV2`, but the create request accepts it. A TLS PSC connection also needs
client-to-node encryption on the cluster.

**Existing cluster:** this needs a rolling restart, and there is no REST call for it. The script checks first
and does nothing if both ports are already set:

```bash
./psc_add_proxy_protocol.bash <cluster_id>          # 10 minutes between node restarts
./psc_add_proxy_protocol.bash <cluster_id> 0s       # no wait (test clusters with no load)
```

Confirm:

```bash
scylla-cloud cluster config get --cluster-id <cluster_id> native_transport_port_ssl_proxy_protocol  # 29142
```

Existing connections do not change. Only new TLS connections use the new listener.

## 2. Create the PSC connection

### Plain: UI

In the ScyllaDB Cloud UI, open the cluster → **Connections** and add a **GCP Private Service Connect**
connection, with `cx-sa-lab` in **Allowed GCP Projects**.

### TLS (or plain): script, REST API

```bash
./psc_connection_setup.bash <cluster_id>                       # TLS connection psc-tls-<cluster_id>
./psc_connection_setup.bash <cluster_id> false                 # plain connection psc-<cluster_id>
./psc_connection_setup.bash <cluster_id> true cx-sa-lab <name> <nat_subnet>
```

The script uses the public REST API, not `scylla-cloud cluster connection create`. When the CLI runs locally
through `cx sc`, it cannot reach Flipt to read the flag ("authentication/authorization error" on
`enable_private_connectivity_tls`), even when the flag is on. The script:

- reads the datacenter ID from the cluster
- picks the next free `/29` NAT subnet in `10.0.201.0/24`. Every PSC connection of a cluster needs its own.
  Reusing the default `10.0.201.0/29` of an existing connection is the likely cause of `{"error":"080015"}`.
- posts the connection with `tls`, `proxyProtocolV2: true`, `ACCEPT_MANUAL` and the consumer project, then
  waits until it is `ACTIVE` (about 3 minutes on 54742)

The request body it sends (the wiki's PascalCase body also works for the other fields):

```json
{
  "name": "psc-tls-<cluster_id>",
  "clusterDcid": <dc_id>,
  "type": "GCP_PRIVATE_SERVICE_CONNECT",
  "cidrList": ["0.0.0.0/0"],
  "data": {
    "tls": true,
    "proxyProtocolV2": true,
    "connectionPreference": "ACCEPT_MANUAL",
    "natSubnetCidr": "10.0.201.8/29",
    "consumerProjects": [{"projectId": "cx-sa-lab"}]
  }
}
```

`POST /account/<account>/cluster/<cluster_id>/network/vpc/connection` returns `data.connectionId` and queues
the create.

Errors that refuse the create:

| Error | Fix |
|---|---|
| TLS for private connections is not enabled for this account. | Enable `enable_private_connectivity_tls` |
| A TLS-only connection needs client-to-node encryption on the cluster. | Turn on client-to-node encryption |
| A TLS-only connection needs a cluster with the CQL interface. | Not available for Alternator clusters |
| The cluster has no TLS Proxy Protocol v2 CQL listener ... | Step 1 |

- The first TLS connection of a cluster re-issues the node certificates with the cluster TLS name. ScyllaDB
  loads them without a restart.
- From the siren-operation pod, the wiki's CLI also works: `scylla-cloud cluster connection create
  --connection-data '{"tls":"true","proxyProtocolV2":"true",...}'` (add `--dc-id <dc>` on a multi-DC cluster).

### Both

**Consumer project list.** With `ACCEPT_MANUAL`, only endpoints from listed projects are accepted. Others stay
`PENDING` until their project is added in the UI (connection → Edit → **Allowed GCP Projects**). They then
change to `ACCEPTED` without being recreated. A `connection_limit` of `0` or no limit means 1024 endpoints per
project.

**Check the connections:**

```bash
./psc_get_connection_ids.bash <cluster_id>     # id, name, status, tls, domain, client route ID + per-node ports
./psc_get_internal_id.bash <cluster_id> [connection_name]   # internal ID(s) only, e.g. 823
```

In `system.client_routes`, a plain connection has `port` set and `tls_port` null. A TLS connection is the
reverse. On 54742, both use 9003-9005.

## 3. Create the consumer endpoint

Run once per connection. Without a connection name, the script handles every PSC connection of the cluster:

```bash
./psc_generate_client.bash <cluster_id> [connection_name]
```

For each connection it:

1. writes `psc_<internal_id>/psc_<internal_id>.tf`, the vendor-generated terraform with defaults filled in
   (for reference, or to apply with terraform instead). This needs `cx`. If `cx` fails or times out, the
   script warns and carries on, because the connection details come from the REST API.
2. creates the endpoint with gcloud: static IP `<prefix>-psc-ip` and forwarding rule `<prefix>-psc-ep` (the
   wiki's step 4), plus a private DNS zone `<prefix>-psc-dns` with an A record for the PSC domain. It skips
   any resource that already exists, so reruns are safe.
3. prints the endpoint's PSC status, which must be `ACCEPTED`, the client route ID and a cqlsh example
4. TLS connections only: saves the cluster CA as `ca-<cluster_id>.pem`, prints the TLS name, and prints an
   `openssl s_client` command that checks the handshake and the certificate names

## 4. Connect: plain CQL

On the client VM:

```bash
cqlsh -u $USERNAME -p $PASSWORD --client-route <client_route_id> <psc_domain> 9000
```

## 5. Connect: TLS

A TLS client needs three values:

| Value | Where to get it |
|---|---|
| TLS name of the cluster | `TLSName` of the cluster (`scylla-cloud cluster describe` → `tlsname:`), for example `ae954dc6-....scylla-cloud.invalid` on 54742. Not in the public API. It stays empty until every node's certificate carries the name. |
| CA certificate | `ca-<cluster_id>.pem` from `psc_generate_client.bash` (API `.../cluster/<id>/certificate`, field `.data.content`; the wiki's `.Content` is wrong) |
| Client route ID | `ClientRoutesConnectionID` of the connection, printed by `psc_generate_client.bash` |

**cqlsh:** copy the CA file to the client VM, then:

```bash
SSL_CERTFILE=ca-<cluster_id>.pem SSL_VALIDATE=true SSL_CHECK_HOSTNAME=false \
  cqlsh --ssl -u $USERNAME -p $PASSWORD --client-route <client_route_id> <psc_domain> 9000
```

The certificate is issued for the TLS name, not for the PSC domain. cqlsh has no ServerName option, so it
checks the CA chain only.

**Drivers:** set ServerName to the TLS name and keep host verification on. With client routes, the driver
reaches each node through its `tls_port` and checks the TLS name on every node. gocql example from the wiki:

```go
pool := x509.NewCertPool()
pool.AppendCertsFromPEM(caPEM)
cluster := gocql.NewCluster("<endpoint-ip>:9000")
cluster.SslOpts = &gocql.SslOptions{
	EnableHostVerification: true,
	Config:                 &tls.Config{RootCAs: pool, ServerName: tlsName},
}
```

## Sample apps

In `sample_apps/`. Build the binaries locally and rsync them to the VM, which may have no Go, Maven or
internet access.

All four samples read the same `SCYLLA_*` variables through `psc_env.bash`. Per-connection values come from
an env file:

- `psc_generate_client.bash` writes `sample_apps/psc-<connection_name>.env` for each connection.
- `PSC_ENV=<file>` picks the file. Without it, the samples use `./psc.env`.
- A variable already set in the environment overrides the file.
- The password never goes in a file.

See `psc.env.example` for the format.

The samples also connect straight to the nodes, for example over VPC peering. Run
`./direct_env.bash <cluster_id>` to write `sample_apps/direct-<id>.env`, plus `direct-tls-<id>.env` when the
cluster has client-to-node encryption. These files set `SCYLLA_MODE=direct` and list the node DNS names as
`SCYLLA_CONTACT_POINTS`. In direct mode, Python, Go and scylla-bench connect on 9042, or 9142 with TLS, and the
driver finds the shard-aware ports itself. Java 4.x has to be given the shard-aware port as its contact port,
so it uses 19042, or 19142 with TLS.

```bash
./build_binaries.bash linux/amd64              # bin/psc_go.* and bin/scylla-bench.*
(cd java-psc && mvn -q package)                # java-psc/target/psc-sample.jar
export SCYLLA_PASSWORD=...

PSC_ENV=psc-<connection_name>.env ./run_sample_go.bash
PSC_ENV=psc-<connection_name>.env ./run_sample_python.bash
PSC_ENV=psc-<connection_name>.env ./run_sample_java.bash
PSC_ENV=psc-<connection_name>.env ./run_benchmark_psc.bash
```

| Variable | Meaning |
|---|---|
| `SCYLLA_PSC_DNS` | PSC endpoint DNS name (required) |
| `SCYLLA_PSC_CONN_ID` | Client route ID(s), comma-separated (required) |
| `SCYLLA_PSC_PORT` | Discovery port, default `9000` |
| `SCYLLA_USER` / `SCYLLA_PASSWORD` | Default to `USERNAME` / `PASSWORD` from `sc_api/cluster-<id>.env` |
| `SCYLLA_DC` | Local datacenter, default `DC`. Required by Java |
| `SCYLLA_CA_FILE` | Cluster CA. Setting it turns on TLS (TLS connections only) |
| `SCYLLA_TLS_NAME` | Cluster TLS name, sent as SNI |

How each sample handles TLS:

| Sample | TLS behavior |
|---|---|
| Go (gocql) and Java | Verify the TLS name against the certificate |
| Python (driver 3.29.12) | Verifies the CA chain and sends the TLS name as SNI, but doesn't check the name: the driver refuses `check_hostname` with client routes |
| `scylla-bench` | Encrypts with the CA but doesn't verify the server: it has no server-name option |

Verified on 54742 from `tjl-client-arm` (2026-10-28):

| Client | Connection | Result |
|---|---|---|
| `openssl s_client` | 823 (TLS) | TLSv1.3, CA verifies; SAN has the TLS name and the node name |
| Python, TLS | 823 (TLS) | CRUD works |
| Python, plain | 823 (TLS) | fails: `does not support protocol version 21` (a TLS alert read as CQL) |
| Python, plain | 822 (plain) | CRUD works |

Java (driver 4.19.2.3) with a shard-awareness check: `run_sample_java.bash`. The VM needs a JRE 11+.

```bash
PSC_ENV=psc-<tls_connection_name>.env ./run_sample_java.bash
PSC_ENV=psc-<tls_connection_name>.env ./run_sample_java.bash --shard-aware false   # compare
```

Unlike the Python driver, the Java sample verifies the TLS name. It sends the TLS name as SNI and checks the
certificate against it.

Before the CRUD run, it reads `system.clients` on each node and prints the source port and shard of each of
its connections. When `port % shard_count == shard_id` on every connection, shard-aware routing works end to
end through PSC. For that, the original source port has to reach the node through Proxy Protocol v2.

In client-routes mode, the driver binds a local port chosen for the shard and connects to `port` or
`tls_port`. It never dials 19042/19142. In 4.19.2.3, `ClientRoutesConfig.withShardAwareness()` is stored
but not read anywhere, so `advanced-shard-awareness.enabled` is the setting that takes effect.

`sample_python_psc.py` runs the same check, with `--shard-aware true|false` and `--pool-wait`. It finds its own
connections by reading their local ports from the driver's pools. The Python driver keeps one connection per
shard, so it has no `--pool-size`.

Results on 54793 from `tjl-client-arm` (2026-10-29):

| Run | Shard-requested connections on the requested shard |
|---|---|
| Java `--pool-size 16`, TLS | 19/45 (chance) |
| Java `--pool-size 16`, non-TLS | 21/45 (chance) |
| Java and Python, 1 connection per shard, both TLS modes | 3/3 in every run, but inconclusive |
| Control: Java `--pool-size 16`, **direct over VPC peering**, TLS and non-TLS | 45/45 in both |

- The nodes see the VM's own address (10.138.0.83) and the driver's source port, so PPv2 carries both through
  PSC.
- The node does not use that port to choose the shard on the PPv2 listeners (29042/29142), with or without TLS.
- The small runs only looked right. The driver then asks only for the shard the first connection missed, and a
  node that hands out shards in turn gets that right too. The samples now report fewer than 10 shard-requested
  connections as inconclusive.
- The control run over VPC peering puts every connection on the requested shard, through the shard-aware ports
  19042/19142. So the drivers and the test are fine, and the gap is in the PSC/PPv2 path.
- Requests are still routed to the right shard's connection. Only filling the pool takes extra connection
  attempts.

## 6. Cleanup

Delete the connection **before** you delete the cluster:

```bash
curl -X DELETE "https://api.cloud.scylladb.com/account/${SC_ACCOUNT}/cluster/<cluster_id>/network/vpc/connection/<internal_id>" \
  -H "Authorization: Bearer ${SC_TOKEN}"
# or: scylla-cloud cluster connection delete --cluster-id <cluster_id> --connection-id <internal_id> --timeout 30m
# or: the UI
```

Deleting the connection does not remove the consumer endpoint. The script removes both: the A record, the DNS
zone, the forwarding rule and the address, then (with `--connection`) the connection itself. It asks first:

```bash
./psc_delete_client.bash <cluster_id> <connection_name>                 # consumer endpoint only
./psc_delete_client.bash <cluster_id> <connection_name> --connection    # endpoint and connection
```

Manual equivalent (`<prefix>` = the first label of the PSC domain):

```bash
gcloud dns record-sets delete <psc_domain>. --type A --zone <prefix>-psc-dns --project cx-sa-lab
gcloud dns managed-zones delete <prefix>-psc-dns --project cx-sa-lab
gcloud compute forwarding-rules delete <prefix>-psc-ep --region us-west1 --project cx-sa-lab
gcloud compute addresses delete <prefix>-psc-ip --region us-west1 --project cx-sa-lab
```

## Open items

- cqlsh is not installed on `tjl-client-arm`; the cqlsh TLS example is untested.
- Does the UI offer a TLS option for PSC? If it does, the TLS connection could also be created there.
- Test the TLS support in `psc.go` and the Java sample (`run_sample_java.bash`). Both were written after 54742 was deleted.

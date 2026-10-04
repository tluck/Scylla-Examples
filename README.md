# Various Scylla Examples

Scripts, small apps, and reference layouts for working with [ScyllaDB](https://www.scylladb.com/) on cloud and Kubernetes: provisioning, APIs, data loading, Alternator, and operational helpers.

## Contents

| Directory | What it is |
|-----------|------------|
| `alternator` | Minimal Python examples around ScyllaDB’s DynamoDB-compatible API (Alternator). |
| `cassandra_stress` | `cassandra-stress` style workloads and helpers (`run-stress.sh`, YAML profiles). |
| `clustering-key` | Rust tooling and scripts for clustering-key / wide-partition experiments. |
| `gcp_psc_setup_test` | GCP Private Service Connect (PSC) / ILB setup scripts, client Terraform, and Go/Python test clients for ScyllaDB Cloud. |
| `golang` | Small Go program demonstrating driver usage. |
| `java` | Java driver samples (`simple`, `java-driver`, zone-aware routing, CQL ingest). |
| `python_parquet_reader` | Ingest and tooling for Parquet/JSON/CSV paths into ScyllaDB (Python, batch scripts). |
| `python_zone-aware` | Standalone Python snippet for AWS zone-aware placement. |
| `sample_apps` | Larger demos: Alternator (Java/Python/Boto3), CQL loaders, Docker/Kubernetes deploy scripts, tombstone/compression experiments. Copied from [Scylla-K8s-Example](https://github.com/tluck/Scylla-K8s-Example) — see below. |
| `sc_api` | Bash/Python helpers for ScyllaDB Cloud (cluster CLI, certs, firewall/CIDR, adding/removing DCs, VM listing). |
| `sc_terraform` | Terraform for ScyllaDB Cloud clusters and networking on **AWS** and **GCP** (see its own README). |

Each folder is meant to be explored on its own; requirements vary (Python `requirements.txt`, Maven `pom.xml`, Go modules, etc.).

## Using this repo

**Clone only this repository**

```bash
git clone https://github.com/tluck/Scylla-Examples.git
cd Scylla-Examples
```

**Used as a submodule inside another repo**, initialize it after cloning the parent (adjust the path to match `.gitmodules`):

```bash
git submodule update --init --recursive
```

## `sample_apps` is a copy

The source of truth for `sample_apps/` is `sample_app/` in [Scylla-K8s-Example](https://github.com/tluck/Scylla-K8s-Example). Make changes there, then copy them here and commit:

```bash
rsync -av --delete --copy-unsafe-links \
  --exclude='.git*' --exclude=__pycache__ --exclude=.ruff_cache \
  --exclude=.DS_Store --exclude='*.jar' --exclude=target/ \
  ../Scylla-K8s-Example/sample_app/ sample_apps/
```

Edits made directly in `sample_apps/` are overwritten on the next copy.

## Conventions

- Treat paths under `sample_apps/` and `sc_api/` as **examples**: review scripts before running them in production; adjust regions, cluster names, and credentials.
- Certificates or sample config files in-tree are for illustration; prefer your own secrets management for real deployments.

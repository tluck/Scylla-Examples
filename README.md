# Various Scylla Examples

Scripts, small apps, and reference layouts for working with [ScyllaDB](https://www.scylladb.com/) on cloud and Kubernetes: provisioning, APIs, data loading, Alternator, and operational helpers.

## Contents

| Directory | What it is |
|-----------|------------|
| `alternator` | Minimal Python examples around ScyllaDB’s DynamoDB-compatible API (Alternator). |
| `cassandra_stress` | `cassandra-stress` style workloads and helpers (`run-stress.sh`, YAML profiles). |
| `clustering-key` | Rust tooling and scripts for clustering-key / wide-partition experiments. |
| `golang` | Small Go program demonstrating driver usage. |
| `java` | Java driver samples (`simple`, `java-driver`, zone-aware routing). |
| `parquet` | Ingest and tooling for Parquet/JSON/CSV paths into ScyllaDB (Python, batch scripts). |
| `psc_test` | Private Service Connect (PSC) / ILB style tests and Go utilities for Scylla on GCP. |
| `python` | Standalone Python snippets (e.g. AWS zone-aware placement ideas). |
| `sample_app` | Larger demos: Alternator (Java/Python/Boto3), CQL loaders, Docker/Kubernetes deploy scripts, tombstone/compression experiments. |
| `sc_api` | Bash/Python helpers for ScyllaDB Cloud (certs, firewall, VM listing, CLI wrappers). |
| `sc_terraform` | Terraform for ScyllaDB-related clusters and networking on **AWS** and **GCP**. |

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

## Conventions

- Treat paths under `sample_app/` and `sc_api/` as **examples**: review scripts before running them in production; adjust regions, cluster names, and credentials.
- Certificates or sample config files in-tree are for illustration; prefer your own secrets management for real deployments.

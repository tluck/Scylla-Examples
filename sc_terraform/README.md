# ScyllaDB Cloud Terraform examples

Working Terraform examples for provisioning **ScyllaDB X Cloud** clusters and
connecting them to your own network, built on the official
[`scylladb/scylladbcloud`](https://registry.terraform.io/providers/scylladb/scylladbcloud/latest/docs)
provider (**v1.13+**).

| Directory | What it builds |
|---|---|
| [`aws/`](aws/) | X Cloud cluster on AWS + **VPC peering** or an **AWS Transit Gateway attachment** |
| [`gcp/`](gcp/) | X Cloud cluster on GCP + **VPC network peering** |

Each example deploys into either **ScyllaDB Cloud's own account** or **your own
cloud account (BYOA)** — a single `byoa_id` variable switches between them.

## Prerequisites

* Terraform >= 1.5
* A ScyllaDB Cloud account and an API token
  ([how to get one](https://cloud.docs.scylladb.com/stable/api-docs/api-get-started.html))
* AWS credentials (`aws sts get-caller-identity` works) or GCP ADC
  (`gcloud auth application-default login`) — only needed if you build client
  networking; a cluster on its own needs neither
* For BYOA: your cloud account added in the ScyllaDB Cloud portal first

## Quick start

```bash
git clone https://github.com/tluck/Scylla-Examples.git
cd Scylla-Examples/sc_terraform

export SCYLLADB_CLOUD_TOKEN="<your token>"   # or: source ./env.sh

cd aws                                        # or: cd gcp
cp terraform.tfvars.example terraform.tfvars  # then edit it
terraform init
terraform plan
terraform apply
```

Tear down with `terraform destroy`.

## Authentication

The provider reads `SCYLLADB_CLOUD_TOKEN` from the environment, so the token
never has to live in a file:

```bash
export SCYLLADB_CLOUD_TOKEN="<your token>"
```

`env.sh` is a convenience wrapper that also accepts `SC_TOKEN`, which is handy
if you keep the token in `~/.bash_profile`:

```bash
source ./env.sh
```

`var.scylla_api_token` exists and defaults to `null`; set it only if you need a
different token per workspace. **Do not commit it** — `.gitignore` excludes
`*.tfvars` for exactly this reason.

## Choosing where the cluster runs

`byoa_id` selects the cloud provider credential:

| `byoa_id` | Deploys into |
|---|---|
| `null` (default) | ScyllaDB Cloud's own AWS/GCP account — nothing to set up |
| a number | **your** cloud account, added in the ScyllaDB Cloud portal first |

Find your credential IDs:

```bash
ACCOUNT=$(curl -s -H "Authorization: Bearer $SCYLLADB_CLOUD_TOKEN" \
  https://api.cloud.scylladb.com/account/default | jq -r .data.accountId)

curl -s -H "Authorization: Bearer $SCYLLADB_CLOUD_TOKEN" \
  https://api.cloud.scylladb.com/account/$ACCOUNT/cloud-account | jq
```

Entries with `"owner": "Account"` are yours; `"owner": "Scylla"` are the
managed defaults that `null` already selects.

> The provider documents `byoa_id` as AWS-only. In practice the API accepts a
> GCP credential ID too, and the GCP example wires it up.

## X Cloud sizing

These examples are **X Cloud only** — the control plane autoscales the fleet
from a `scaling` policy, so `node_type` and `min_nodes` are never set:

```hcl
xcloud_instance_families          = ["i8g"]   # GCP: ["n2-highmem"]
xcloud_storage_min_gb             = 500
xcloud_storage_target_utilization = 0.8       # max 0.9; <= 0.85 for write-heavy
xcloud_vcpu_min                   = 6
```

List the instance families available in your region:

```bash
# cloud-provider id: 1 = AWS, 2 = GCP
curl -s -H "Authorization: Bearer $SCYLLADB_CLOUD_TOKEN" \
  "https://api.cloud.scylladb.com/deployment/cloud-provider/1/regions?defaults=true" \
  | jq -r '.data.instances[].instanceFamily' | sort -u
```

Typical values: AWS `i3`, `i3en`, `i4i`, `i7i`, `i7ie`, `i8g`, `i8ge`;
GCP `n2-highmem`, `n2d-highmem`, `z3-highmem`.

### X Cloud notes

* Single datacenter only — multi-DC is not supported yet.
* Replication factor is fixed at RF=3 across three AZs.
* `node_type` reads back empty and `node_disk_size` / `min_nodes` read back as
  `0`. Use the `node_count` output to see the current fleet size.
* Changing `cidr_block`, `region`, `name` or `cloud` **replaces** the cluster.

## Networking (AWS)

`connection_type` picks how your network reaches the cluster:

| Value | Creates |
|---|---|
| `vpc_peering` | VPC peering, accepter, route(s) |
| `transit_gateway` | An `AWS_TGW_ATTACHMENT` cluster connection (set `tgw_id`) |
| `none` | Cluster only — needs no AWS credentials at all |

With `vpc_peering`, `use_default_vpc` picks the client side:

* `false` (default) — builds a dedicated client VPC, subnet, route table and
  association from `client_vpc_cidr` / `client_subnet_cidr`.
* `true` — peers to your account's **default VPC** and adds a route to each of
  its route tables.

> **CIDR planning matters.** A route table can hold only one route per
> destination prefix. If you peer several clusters into the same client
> network, give each cluster a distinct `cluster_vpc_cidr`, and make sure it
> does not collide with a route already present there. Check first:
> ```bash
> aws ec2 describe-route-tables --filters "Name=vpc-id,Values=<vpc>" \
>   --query 'RouteTables[*].Routes[*].DestinationCidrBlock'
> ```
> Watch for `blackhole` routes left behind by deleted peerings — they still
> occupy the prefix and will block a new route.

## Networking (GCP)

The GCP example always builds an auto-mode client VPC and a two-sided peering:
`scylladbcloud_vpc_peering` on the ScyllaDB side and
`google_compute_network_peering` on yours. GCP exchanges routes automatically,
so no route resources are needed.

## Running several clusters side by side

Each directory holds a single cluster resource, so use workspaces to keep
multiple clusters apart:

```bash
cd aws
terraform workspace new byoa
cp byoa.tfvars.example byoa.tfvars   # then edit it
terraform apply -var-file=byoa.tfvars
```

To run two applies in the same directory concurrently, set `TF_WORKSPACE`
instead of `terraform workspace select` — the latter writes a shared
`.terraform/environment` file that the two processes would fight over:

```bash
TF_WORKSPACE=byoa terraform apply -var-file=byoa.tfvars
```

## Useful outputs

```bash
terraform output scylladbcloud_cluster_id
terraform output scylladbcloud_cluster_node_dns_names
terraform output scylladbcloud_cluster_node_count      # changes as X Cloud scales
terraform output scylladbcloud_cluster_ca_certificate  # for client-to-node TLS
```

## Security

* `.gitignore` excludes `*.tfstate*`, `terraform.tfstate.d/` and `*.tfvars`.
  **State files contain node IPs, cluster IDs and CA certificates** — never
  commit them.
* Keep the API token in the environment, not in `.tfvars`.
* Encryption at rest is enabled by default with a ScyllaDB-managed key. To use
  a customer-managed key, create it in the portal and uncomment the
  `encryption_at_rest` block in the cluster resource.

## Troubleshooting

**`token is required`** — `SCYLLADB_CLOUD_TOKEN` is not exported. Run
`source ./env.sh`.

**`unsupported scaling instance_family "x" in region y`** — that family is not
offered there; list the valid ones with the API call above.

**`RouteAlreadyExists`** — something already routes that prefix in the target
route table, often a `blackhole` route from a deleted peering. Remove it or
pick a different `cluster_vpc_cidr`.

**AWS provider fails with `ExpiredToken` even for a cluster-only run** — the
provider validates credentials when it is configured. Set
`connection_type = "none"`, which skips that probe.

**Cluster stuck in `QUEUED`/`BOOTSTRAPPING`** — normal; creation takes roughly
5–10 minutes. The provider waits up to 40 minutes.

## License

See the repository root.

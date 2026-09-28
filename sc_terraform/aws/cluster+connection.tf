# End-to-end example for a ScyllaDB X Cloud cluster + networking on AWS.
#
# X Cloud only: the control plane autoscales the fleet from the scaling policy
# below. There is no node_type / min_nodes here by design - those attributes
# conflict with the scaling block.
#
# Client connectivity is selected with var.connection_type:
#   vpc_peering | transit_gateway | none

terraform {
  required_version = ">= 1.5"

  required_providers {
    scylladbcloud = {
      source  = "scylladb/scylladbcloud"
      version = "~> 1.13"
    }
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
  }
}

# Providers
provider "scylladbcloud" {
  # Falls back to the SCYLLADB_CLOUD_TOKEN environment variable when null.
  #   export SCYLLADB_CLOUD_TOKEN="$SC_TOKEN"
  token = var.scylla_api_token
}

provider "aws" {
  region = var.region

  # With connection_type = "none" no AWS resources are created, so skip the
  # STS credential probe the provider otherwise runs at configure time. That
  # lets you create a ScyllaDB-only cluster without valid AWS credentials.
  skip_credentials_validation = var.connection_type == "none"
  skip_requesting_account_id  = var.connection_type == "none"
  skip_metadata_api_check     = var.connection_type == "none"
}

# Data sources
# Only read when a client VPC is actually peered, so connection_type = "none"
# can create a cluster without any AWS credentials present.
data "aws_caller_identity" "current" {
  count = var.connection_type == "vpc_peering" ? 1 : 0
}

# The account's default VPC in var.region, used when var.use_default_vpc is set.
data "aws_vpc" "default" {
  count   = local.peering && var.use_default_vpc ? 1 : 0
  default = true
}

data "aws_route_tables" "default" {
  count  = local.peering && var.use_default_vpc ? 1 : 0
  vpc_id = data.aws_vpc.default[0].id
}

locals {
  peering = var.connection_type == "vpc_peering"
  tgw     = var.connection_type == "transit_gateway"

  # Peer either to the account's default VPC or to a purpose-built client VPC.
  dedicated_vpc = local.peering && !var.use_default_vpc

  client_vpc_id = var.use_default_vpc ? one(data.aws_vpc.default[*].id) : one(aws_vpc.client[*].id)
  client_cidr   = var.use_default_vpc ? one(data.aws_vpc.default[*].cidr_block) : one(aws_vpc.client[*].cidr_block)
}

# Create the ScyllaDB X Cloud cluster
resource "scylladbcloud_cluster" "demo" {
  name       = var.cluster_name
  cloud      = var.cloud
  region     = var.region
  cidr_block = var.cluster_vpc_cidr
  byoa_id    = var.byoa_id

  # Autoscaling policy. The control plane picks the instance type and node
  # count from this and reports them back through node_type / node_count.
  scaling {
    instance_families = var.xcloud_instance_families

    storage_policy {
      min_gb             = var.xcloud_storage_min_gb
      target_utilization = var.xcloud_storage_target_utilization
    }

    vcpu_policy {
      min = var.xcloud_vcpu_min
    }
  }

  user_api_interface         = var.user_api_interface
  alternator_write_isolation = var.user_api_interface == "ALTERNATOR" ? var.alternator_write_isolation : null

  # Set to 0 to drop backups as soon as the cluster is deleted.
  backup_retention_days = var.backup_retention_days

  enable_vpc_peering = true
  enable_dns         = true

  # Encryption at rest is on by default with a ScyllaDB-managed key. Uncomment
  # to use a customer-managed key created beforehand in the ScyllaDB Cloud portal.
  #
  # encryption_at_rest {
  #   key_id = "key-deadbeef"
  # }
}

# VPC PEERING RESOURCES (conditionally created)
resource "aws_vpc" "client" {
  count      = local.dedicated_vpc ? 1 : 0
  depends_on = [scylladbcloud_cluster.demo] # let the cluster and its VPC (BYOA) come up first
  cidr_block = var.client_vpc_cidr
  tags       = { Name = "${var.cluster_name}-client-vpc" }
}

resource "scylladbcloud_vpc_peering" "demo" {
  count            = local.peering ? 1 : 0
  cluster_id       = scylladbcloud_cluster.demo.cluster_id
  datacenter       = scylladbcloud_cluster.demo.datacenter
  peer_vpc_id      = local.client_vpc_id
  peer_cidr_blocks = [local.client_cidr]
  peer_region      = var.region
  peer_account_id  = data.aws_caller_identity.current[0].account_id
  allow_cql        = true
}

resource "aws_vpc_peering_connection_accepter" "client" {
  count                     = local.peering ? 1 : 0
  vpc_peering_connection_id = scylladbcloud_vpc_peering.demo[0].connection_id
  auto_accept               = true
  tags                      = { Name = "${var.cluster_name}-peering-accepter" }
}

# A subnet to place client workloads in. Without it the VPC has nowhere to run
# an instance, and the route table below has nothing to attach to.
resource "aws_subnet" "client" {
  count      = local.dedicated_vpc ? 1 : 0
  vpc_id     = aws_vpc.client[0].id
  cidr_block = var.client_subnet_cidr
  tags       = { Name = "${var.cluster_name}-client-subnet" }
}

resource "aws_route_table" "client" {
  count  = local.dedicated_vpc ? 1 : 0
  vpc_id = aws_vpc.client[0].id

  route {
    cidr_block                = scylladbcloud_cluster.demo.cidr_block
    vpc_peering_connection_id = aws_vpc_peering_connection_accepter.client[0].vpc_peering_connection_id
  }

  tags = { Name = "${var.cluster_name}-client-routes" }
}

# Without this association the route table exists but nothing uses it, so the
# peering comes up "active" while traffic to the cluster still goes nowhere.
resource "aws_route_table_association" "client" {
  count          = local.dedicated_vpc ? 1 : 0
  subnet_id      = aws_subnet.client[0].id
  route_table_id = aws_route_table.client[0].id
}

# When peering to the default VPC we do not own its route tables, so add a
# single route to each of them instead of managing a route table resource.
resource "aws_route" "default_vpc" {
  for_each = local.peering && var.use_default_vpc ? toset(one(data.aws_route_tables.default[*].ids)) : toset([])

  route_table_id            = each.value
  destination_cidr_block    = scylladbcloud_cluster.demo.cidr_block
  vpc_peering_connection_id = aws_vpc_peering_connection_accepter.client[0].vpc_peering_connection_id
}

# TRANSIT GATEWAY RESOURCES (conditionally created)
resource "scylladbcloud_cluster_connection" "demo" {
  count      = local.tgw ? 1 : 0
  depends_on = [scylladbcloud_cluster.demo]
  cluster_id = scylladbcloud_cluster.demo.cluster_id
  name       = "aws-tgw-attachment"
  type       = "AWS_TGW_ATTACHMENT"
  datacenter = scylladbcloud_cluster.demo.datacenter
  cidrlist   = [var.client_vpc_cidr]

  data = merge(
    { tgwid = var.tgw_id },
    var.tgw_ram_arn == null ? {} : { ramarn = var.tgw_ram_arn },
  )
}

# OUTPUTS
output "scylladbcloud_cluster_id" {
  value = scylladbcloud_cluster.demo.id
}

output "scylladbcloud_cluster_datacenter" {
  value = scylladbcloud_cluster.demo.datacenter
}

output "scylladbcloud_cluster_status" {
  value = scylladbcloud_cluster.demo.status
}

output "scylladbcloud_cluster_node_type" {
  description = "Instance type the control plane picked for the current fleet."
  value       = scylladbcloud_cluster.demo.node_type
}

output "scylladbcloud_cluster_node_count" {
  description = "Nodes the cluster currently runs. Changes as X Cloud scales."
  value       = scylladbcloud_cluster.demo.node_count
}

output "scylladbcloud_cluster_node_dns_names" {
  value = scylladbcloud_cluster.demo.node_dns_names
}

output "scylladbcloud_cluster_ca_certificate" {
  description = "PEM CA certificate for client-to-node TLS. Empty when in-transit encryption is off."
  value       = scylladbcloud_cluster.demo.ca_certificate
}

output "connection_type" {
  value = var.connection_type
}

# Conditional outputs
output "scylladbcloud_cluster_connection_id" {
  value = local.tgw ? scylladbcloud_cluster_connection.demo[0].id : null
}

output "vpc_peering_connection_id" {
  value = local.peering ? scylladbcloud_vpc_peering.demo[0].connection_id : null
}

output "client_vpc_id" {
  value = local.peering ? local.client_vpc_id : null
}

output "client_subnet_id" {
  value = local.dedicated_vpc ? one(aws_subnet.client[*].id) : null
}

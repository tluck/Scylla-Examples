# End-to-end example for ScyllaDB + network peering on AWS.

terraform {
  required_providers {
    scylladbcloud = {
      source  = "scylladb/scylladbcloud"
      version = ">= 1.8"
    }
    aws = {
      source  = "hashicorp/aws"
      version = ">= 4.0"
    }
  }
}

# Providers
provider "scylladbcloud" {
  token = var.scylla_api_token
}

provider "aws" {
  region = var.region
}

# Data sources
data "aws_caller_identity" "current" {}

# Create ScyllaDB cluster (same for both approaches)
resource "scylladbcloud_cluster" "demo" {
  cloud                      = var.cloud
  name                       = var.cluster_name
  region                     = var.region
  node_count                 = var.cluster_node_count
  node_type                  = var.cluster_node_type
  cidr_block                 = var.cluster_vpc_cdir
  byoa_id                    = var.byoa_id
  alternator_write_isolation = ""
  enable_vpc_peering         = true
  enable_dns                 = true
}

# VPC PEERING RESOURCES (conditionally created)
resource "aws_vpc" "client" {
  count         = var.connection_type == "vpc_peering" ? 1 : 0
  depends_on    = [scylladbcloud_cluster.demo] # Let the cluster and its VPC (BYOA) be created first
  cidr_block    = var.client_vpc_cdir
  tags = { Name = "${var.cluster_name}-client-vpc" }
}

resource "scylladbcloud_vpc_peering" "demo" {
  count            = var.connection_type == "vpc_peering" ? 1 : 0
  cluster_id       = scylladbcloud_cluster.demo.id
  datacenter       = scylladbcloud_cluster.demo.datacenter
  peer_vpc_id      = aws_vpc.client[0].id
  peer_cidr_blocks = [aws_vpc.client[0].cidr_block]
  peer_region      = var.region
  peer_account_id  = data.aws_caller_identity.current.account_id
  allow_cql        = true
}

resource "aws_vpc_peering_connection_accepter" "client" {
  count                     = var.connection_type == "vpc_peering" ? 1 : 0
  vpc_peering_connection_id = scylladbcloud_vpc_peering.demo[0].connection_id
  auto_accept               = true
  tags                      = { Name = "${var.cluster_name}-peering-accepter" }
}

resource "aws_route_table" "client" {
  count  = var.connection_type == "vpc_peering" ? 1 : 0
  vpc_id = aws_vpc.client[0].id
  route {
    cidr_block                = scylladbcloud_cluster.demo.cidr_block
    vpc_peering_connection_id = aws_vpc_peering_connection_accepter.client[0].vpc_peering_connection_id
  }
  tags = { Name = "${var.cluster_name}-client-routes" }
}

# TRANSIT GATEWAY RESOURCES (conditionally created)
resource "scylladbcloud_cluster_connection" "demo" {
  count      = var.connection_type == "transit_gateway" ? 1 : 0
  depends_on = [scylladbcloud_cluster.demo]
  cluster_id = scylladbcloud_cluster.demo.id
  name       = "aws-tgw-attachment"
  cidrlist   = [var.client_vpc_cdir]
  type       = "AWS_TGW_ATTACHMENT"
  datacenter = scylladbcloud_cluster.demo.datacenter
  data       = { tgwid = var.tgw_id }
}

# OUTPUTS
output "scylladbcloud_cluster_id" {
  value = scylladbcloud_cluster.demo.id
}

output "scylladbcloud_cluster_datacenter" {
  value = scylladbcloud_cluster.demo.datacenter
}

output "connection_type" {
  value = var.connection_type
}

# Conditional outputs
output "scylladbcloud_cluster_connection_id" {
  value = var.connection_type == "transit_gateway" ? scylladbcloud_cluster_connection.demo[0].id : null
}

output "vpc_peering_connection_id" {
  value = var.connection_type == "vpc_peering" ? scylladbcloud_vpc_peering.demo[0].connection_id : null
}

output "client_vpc_id" {
  value = var.connection_type == "vpc_peering" ? aws_vpc.client[0].id : null
}


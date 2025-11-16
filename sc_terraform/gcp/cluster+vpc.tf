# End-to-end example for ScyllaDB Datacenter network peering on GCP.

terraform {
  required_providers {
    scylladbcloud = {
      source  = "scylladb/scylladbcloud"
      version = ">= 1.8" # Use the latest version available
    }
  }
}

provider "google" {
  project = var.gcp_project_id
  region  = var.region
}

provider "scylladbcloud" {
  token = var.scylla_api_token
}
# Create a ScyllaDB cluster
resource "scylladbcloud_cluster" "demo" {
  cloud                      = var.cloud
  name                       = var.cluster_name
  region                     = var.region
  node_count                 = var.cluster_node_count
  node_type                  = var.cluster_node_type # Node instance type for the cluster
  cidr_block                 = var.cluster_vpc_cdir # CIDR block for the cluster
  alternator_write_isolation = "" #"only_rmw_uses_lwt"
  enable_vpc_peering         = true
  enable_dns                 = true
}

locals {
  datacenter = scylladbcloud_cluster.demo.datacenter
}

resource "google_compute_network" "client" {
    depends_on              = [scylladbcloud_cluster.demo]
    name                    = "${lower(var.cluster_name)}-client"
    auto_create_subnetworks = true
}

resource "scylladbcloud_vpc_peering" "demo" {
    cluster_id      = scylladbcloud_cluster.demo.id
    datacenter      = local.datacenter # "GCE_US_WEST_1"
    peer_vpc_id     = google_compute_network.client.name
    peer_region     = var.region
    peer_account_id = "cx-sa-lab"
    allow_cql       = true
}

resource "google_compute_network_peering" "client" {
    name         = "${lower(var.cluster_name)}-client-peering"
    network      = google_compute_network.client.self_link
    peer_network = scylladbcloud_vpc_peering.demo.network_link
}

output "scylladbcloud_cluster_id" {
  value = scylladbcloud_cluster.demo.id
}

output "scylladbcloud_cluster_datacenter" {
  value = scylladbcloud_cluster.demo.datacenter
}

output "google_compute_network_peering_client_id" {
  value = google_compute_network_peering.client.id
}

output "client_vpc_id" {
  value = google_compute_network.client.id
}

output "vpc_peering_id" {
  value = scylladbcloud_vpc_peering.demo.id
}

# End-to-end example for a ScyllaDB X Cloud cluster + VPC network peering on GCP.
#
# X Cloud only: the control plane autoscales the fleet from the scaling policy
# below. There is no node_type / min_nodes here by design - those attributes
# conflict with the scaling block.

terraform {
  required_version = ">= 1.5"

  required_providers {
    scylladbcloud = {
      source  = "scylladb/scylladbcloud"
      version = "~> 1.13"
    }
    google = {
      source  = "hashicorp/google"
      version = ">= 5.0"
    }
  }
}

provider "google" {
  project = var.gcp_project_id
  region  = var.region
}

provider "scylladbcloud" {
  # Falls back to the SCYLLADB_CLOUD_TOKEN environment variable when null.
  #   export SCYLLADB_CLOUD_TOKEN="$SC_TOKEN"
  token = var.scylla_api_token
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

resource "google_compute_network" "client" {
  depends_on              = [scylladbcloud_cluster.demo] # let the cluster and its VPC come up first
  name                    = "${lower(var.cluster_name)}-client"
  auto_create_subnetworks = true
}

resource "scylladbcloud_vpc_peering" "demo" {
  cluster_id      = scylladbcloud_cluster.demo.cluster_id
  datacenter      = scylladbcloud_cluster.demo.datacenter # e.g. GCE_US_WEST_1
  peer_vpc_id     = google_compute_network.client.name
  peer_region     = var.region
  peer_account_id = var.gcp_project_id
  allow_cql       = true
}

resource "google_compute_network_peering" "client" {
  name         = "${lower(var.cluster_name)}-client-peering"
  network      = google_compute_network.client.self_link
  peer_network = scylladbcloud_vpc_peering.demo.network_link
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

output "google_compute_network_peering_client_id" {
  value = google_compute_network_peering.client.id
}

output "client_vpc_id" {
  value = google_compute_network.client.id
}

output "vpc_peering_id" {
  value = scylladbcloud_vpc_peering.demo.id
}

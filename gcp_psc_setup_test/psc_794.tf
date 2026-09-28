terraform {
  required_version = ">= 1.3.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

variable "project_id" {
  description = "Your GCP project ID"
  type        = string
}

variable "network" {
  description = "VPC network self-link (e.g. projects/my-project/global/networks/default)"
  type        = string
}

variable "endpoints" {
  description = <<-EOT
    Per-endpoint configuration. Keys must match the endpoint names in locals.connections.
    Example (in terraform.tfvars):

      endpoints = {
        "scylla_psc_nr67594_df67f06f" = {
          region     = "REGION"
          subnetwork = "projects/PROJECT/regions/REGION/subnetworks/SUBNET"
        }
      }
  EOT
  type = map(object({
    region     = string
    subnetwork = string
  }))
}

provider "google" {
  project = var.project_id
}

locals {
  connections = {
    "scylla_psc_nr67594_df67f06f" = {
      service_attachment = "projects/cx-sa-lab/regions/us-west1/serviceAttachments/scylla-psc-nr67594-df67f06f-sa"
      domain             = "scylla-psc-nr67594-df67f06f.clusters.scylla.cloud"
      name_prefix        = "scylla-psc-nr67594-df67f06f"
    }
  }
}

# --- Static IPs for PSC endpoints ---

resource "google_compute_address" "psc" {
  for_each     = local.connections
  name         = "${each.value.name_prefix}-psc-ip"
  region       = var.endpoints[each.key].region
  address_type = "INTERNAL"
  subnetwork   = var.endpoints[each.key].subnetwork
}

# --- PSC endpoints (forwarding rule -> service attachment) ---

resource "google_compute_forwarding_rule" "psc" {
  for_each                = local.connections
  name                    = "${each.value.name_prefix}-psc-ep"
  region                  = var.endpoints[each.key].region
  load_balancing_scheme   = ""
  ip_address              = google_compute_address.psc[each.key].id
  network                 = var.network
  target                  = each.value.service_attachment
  allow_psc_global_access = true
}

# --- Private DNS zones (scoped to consumer's VPC) ---

resource "google_dns_managed_zone" "psc" {
  for_each   = local.connections
  name       = "${each.value.name_prefix}-psc-dns"
  dns_name   = "${each.value.domain}."
  visibility = "private"

  private_visibility_config {
    networks {
      network_url = var.network
    }
  }
}

# --- A records: domain -> endpoint IP ---

resource "google_dns_record_set" "psc" {
  for_each     = local.connections
  managed_zone = google_dns_managed_zone.psc[each.key].name
  name         = "${each.value.domain}."
  type         = "A"
  ttl          = 300
  rrdatas      = [google_compute_address.psc[each.key].address]
}

output "endpoint_ips" {
  value = { for k, v in google_compute_address.psc : k => v.address }
}

output "dns_names" {
  value = { for k, v in local.connections : k => v.domain }
}
output "port_info" {
  value = <<-INFO
  Discovery ports:  9000-9002 (each port maps to a specific node; any port reaches CQL)
  Dedicated ports:  9003-9005 (each port maps to a specific node)
  INFO
}

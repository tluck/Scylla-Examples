# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------

variable "scylla_api_token" {
  description = <<-EOT
    ScyllaDB Cloud API bearer token.

    Leave this null (the default) and export the token in your shell instead:

        export SCYLLADB_CLOUD_TOKEN="$SC_TOKEN"

    The provider reads SCYLLADB_CLOUD_TOKEN natively, which keeps the token out
    of terraform.tfvars.
  EOT
  type        = string
  sensitive   = true
  default     = null
}

variable "gcp_project_id" {
  description = "GCP project that owns the client VPC. Also used as peer_account_id."
  type        = string
}

variable "byoa_id" {
  description = <<-EOT
    Cloud provider credential ID in ScyllaDB Cloud (the portal calls this BYOA).

    Leave null to deploy into ScyllaDB Cloud's own GCP project - no setup
    required. To deploy into your own project, add it in the ScyllaDB Cloud
    portal first, then look up its numeric ID:

      curl -s -H "Authorization: Bearer $SCYLLADB_CLOUD_TOKEN" \
        https://api.cloud.scylladb.com/account/<accountId>/cloud-account

    The provider docs describe byoa_id as AWS-only, but the API accepts a GCP
    credential ID here and it has been verified working.
  EOT
  type        = number
  default     = null
  sensitive   = true
}

# ---------------------------------------------------------------------------
# Cluster shape
# ---------------------------------------------------------------------------

variable "cluster_name" {
  description = "Cluster Name"
  type        = string
  default     = "scylla-demo"
}

variable "cloud" {
  description = "Cloud provider. Accepted values: AWS, GCP."
  type        = string
  default     = "GCP"
}

variable "region" {
  description = "Cloud region"
  type        = string
  default     = "us-central1"
}

variable "cluster_vpc_cidr" {
  description = "CIDR block for the ScyllaDB cluster VPC."
  type        = string
  default     = "172.30.0.0/24"
}

variable "backup_retention_days" {
  description = "Days to retain backups after the cluster is deleted (0-60). 0 deletes them immediately."
  type        = number
  default     = 1
}

variable "user_api_interface" {
  description = "User API interface: CQL or ALTERNATOR."
  type        = string
  default     = "CQL"

  validation {
    condition     = contains(["CQL", "ALTERNATOR"], var.user_api_interface)
    error_message = "User API interface must be either 'CQL' or 'ALTERNATOR'."
  }
}

variable "alternator_write_isolation" {
  description = "Write isolation policy. Only applied when user_api_interface is ALTERNATOR."
  type        = string
  default     = "only_rmw_uses_lwt"
}

# --- X Cloud autoscaling policy ---

variable "xcloud_instance_families" {
  description = <<-EOT
    Instance families X Cloud may autoscale within.

    GCP families: n2-highmem (the default, verified with X Cloud) and
    z3-highmem, the local-SSD alternative. Availability varies by region.
  EOT
  type        = list(string)
  default     = ["n2-highmem"]
}

variable "xcloud_storage_min_gb" {
  description = "Minimum provisioned storage in GB across the X Cloud cluster."
  type        = number
  default     = 500
}

variable "xcloud_storage_target_utilization" {
  description = "Target storage utilization (0-0.9). Defaults to 0.8; use <= 0.85 for write-heavy workloads."
  type        = number
  default     = 0.8
}

variable "xcloud_vcpu_min" {
  description = "Minimum vCPU count to keep provisioned across the X Cloud cluster."
  type        = number
  default     = 6
}

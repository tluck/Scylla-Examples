# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------

variable "scylla_api_token" {
  description = <<-EOT
    ScyllaDB Cloud API bearer token.

    Leave this null (the default) and export the token in your shell instead:

        export SCYLLADB_CLOUD_TOKEN="$SC_TOKEN"

    The provider reads SCYLLADB_CLOUD_TOKEN natively, which keeps the token out
    of terraform.tfvars and out of the state file diff noise.
  EOT
  type        = string
  sensitive   = true
  default     = null
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
  default     = "AWS"
}

variable "region" {
  description = "Cloud region"
  type        = string
  default     = "us-east-1"
}

variable "cluster_vpc_cidr" {
  description = "CIDR block for the ScyllaDB cluster VPC. Must not overlap with client_vpc_cidr."
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

variable "byoa_id" {
  description = <<-EOT
    Cloud provider credential ID in ScyllaDB Cloud (the portal calls this BYOA).

    Leave null to deploy into ScyllaDB Cloud's own AWS account - no setup
    required. To deploy into your own AWS account, add it in the ScyllaDB Cloud
    portal first, then look up its numeric ID:

      curl -s -H "Authorization: Bearer $SCYLLADB_CLOUD_TOKEN" \
        https://api.cloud.scylladb.com/account/<accountId>/cloud-account

    Entries with "owner": "Account" are your own; "owner": "Scylla" are the
    ScyllaDB-managed defaults that null already selects.
  EOT
  type        = number
  default     = null
  sensitive   = true
}

# --- X Cloud autoscaling policy ---

variable "xcloud_instance_families" {
  description = "Instance families X Cloud may autoscale within, e.g. [\"i8g\"]. AWS families: i3, i3en, i4i, i7i, i7ie, i8g, i8ge."
  type        = list(string)
  default     = ["i8g"]
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

# ---------------------------------------------------------------------------
# Client-side networking
# ---------------------------------------------------------------------------

variable "connection_type" {
  description = "How the client network reaches the cluster: 'vpc_peering', 'transit_gateway' or 'none'."
  type        = string
  default     = "vpc_peering"

  validation {
    condition     = contains(["vpc_peering", "transit_gateway", "none"], var.connection_type)
    error_message = "Connection type must be 'vpc_peering', 'transit_gateway' or 'none'."
  }
}

variable "client_vpc_cidr" {
  description = "CIDR block of the client VPC. Must not overlap with cluster_vpc_cidr."
  type        = string
  default     = "10.100.0.0/16"
}

variable "use_default_vpc" {
  description = <<-EOT
    Peer to the account's default VPC in var.region instead of creating a
    dedicated client VPC. Routes are added to every route table in that VPC,
    so the cluster CIDR must not collide with a route already present there.
  EOT
  type        = bool
  default     = false
}

variable "client_subnet_cidr" {
  description = "CIDR of the client subnet, carved out of client_vpc_cidr."
  type        = string
  default     = "10.100.1.0/24"
}

variable "tgw_id" {
  description = "Transit Gateway ID, required when connection_type = 'transit_gateway'."
  type        = string
  default     = null
}

variable "tgw_ram_arn" {
  description = "Optional RAM resource-share ARN for the Transit Gateway, when it is shared from another account."
  type        = string
  default     = null
}

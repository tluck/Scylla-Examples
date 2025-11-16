variable "scylla_api_token" { # set in terraform.tvars
  description = "SC API token"
  type        = string
  sensitive   = true
}

variable "byoa_id" { # set in terraform.tvars
  description = "BYOA ID for account configuration"
  type        = string
  sensitive   = true
}

variable "gcp_project_id" { # set in terraform.tvars
  description = "project ID for GCP account configuration"
  type        = string
  default   = "cx-sa-lab"
}

variable "cluster_name" {
  description = "Cluster Name"
  type        = string
  default     = "tjl-Cluster-1"
}

variable "cloud" {
  description = "Cloud provider"
  type        = string
  default     = "GCP"
}

variable "region" {
  description = "Cloud region"
  type        = string
  default     = "us-west1"
}

variable "cluster_node_type" {
  description = "Cluster Node Type"
  type        = string
  default     = "n2-highmem-8"
}

variable "cluster_node_count" {
  description = "Cluster Node Count"
  type        = number
  default     = 3
}

variable "cluster_vpc_cdir" {
  description = "Cluster VPC CIDR"
  type        = string
  default     = "172.30.0.0/24"
}

variable "client_vpc_cdir" {
  description = "Client VPC CIDR"
  type        = string
  default     = "172.30.0.0/24"
}

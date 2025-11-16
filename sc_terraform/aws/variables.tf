# Add this variable to control networking type
variable "connection_type" {
  description = "Type of connection to use: 'vpc_peering' or 'transit_gateway'"
  type        = string
  default     = "vpc_peering"
  
  validation {
    condition     = contains(["vpc_peering", "transit_gateway"], var.connection_type)
    error_message = "Connection type must be either 'vpc_peering' or 'transit_gateway'."
  }
}

variable "scylla_api_token" { # set in terraform.tvars
  description = "SC API token"
  type        = string
  sensitive   = true
}

variable "byoa_id" { # set in terraform.tvars
  description = "BYOA ID for AWS account configuration"
  type        = string
  sensitive   = true
}

variable "cluster_name" {
  description = "Cluster Name"
  type        = string
  default     = "tjl-Cluster-1"
}

variable "cloud" {
  description = "Cloud provider"
  type        = string
  default     = "AWS"
}

variable "region" {
  description = "Cloud region"
  type        = string
  default     = "us-west-2"
}

variable "cluster_node_type" {
  description = "Cluster Node Type"
  type        = string
  default     = "i4i.large" # Example: i3.large, i3.xlarge, etc.
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

# for the cluster TGW and VPC peering
variable "tgw_id" { # see terraform.tvars for the actual value
  description = "TGW ID"
  type        = string
  # default     = "tgw-043a65fd02590511a"
}

variable "client_vpc_cdir" {
  description = "Client VPC CIDR"
  type        = string
  default     = "172.30.0.0/24"
}

# variable "ramarn" {
#   description = "TGW RAM ARN"
#   type        = string
#   default     = "arn:aws:ram:us-east-1:043400831220:resource-share/be3b0395-1782-47cb-9ae4-6d3517c6a721"
# }

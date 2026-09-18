variable "aws_region" {
  description = "AWS region for all PayWatch infrastructure"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project name, used as a prefix/tag across all resources"
  type        = string
  default     = "paywatch"
}

variable "environment" {
  description = "Deployment environment (e.g. dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "your_ip_cidr" {
  description = "Your public IP in CIDR notation (e.g. 1.2.3.4/32), used to restrict SSH access in security groups added in a later phase"
  type        = string
}

variable "github_repo" {
  description = "GitHub repo (owner/name) allowed to assume github_deploy_role via OIDC, scoped to the main branch"
  type        = string
  default     = "SumantMunagala/paywatch"
}

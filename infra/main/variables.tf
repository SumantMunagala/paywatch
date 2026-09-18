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

# GitHub's immutable OIDC subject-claim format (repos created after
# 2026-07-15 default to it - confirmed live via CloudTrail's actual token
# claims, then cross-checked against GitHub's current OIDC docs) embeds
# these numeric, never-reassigned IDs alongside the owner/repo names in the
# "sub" claim - see iam.tf's github_deploy_assume_role trust condition.
variable "github_owner_id" {
  description = "GitHub's numeric, immutable user ID for var.github_repo's owner"
  type        = string
  default     = "80919202"
}

variable "github_repo_id" {
  description = "GitHub's numeric, immutable repository ID for var.github_repo"
  type        = string
  default     = "1333753566"
}

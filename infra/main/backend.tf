terraform {
  backend "s3" {
    # Filled in by hand after `terraform apply` in infra/bootstrap — backend
    # blocks can't reference resources or variables, only literal values.
    bucket         = "paywatch-tf-state-67b52673"
    key            = "paywatch/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "paywatch-tf-locks"
    encrypt        = true
  }
}

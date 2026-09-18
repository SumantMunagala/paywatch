terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# S3 bucket names are globally unique across all AWS accounts, so a fixed
# name like "paywatch-tf-state" would collide with anyone else who picked
# the same name. This suffix makes it unique without needing a manual pick.
resource "random_id" "state_bucket_suffix" {
  byte_length = 4
}

resource "aws_s3_bucket" "terraform_state" {
  bucket = "paywatch-tf-state-${random_id.state_bucket_suffix.hex}"

  tags = {
    Project     = "paywatch"
    ManagedBy   = "terraform"
    Environment = "dev"
    Purpose     = "terraform-remote-state"
  }
}

resource "aws_s3_bucket_versioning" "terraform_state" {
  bucket = aws_s3_bucket.terraform_state.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "terraform_state" {
  bucket = aws_s3_bucket.terraform_state.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# PAY_PER_REQUEST avoids provisioning fixed read/write capacity for a table
# that only ever sees one write per "terraform apply" (the lock row).
resource "aws_dynamodb_table" "terraform_locks" {
  name         = "paywatch-tf-locks"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "LockID"

  attribute {
    name = "LockID"
    type = "S"
  }

  tags = {
    Project   = "paywatch"
    ManagedBy = "terraform"
    Purpose   = "terraform-state-locking"
  }
}

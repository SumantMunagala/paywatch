# ECR repositories for PayWatch (Phase 4, Task 2).
#
# Names match infra/main/templates/user_data.sh.tpl's existing image-pull
# loop (svc in api, producer, indexer-consumer, stats-consumer) from Phase 3,
# Task 3, rather than the shorter "paywatch-indexer"/"paywatch-stats" - that
# script already expects the "-consumer" suffix, so matching it avoids a
# second, conflicting naming scheme. iam.tf's ECR permissions are already
# wildcarded to repository/paywatch-*, so no IAM change is needed either way.
#
# Four explicit named resources, not a for_each/count loop - matches this
# project's established style (vpc.tf's 4 explicit subnets,
# security_groups.tf's per-rule resources).

resource "aws_ecr_repository" "producer" {
  name                 = "paywatch-producer"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = {
    Name = "paywatch-producer"
  }
}

resource "aws_ecr_repository" "indexer_consumer" {
  name                 = "paywatch-indexer-consumer"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = {
    Name = "paywatch-indexer-consumer"
  }
}

resource "aws_ecr_repository" "stats_consumer" {
  name                 = "paywatch-stats-consumer"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = {
    Name = "paywatch-stats-consumer"
  }
}

resource "aws_ecr_repository" "api" {
  name                 = "paywatch-api"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = {
    Name = "paywatch-api"
  }
}

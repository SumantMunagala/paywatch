# IAM roles for PayWatch.
#
# Two roles: ec2_role (assumed by the EC2 app host itself, via an instance
# profile) and github_deploy_role (assumed by GitHub Actions via OIDC - no
# stored AWS credentials in CI, see the "OIDC federation over stored
# credentials" decision in CLAUDE.md). Trust and permissions policies are
# both written as aws_iam_policy_document data sources rather than inline
# jsonencode({...}) - HCL-native, catches syntax errors at plan time, and
# each statement reads close to plain English.

data "aws_caller_identity" "current" {}

# --- EC2 instance role -----------------------------------------------
# Assumed by the EC2 app host (Phase 3) via its instance profile. Needs to
# pull images from ECR and read app secrets from Secrets Manager - nothing
# else.

data "aws_iam_policy_document" "ec2_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "ec2_role" {
  name               = "${var.project_name}-ec2-role"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume_role.json

  tags = {
    Name = "${var.project_name}-ec2-role"
  }
}

data "aws_iam_policy_document" "ec2_permissions" {
  # ecr:GetAuthorizationToken has no resource-level permissions at all
  # (AWS IAM action reference) - it MUST be Resource = "*", not a scoping
  # choice.
  statement {
    sid       = "EcrAuth"
    effect    = "Allow"
    actions   = ["ecr:GetAuthorizationToken"]
    resources = ["*"]
  }

  statement {
    sid    = "EcrPull"
    effect = "Allow"
    actions = [
      "ecr:BatchGetImage",
      "ecr:GetDownloadUrlForLayer",
    ]
    resources = [
      "arn:aws:ecr:${var.aws_region}:${data.aws_caller_identity.current.account_id}:repository/paywatch-*",
    ]
  }

  statement {
    sid    = "SecretsManagerRead"
    effect = "Allow"
    actions = [
      "secretsmanager:GetSecretValue",
    ]
    resources = [
      "arn:aws:secretsmanager:${var.aws_region}:${data.aws_caller_identity.current.account_id}:secret:paywatch/*",
    ]
  }
}

resource "aws_iam_role_policy" "ec2_role_inline" {
  name   = "${var.project_name}-ec2-role-policy"
  role   = aws_iam_role.ec2_role.id
  policy = data.aws_iam_policy_document.ec2_permissions.json
}

resource "aws_iam_instance_profile" "ec2_instance_profile" {
  name = "${var.project_name}-ec2-instance-profile"
  role = aws_iam_role.ec2_role.name

  tags = {
    Name = "${var.project_name}-ec2-instance-profile"
  }
}

# --- GitHub Actions OIDC provider + deploy role -----------------------
# GitHub Actions assumes github_deploy_role directly via OIDC - no long-
# lived AWS access keys stored as GitHub secrets. Temporary credentials are
# issued per workflow run and expire when the run completes.
#
# thumbprint_list is hardcoded rather than fetched via a tls_certificate
# data source: AWS has validated GitHub's OIDC provider against its own
# trusted CA store (not the supplied thumbprint) since 2022, so the value
# below only needs to satisfy the resource schema. Adding hashicorp/tls as
# a second provider for a single derived value isn't worth it given this
# project's minimal-provider footprint (providers.tf declares only aws).

resource "aws_iam_openid_connect_provider" "github_actions" {
  url             = "https://token.actions.githubusercontent.com"
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = ["6938fd4d98bab03faadb97b34396831e3780aea1"]

  tags = {
    Name = "${var.project_name}-github-actions-oidc"
  }
}

data "aws_iam_policy_document" "github_deploy_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.github_actions.arn]
    }

    # StringEquals, not StringLike: both condition values below are exact
    # literal strings with no wildcard in them, so an exact match is both
    # correct and the tighter choice - StringLike is for when the value
    # itself contains a glob pattern (e.g. "repo:org/repo:*" to allow any
    # branch), which isn't the case here (scoped to refs/heads/main only).
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }

    # "sub" uses GitHub's immutable subject-claim format
    # (repo:<owner>@<owner_id>/<repo>@<repo_id>:ref:...), not the classic
    # repo:<owner>/<repo>:ref:... format AWS's own docs still show as the
    # default example. Repos created after 2026-07-15 (this one included)
    # get the immutable format by default - found live via a real
    # AccessDenied in CloudTrail showing the actual token's sub claim,
    # confirmed against GitHub's current OIDC docs rather than assumed.
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:sub"
      values = [
        "repo:${split("/", var.github_repo)[0]}@${var.github_owner_id}/${split("/", var.github_repo)[1]}@${var.github_repo_id}:ref:refs/heads/main"
      ]
    }
  }
}

resource "aws_iam_role" "github_deploy_role" {
  name               = "${var.project_name}-github-deploy-role"
  assume_role_policy = data.aws_iam_policy_document.github_deploy_assume_role.json

  tags = {
    Name = "${var.project_name}-github-deploy-role"
  }
}

data "aws_iam_policy_document" "github_deploy_permissions" {
  # Same hard AWS constraint as ec2_permissions above.
  statement {
    sid       = "EcrAuth"
    effect    = "Allow"
    actions   = ["ecr:GetAuthorizationToken"]
    resources = ["*"]
  }

  statement {
    sid    = "EcrPush"
    effect = "Allow"
    actions = [
      "ecr:BatchCheckLayerAvailability",
      "ecr:PutImage",
      "ecr:InitiateLayerUpload",
      "ecr:UploadLayerPart",
      "ecr:CompleteLayerUpload",
    ]
    resources = [
      "arn:aws:ecr:${var.aws_region}:${data.aws_caller_identity.current.account_id}:repository/paywatch-*",
    ]
  }

  # ec2:DescribeInstances has no resource-level permissions either - MUST
  # be Resource = "*", needed by the deploy script to find the EC2 app
  # host's IP.
  statement {
    sid       = "DescribeInstancesForDeploy"
    effect    = "Allow"
    actions   = ["ec2:DescribeInstances"]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "github_deploy_role_inline" {
  name   = "${var.project_name}-github-deploy-role-policy"
  role   = aws_iam_role.github_deploy_role.id
  policy = data.aws_iam_policy_document.github_deploy_permissions.json
}

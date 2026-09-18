output "vpc_id" {
  description = "ID of the PayWatch VPC"
  value       = aws_vpc.main.id
}

output "public_subnet_ids" {
  description = "IDs of the public subnets (us-east-1a, us-east-1b)"
  value = [
    aws_subnet.public_us_east_1a.id,
    aws_subnet.public_us_east_1b.id,
  ]
}

output "private_subnet_ids" {
  description = "IDs of the private subnets (us-east-1a, us-east-1b)"
  value = [
    aws_subnet.private_us_east_1a.id,
    aws_subnet.private_us_east_1b.id,
  ]
}

output "sg_ec2_id" {
  description = "ID of the security group attached to the PayWatch EC2 app host"
  value       = aws_security_group.sg_ec2.id
}

output "sg_rds_id" {
  description = "ID of the security group attached to the PayWatch RDS instance"
  value       = aws_security_group.sg_rds.id
}

output "sg_elasticache_id" {
  description = "ID of the security group attached to the PayWatch ElastiCache cluster"
  value       = aws_security_group.sg_elasticache.id
}

output "ec2_instance_profile_name" {
  description = "Name of the instance profile to attach to the PayWatch EC2 app host (Phase 3)"
  value       = aws_iam_instance_profile.ec2_instance_profile.name
}

output "github_deploy_role_arn" {
  description = "ARN of the IAM role GitHub Actions assumes via OIDC to push images to ECR and deploy"
  value       = aws_iam_role.github_deploy_role.arn
}

output "rds_endpoint" {
  description = "Connection endpoint (host:port) of the PayWatch RDS Postgres instance"
  value       = aws_db_instance.main.endpoint
}

output "rds_secrets_manager_secret_name" {
  description = "Name of the Secrets Manager secret holding RDS master credentials"
  value       = aws_secretsmanager_secret.rds_credentials.name
}

output "redis_endpoint" {
  description = "Primary endpoint address of the PayWatch Redis replication group"
  value       = aws_elasticache_replication_group.redis.primary_endpoint_address
}

output "ec2_public_ip" {
  description = "Public IP address of the PayWatch EC2 app host"
  value       = aws_instance.app.public_ip
}

output "elastic_ip" {
  description = "Elastic IP address associated with the PayWatch EC2 app host (stable across restarts)"
  value       = aws_eip.app.public_ip
}

output "ecr_producer_repository_url" {
  description = "URL of the ECR repository for the producer image"
  value       = aws_ecr_repository.producer.repository_url
}

output "ecr_indexer_consumer_repository_url" {
  description = "URL of the ECR repository for the indexer consumer image"
  value       = aws_ecr_repository.indexer_consumer.repository_url
}

output "ecr_stats_consumer_repository_url" {
  description = "URL of the ECR repository for the stats consumer image"
  value       = aws_ecr_repository.stats_consumer.repository_url
}

output "ecr_api_repository_url" {
  description = "URL of the ECR repository for the API image"
  value       = aws_ecr_repository.api.repository_url
}

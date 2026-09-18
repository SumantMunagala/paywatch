# ElastiCache Redis for PayWatch (Phase 3, Task 2).
#
# aws_elasticache_replication_group (not the older aws_elasticache_cluster)
# is used even for this single-node setup because it's the only resource
# that exposes primary_endpoint_address. num_cache_clusters = 1 with
# automatic_failover_enabled left unset (defaults false) is AWS's own
# documented single-node pattern - no replica, cluster mode disabled.

# --- Subnet group -------------------------------------------------------
# Same two private subnets as RDS - no public exposure path.

resource "aws_elasticache_subnet_group" "redis" {
  name = "${var.project_name}-redis-subnet-group"
  subnet_ids = [
    aws_subnet.private_us_east_1a.id,
    aws_subnet.private_us_east_1b.id,
  ]

  tags = {
    Name = "${var.project_name}-redis-subnet-group"
  }
}

# --- Redis replication group (single node) --------------------------------
# cache.t3.micro / 1 node - matches the project's dev-environment cost
# posture (same reasoning as RDS's db.t3.micro single-AZ choice).

resource "aws_elasticache_replication_group" "redis" {
  replication_group_id = "${var.project_name}-redis"
  description          = "PayWatch Redis cache"

  engine         = "redis"
  engine_version = "7.0"
  node_type      = "cache.t3.micro"

  num_cache_clusters = 1

  subnet_group_name  = aws_elasticache_subnet_group.redis.name
  security_group_ids = [aws_security_group.sg_elasticache.id]

  tags = {
    Name = "${var.project_name}-redis"
  }
}

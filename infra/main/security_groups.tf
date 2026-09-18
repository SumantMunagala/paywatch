# Security groups for PayWatch.
#
# One security group per tier: EC2 (app host), RDS (Postgres), ElastiCache
# (Redis). Rules are defined as separate aws_vpc_security_group_ingress_rule/
# aws_vpc_security_group_egress_rule resources (AWS's current recommended
# pattern) rather than inline ingress {}/egress {} blocks on the security
# group itself. Each rule becomes its own named, independently visible
# resource with its own description - and mixing inline blocks with
# separately-managed rules on the same security group is a well-known
# source of Terraform drift, so this file uses only the standalone-rule
# style throughout (no inline blocks at all).

# --- EC2 security group -------------------------------------------------
# Attached to the future EC2 app host (Phase 3). Allows SSH from the admin
# IP only and public FastAPI traffic; outbound is unrestricted since the
# app host may need to reach arbitrary external services.

resource "aws_security_group" "sg_ec2" {
  name        = "${var.project_name}-ec2-sg"
  description = "PayWatch EC2 app host - SSH from admin IP, FastAPI from internet"
  vpc_id      = aws_vpc.main.id

  tags = {
    Name = "${var.project_name}-ec2-sg"
  }
}

resource "aws_vpc_security_group_ingress_rule" "ec2_ssh_from_admin_ip" {
  security_group_id = aws_security_group.sg_ec2.id
  description       = "Allow SSH from the admin IP only"
  cidr_ipv4         = var.your_ip_cidr
  from_port         = 22
  to_port           = 22
  ip_protocol       = "tcp"
}

resource "aws_vpc_security_group_ingress_rule" "ec2_fastapi_from_internet" {
  security_group_id = aws_security_group.sg_ec2.id
  description       = "Allow FastAPI (8000) from anywhere - the public entry point for the app"
  cidr_ipv4         = "0.0.0.0/0"
  from_port         = 8000
  to_port           = 8000
  ip_protocol       = "tcp"
}

resource "aws_vpc_security_group_egress_rule" "ec2_all_outbound" {
  security_group_id = aws_security_group.sg_ec2.id
  description       = "Allow all outbound traffic from the EC2 host"
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

# --- RDS security group --------------------------------------------------
# Attached to the future RDS Postgres instance (Phase 3). Inbound is
# restricted to the EC2 app host's security group - no CIDR-based inbound,
# no internet access at all. Outbound is restricted to inside the VPC.

resource "aws_security_group" "sg_rds" {
  name        = "${var.project_name}-rds-sg"
  description = "PayWatch RDS Postgres - inbound from EC2 app host only, no internet"
  vpc_id      = aws_vpc.main.id

  tags = {
    Name = "${var.project_name}-rds-sg"
  }
}

resource "aws_vpc_security_group_ingress_rule" "rds_postgres_from_ec2" {
  security_group_id            = aws_security_group.sg_rds.id
  description                  = "Allow Postgres (5432) from the EC2 app host security group only"
  referenced_security_group_id = aws_security_group.sg_ec2.id
  from_port                    = 5432
  to_port                      = 5432
  ip_protocol                  = "tcp"
}

resource "aws_vpc_security_group_egress_rule" "rds_all_outbound_within_vpc" {
  security_group_id = aws_security_group.sg_rds.id
  description       = "Allow all outbound traffic within the VPC only - RDS never needs outbound internet"
  cidr_ipv4         = aws_vpc.main.cidr_block
  ip_protocol       = "-1"
}

# --- ElastiCache security group -------------------------------------------
# Attached to the future ElastiCache Redis cluster (Phase 3). Same shape
# as sg_rds: inbound from the EC2 app host's security group only, outbound
# restricted to inside the VPC.

resource "aws_security_group" "sg_elasticache" {
  name        = "${var.project_name}-elasticache-sg"
  description = "PayWatch ElastiCache Redis - inbound from EC2 app host only, no internet"
  vpc_id      = aws_vpc.main.id

  tags = {
    Name = "${var.project_name}-elasticache-sg"
  }
}

resource "aws_vpc_security_group_ingress_rule" "elasticache_redis_from_ec2" {
  security_group_id            = aws_security_group.sg_elasticache.id
  description                  = "Allow Redis (6379) from the EC2 app host security group only"
  referenced_security_group_id = aws_security_group.sg_ec2.id
  from_port                    = 6379
  to_port                      = 6379
  ip_protocol                  = "tcp"
}

resource "aws_vpc_security_group_egress_rule" "elasticache_all_outbound_within_vpc" {
  security_group_id = aws_security_group.sg_elasticache.id
  description       = "Allow all outbound traffic within the VPC only - ElastiCache never needs outbound internet"
  cidr_ipv4         = aws_vpc.main.cidr_block
  ip_protocol       = "-1"
}

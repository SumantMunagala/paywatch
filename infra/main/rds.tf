# RDS Postgres for PayWatch (Phase 3, Task 1).
#
# random_password generates the master password instead of AWS's native
# manage_master_user_password (RDS-managed Secrets Manager secret) - this
# project explicitly wants a hand-rolled secret whose exact JSON shape
# (username/password/host/port/dbname) the app's config code will read in
# a later task, not AWS's own managed-secret format.

resource "random_password" "rds_master_password" {
  # Alphanumeric only, no special characters. Found live (Phase 3 Task 3):
  # a password containing '$' broke once it round-tripped through the EC2
  # user-data script's .env file - Docker Compose auto-loads .env from its
  # working directory and interpolates "$VAR"/"${VAR}" patterns in it for
  # its own templating, so "$GzdRP" inside the password was silently
  # swallowed as an unset-variable reference. The password also contained
  # '[', '%', ')' - all reserved/significant in URI syntax, risky once
  # embedded in a postgresql:// connection string. Rather than enumerate a
  # "safe" special-character subset that might still break some other
  # future consumer, dropping special characters entirely sidesteps the
  # whole class of escaping bugs; length is bumped to 24 (from 20) to keep
  # equivalent entropy (62^24 alphanumeric combinations is still enormous
  # for a master password).
  length  = 24
  special = false
}

# --- DB subnet group -------------------------------------------------------
# RDS requires a subnet group spanning >= 2 AZs even for a single-AZ
# instance. Uses the two private subnets - no public exposure path.

resource "aws_db_subnet_group" "rds" {
  name = "${var.project_name}-rds-subnet-group"
  subnet_ids = [
    aws_subnet.private_us_east_1a.id,
    aws_subnet.private_us_east_1b.id,
  ]

  tags = {
    Name = "${var.project_name}-rds-subnet-group"
  }
}

# --- RDS instance ------------------------------------------------------------
# db.t3.micro / 20GB gp2, single-AZ, no deletion protection - matches the
# project's dev-environment cost posture (same reasoning as "EC2 + Docker
# Compose over EKS" in CLAUDE.md).

resource "aws_db_instance" "main" {
  identifier     = "${var.project_name}-rds"
  engine         = "postgres"
  engine_version = "15"
  instance_class = "db.t3.micro"

  allocated_storage = 20
  storage_type      = "gp2"

  db_name  = var.project_name
  username = var.project_name
  password = random_password.rds_master_password.result

  db_subnet_group_name   = aws_db_subnet_group.rds.name
  vpc_security_group_ids = [aws_security_group.sg_rds.id]

  publicly_accessible = false
  deletion_protection = false
  skip_final_snapshot = true

  tags = {
    Name = "${var.project_name}-rds"
  }

  # engine_version = "15" (major-version-only) lets RDS pick the latest
  # 15.x minor at create time, but AWS then reports the full version back
  # (e.g. "15.14") - every subsequent plan would otherwise show a diff
  # trying to "downgrade" back to "15". Ignoring it here is the standard
  # workaround for this well-known aws_db_instance gotcha.
  lifecycle {
    ignore_changes = [engine_version]
  }
}

# --- Secrets Manager: RDS credentials ---------------------------------------
# Secret name is the literal "paywatch/rds-credentials" (not
# "${var.project_name}/rds-credentials") because iam.tf's ec2_role policy
# is already scoped to the literal ARN pattern paywatch/* - keep this
# name matching that exactly. JSON shape matches what the app's config
# code (Phase 3 Task 4) will expect.

resource "aws_secretsmanager_secret" "rds_credentials" {
  name        = "paywatch/rds-credentials"
  description = "PayWatch RDS Postgres master credentials - read by ec2_role at runtime"

  tags = {
    Name = "${var.project_name}-rds-credentials"
  }
}

resource "aws_secretsmanager_secret_version" "rds_credentials" {
  secret_id = aws_secretsmanager_secret.rds_credentials.id

  # host uses .address (bare hostname, no port) because this JSON needs a
  # separate numeric "port" field - .endpoint would embed ":5432" into the
  # host string itself, doubling up the port information.
  secret_string = jsonencode({
    username = aws_db_instance.main.username
    password = random_password.rds_master_password.result
    host     = aws_db_instance.main.address
    port     = aws_db_instance.main.port
    dbname   = aws_db_instance.main.db_name
  })
}

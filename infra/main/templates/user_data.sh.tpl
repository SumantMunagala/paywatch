#!/bin/bash
set -euo pipefail
exec > >(tee /var/log/user-data.log) 2>&1

echo "=== PayWatch EC2 bootstrap starting: $(date) ==="

# --- Docker + prerequisites ---------------------------------------------
dnf install -y docker git unzip
systemctl enable --now docker
usermod -aG docker ec2-user

# --- Docker Compose v2 CLI plugin (not in AL2023's own dnf repos) -------
mkdir -p /usr/local/lib/docker/cli-plugins
curl -SL https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64 \
  -o /usr/local/lib/docker/cli-plugins/docker-compose
chmod +x /usr/local/lib/docker/cli-plugins/docker-compose

# --- AWS CLI v2 (AL2023 usually ships it already - idempotent) ---------
if ! command -v aws &> /dev/null; then
  curl -s "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o /tmp/awscliv2.zip
  unzip -q /tmp/awscliv2.zip -d /tmp
  /tmp/aws/install
fi

# --- ECR registry login (registry-level - succeeds even with 0 repos) --
aws ecr get-login-password --region ${aws_region} | \
  docker login --username AWS --password-stdin ${account_id}.dkr.ecr.${aws_region}.amazonaws.com

# --- Fetch the app repo for docker-compose.aws.yml ----------------------
git clone https://github.com/${github_repo}.git /opt/paywatch || true
cd /opt/paywatch

# --- .env for docker compose / the app -----------------------------------
# No RDS credential fetch here (Phase 4 change) - AWS_REGION alone is enough
# for shared/config.py to fetch POSTGRES_URL live from Secrets Manager on
# every container start (see CLAUDE.md's Decisions log). REDIS_URL has no
# equivalent Secrets Manager path - ElastiCache has no auth_token configured,
# so there's no real Redis credential to protect - and stays a plain value
# from Terraform's own redis_endpoint. KAFKA_BOOTSTRAP_SERVERS points at
# redpanda's *internal* listener (9093): on EC2, producer/indexer/stats/api
# all run as containers on the compose network now, not host processes, so
# they need the listener that advertises itself as "redpanda:9093" - not the
# external one (9092), which advertises "localhost:9092" and would just have
# each container trying to reach itself.
cat > /opt/paywatch/.env <<EOF
AWS_REGION=${aws_region}
ECR_REGISTRY=${account_id}.dkr.ecr.${aws_region}.amazonaws.com
KAFKA_BOOTSTRAP_SERVERS=redpanda:9093
ELASTICSEARCH_URL=http://elasticsearch:9200
REDIS_URL=redis://${redis_endpoint}:6379
EOF

# --- Pull PayWatch images from ECR ---------------------------------------
# Repo names match iam.tf's paywatch-* ECR ARN scope. Non-fatal on failure -
# e.g. if this instance booted before a fresh clone of main actually has
# these images pushed - so one bad pull doesn't abort the rest of the script.
for svc in api producer indexer-consumer stats-consumer; do
  docker pull ${account_id}.dkr.ecr.${aws_region}.amazonaws.com/paywatch-$svc:latest \
    || echo "WARN: pull failed for paywatch-$svc"
done

# --- Bring the stack up ----------------------------------------------------
# Non-fatal on failure - e.g. if the cloned main branch doesn't have
# docker-compose.aws.yml yet - logged rather than aborting the script.
docker compose -f docker-compose.aws.yml up -d \
  || echo "WARN: docker compose up failed"

echo "=== PayWatch EC2 bootstrap finished: $(date) ==="

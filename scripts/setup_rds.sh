#!/bin/bash
# Applies infra/schema.sql to the PayWatch RDS instance and verifies it.
# Run this from the EC2 app host (inside the cloned repo) any time the RDS
# instance is recreated and needs its schema/seed data re-applied.
# Requires: AWS CLI + the ec2_role IAM permission already granted
# (secretsmanager:GetSecretValue scoped to paywatch/*), and this repo
# checked out at the location this script is run from.
set -euo pipefail
cd "$(dirname "$0")/.."

AWS_REGION="${AWS_REGION:-us-east-1}"
SECRET_ID="paywatch/rds-credentials"

echo "=== Fetching RDS credentials from Secrets Manager ($SECRET_ID) ==="
SECRET_JSON=$(aws secretsmanager get-secret-value \
  --secret-id "$SECRET_ID" \
  --region "$AWS_REGION" \
  --query SecretString --output text)

DB_USER=$(echo "$SECRET_JSON" | python3 -c "import sys, json; print(json.load(sys.stdin)['username'])")
DB_PASS=$(echo "$SECRET_JSON" | python3 -c "import sys, json; print(json.load(sys.stdin)['password'])")
DB_HOST=$(echo "$SECRET_JSON" | python3 -c "import sys, json; print(json.load(sys.stdin)['host'])")
DB_PORT=$(echo "$SECRET_JSON" | python3 -c "import sys, json; print(json.load(sys.stdin)['port'])")
DB_NAME=$(echo "$SECRET_JSON" | python3 -c "import sys, json; print(json.load(sys.stdin)['dbname'])")

export POSTGRES_URL="postgresql://$DB_USER:$DB_PASS@$DB_HOST:$DB_PORT/$DB_NAME"

echo "=== Applying schema.sql (via the existing, unmodified apply_schema.py) ==="
python3 scripts/apply_schema.py

echo "=== Verifying via psql ==="
if ! command -v psql &> /dev/null; then
  sudo dnf install -y postgresql15
fi

PGPASSWORD="$DB_PASS" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "\dt"
PGPASSWORD="$DB_PASS" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "SELECT * FROM merchants;"

echo "=== Done ==="

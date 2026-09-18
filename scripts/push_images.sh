#!/bin/bash
# Builds all 4 PayWatch app images and pushes them to their ECR repos
# (infra/main/ecr.tf, Phase 4 Task 2), each tagged both :latest and with the
# current short git SHA. Requires: AWS CLI configured locally, Docker
# running, and this repo checked out at the location this script is run
# from (build context is the repo root for every image - see the Dockerfiles
# for why: every service's main.py needs shared/ present as a sibling on
# disk).
set -euo pipefail
cd "$(dirname "$0")/.."

AWS_REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
ECR_REGISTRY="$ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com"
GIT_SHA=$(git rev-parse --short HEAD)

echo "=== Logging in to ECR ($ECR_REGISTRY) ==="
aws ecr get-login-password --region "$AWS_REGION" | \
  docker login --username AWS --password-stdin "$ECR_REGISTRY"

dockerfile_for() {
  case "$1" in
    producer) echo "producer/Dockerfile" ;;
    indexer-consumer) echo "consumers/indexer/Dockerfile" ;;
    stats-consumer) echo "consumers/stats/Dockerfile" ;;
    api) echo "api/Dockerfile" ;;
  esac
}

for component in producer indexer-consumer stats-consumer api; do
  dockerfile=$(dockerfile_for "$component")
  repo="$ECR_REGISTRY/paywatch-$component"

  echo "=== Building paywatch-$component (from $dockerfile) ==="
  docker build \
    -f "$dockerfile" \
    -t "$repo:latest" \
    -t "$repo:$GIT_SHA" \
    .

  echo "=== Pushing paywatch-$component ==="
  docker push "$repo:latest"
  docker push "$repo:$GIT_SHA"

  echo "Pushed: $repo:latest"
  echo "Pushed: $repo:$GIT_SHA"
done

echo "=== Done ==="

#!/usr/bin/env bash
set -euo pipefail

# Parameterized deployment script for Cloud Run Bigtable MCP & Vertex AI Agent Runtime
PROJECT_ID="${PROJECT_ID:?PROJECT_ID environment variable must be set}"
REGION="${REGION:-us-central1}"
BIGTABLE_INSTANCE_ID="${BIGTABLE_INSTANCE_ID:-operations-db}"
AGENT_SERVICE_ACCOUNT="${AGENT_SERVICE_ACCOUNT:-cymbal-sa-data@${PROJECT_ID}.iam.gserviceaccount.com}"

echo "=== 1. Deploying Cloud Run Bigtable MCP Toolbox Microservice ==="
TMP_TOOLS="$(mktemp)"
sed -e "s/\${PROJECT_ID}/${PROJECT_ID}/g" \
    -e "s/\${BIGTABLE_INSTANCE_ID}/${BIGTABLE_INSTANCE_ID}/g" \
    deployment/tools.yaml > "${TMP_TOOLS}"

gcloud secrets versions add bigtable-mcp-tools-secret \
  --data-file="${TMP_TOOLS}" \
  --project="${PROJECT_ID}"
rm -f "${TMP_TOOLS}"

gcloud run deploy mcp-toolbox-bigtable \
  --image="us-central1-docker.pkg.dev/database-toolbox/toolbox/toolbox:latest" \
  --region="${REGION}" \
  --project="${PROJECT_ID}" \
  --service-account="${AGENT_SERVICE_ACCOUNT}" \
  --set-secrets="/config/tools.yaml=bigtable-mcp-tools-secret:latest" \
  --args="--tools-file=/config/tools.yaml,--address=0.0.0.0,--port=8080" \
  --no-allow-unauthenticated

echo "=== 2. Deploying Cymbal Operations Agent to Vertex AI Agent Runtime ==="
agents-cli deploy \
  --deployment-target agent_runtime \
  --project "${PROJECT_ID}" \
  --region "${REGION}" \
  --service-name cymbal_operations_agent \
  --service-account "${AGENT_SERVICE_ACCOUNT}" \
  --no-confirm-project

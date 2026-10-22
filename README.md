# Cymbal Retail Unified Operations Platform & Multi-Tool ADK Coordinator Agent

Enterprise lakehouse and multi-tool AI agent implementation for **Cymbal Retail**, fulfilling the **Business Requirements Document (`brd.md`)** and **Software Design Document (`SDD.md`)** across all 7 operational use cases (`UC-1.1` through `UC-2.4`).

---

## Architecture & Component Topology

```text
├── app/
│   ├── __init__.py
│   ├── agent.py                        # Root ADK Coordinator (`cymbal_operations_agent`) + BigQueryAgentAnalyticsPlugin
│   ├── fast_api_app.py                 # FastAPI / A2A / Reasoning Engine HTTP Server
│   ├── app_utils/                      # Session, Artifact, A2A, and Reasoning Engine adapters
│   └── tools/
│       ├── __init__.py
│       ├── analytics_tool.py           # Tool 1: `cymbal_analytics_tool` (BigQuery Conversational Data Agent NL2SQL)
│       ├── rag_tool.py                 # Tool 2: `pos_troubleshooting_rag_tool` (500-char chunking + N-1..N+1 stitching)
│       └── bigtable_mcp_tool.py        # Tool 3: `read_cashier_realtime_alerts` (Cloud Run MCP Toolbox + OIDC Auth)
├── deployment/
│   ├── tools.yaml                      # Cloud Bigtable MCP Toolbox configuration (`operations-db`)
│   ├── deploy.sh                       # Parameterized Cloud Run MCP & Agent Runtime deployment script
│   └── terraform/                      # Infrastructure-as-Code manifests
├── tests/
│   ├── eval/
│   │   ├── datasets/
│   │   │   ├── basic-dataset.json      # Baseline 10-case operational benchmark
│   │   │   ├── eval-data.json          # 4-Tier stratified single-turn golden dataset (12 cases)
│   │   │   └── eval-data2.json         # Multi-turn context retention, intent switching & guardrail dataset (6 trajectories)
│   │   ├── eval_config.yaml            # Automated Quality Gate metrics & custom evaluators
│   │   ├── response_quality.py         # LLM-as-a-Judge evaluator (`gemini-3.7-flash`)
│   │   └── evaluation_report.md        # 2-Section Comprehensive Agent Evaluation Report
│   ├── integration/                    # End-to-end integration test suite
│   └── unit/                           # Hermetic unit tests (`pytest`)
├── .env.example                        # Environment variable template (zero hardcoded secrets/IDs)
├── agents-cli-manifest.yaml            # Agents CLI deployment manifest (`agent_runtime`)
├── Dockerfile                          # Production container image definition
├── Makefile                            # Build, test, lint, eval, and deploy automation targets
├── pyproject.toml                      # Project metadata and locked dependencies
├── SDD.md                              # Authoritative Software Design Document
└── brd.md                              # Business Requirements Document
```

---

## Environment Setup & Configuration

1. Copy `.env.example` to `.env` and populate environment variables for your target GCP project:
   ```bash
   cp .env.example .env
   ```
2. Install dependencies using `uv`:
   ```bash
   make install
   ```
3. Run unit tests and linter:
   ```bash
   make test
   make lint
   ```

---

## Evaluation & Quality Gate (`>= 4.0 / 5.0`)

Run the automated evaluation suite via `agents-cli`:

```bash
# Grade populated trace benchmark (tool_use_quality & grounding >= 4.0)
agents-cli eval grade \
  --traces tests/eval/datasets/basic-dataset.json \
  --metrics tool_use_quality,grounding \
  --project "${PROJECT_ID}" \
  --region "${REGION}"

# Run full evaluation pipeline with custom metrics defined in tests/eval/eval_config.yaml
agents-cli eval grade \
  --traces tests/eval/datasets/eval-data.json \
  --config tests/eval/eval_config.yaml \
  --project "${PROJECT_ID}" \
  --region "${REGION}"
```

---

## Cloud Deployment (`Vertex AI Agent Runtime`)

Deploy the Cloud Run MCP Toolbox microservice and the `cymbal_operations_agent` to Vertex AI Agent Runtime (`us-central1`):

```bash
make deploy
```

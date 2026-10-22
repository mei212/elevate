"""Cloud Bigtable MCP Toolset Gateway (`read_cashier_realtime_alerts`).

Connects to the Cloud Run MCP Toolbox microservice (`mcp-toolbox-bigtable`)
using OIDC bearer token authentication (`google.oauth2.id_token` with local
`gcloud auth print-identity-token` fallback) and 3 exponential backoff retries.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import time
from typing import Any

from google.auth.transport.requests import Request
from google.oauth2 import id_token
import requests

logger = logging.getLogger(__name__)


def _get_bigtable_mcp_base_url() -> str:
    """Resolve the Cloud Run Bigtable MCP base URL from environment."""
    url = os.environ.get("BIGTABLE_MCP_URL", "").strip().rstrip("/")
    if not url:
        raise ValueError(
            "BIGTABLE_MCP_URL environment variable is required for Cloud Bigtable MCP."
        )
    return url


def _fetch_oidc_bearer_token(audience: str, prefer_gcloud: bool = False) -> str:
    """Fetch a GCP OIDC ID token for the target Cloud Run service audience."""
    if not prefer_gcloud:
        try:
            token = id_token.fetch_id_token(Request(), audience)
            if token and os.environ.get("K_SERVICE"):
                return token
        except Exception as exc:
            logger.debug("Metadata OIDC fetch fell back to gcloud CLI: %s", exc)

    try:
        proc = subprocess.run(
            ["gcloud", "auth", "print-identity-token"],
            capture_output=True,
            text=True,
            check=True,
            timeout=15,
        )
        token = proc.stdout.strip()
        if token:
            return token
    except Exception as exc:
        logger.debug("gcloud print-identity-token fallback to metadata server: %s", exc)

    try:
        token = id_token.fetch_id_token(Request(), audience)
        if token:
            return token
    except Exception as exc:
        logger.warning("Failed to obtain OIDC token via metadata server: %s", exc)

    return ""


def get_bigtable_mcp_toolset() -> Any:
    """Create an ADK McpToolset configured with OIDC bearer authentication."""
    from google.adk.tools.mcp_tool import McpToolset
    from google.adk.tools.mcp_tool.mcp_session_manager import (
        StreamableHTTPConnectionParams,
    )

    base_url = _get_bigtable_mcp_base_url()
    token = _fetch_oidc_bearer_token(base_url)
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    return McpToolset(
        connection_params=StreamableHTTPConnectionParams(
            url=f"{base_url}/mcp",
            headers=headers,
        )
    )


def _normalize_mcp_row(raw_row: dict[str, Any]) -> dict[str, Any]:
    """Format a Bigtable MCP row into the canonical Cymbal Operations schema."""
    return {
        "source": "Cloud Run MCP Toolbox for Databases (Direct /mcp JSON-RPC)",
        "snapshot_type": (
            "Latest 1-Hour Rolling Window Aggregation Snapshot (Already aggregated "
            "over the past 60 minutes. Do NOT sum or aggregate across multiple rows.)"
        ),
        "row_key": raw_row.get("row_key"),
        "audit_status": raw_row.get("audit_status"),
        "cashier_1h_total_discount_usd": (
            round(float(raw_row["cashier_1h_total_discount_usd"]), 4)
            if raw_row.get("cashier_1h_total_discount_usd") is not None
            else None
        ),
        "cashier_1h_txn_count": (
            int(raw_row["cashier_1h_txn_count"])
            if raw_row.get("cashier_1h_txn_count") is not None
            else None
        ),
        "cashier_1h_avg_discount_pct": (
            round(float(raw_row["cashier_1h_avg_discount_pct"]), 4)
            if raw_row.get("cashier_1h_avg_discount_pct") is not None
            else None
        ),
        "cashier_1h_manual_override_count": (
            int(raw_row["cashier_1h_manual_override_count"])
            if raw_row.get("cashier_1h_manual_override_count") is not None
            else None
        ),
        "cashier_1h_promo_count": (
            int(raw_row["cashier_1h_promo_count"])
            if raw_row.get("cashier_1h_promo_count") is not None
            else None
        ),
        "cashier_1h_promo_rate": (
            round(float(raw_row["cashier_1h_promo_rate"]), 4)
            if raw_row.get("cashier_1h_promo_rate") is not None
            else None
        ),
        "last_event_ts": raw_row.get("last_event_ts"),
        "risk_score": (
            round(float(raw_row["risk_score"]), 4)
            if raw_row.get("risk_score") is not None
            else None
        ),
    }


def read_cashier_realtime_alerts(row_key_prefix: str) -> str:
    """Reads the latest 1-hour rolling metrics and audit status flags for a cashier from Cloud Bigtable via Cloud Run MCP.

    Executes directly against the Cloud Run MCP Toolbox microservice (`mcp-toolbox-bigtable/mcp` JSON-RPC 2.0)
    both in the cloud (via metadata server OIDC token) and locally (via gcloud auth print-identity-token).

    Args:
        row_key_prefix: Row key prefix formatted as 'STORE_<ID>#CASH_<ID>' (e.g. 'STORE_048#CASH_1190').

    Returns:
        Structured JSON containing the latest 1-hour rolling window metrics and audit status.
    """
    base_url = _get_bigtable_mcp_base_url()
    mcp_endpoint = f"{base_url}/mcp"

    rpc_payload = {
        "jsonrpc": "2.0",
        "id": "read-cashier-alerts-1",
        "method": "tools/call",
        "params": {
            "name": "read_cashier_realtime_alerts",
            "arguments": {"row_key_prefix": row_key_prefix},
        },
    }

    max_retries = 3
    backoff_seconds = 1.0
    last_error: Exception | None = None

    for attempt in range(1, max_retries + 1):
        try:
            token = _fetch_oidc_bearer_token(base_url)
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json, text/event-stream",
            }
            if token:
                headers["Authorization"] = f"Bearer {token}"

            resp = requests.post(
                mcp_endpoint,
                json=rpc_payload,
                headers=headers,
                timeout=30,
            )
            resp.raise_for_status()
            body = resp.json()

            content_items = body.get("result", {}).get("content", [])
            parsed_rows: list[dict[str, Any]] = []
            for item in content_items:
                text_val = item.get("text", "")
                if not text_val:
                    continue
                try:
                    loaded = json.loads(text_val)
                    if isinstance(loaded, dict):
                        parsed_rows.append(loaded)
                    elif isinstance(loaded, list):
                        parsed_rows.extend(r for r in loaded if isinstance(r, dict))
                except json.JSONDecodeError:
                    continue

            if parsed_rows:
                normalized = _normalize_mcp_row(parsed_rows[0])
                return json.dumps(normalized, indent=2)

            return json.dumps(
                {
                    "source": "Cloud Run MCP Toolbox for Databases (Direct /mcp JSON-RPC)",
                    "row_key_prefix": row_key_prefix,
                    "status": "NO_MATCHING_ROWS",
                    "raw_response": body,
                },
                indent=2,
            )

        except Exception as exc:
            last_error = exc
            logger.warning(
                "read_cashier_realtime_alerts attempt %d/%d failed: %s",
                attempt,
                max_retries,
                exc,
            )
            if attempt < max_retries:
                time.sleep(backoff_seconds)
                backoff_seconds *= 2.0

    return json.dumps(
        {
            "source": "Cloud Run MCP Toolbox for Databases (Direct /mcp JSON-RPC)",
            "row_key_prefix": row_key_prefix,
            "status": "ERROR",
            "message": f"Bigtable MCP service unreachable after {max_retries} attempts: {last_error}",
        },
        indent=2,
    )

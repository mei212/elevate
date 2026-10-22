"""Unit tests for the Cymbal Retail Operations Agent and Tool Gateways."""

from __future__ import annotations

import json
import os
from unittest.mock import MagicMock, patch

from app.agent import app, root_agent
from app.tools.analytics_tool import (
    _format_gda_stream_chunks,
    cymbal_analytics_tool,
    get_data_agent_resource_name,
)
from app.tools.bigtable_mcp_tool import (
    _normalize_mcp_row,
    read_cashier_realtime_alerts,
)
from app.tools.rag_tool import (
    UNCERTIFIED_HARDWARE_WARNING,
    _gcs_to_https_url,
    pos_troubleshooting_rag_tool,
)


def test_agent_topology_and_tools() -> None:
    """Verify coordinator agent name, description, and 3 bound tool gateways."""
    assert root_agent.name == "cymbal_operations_agent"
    tool_names = [getattr(t, "__name__", getattr(t, "name", "")) for t in root_agent.tools]
    assert "cymbal_analytics_tool" in tool_names
    assert "pos_troubleshooting_rag_tool" in tool_names
    assert "read_cashier_realtime_alerts" in tool_names
    assert app.name == "app"


def test_gcs_to_https_url_conversion() -> None:
    """Verify GCS URI is converted to clickable HTTPS storage link."""
    gcs_uri = "gs://da-advanced-bucket/store_pos_manual_generic/Toshiba_TCx_810_Guide.pdf"
    expected = "https://storage.cloud.google.com/da-advanced-bucket/store_pos_manual_generic/Toshiba_TCx_810_Guide.pdf"
    assert _gcs_to_https_url(gcs_uri) == expected


def test_format_gda_stream_chunks() -> None:
    """Verify Gemini Data Analytics streaming chunks format SQL and rows cleanly."""
    sample_chunks = [
        {
            "systemMessage": {
                "text": {
                    "parts": ["Analyzing context", "Retrieved context for 6 tables."],
                    "textType": "THOUGHT",
                }
            }
        },
        {
            "systemMessage": {
                "data": {
                    "generatedSql": "SELECT 1 AS net_revenue",
                    "result": {"data": [{"net_revenue": 100.5}]},
                }
            }
        },
        {
            "systemMessage": {
                "text": {
                    "parts": ["Net revenue is $100.50."],
                    "textType": "FINAL_RESPONSE",
                }
            }
        },
    ]
    formatted = _format_gda_stream_chunks(sample_chunks)
    assert "Analyzing context" in formatted
    assert "SELECT 1 AS net_revenue" in formatted
    assert "100.5" in formatted
    assert "Net revenue is $100.50." in formatted


def test_normalize_mcp_row() -> None:
    """Verify Bigtable MCP row normalization preserves rolling 1h fields."""
    raw = {
        "row_key": "STORE_048#CASH_1190#9221583260600950807",
        "audit_status": "clear",
        "cashier_1h_total_discount_usd": 10311.64,
        "cashier_1h_txn_count": 36,
        "cashier_1h_avg_discount_pct": 45.8694,
        "cashier_1h_manual_override_count": 29,
        "cashier_1h_promo_count": 31,
        "cashier_1h_promo_rate": 0.8611,
        "last_event_ts": "2026-09-07T10:17:33.825Z",
        "risk_score": 0.12,
    }
    norm = _normalize_mcp_row(raw)
    assert norm["audit_status"] == "clear"
    assert norm["cashier_1h_manual_override_count"] == 29
    assert norm["risk_score"] == 0.12


@patch("app.tools.rag_tool.bigquery.Client")
@patch("app.tools.rag_tool._resolve_credentials")
def test_rag_tool_out_of_scope_refusal(mock_creds: MagicMock, mock_bq: MagicMock) -> None:
    """Verify out-of-scope hardware query returns exact safety refusal warning."""
    mock_client = MagicMock()
    mock_client.query.return_value.result.return_value = [
        {"hybrid_score": 0.41, "vector_score": 0.41, "stitched_context": "irrelevant"}
    ]
    mock_bq.return_value = mock_client
    with patch.dict(os.environ, {"PROJECT_ID": "test-proj"}):
        res = pos_troubleshooting_rag_tool("How do I replace the engine oil on a Ford F-150 truck?")
    assert res == UNCERTIFIED_HARDWARE_WARNING

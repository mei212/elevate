"""BigQuery Conversational Data Agent Tool Gateway (`cymbal_analytics_tool`).

Implements the NL2SQL analytical gateway bound to the published BigQuery
Conversational Data Agent in `global` location, supporting:
- Verbatim business glossary term propagation (Net Transaction Revenue,
  Total On-Hand Inventory, Estimated Inventory Cover Hours, Cashier Manual Override Rate)
- Delegated end-user OAuth access token propagation with ADC fallback
- Query cost guardrail (`MAX_BYTES_BILLED`)
- Transient fault tolerance via 3 exponential backoff retries
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

import google.auth
from google.adk.tools.data_agent import data_agent_tool
from google.adk.tools.data_agent.config import DataAgentToolConfig
from google.adk.tools.data_agent.data_agent_toolset import DataAgentToolset
from google.auth.credentials import Credentials
from google.auth.transport.requests import AuthorizedSession
from google.oauth2 import credentials as oauth2_credentials

logger = logging.getLogger(__name__)

_GDA_CLIENT_ID = "GOOGLE_ADK"


def _resolve_credentials(external_access_token: str | None = None) -> Credentials:
    """Resolve delegated end-user OAuth credentials or fall back to ADC."""
    token = external_access_token or os.environ.get("USER_OAUTH_ACCESS_TOKEN")
    if token:
        return oauth2_credentials.Credentials(token=token)
    creds, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    return creds


def get_data_agent_resource_name() -> str:
    """Construct the full resource name of the published BigQuery Data Agent."""
    explicit_name = os.environ.get("DATA_AGENT_RESOURCE_NAME")
    if explicit_name:
        return explicit_name
    project_id = os.environ.get("PROJECT_ID") or os.environ.get("GOOGLE_CLOUD_PROJECT")
    if not project_id:
        raise ValueError("PROJECT_ID or GOOGLE_CLOUD_PROJECT must be set in environment.")
    location = os.environ.get("DATA_AGENT_LOCATION", "global")
    agent_id = os.environ.get("DATA_AGENT_ID", "cymbal_retail_analytics_data_agent")
    return f"projects/{project_id}/locations/{location}/dataAgents/{agent_id}"


def get_data_agent_toolset() -> DataAgentToolset:
    """Return a configured ADK DataAgentToolset instance for Cymbal Retail."""
    config = DataAgentToolConfig(max_query_result_rows=20)
    return DataAgentToolset(tool_config=config)


def _format_gda_stream_chunks(chunks: list[dict[str, Any]], max_rows: int = 20) -> str:
    """Format streaming response chunks from the Gemini Data Analytics `:chat` API."""
    thought_blocks: list[str] = []
    final_blocks: list[str] = []
    followup_blocks: list[str] = []
    generated_sql: str | None = None
    result_rows: list[dict[str, Any]] | None = None

    for chunk in chunks:
        if not isinstance(chunk, dict):
            continue
        sm = chunk.get("systemMessage")
        if not isinstance(sm, dict):
            continue

        text_obj = sm.get("text")
        if isinstance(text_obj, dict):
            parts = [str(p) for p in (text_obj.get("parts") or []) if p]
            text_type = text_obj.get("textType", "THOUGHT")
            joined = "\n".join(parts).strip()
            if joined:
                if text_type == "FINAL_RESPONSE":
                    final_blocks.append(joined)
                elif text_type == "FOLLOWUP_QUESTIONS":
                    followup_blocks.append(joined)
                else:
                    thought_blocks.append(joined)

        data_obj = sm.get("data")
        if isinstance(data_obj, dict):
            if "generatedSql" in data_obj and data_obj["generatedSql"]:
                generated_sql = str(data_obj["generatedSql"]).strip()
            matched = data_obj.get("matchedQuery", {}).get("exampleQuery", {})
            if isinstance(matched, dict) and matched.get("sqlQuery") and not generated_sql:
                generated_sql = str(matched["sqlQuery"]).strip()

            res_obj = data_obj.get("result")
            if isinstance(res_obj, dict) and isinstance(res_obj.get("data"), list):
                result_rows = res_obj["data"]

    sections: list[str] = []
    if thought_blocks:
        sections.append("\n\n".join(thought_blocks))
    if final_blocks:
        sections.append("\n\n".join(final_blocks))
    if generated_sql:
        sections.append(f"```sql\n{generated_sql}\n```")
    if result_rows is not None:
        displayed = result_rows[:max_rows]
        sections.append(
            f"Query Result Rows ({len(displayed)} rows):\n"
            f"{json.dumps(displayed, indent=2)}"
        )
    if followup_blocks:
        sections.append("\n".join(followup_blocks))

    return "\n\n".join(sections).strip()


def cymbal_analytics_tool(query: str) -> str:
    """Queries the Cymbal Retail Analytics BigQuery Conversational Data Agent in natural language.

    Args:
        query: Verbatim natural language business inquiry referencing retail metrics,
            inventory positions, historical purchases, warranty terms, or cross-cloud audits.

    Returns:
        Structured analytical answer including generated SQL and actual query result rows,
        or a fallback message if transient database connectivity fails.
    """
    data_agent_name = get_data_agent_resource_name()
    location = os.environ.get("DATA_AGENT_LOCATION", "global")
    parent = data_agent_name.rsplit("/", 2)[0]

    if location != "global":
        base_url = f"https://geminidataanalytics.{location}.rep.googleapis.com/v1beta"
        data_agent_tool.BASE_URL = base_url
    else:
        base_url = os.environ.get(
            "GDA_BASE_URL", "https://geminidataanalytics.googleapis.com/v1beta"
        )

    chat_url = f"{base_url}/{parent}:chat"
    max_bytes_billed = int(os.environ.get("MAX_BYTES_BILLED", "10737418240"))

    payload: dict[str, Any] = {
        "messages": [{"userMessage": {"text": query}}],
        "dataAgentContext": {
            "dataAgent": data_agent_name,
        },
        "clientIdEnum": _GDA_CLIENT_ID,
    }

    headers = {
        "Content-Type": "application/json",
        "X-Goog-API-Client": _GDA_CLIENT_ID,
        "X-Goog-Max-Bytes-Billed": str(max_bytes_billed),
    }

    max_retries = 3
    backoff_seconds = 1.0
    last_error: Exception | None = None

    for attempt in range(1, max_retries + 1):
        try:
            credentials = _resolve_credentials()
            with AuthorizedSession(credentials) as session:
                resp = session.post(
                    chat_url,
                    json=payload,
                    headers=headers,
                    timeout=120,
                )
                resp.raise_for_status()
                chunks = resp.json()
                if isinstance(chunks, list) and chunks:
                    formatted = _format_gda_stream_chunks(chunks, max_rows=20)
                    if formatted:
                        return formatted
                return json.dumps(chunks, indent=2)
        except Exception as exc:
            last_error = exc
            logger.warning(
                "cymbal_analytics_tool attempt %d/%d failed: %s",
                attempt,
                max_retries,
                exc,
            )
            if attempt < max_retries:
                time.sleep(backoff_seconds)
                backoff_seconds *= 2.0

    logger.error(
        "cymbal_analytics_tool exhausted all %d retries. Last error: %s",
        max_retries,
        last_error,
    )
    return (
        "[FALLBACK: Store analytics database is temporarily unreachable after "
        f"{max_retries} retry attempts ({last_error}). Please retry shortly or "
        "check BigQuery Data Agent availability.]"
    )

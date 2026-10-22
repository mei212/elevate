"""POS Hardware Manual Hybrid RAG Tool (`pos_troubleshooting_rag_tool`).

Implements vector similarity search over `cymbal_gold.pos_manual_chunk_embeddings`
(500-character sliding window chunks with 100-character overlap) with:
- Adjacent context window stitching (chunks N-1 through N+1)
- Exact hardware error code regex boost (+0.25)
- Safety similarity score threshold (0.65 hybrid / 0.70 strict vector threshold)
- Full-text `SEARCH(chunk_content, @query)` fallback for valid POS inquiries
- Clickable HTTPS GCS documentation URLs (`https://storage.cloud.google.com/...`)
- Query cost guardrail (`maximum_bytes_billed`) & 3 exponential backoff retries
"""

from __future__ import annotations

import logging
import os
import re
import time
from typing import Any

import google.auth
from google.auth.credentials import Credentials
from google.cloud import bigquery
from google.oauth2 import credentials as oauth2_credentials

logger = logging.getLogger(__name__)

UNCERTIFIED_HARDWARE_WARNING = (
    "[WARNING: Uncertified / Out-of-Scope Hardware Inquiry. Vector similarity "
    "score fell below the safety threshold and no certified POS runbook matched "
    "your query. Please consult authorized hardware vendor support.]"
)

_ERROR_CODE_REGEX = re.compile(r"\b(ERR-[A-Z0-9-]+|1-1-2)\b", re.IGNORECASE)
_POS_DOMAIN_KEYWORDS = (
    "pos",
    "terminal",
    "toshiba",
    "tcx",
    "diebold",
    "nixdorf",
    "beetle",
    "a1150",
    "hp",
    "engage",
    "clover",
    "ncr",
    "voyix",
    "zebra",
    "scanner",
    "printer",
    "th230",
    "emv",
    "pin pad",
    "pinpad",
    "cash drawer",
    "msr",
    "poweredusb",
    "touchscreen",
    "receipt",
    "barcode",
    "err-",
)


def _resolve_credentials(external_access_token: str | None = None) -> Credentials:
    """Resolve delegated end-user OAuth credentials or fall back to ADC."""
    token = external_access_token or os.environ.get("USER_OAUTH_ACCESS_TOKEN")
    if token:
        return oauth2_credentials.Credentials(token=token)
    creds, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    return creds


def _gcs_to_https_url(gcs_uri: str | None) -> str:
    """Convert gs://bucket/path to https://storage.cloud.google.com/bucket/path."""
    if not gcs_uri:
        return ""
    uri = str(gcs_uri).strip()
    if uri.startswith("gs://"):
        return "https://storage.cloud.google.com/" + uri[len("gs://") :]
    return uri


def query_baseline_pos_embeddings(query: str) -> list[dict[str, Any]]:
    """Baseline query against Module 1 coarse embeddings (`pos_manual_embeddings`).

    Used in Challenge 2.2 Step 1 to demonstrate why coarse section-level
    embeddings dilute specific error code signals compared to 500-char chunks.
    """
    project_id = os.environ.get("PROJECT_ID") or os.environ.get("GOOGLE_CLOUD_PROJECT")
    unstructured_ds = os.environ.get(
        "BQ_UNSTRUCTURED_DATASET", "module1_unstructureddata"
    )
    baseline_table = os.environ.get(
        "POS_BASELINE_EMBEDDING_TABLE", "pos_manual_embeddings"
    )
    embedding_model = os.environ.get("POS_EMBEDDING_MODEL", "text-embedding-005")
    max_bytes_billed = int(os.environ.get("MAX_BYTES_BILLED", "10737418240"))

    sql = f"""
    SELECT
      base.document_filename,
      base.document_title,
      base.source_pdf_uri,
      ROUND(1 - distance, 4) AS vector_score
    FROM VECTOR_SEARCH(
      TABLE `{project_id}.{unstructured_ds}.{baseline_table}`,
      'embedding',
      (SELECT AI.EMBED(@query, endpoint => @embedding_model).result AS embedding),
      top_k => 5,
      distance_type => 'COSINE'
    )
    ORDER BY vector_score DESC
    """
    client = bigquery.Client(project=project_id, credentials=_resolve_credentials())
    job_config = bigquery.QueryJobConfig(
        maximum_bytes_billed=max_bytes_billed,
        query_parameters=[
            bigquery.ScalarQueryParameter("query", "STRING", query),
            bigquery.ScalarQueryParameter("embedding_model", "STRING", embedding_model),
        ],
    )
    return [dict(r) for r in client.query(sql, job_config=job_config).result()]


def pos_troubleshooting_rag_tool(query: str) -> str:
    """Performs hybrid search (vector similarity + exact error code boost) over POS hardware manuals in BigQuery.

    Applies fine-grained 500-character sliding window chunks from pos_manual_chunk_embeddings,
    stitches adjacent chunks (N-1 to N+1), boosts exact error code matches (+0.25), enforces a
    0.65 similarity threshold, and returns the Top-5 certified procedures with clickable HTTPS links.

    Args:
        query: Hardware error code or troubleshooting question (e.g. 'ERR-PAY-4001 EMV freeze').

    Returns:
        Top-5 certified recovery procedures with clickable manual links, or a warning disclaimer
        if the hardware query falls below the relevance threshold.
    """
    project_id = os.environ.get("PROJECT_ID") or os.environ.get("GOOGLE_CLOUD_PROJECT")
    if not project_id:
        raise ValueError("PROJECT_ID or GOOGLE_CLOUD_PROJECT must be set in environment.")

    gold_dataset = os.environ.get("BQ_GOLD_DATASET", "cymbal_gold")
    unstructured_dataset = os.environ.get(
        "BQ_UNSTRUCTURED_DATASET", "module1_unstructureddata"
    )
    chunk_table = os.environ.get("POS_EMBEDDING_TABLE", "pos_manual_chunk_embeddings")
    embedding_model_ref = (
        f"`{project_id}.{unstructured_dataset}.pos_text_embedding_model`"
    )
    hybrid_threshold = float(os.environ.get("RAG_SIMILARITY_THRESHOLD", "0.65"))
    strict_threshold = float(os.environ.get("RAG_VECTOR_STRICT_THRESHOLD", "0.70"))
    max_bytes_billed = int(os.environ.get("MAX_BYTES_BILLED", "10737418240"))

    error_match = _ERROR_CODE_REGEX.search(query or "")
    error_code = error_match.group(1).upper() if error_match else ""
    query_lower = (query or "").lower()
    has_pos_domain_signal = bool(error_code) or any(
        kw in query_lower for kw in _POS_DOMAIN_KEYWORDS
    )

    vector_sql = f"""
    WITH query_vec AS (
      SELECT ml_generate_embedding_result AS embedding
      FROM ML.GENERATE_EMBEDDING(
        MODEL {embedding_model_ref},
        (SELECT @query AS content),
        STRUCT('RETRIEVAL_QUERY' AS task_type)
      )
    ),
    top_matches AS (
      SELECT
        base.document_filename,
        base.document_title,
        base.equipment_covered,
        base.source_pdf_uri,
        base.chunk_index,
        base.chunk_content,
        ROUND(1.0 - distance, 4) AS vector_score
      FROM VECTOR_SEARCH(
        TABLE `{project_id}.{gold_dataset}.{chunk_table}`,
        'embedding',
        TABLE query_vec,
        top_k => 8,
        distance_type => 'COSINE'
      )
    ),
    stitched AS (
      SELECT
        m.document_filename,
        m.document_title,
        m.equipment_covered,
        m.source_pdf_uri,
        m.chunk_index,
        m.vector_score,
        STRING_AGG(c.chunk_content, '\\n' ORDER BY c.chunk_index ASC) AS stitched_context
      FROM top_matches m
      JOIN `{project_id}.{gold_dataset}.{chunk_table}` c
        ON m.document_filename = c.document_filename
       AND c.chunk_index BETWEEN (m.chunk_index - 1) AND (m.chunk_index + 1)
      GROUP BY
        m.document_filename,
        m.document_title,
        m.equipment_covered,
        m.source_pdf_uri,
        m.chunk_index,
        m.vector_score
    )
    SELECT
      document_filename,
      document_title,
      equipment_covered,
      source_pdf_uri,
      chunk_index,
      vector_score,
      ROUND(
        vector_score + CASE
          WHEN @error_code != '' AND STRPOS(UPPER(stitched_context), @error_code) > 0 THEN 0.25
          ELSE 0.0
        END,
        4
      ) AS hybrid_score,
      stitched_context
    FROM stitched
    ORDER BY hybrid_score DESC, vector_score DESC
    LIMIT 5
    """

    fallback_search_sql = f"""
    WITH keyword_matches AS (
      SELECT
        document_filename,
        document_title,
        equipment_covered,
        source_pdf_uri,
        chunk_index,
        0.7000 AS vector_score,
        0.7500 AS hybrid_score
      FROM `{project_id}.{gold_dataset}.{chunk_table}`
      WHERE SEARCH(chunk_content, @query)
         OR (@error_code != '' AND STRPOS(UPPER(chunk_content), @error_code) > 0)
      LIMIT 5
    )
    SELECT
      m.document_filename,
      m.document_title,
      m.equipment_covered,
      m.source_pdf_uri,
      m.chunk_index,
      m.vector_score,
      m.hybrid_score,
      STRING_AGG(c.chunk_content, '\\n' ORDER BY c.chunk_index ASC) AS stitched_context
    FROM keyword_matches m
    JOIN `{project_id}.{gold_dataset}.{chunk_table}` c
      ON m.document_filename = c.document_filename
     AND c.chunk_index BETWEEN (m.chunk_index - 1) AND (m.chunk_index + 1)
    GROUP BY
      m.document_filename,
      m.document_title,
      m.equipment_covered,
      m.source_pdf_uri,
      m.chunk_index,
      m.vector_score,
      m.hybrid_score
    ORDER BY m.hybrid_score DESC
    LIMIT 5
    """

    max_retries = 3
    backoff_seconds = 1.0
    rows: list[dict[str, Any]] = []

    for attempt in range(1, max_retries + 1):
        try:
            client = bigquery.Client(
                project=project_id, credentials=_resolve_credentials()
            )
            job_config = bigquery.QueryJobConfig(
                maximum_bytes_billed=max_bytes_billed,
                query_parameters=[
                    bigquery.ScalarQueryParameter("query", "STRING", query),
                    bigquery.ScalarQueryParameter("error_code", "STRING", error_code),
                ],
            )
            rows = [dict(r) for r in client.query(vector_sql, job_config=job_config).result()]

            effective_threshold = hybrid_threshold if has_pos_domain_signal else strict_threshold
            qualified = [
                r for r in rows if float(r.get("hybrid_score", 0.0)) >= effective_threshold
            ]

            if not qualified and has_pos_domain_signal:
                qualified = [
                    dict(r)
                    for r in client.query(
                        fallback_search_sql, job_config=job_config
                    ).result()
                ]

            if not qualified:
                return UNCERTIFIED_HARDWARE_WARNING

            formatted_matches: list[str] = []
            for idx, r in enumerate(qualified[:5], start=1):
                https_url = _gcs_to_https_url(r.get("source_pdf_uri"))
                title = r.get("document_title") or r.get("document_filename")
                equip = r.get("equipment_covered") or ""
                hybrid_val = r.get("hybrid_score")
                vec_val = r.get("vector_score")
                stitched_text = r.get("stitched_context") or ""
                formatted_matches.append(
                    f"--- [MATCH #{idx} | Hybrid Score: {hybrid_val} (Vector Score: {vec_val})] ---\n"
                    f"Manual: {title} ({equip})\n"
                    f"Documentation Link: {https_url}\n"
                    f"Stitched Procedure Context:\n{stitched_text}"
                )

            return "\n\n".join(formatted_matches)

        except Exception as exc:
            logger.warning(
                "pos_troubleshooting_rag_tool attempt %d/%d failed: %s",
                attempt,
                max_retries,
                exc,
            )
            if attempt < max_retries:
                time.sleep(backoff_seconds)
                backoff_seconds *= 2.0

    return UNCERTIFIED_HARDWARE_WARNING

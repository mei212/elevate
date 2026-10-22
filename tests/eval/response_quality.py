"""Local LLM-as-judge for `custom_response_quality` (see eval_config.yaml)."""

from __future__ import annotations

import os
import threading

from google import genai
from google.genai import types
from pydantic import BaseModel

_local = threading.local()


class _Verdict(BaseModel):
    score: int  # 1-5
    explanation: str


def _client() -> genai.Client:
    """One client per grading thread configured for global Vertex AI endpoint."""
    client = getattr(_local, "client", None)
    if client is None:
        os.environ["GOOGLE_CLOUD_LOCATION"] = os.environ.get("GENAI_LOCATION", "global")
        client = _local.client = genai.Client()
    return client


def evaluate(instance: dict) -> dict:
    """Evaluate agent response quality on a 1-5 rubric scale."""
    reference = instance.get("reference")
    rubric = (
        "Grade the Cymbal Retail Operations Agent's final response on a 1-5 scale "
        "(1 poor, 5 excellent) for factual grounding in tool outputs, adherence to "
        "standardized business glossary definitions, clickable HTTPS manual links "
        "for POS hardware diagnostics, and strict refusal of out-of-scope inquiries."
    )
    if reference:
        rubric += (
            " The response should align with the expected reference structure and "
            "domain logic below."
        )
    prompt = (
        f"You are an expert QA evaluator for an enterprise retail AI assistant. {rubric}\n"
        f"User Prompt: {instance.get('prompt', '')}\n"
        f"Final Response: {instance.get('response', '')}\n"
    )
    if reference:
        prompt += f"Expected Reference: {reference}\n"
    prompt += f"Full Agent Trace: {instance.get('agent_data', '')}\n"

    judge_model = os.environ.get("EVAL_JUDGE_MODEL", "gemini-3.7-flash")
    response = _client().models.generate_content(
        model=judge_model,
        contents=prompt,
        config=types.GenerateContentConfig(
            temperature=0,
            response_mime_type="application/json",
            response_schema=_Verdict,
        ),
    )
    verdict = response.parsed
    if verdict is None:
        return {"score": 0, "explanation": response.text or ""}
    return {"score": max(1, min(5, verdict.score)), "explanation": verdict.explanation}

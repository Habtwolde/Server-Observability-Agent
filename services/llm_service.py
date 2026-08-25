from __future__ import annotations

import json
import os
import re
import time
from typing import Any, Dict, List, Optional

import streamlit as st
from databricks.sdk import WorkspaceClient

DEFAULT_ENDPOINT = os.getenv(
    "MODEL_ENDPOINT_NAME",
    "databricks-gpt-5-4",
).strip()
FALLBACK_ENDPOINT = os.getenv(
    "MODEL_ENDPOINT_FALLBACK",
    "databricks-meta-llama-3-3-70b-instruct",
).strip()

DEFAULT_TIMEOUT_SECONDS = int(os.getenv("MODEL_TIMEOUT_SECONDS", "120"))
DEFAULT_MAX_RETRIES = int(os.getenv("MODEL_MAX_RETRIES", "2"))
DEFAULT_BACKOFF_SECONDS = float(os.getenv("MODEL_BACKOFF_SECONDS", "1.5"))


@st.cache_resource
def _w() -> WorkspaceClient:
    return WorkspaceClient()


def _coerce_response_dict(d: Any) -> Any:
    if hasattr(d, "as_dict"):
        try:
            return d.as_dict()
        except Exception:
            return d
    return d


def _pick_text_from_response(d: Any) -> str:
    d = _coerce_response_dict(d)

    if isinstance(d, dict):
        # OpenAI-style chat
        if "choices" in d and d["choices"]:
            ch0 = d["choices"][0]
            if isinstance(ch0, dict):
                msg = ch0.get("message")
                if isinstance(msg, dict):
                    content = msg.get("content")
                    if isinstance(content, str) and content.strip():
                        return content
                    if isinstance(content, list):
                        parts: List[str] = []
                        for item in content:
                            if isinstance(item, dict) and item.get("type") == "text" and item.get("text"):
                                parts.append(str(item["text"]))
                        if parts:
                            return "\n".join(parts)
                if ch0.get("text"):
                    return str(ch0["text"])

        # MLflow / serving responses
        if "predictions" in d and d["predictions"]:
            p0 = d["predictions"][0]
            if isinstance(p0, dict):
                for key in ("content", "text", "prediction"):
                    if p0.get(key):
                        return str(p0[key])
                if isinstance(p0.get("choices"), list) and p0["choices"]:
                    inner = _pick_text_from_response({"choices": p0["choices"]})
                    if inner:
                        return inner
            if isinstance(p0, str) and p0.strip():
                return p0

        # Generic keys
        for k in ("output", "result", "response", "content", "text"):
            if isinstance(d.get(k), str) and d[k].strip():
                return d[k]

    if isinstance(d, str):
        return d

    return ""


def _normalize_messages(messages: List[Dict[str, str]]) -> List[Dict[str, str]]:
    clean: List[Dict[str, str]] = []
    for m in messages:
        role = str(m.get("role") or "user").strip()
        content = m.get("content")
        if content is None:
            content = ""
        clean.append({"role": role, "content": str(content)})
    return clean


def _extract_json_block(text: str) -> Optional[str]:
    if not text:
        return None

    fence = re.search(r"```(?:json)?\s*(\{.*?\}|\[.*?\])\s*```", text, flags=re.DOTALL | re.IGNORECASE)
    if fence:
        return fence.group(1).strip()

    start_obj = text.find("{")
    start_arr = text.find("[")
    candidates = [i for i in (start_obj, start_arr) if i >= 0]
    if not candidates:
        return None
    start = min(candidates)

    for end in range(len(text), start, -1):
        snippet = text[start:end].strip()
        try:
            json.loads(snippet)
            return snippet
        except Exception:
            continue
    return None


def parse_json_response(text: str) -> Dict[str, Any]:
    raw = _extract_json_block(text)
    if not raw:
        raise ValueError("LLM response did not contain valid JSON.")
    data = json.loads(raw)
    if not isinstance(data, dict):
        raise ValueError("LLM JSON response must be an object.")
    return data


def chat_completion(
    messages: List[Dict[str, str]],
    *,
    endpoint_name: str = DEFAULT_ENDPOINT,
    temperature: float = 0.15,
    max_tokens: int = 2000,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    max_retries: int = DEFAULT_MAX_RETRIES,
    extra_params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Call Databricks model serving via REST invocations.

    Improvements over the old version:
    - normalizes messages
    - supports more response shapes
    - retries transient failures
    - passes request timeout where supported
    - keeps the same return contract: plain text
    """

    w = _w()
    payload: Dict[str, Any] = {
        "messages": _normalize_messages(messages),
        "temperature": float(temperature),
        "max_tokens": int(max_tokens),
    }
    if extra_params:
        payload.update(extra_params)

    endpoint_candidates = [endpoint_name]
    if endpoint_name == DEFAULT_ENDPOINT and FALLBACK_ENDPOINT and FALLBACK_ENDPOINT != endpoint_name:
        endpoint_candidates.append(FALLBACK_ENDPOINT)

    last_err: Exception | None = None

    for attempt in range(max_retries + 1):
        for endpoint in endpoint_candidates:
            paths = [
                f"/api/2.0/serving-endpoints/{endpoint}/invocations",
                f"/serving-endpoints/{endpoint}/invocations",
            ]
            for path in paths:
                try:
                    try:
                        resp = w.api_client.do("POST", path, body=payload, timeout=timeout_seconds)
                    except TypeError:
                        try:
                            resp = w.api_client.do("POST", path, body=payload)
                        except TypeError:
                            try:
                                resp = w.api_client.do("POST", path, payload, timeout=timeout_seconds)
                            except TypeError:
                                resp = w.api_client.do("POST", path, payload)

                    text = _pick_text_from_response(resp).strip()
                    if not text:
                        raise RuntimeError(f"LLM returned empty response. Raw response: {resp}")
                    return text
                except Exception as e:
                    last_err = e

        if attempt < max_retries:
            time.sleep(DEFAULT_BACKOFF_SECONDS * (attempt + 1))

    raise last_err if last_err else RuntimeError("LLM call failed for unknown reason.")


def chat_json(
    messages: List[Dict[str, str]],
    *,
    endpoint_name: str = DEFAULT_ENDPOINT,
    temperature: float = 0.1,
    max_tokens: int = 2600,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    max_retries: int = DEFAULT_MAX_RETRIES,
) -> Dict[str, Any]:
    """
    Convenience helper for report-generation flows that require valid JSON.
    The current report service can adopt this later without breaking chat_completion.
    """
    text = chat_completion(
        messages,
        endpoint_name=endpoint_name,
        temperature=temperature,
        max_tokens=max_tokens,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        extra_params={"response_format": {"type": "json_object"}},
    )
    return parse_json_response(text)

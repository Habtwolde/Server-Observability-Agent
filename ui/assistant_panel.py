from __future__ import annotations

from html import escape
import re
from typing import Any, Dict, List

import streamlit as st

from services.ai_service import ask_server_ai


_MAX_TURNS = 24

_SUGGESTIONS = [
    "Summarize this server",
    "Explain the highest risks",
    "What should I investigate next?",
    "Compare this ingestion with the previous one",
]


_ASSISTANT_SOFT_PANEL_CSS = """
<style>
section[data-testid="stSidebar"],
section[data-testid="stSidebar"] > div,
section[data-testid="stSidebar"] [data-testid="stSidebarContent"] {
  background: linear-gradient(180deg, #f7f9fc 0%, #eef3f8 100%) !important;
  color: #111827 !important;
}
section[data-testid="stSidebar"] div[data-testid="stButton"] > button,
section[data-testid="stSidebar"] div[data-testid="stFormSubmitButton"] > button {
  background: #ffffff !important;
  color: #111827 !important;
  border: 1px solid rgba(17, 24, 39, 0.14) !important;
}
section[data-testid="stSidebar"] div[data-testid="stButton"] > button:hover,
section[data-testid="stSidebar"] div[data-testid="stFormSubmitButton"] > button:hover {
  background: #f8fafc !important;
  color: #111827 !important;
}
section[data-testid="stSidebar"] input,
section[data-testid="stSidebar"] textarea {
  background: #ffffff !important;
  color: #111827 !important;
  border-color: rgba(17, 24, 39, 0.16) !important;
}
</style>
"""


def _scope_key(selected_server: str, selected_ingestion_date: str | None) -> str:
    return f"{selected_server}::{selected_ingestion_date or 'latest'}"


def _history_key(selected_server: str, selected_ingestion_date: str | None) -> str:
    return f"global_ai_history::{_scope_key(selected_server, selected_ingestion_date)}"


def _pending_key(selected_server: str, selected_ingestion_date: str | None) -> str:
    return f"global_ai_pending::{_scope_key(selected_server, selected_ingestion_date)}"


def _inject_panel_css() -> None:
    """Move Streamlit's native sidebar to the right and style it as the Agent panel."""
    panel_width = "420px"
    main_width = "calc(100vw - 452px)"
    sidebar_inner_padding = "16px"
    sidebar_user_top = "14px"
    streamlit_header_offset = "48px"
    button_mode_css = """
section[data-testid=\"stSidebar\"] div[data-testid=\"stButton\"] > button {
  min-height: 42px !important;
  border-radius: 12px !important;
  font-weight: 700 !important;
}

section[data-testid=\"stSidebar\"] div[data-testid=\"stButton\"] {
  position: relative !important;
  z-index: 10002 !important;
}
"""

    st.markdown(
        f"""
<style>
/* Hide Streamlit's native sidebar collapse affordance so the Agent exposes
   only its purpose-built arrow control. Streamlit has used different labels
   and test ids across releases, so keep this selector list intentionally
   defensive and scoped to sidebar open/close controls. */
[data-testid="stSidebarCollapseButton"],
[data-testid="stSidebarCollapsedControl"],
button[aria-label="Close sidebar"],
button[aria-label="Open sidebar"],
button[aria-label="Collapse sidebar"],
button[aria-label="Expand sidebar"],
button[title="Close sidebar"],
button[title="Open sidebar"],
button[title="Collapse sidebar"],
button[title="Expand sidebar"] {{
  display: none !important;
  pointer-events: none !important;
}}

/* Use Streamlit's real sidebar as the assistant container. This keeps all
   Streamlit widgets physically inside the right-side panel and avoids nesting
   the dashboard tabs inside a root column. */
section[data-testid="stSidebar"] {{
  position: fixed !important;
  top: {streamlit_header_offset} !important;
  left: auto !important;
  right: 0 !important;
  height: calc(100vh - {streamlit_header_offset}) !important;
  z-index: 9998 !important;
  width: {panel_width} !important;
  min-width: {panel_width} !important;
  max-width: {panel_width} !important;
  border-left: 1px solid rgba(15, 23, 42, 0.10);
  border-right: none !important;
  box-shadow: -10px 0 28px rgba(15, 23, 42, 0.06);
  background: #ffffff !important;
  overflow: hidden !important;
}}

section[data-testid="stSidebar"] > div {{
  background: #ffffff !important;
}}

section[data-testid="stSidebar"] [data-testid="stSidebarContent"] {{
  position: fixed !important;
  top: {streamlit_header_offset} !important;
  right: 0 !important;
  width: {panel_width} !important;
  height: calc(100vh - {streamlit_header_offset}) !important;
  padding: 14px {sidebar_inner_padding} 16px {sidebar_inner_padding};
  margin: 0 !important;
  overflow: hidden !important;
  transform: none !important;
  background: #ffffff !important;
  box-sizing: border-box !important;
}}

section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] {{
  position: fixed !important;
  right: {sidebar_inner_padding} !important;
  top: calc({streamlit_header_offset} + {sidebar_user_top}) !important;
  width: calc({panel_width} - ({sidebar_inner_padding} * 2)) !important;
  height: calc(100vh - {streamlit_header_offset} - 30px) !important;
  padding: 0 0 126px 0 !important;
  margin: 0 !important;
  overflow: hidden !important;
  transform: none !important;
  box-sizing: border-box !important;
}}

section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] > div {{
  height: 100% !important;
  min-height: 100% !important;
  overflow: hidden !important;
}}

section[data-testid="stSidebar"] [data-testid="stVerticalBlock"] {{
  gap: 0.45rem !important;
}}

/* The Agent panel is intentionally fixed open. Hide Streamlit's native
   sidebar collapse/reopen affordances so there is no collapsing feature. */
section[data-testid="stSidebar"] {{
  transform: none !important;
}}

section[data-testid="stSidebar"] [data-testid="stSidebarHeader"],
section[data-testid="stSidebar"] [data-testid="stSidebarCollapseButton"],
[data-testid="stExpandSidebarButton"] {{
  display: none !important;
  visibility: hidden !important;
  opacity: 0 !important;
  pointer-events: none !important;
}}

.block-container {{
  width: {main_width} !important;
  max-width: {main_width} !important;
  margin-left: 24px !important;
  margin-right: auto !important;
  padding-left: 1.4rem !important;
  padding-right: 1.4rem !important;
}}

.global-ai-panel-card {{
  background: #ffffff;
}}

.global-ai-topbar {{
  padding: 8px 0 12px 0;
  border-bottom: 1px solid rgba(15, 23, 42, 0.08);
  margin-bottom: 4px;
}}

.global-ai-topbar-title {{
  min-height: 32px;
  display: flex;
  align-items: center;
  justify-content: flex-start;
  font-size: 1rem;
  font-weight: 760;
  color: #0f172a;
  letter-spacing: -0.01em;
}}

.global-ai-brand {{
  position: relative;
  z-index: 10002;
  margin: 10px 0 12px 0;
  text-align: center;
  color: #0f172a;
  background: #ffffff;
}}

.global-ai-brand-icon {{
  margin: 0 auto 10px auto;
  width: 44px;
  height: 44px;
  border-radius: 14px;
  display: flex;
  align-items: center;
  justify-content: center;
  background: linear-gradient(135deg, rgba(37, 99, 235, 0.12), rgba(14, 165, 233, 0.10));
  color: #2563eb;
  font-size: 0.9rem;
  letter-spacing: 0.02em;
  font-weight: 820;
}}

.global-ai-brand-title {{
  font-size: 0.98rem;
  font-weight: 720;
  color: #475569;
  margin-bottom: 12px;
}}

.global-ai-scope {{
  display: inline-flex;
  max-width: 100%;
  align-items: center;
  justify-content: center;
  gap: 6px;
  margin: 0 auto 14px auto;
  padding: 5px 10px;
  border-radius: 999px;
  background: #f8fafc;
  color: #64748b;
  border: 1px solid rgba(15, 23, 42, 0.08);
  font-size: 0.77rem;
}}

.global-ai-suggestion-note {{
  color: #64748b;
  font-size: 0.83rem;
  line-height: 1.4;
  text-align: center;
  margin: 0 10px 10px 10px;
}}

.global-ai-thread {{
  position: relative;
  z-index: 1;
  width: 100%;
  height: max(260px, calc(100vh - {streamlit_header_offset} - 300px));
  max-height: calc(100vh - {streamlit_header_offset} - 220px);
  display: block;
  overflow-y: auto;
  overflow-x: hidden;
  margin-top: 10px;
  padding: 8px 6px 20px 0;
  scroll-behavior: smooth;
  scrollbar-gutter: stable;
  overscroll-behavior: contain;
  box-sizing: border-box;
}}

.global-ai-thread-inner {{
  min-height: 100%;
  display: flex;
  flex-direction: column;
  justify-content: flex-end;
  gap: 8px;
}}

.global-ai-thread::-webkit-scrollbar {{
  width: 8px;
}}

.global-ai-thread::-webkit-scrollbar-track {{
  background: transparent;
}}

.global-ai-thread::-webkit-scrollbar-thumb {{
  background: rgba(148, 163, 184, 0.55);
  border-radius: 999px;
}}

.global-ai-thread::-webkit-scrollbar-thumb:hover {{
  background: rgba(100, 116, 139, 0.72);
}}

.global-ai-message {{
  border-radius: 16px;
  padding: 10px 12px;
  margin: 0;
  font-size: 0.9rem;
  line-height: 1.42;
  white-space: normal;
  overflow-wrap: anywhere;
}}

.global-ai-message p {{
  margin: 0 0 0.46rem 0;
}}

.global-ai-message p:last-child {{
  margin-bottom: 0;
}}

.global-ai-message ul,
.global-ai-message ol {{
  margin: 0.25rem 0 0.46rem 1.1rem;
  padding: 0;
}}

.global-ai-message li {{
  margin: 0.12rem 0;
}}

.global-ai-message.user {{
  margin-left: 28px;
  background: #eef2ff;
  color: #1e1b4b;
  border: 1px solid rgba(79, 70, 229, 0.12);
}}

.global-ai-message.assistant {{
  margin-right: 12px;
  background: #f8fafc;
  color: #0f172a;
  border: 1px solid rgba(15, 23, 42, 0.08);
}}

.global-ai-message.thinking {{
  margin-right: 46px;
  color: #475569;
  border-style: dashed;
}}

.global-ai-meta {{
  margin: -3px 12px 0 0;
  font-size: 0.72rem;
  color: #94a3b8;
}}

.global-ai-meta.user-meta {{
  margin: -3px 0 0 28px;
  text-align: right;
}}

.global-ai-input-wrap {{
  height: 0;
}}

section[data-testid="stSidebar"] div[data-testid="stForm"] {{
  position: fixed !important;
  right: {sidebar_inner_padding} !important;
  bottom: 16px !important;
  width: calc({panel_width} - ({sidebar_inner_padding} * 2)) !important;
  z-index: 10000 !important;
  padding: 16px 12px 14px 12px !important;
  margin: 0 !important;
  border: 1px solid rgba(15, 23, 42, 0.14) !important;
  border-radius: 14px !important;
  background: #ffffff !important;
  box-shadow: 0 -10px 24px rgba(255, 255, 255, 0.92);
  box-sizing: border-box !important;
}}


{button_mode_css}

@media (max-width: 1120px) {{
  section[data-testid="stSidebar"] {{
    position: relative !important;
    width: 100% !important;
    min-width: 100% !important;
    max-width: 100% !important;
    box-shadow: none;
    border-left: none;
    border-top: 1px solid rgba(15, 23, 42, 0.10);
  }}

  section[data-testid="stSidebar"] [data-testid="stSidebarContent"],
  section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"],
  section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] > div {{
    position: relative !important;
    top: auto !important;
    right: auto !important;
    width: 100% !important;
    height: auto !important;
    overflow: visible !important;
  }}

  .global-ai-thread,
  section[data-testid="stSidebar"] div[data-testid="stForm"] {{
    position: relative !important;
    top: auto !important;
    right: auto !important;
    bottom: auto !important;
    width: 100% !important;
  }}

  .block-container {{
    width: 100% !important;
    max-width: 100% !important;
    margin-left: auto !important;
    margin-right: auto !important;
    padding-left: 1rem !important;
    padding-right: 1rem !important;
  }}
}}
</style>
        """,
        unsafe_allow_html=True,
    )

def _append_turn(history: List[Dict[str, Any]], turn: Dict[str, Any]) -> List[Dict[str, Any]]:
    history.append(turn)
    return history[-_MAX_TURNS:]


def _queue_question(
    *,
    selected_server: str,
    selected_ingestion_date: str | None,
    question: str,
) -> None:
    """Store the user turn immediately, then resolve it on the next rerun."""
    question = (question or "").strip()
    if not question:
        return

    history_key = _history_key(selected_server, selected_ingestion_date)
    pending_key = _pending_key(selected_server, selected_ingestion_date)
    history = st.session_state.setdefault(history_key, [])
    st.session_state[history_key] = _append_turn(history, {"role": "user", "content": question})
    st.session_state[pending_key] = question


def _answer_pending_and_store(
    *,
    selected_server: str,
    selected_ingestion_date: str | None,
    question: str,
) -> None:
    """Resolve a queued assistant question without rendering a main-page spinner."""
    history_key = _history_key(selected_server, selected_ingestion_date)
    pending_key = _pending_key(selected_server, selected_ingestion_date)

    try:
        response = ask_server_ai(
            server_name=selected_server,
            ingestion_date=selected_ingestion_date or "",
            question=question,
        )
        assistant_turn = {
            "role": "assistant",
            "content": response.get("answer", ""),
            "mode": response.get("mode", "single"),
            "found": response.get("found", False),
            "resolved_server": response.get("resolved_server"),
            "resolved_ingestion_date": response.get("resolved_ingestion_date"),
            "compare_servers": response.get("compare_servers", []),
            "compare_dates": response.get("compare_dates", []),
        }
    except Exception as exc:  # Defensive UI guard so a model failure does not break the page.
        assistant_turn = {
            "role": "assistant",
            "content": f"I could not complete that request. Please try again.\n\nError: {exc}",
            "mode": "error",
            "found": False,
        }

    history = st.session_state.setdefault(history_key, [])
    st.session_state[history_key] = _append_turn(history, assistant_turn)
    st.session_state.pop(pending_key, None)


def _message_content_html(value: Any) -> str:
    """Render assistant/user text compactly without excessive paragraph gaps."""
    text = str(value or "").replace("\r\n", "\n").replace("\r", "\n").strip()
    if not text:
        return ""

    # Trim each line and collapse repeated blank lines. The model sometimes
    # returns paragraph blocks with multiple blank lines, which become very
    # large inside the fixed assistant panel.
    compact_lines: List[str] = []
    previous_blank = False
    for raw_line in text.split("\n"):
        line = raw_line.strip()
        if not line:
            if not previous_blank:
                compact_lines.append("")
            previous_blank = True
        else:
            compact_lines.append(line)
            previous_blank = False

    text = "\n".join(compact_lines).strip()
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = _remove_assistant_markdown_artifacts(text)

    paragraphs = [p.strip() for p in re.split(r"\n\s*\n", text) if p.strip()]
    if not paragraphs:
        return ""

    rendered: List[str] = []
    for paragraph in paragraphs:
        safe = escape(paragraph)
        safe = safe.replace("\n", "<br>")
        rendered.append(f"<p>{safe}</p>")
    return "".join(rendered)


def _remove_assistant_markdown_artifacts(text: str) -> str:
    """Remove common LLM Markdown markers while preserving SQL wildcards and safety escaping.

    The assistant panel renders escaped custom HTML rather than Markdown. Without
    this normalization, model output such as ``**Finding**`` or ``* item`` shows
    literal asterisks to the user. Keep the cleanup intentionally narrow so SQL
    expressions like ``COUNT(*)`` or ``SELECT * FROM`` remain unchanged.
    """
    if not text:
        return ""

    cleaned_lines: List[str] = []
    for line in text.split("\n"):
        stripped = line.lstrip()
        leading = line[: len(line) - len(stripped)]

        # Convert Markdown unordered-list markers into plain text list lines.
        # Only match markers at the start of a line followed by whitespace.
        stripped = re.sub(r"^(?:[-*+])\s+", "", stripped)

        # Drop Markdown emphasis markers only when they wrap non-whitespace text.
        stripped = re.sub(r"\*\*([^\n*][^\n]*?[^\n*]?)\*\*", r"\1", stripped)
        stripped = re.sub(r"(?<!\*)\*([^\s*][^*\n]*?[^\s*]?)\*(?!\*)", r"\1", stripped)
        cleaned_lines.append(f"{leading}{stripped}")

    return "\n".join(cleaned_lines).strip()


def _turn_html(turn: Dict[str, Any]) -> str:
    role = str(turn.get("role") or "assistant")
    content = _message_content_html(turn.get("content") or "")
    css_role = "user" if role == "user" else "assistant"
    parts = [f'<div class="global-ai-message {css_role}">{content}</div>']

    if role == "user":
        parts.append('<div class="global-ai-meta user-meta">user</div>')
    else:
        meta_bits = ["Agent"]
        if turn.get("resolved_server"):
            meta_bits.append(f"server: {turn['resolved_server']}")
        if turn.get("resolved_ingestion_date"):
            meta_bits.append(f"date: {turn['resolved_ingestion_date']}")
        if turn.get("compare_servers"):
            meta_bits.append("comparison")
        meta_text = " • ".join(escape(x) for x in meta_bits)
        parts.append(f'<div class="global-ai-meta">{meta_text}</div>')

    return "\n".join(parts)


def _render_thread(history: List[Dict[str, Any]], pending_question: str | None) -> None:
    turns_html = "\n".join(_turn_html(turn) for turn in history)
    if pending_question:
        turns_html += (
            '\n<div class="global-ai-message assistant thinking">'
            "Thinking about the selected server scope…"
            "</div>"
        )
    st.markdown(
        f'<div class="global-ai-thread"><div class="global-ai-thread-inner">{turns_html}</div></div>',
        unsafe_allow_html=True,
    )


def render_global_ai_assistant(
    selected_server: str,
    selected_ingestion_date: str | None,
) -> None:
    """Render the persistent right-side assistant for the active dashboard scope."""
    _inject_panel_css()

    history_key = _history_key(selected_server, selected_ingestion_date)
    pending_key = _pending_key(selected_server, selected_ingestion_date)
    submitted_question = ""
    pending_question = st.session_state.get(pending_key)

    with st.sidebar:
        history = st.session_state.setdefault(history_key, [])
        st.markdown('<div class="global-ai-panel-card">', unsafe_allow_html=True)
        safe_server = escape(str(selected_server))
        safe_date = escape(str(selected_ingestion_date or "latest"))
        st.markdown(
            f"""
<div class="global-ai-brand">
  <div class="global-ai-brand-icon">SQL</div>
  <div class="global-ai-brand-title">SQL Server Observability Agent</div>
  <div class="global-ai-scope">{safe_server} • {safe_date}</div>
</div>
            """,
            unsafe_allow_html=True,
        )

        suggestions_slot = st.empty()

        def render_suggestions() -> None:
            current_history = st.session_state.setdefault(history_key, [])
            if current_history or pending_question:
                suggestions_slot.empty()
                return
            with suggestions_slot.container():
                st.markdown(
                    '<div class="global-ai-suggestion-note">Ask anything about the active server scope, or start with one of these prompts.</div>',
                    unsafe_allow_html=True,
                )
                for idx, suggestion in enumerate(_SUGGESTIONS):
                    if st.button(
                        suggestion,
                        key=f"global_ai_suggestion::{history_key}::{idx}",
                        use_container_width=True,
                        disabled=bool(pending_question),
                    ):
                        nonlocal_submitted["question"] = suggestion
                        suggestions_slot.empty()

        nonlocal_submitted = {"question": ""}
        render_suggestions()
        submitted_question = nonlocal_submitted["question"]

        body_slot = st.empty()

        def render_body(active_pending: str | None = None) -> None:
            current_history = st.session_state.setdefault(history_key, [])
            with body_slot.container():
                if current_history or active_pending:
                    _render_thread(current_history, active_pending)

        render_body(str(pending_question) if pending_question else None)

        st.markdown('<div class="global-ai-input-wrap">', unsafe_allow_html=True)
        with st.form(key=f"global_ai_form::{history_key}", clear_on_submit=True):
            prompt = st.text_input(
                "Ask anything",
                placeholder="Ask anything",
                label_visibility="collapsed",
                disabled=bool(pending_question),
            )
            sent = st.form_submit_button("↑", use_container_width=True, disabled=bool(pending_question))
            if sent:
                submitted_question = prompt
        st.markdown('</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

        question_to_answer = str(pending_question) if pending_question else submitted_question
        if question_to_answer:
            suggestions_slot.empty()
            if not pending_question:
                _queue_question(
                    selected_server=selected_server,
                    selected_ingestion_date=selected_ingestion_date,
                    question=question_to_answer,
                )
            render_body(question_to_answer)
            _answer_pending_and_store(
                selected_server=selected_server,
                selected_ingestion_date=selected_ingestion_date,
                question=question_to_answer,
            )
            render_body(None)

"""Compact evidence-grounded Agent question panel."""
from __future__ import annotations

import streamlit as st

from services.agent_ai_service import answer_agent_question


def render_agent_assistant_panel(server_name: str | None, top_n: int = 5) -> None:
    scope = server_name.strip().upper() if server_name else "ALL SERVERS"
    history_key = f"agent_chat::{scope}::{top_n}"
    history = st.session_state.setdefault(history_key, [])

    with st.expander(f"Ask the Agent · {scope}", expanded=False):
        for turn in history[-6:]:
            label = "You" if turn["role"] == "user" else "Agent"
            st.markdown(f"**{label}:** {turn['content']}")

        with st.form(f"agent_question_form::{scope}::{top_n}", clear_on_submit=True):
            question = st.text_input(
                "Question",
                placeholder="Why does this server need immediate attention?",
            )
            submitted = st.form_submit_button("Ask Agent", type="primary")

        if submitted:
            clean_question = question.strip()
            if not clean_question:
                st.warning("Enter a question.")
            else:
                with st.spinner("Reviewing deterministic Agent evidence…"):
                    answer = answer_agent_question(
                        clean_question,
                        server_name=server_name,
                        top_n=top_n,
                    )
                history.extend(
                    [
                        {"role": "user", "content": clean_question},
                        {"role": "assistant", "content": answer},
                    ]
                )
                st.session_state[history_key] = history[-12:]
                st.markdown(f"**Agent:** {answer}")


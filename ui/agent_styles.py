"""Shared visual styling for the Agent application."""
from __future__ import annotations

import streamlit as st


def apply_agent_styles() -> None:
    st.markdown(
        """
        <style>
        .block-container {padding-top: 2.25rem; padding-bottom: 2rem; max-width: 1500px;}
        div[data-testid="stMarkdownContainer"]:has(.agent-header) {
            position: sticky;
            top: 0;
            z-index: 1000;
            padding: .35rem 0 .7rem;
            margin-bottom: .55rem;
            background: var(--background-color, #ffffff);
            border-bottom: 1px solid rgba(128, 128, 128, .18);
        }
        .agent-header {width: 100%;}
        .agent-title {font-size: 2rem; font-weight: 750; letter-spacing: -0.02em;}
        .agent-subtitle {color: #667085; margin-top: -.2rem;}
        .scope-banner {padding: .75rem 1rem; border: 1px solid #d0d5dd; border-radius: 10px;
                       background: #f8fafc; margin-bottom: 1rem;}
        .finding-card {border: 1px solid #d0d5dd; border-left-width: 7px; border-radius: 10px;
                       padding: .9rem 1rem; margin: .65rem 0; background: white;}
        .finding-critical {border-left-color: #b42318;}
        .finding-high {border-left-color: #dc6803;}
        .finding-medium {border-left-color: #fdb022;}
        .finding-low {border-left-color: #1570ef;}
        .finding-label {font-size: .78rem; font-weight: 700; letter-spacing: .04em;}
        .finding-title {font-size: 1.05rem; font-weight: 720; margin: .2rem 0 .45rem;}
        .finding-meta {color: #667085; font-size: .86rem;}
        .finding-section {margin-top: .5rem;}
        .finding-section strong {color: #344054;}
        .test-warning {padding: .75rem 1rem; border-radius: 9px; background: #fffaeb;
                       border: 1px solid #fedf89; color: #7a2e0e;}
        </style>
        """,
        unsafe_allow_html=True,
    )

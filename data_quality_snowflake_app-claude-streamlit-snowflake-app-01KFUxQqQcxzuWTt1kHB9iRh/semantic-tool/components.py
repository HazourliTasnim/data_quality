"""
UI Components module for LeonDQ application.
Contains reusable UI helper functions and components.
"""

import streamlit as st
from typing import Optional


def render_dark_card(title: Optional[str] = None, subtitle: Optional[str] = None):
    """Context manager for rendering a dark-themed card."""
    return st.container(border=True)


def render_header(title: str, subtitle: str = ""):
    """Render a dark-themed header with title and optional subtitle."""
    col1, col2 = st.columns([1, 10])
    with col1:
        st.write("")
        st.markdown("### 🧩")
    with col2:
        st.markdown(f"# {title}")
        if subtitle:
            st.caption(subtitle)
    st.markdown("")


def render_section_title(title: str, icon: str = ""):
    """Render a section title with optional icon."""
    if icon:
        st.markdown(f"### {icon} {title}")
    else:
        st.markdown(f"### {title}")


def render_status_badge(status: str, severity: str = "info"):
    """Render a colored status badge."""
    if severity == "critical":
        st.markdown(f'<span class="badge-critical">{status}</span>', unsafe_allow_html=True)
    elif severity == "warning":
        st.markdown(f'<span class="badge-warning">{status}</span>', unsafe_allow_html=True)
    else:  # info
        st.markdown(f'<span class="badge-info">{status}</span>', unsafe_allow_html=True)

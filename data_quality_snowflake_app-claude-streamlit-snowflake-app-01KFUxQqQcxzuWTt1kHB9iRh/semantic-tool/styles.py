"""
Styling module for LeonDQ application.
Contains all CSS definitions and theme-related styling functions.
"""

import streamlit as st


def apply_base_styles():
    """Apply base CSS styles for the LeonDQ application."""
    st.markdown("""
<style>
    /* ==================== LeonDQ Dark-Tech Brand Colors ==================== */
    :root {
        /* Dark Theme (Default) */
        --bg-canvas: #020617;
        --bg-surface: #0B1120;
        --bg-surface-alt: #020617;
        --accent-teal: #22D3EE;
        --accent-teal-soft: #0E7490;
        --accent-muted: #38BDF8;
        --text-primary: #E5E7EB;
        --text-muted: #9CA3AF;
        --text-subtle: #6B7280;
        --success: #22C55E;
        --warning: #FACC15;
        --error: #F97373;
        --border-default: #1E293B;
        --border-subtle: #0F172A;

        --card-radius: 12px;
        --button-radius: 8px;
    }

    /* Light Theme */
    body.light-theme {
        --bg-canvas: #F5F7FA;
        --bg-surface: #FFFFFF;
        --bg-surface-alt: #F9FAFB;
        --accent-teal: #0891B2;
        --accent-teal-soft: #06B6D4;
        --accent-muted: #22D3EE;
        --text-primary: #1F2937;
        --text-muted: #6B7280;
        --text-subtle: #9CA3AF;
        --success: #16A34A;
        --warning: #CA8A04;
        --error: #DC2626;
        --border-default: #E5E7EB;
        --border-subtle: #F3F4F6;
    }

    /* ==================== Typography & Spacing ==================== */
    /* Main container background */
    .stApp {
        background-color: var(--bg-canvas);
    }

    /* H1: Headings with Inter Semibold style */
    h1 {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
        font-weight: 600;
        font-size: 28px;
        color: var(--text-primary);
        margin-bottom: 0.25rem;
        line-height: 1.3;
    }

    h2 {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
        font-weight: 600;
        font-size: 20px;
        color: var(--text-primary);
        margin-top: 1rem;
        margin-bottom: 0.75rem;
    }

    h3 {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
        font-weight: 600;
        font-size: 16px;
        color: var(--text-muted);
        margin-top: 0.75rem;
        margin-bottom: 0.5rem;
    }

    /* Body text: Inter Regular */
    body, .stMarkdown, p {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
        font-weight: 400;
        font-size: 14px;
        line-height: 1.6;
        color: var(--text-primary);
    }

    /* Caption/Muted text */
    .stCaption, small {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
        font-size: 13px;
        color: var(--text-muted);
        line-height: 1.5;
    }

    /* Code/YAML: JetBrains Mono */
    code, pre, .stCodeBlock {
        font-family: 'JetBrains Mono', 'Courier New', monospace;
        font-size: 13px;
        line-height: 1.5;
        color: var(--text-primary);
    }

    /* ==================== Image Styling ==================== */
    /* Reduce logo spacing - tighter vertical layout */
    .stImage {
        margin-bottom: 0 !important;
    }

    .stImage > img {
        margin-bottom: 0 !important;
        display: block !important;
    }

    /* Remove extra padding from image containers */
    [data-testid="stImage"] {
        margin-bottom: 0.5rem !important;
        padding-bottom: 0 !important;
    }

    /* ==================== Sidebar Styling ==================== */
    section[data-testid="stSidebar"] {
        background-color: var(--bg-surface-alt);
        border-right: 1px solid var(--border-default);
    }

    section[data-testid="stSidebar"] h2 {
        font-size: 14px;
        font-weight: 600;
        margin-top: 1rem;
        margin-bottom: 0.75rem;
        color: var(--text-primary);
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }

    /* Compress sidebar height */
    section[data-testid="stSidebar"] .st-emotion-cache-1cypcdb {
        padding-top: 1rem;
        padding-bottom: 1rem;
    }

    /* Sidebar card styling */
    section[data-testid="stSidebar"] [data-testid="stVerticalBlock"] {
        background-color: var(--bg-surface);
        border: 1px solid var(--border-default);
        border-radius: var(--card-radius);
        padding: 16px;
        margin-bottom: 12px;
    }

    /* Connection status dot - minimal, teal when connected */
    .connection-status-dot {
        display: inline-block;
        width: 8px;
        height: 8px;
        border-radius: 50%;
        margin-right: 8px;
    }

    .connection-status-dot.connected {
        background-color: var(--accent-teal);
    }

    .connection-status-dot.disconnected {
        background-color: var(--text-subtle);
    }

    /* ==================== Tab Styling ==================== */
    /* Active tab underline indicator */
    .stTabs [data-baseweb="tab-list"] {
        gap: 0px;
        background-color: transparent;
        border-bottom: 1px solid var(--border-default);
        padding-bottom: 0;
    }

    .stTabs [data-baseweb="tab"] {
        padding: 12px 24px;
        font-weight: 500;
        font-size: 14px;
        color: var(--text-muted);
        background-color: transparent;
        border: none;
        border-bottom: 3px solid transparent;
        border-radius: 0;
        transition: all 0.25s ease;
        margin-bottom: -1px;
    }

    .stTabs [data-baseweb="tab"]:hover {
        color: var(--text-primary);
        border-bottom-color: var(--accent-teal-soft);
    }

    .stTabs [data-baseweb="tab"][aria-selected="true"] {
        color: var(--accent-teal);
        font-weight: 600;
        border-bottom-color: var(--accent-teal);
    }

    /* ==================== Card & Container Styling ==================== */
    /* Main content cards - dark background with subtle borders */
    [data-testid="stVerticalBlock"] > div {
        background-color: transparent;
    }

    /* Container styling */
    div[data-testid="stContainer"] {
        border-radius: var(--card-radius);
    }

    /* Expander styling */
    .streamlit-expanderHeader {
        background-color: transparent;
        border: 1px solid var(--border-default);
        border-radius: var(--button-radius);
        font-weight: 500;
        color: var(--text-primary);
        transition: all 0.2s ease;
        padding: 12px 16px;
    }

    .streamlit-expanderHeader:hover {
        background-color: rgba(34, 211, 238, 0.05);
        border-color: var(--accent-teal-soft);
    }

    /* ==================== Button Styling ==================== */
    /* Primary buttons - teal background */
    .stButton > button[kind="primary"] {
        background-color: var(--accent-teal);
        color: #020617;
        font-weight: 600;
        border: none;
        border-radius: var(--button-radius);
        padding: 0.6rem 1.5rem;
        transition: all 0.2s ease;
        font-size: 14px;
    }

    .stButton > button[kind="primary"]:hover {
        background-color: var(--accent-muted);
        box-shadow: 0 4px 16px rgba(34, 211, 238, 0.3);
        transform: translateY(-1px);
    }

    .stButton > button[kind="primary"]:active {
        background-color: var(--accent-teal-soft);
    }

    /* Secondary buttons - transparent with teal border */
    .stButton > button[kind="secondary"] {
        background-color: transparent;
        color: var(--accent-teal);
        font-weight: 500;
        border: 1px solid var(--accent-teal);
        border-radius: var(--button-radius);
        padding: 0.6rem 1.5rem;
        transition: all 0.2s ease;
        font-size: 14px;
    }

    .stButton > button[kind="secondary"]:hover {
        background-color: rgba(34, 211, 238, 0.1);
        border-color: var(--accent-muted);
        color: var(--accent-muted);
    }

    /* ==================== Input Fields ==================== */
    .stTextInput > div > div > input,
    .stNumberInput > div > div > input,
    .stSelectbox > div > div > div,
    .stDateInput > div > div > input {
        border: 1px solid var(--border-default);
        border-radius: var(--button-radius);
        font-size: 14px;
        transition: all 0.2s ease;
        background-color: var(--bg-surface);
        color: var(--text-primary);
    }

    .stTextInput > div > div > input:focus,
    .stNumberInput > div > div > input:focus,
    .stDateInput > div > div > input:focus {
        border-color: var(--accent-teal);
        box-shadow: 0 0 0 3px rgba(34, 211, 238, 0.15);
    }

    .stSelectbox > div > div > div:focus,
    .stSelectbox > div > div > div:active {
        border-color: var(--accent-teal);
    }

    /* Placeholder text color */
    .stTextInput input::placeholder,
    .stNumberInput input::placeholder {
        color: var(--text-subtle);
    }

    .stTextArea > div > div > textarea {
        border: 1px solid var(--border-default);
        border-radius: var(--button-radius);
        font-size: 13px;
        font-family: 'JetBrains Mono', monospace;
        background-color: var(--bg-surface);
        color: var(--text-primary);
    }

    .stTextArea > div > div > textarea:focus {
        border-color: var(--accent-teal);
        box-shadow: 0 0 0 3px rgba(34, 211, 238, 0.15);
    }

    /* ==================== Alert Boxes ==================== */
    .stSuccess {
        background-color: rgba(34, 197, 94, 0.1);
        border: 1px solid var(--success);
        border-left: 4px solid var(--success);
        border-radius: var(--button-radius);
        padding: 1rem;
    }

    .stError {
        background-color: rgba(249, 115, 115, 0.1);
        border: 1px solid var(--error);
        border-left: 4px solid var(--error);
        border-radius: var(--button-radius);
        padding: 1rem;
    }

    .stWarning {
        background-color: rgba(250, 204, 21, 0.1);
        border: 1px solid var(--warning);
        border-left: 4px solid var(--warning);
        border-radius: var(--button-radius);
        padding: 1rem;
    }

    .stInfo {
        background-color: rgba(34, 211, 238, 0.1);
        border: 1px solid var(--accent-teal);
        border-left: 4px solid var(--accent-teal);
        border-radius: var(--button-radius);
        padding: 1rem;
    }

    /* ==================== Data Tables ==================== */
    .stDataFrame {
        border-radius: var(--card-radius);
        border: 1px solid var(--border-default);
        overflow: hidden;
        background-color: var(--bg-surface);
    }

    .stDataFrame > div {
        border: none;
    }

    /* Table header styling */
    .stDataFrame thead {
        background-color: var(--bg-surface);
    }

    .stDataFrame thead th {
        color: var(--text-muted);
        border-bottom: 1px solid var(--border-default);
    }

    .stDataFrame tbody td {
        color: var(--text-primary);
        border-bottom: 1px solid var(--border-subtle);
    }

    /* ==================== Code Block ==================== */
    .stCodeBlock {
        border-radius: var(--button-radius);
        border: 1px solid var(--border-default);
        background-color: var(--bg-surface);
        padding: 1rem;
    }

    .stCodeBlock code {
        color: var(--text-primary);
        background-color: transparent;
    }

    /* ==================== Metric Cards ==================== */
    [data-testid="stMetricValue"] {
        font-size: 28px;
        font-weight: 600;
        color: var(--accent-teal);
    }

    [data-testid="stMetricLabel"] {
        font-size: 13px;
        color: var(--text-muted);
    }

    /* ==================== Severity Badges ==================== */
    .severity-critical {
        color: var(--error);
        font-weight: 600;
    }

    .severity-warning {
        color: var(--warning);
        font-weight: 600;
    }

    .severity-info {
        color: var(--accent-teal);
        font-weight: 600;
    }

    .badge-info {
        background-color: rgba(34, 211, 238, 0.15);
        border: 1px solid var(--accent-teal);
        color: var(--accent-teal);
        padding: 2px 8px;
        border-radius: 4px;
        font-size: 12px;
        font-weight: 500;
    }

    .badge-warning {
        background-color: rgba(250, 204, 21, 0.15);
        border: 1px solid var(--warning);
        color: var(--warning);
        padding: 2px 8px;
        border-radius: 4px;
        font-size: 12px;
        font-weight: 500;
    }

    .badge-critical {
        background-color: rgba(249, 115, 115, 0.15);
        border: 1px solid var(--error);
        color: var(--error);
        padding: 2px 8px;
        border-radius: 4px;
        font-size: 12px;
        font-weight: 500;
    }

    /* ==================== Spacing Utilities ==================== */
    .spacer-sm { margin-top: 0.5rem; }
    .spacer-md { margin-top: 1rem; }
    .spacer-lg { margin-top: 1.5rem; }

    /* ==================== Card Container ==================== */
    .dark-card {
        background-color: var(--bg-surface);
        border: 1px solid var(--border-default);
        border-radius: var(--card-radius);
        padding: 16px;
        margin-bottom: 12px;
    }

    /* Subtle text for secondary info */
    em {
        color: var(--text-subtle);
        opacity: 1;
    }

    /* ==================== Theme Toggle Button ==================== */
    .theme-toggle-btn {
        display: inline-block;
        padding: 6px 12px;
        background-color: transparent;
        border: 1px solid var(--border-default);
        border-radius: var(--button-radius);
        color: var(--text-primary);
        cursor: pointer;
        font-size: 13px;
        font-weight: 500;
        transition: all 0.2s ease;
    }

    .theme-toggle-btn:hover {
        background-color: rgba(34, 211, 238, 0.1);
        border-color: var(--accent-teal);
        color: var(--accent-teal);
    }
</style>
""", unsafe_allow_html=True)


def apply_light_theme():
    """Apply light theme overrides."""
    st.markdown("""
    <style>
        :root {
            /* Light Theme Variables */
            --bg-canvas: #F5F7FA;
            --bg-surface: #FFFFFF;
            --bg-surface-alt: #F9FAFB;
            --accent-teal: #0891B2;
            --accent-teal-soft: #06B6D4;
            --accent-muted: #22D3EE;
            --text-primary: #1F2937;
            --text-muted: #6B7280;
            --text-subtle: #9CA3AF;
            --success: #16A34A;
            --warning: #CA8A04;
            --error: #DC2626;
            --border-default: #E5E7EB;
            --border-subtle: #F3F4F6;

            /* Dropdown-specific Design System */
            --dropdown-bg: #FFFFFF;
            --dropdown-border: #D1D5DB;
            --dropdown-border-hover: #9CA3AF;
            --dropdown-border-focus: #0891B2;
            --dropdown-text: #1F2937;
            --dropdown-text-placeholder: #9CA3AF;
            --dropdown-hover-bg: #F3F4F6;
            --dropdown-selected-bg: #E0F2FE;
            --dropdown-selected-text: #0891B2;
            --dropdown-shadow: 0 1px 2px rgba(0, 0, 0, 0.05);
            --dropdown-shadow-focus: 0 0 0 3px rgba(8, 145, 178, 0.1);
            --dropdown-menu-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
        }

        /* ==================== DROPDOWN DESIGN SYSTEM ==================== */

        /* Selectbox Input Field - The closed dropdown */
        .stSelectbox [data-baseweb="select"] > div {
            background-color: var(--dropdown-bg) !important;
            border: 1.5px solid var(--dropdown-border) !important;
            border-radius: 8px !important;
            min-height: 40px !important;
            box-shadow: var(--dropdown-shadow) !important;
            transition: all 0.2s ease !important;
        }

        /* Selectbox hover state */
        .stSelectbox [data-baseweb="select"] > div:hover {
            border-color: var(--dropdown-border-hover) !important;
            box-shadow: 0 2px 4px rgba(0, 0, 0, 0.08) !important;
        }

        /* Selectbox focus state */
        .stSelectbox [data-baseweb="select"] > div:focus-within {
            border-color: var(--dropdown-border-focus) !important;
            box-shadow: var(--dropdown-shadow-focus) !important;
        }

        /* Selectbox text (selected value) */
        .stSelectbox [data-baseweb="select"] span {
            color: var(--dropdown-text) !important;
            font-size: 14px !important;
            font-weight: 500 !important;
        }

        /* Selectbox placeholder text */
        .stSelectbox [data-baseweb="select"] [aria-selected="false"] {
            color: var(--dropdown-text-placeholder) !important;
        }

        /* Dropdown arrow icon */
        .stSelectbox [data-baseweb="select"] svg {
            color: var(--text-muted) !important;
        }

        /* ==================== DROPDOWN MENU (Popover) ==================== */

        /* Dropdown menu container */
        .stSelectbox [data-baseweb="popover"] {
            background-color: var(--dropdown-bg) !important;
            border: 1px solid var(--dropdown-border) !important;
            border-radius: 8px !important;
            box-shadow: var(--dropdown-menu-shadow) !important;
            margin-top: 4px !important;
            max-height: 300px !important;
            overflow-y: auto !important;
        }

        /* Dropdown menu list */
        .stSelectbox [role="listbox"] {
            background-color: var(--dropdown-bg) !important;
            padding: 4px !important;
        }

        /* Dropdown menu items */
        .stSelectbox [role="option"] {
            background-color: transparent !important;
            color: var(--dropdown-text) !important;
            font-size: 14px !important;
            padding: 10px 12px !important;
            border-radius: 6px !important;
            margin: 2px 0 !important;
            cursor: pointer !important;
            transition: all 0.15s ease !important;
        }

        /* Dropdown item hover state */
        .stSelectbox [role="option"]:hover {
            background-color: var(--dropdown-hover-bg) !important;
            color: var(--dropdown-text) !important;
        }

        /* Dropdown item selected/active state */
        .stSelectbox [role="option"][aria-selected="true"] {
            background-color: var(--dropdown-selected-bg) !important;
            color: var(--dropdown-selected-text) !important;
            font-weight: 600 !important;
        }

        /* Selected item with hover */
        .stSelectbox [role="option"][aria-selected="true"]:hover {
            background-color: #BAE6FD !important;
            color: var(--dropdown-selected-text) !important;
        }

        /* ==================== MULTISELECT DROPDOWNS ==================== */

        /* Multiselect input field */
        .stMultiSelect [data-baseweb="select"] > div {
            background-color: var(--dropdown-bg) !important;
            border: 1.5px solid var(--dropdown-border) !important;
            border-radius: 8px !important;
            min-height: 40px !important;
            box-shadow: var(--dropdown-shadow) !important;
            transition: all 0.2s ease !important;
        }

        .stMultiSelect [data-baseweb="select"] > div:hover {
            border-color: var(--dropdown-border-hover) !important;
            box-shadow: 0 2px 4px rgba(0, 0, 0, 0.08) !important;
        }

        .stMultiSelect [data-baseweb="select"] > div:focus-within {
            border-color: var(--dropdown-border-focus) !important;
            box-shadow: var(--dropdown-shadow-focus) !important;
        }

        /* Multiselect selected tags */
        .stMultiSelect [data-baseweb="tag"] {
            background-color: var(--dropdown-selected-bg) !important;
            color: var(--dropdown-selected-text) !important;
            border: 1px solid #7DD3FC !important;
            border-radius: 6px !important;
            font-size: 13px !important;
            padding: 4px 8px !important;
            margin: 2px !important;
        }

        /* Multiselect tag close button */
        .stMultiSelect [data-baseweb="tag"] svg {
            color: var(--dropdown-selected-text) !important;
        }

        /* Multiselect dropdown menu */
        .stMultiSelect [data-baseweb="popover"] {
            background-color: var(--dropdown-bg) !important;
            border: 1px solid var(--dropdown-border) !important;
            border-radius: 8px !important;
            box-shadow: var(--dropdown-menu-shadow) !important;
            margin-top: 4px !important;
        }

        /* Multiselect menu items */
        .stMultiSelect [role="option"] {
            background-color: transparent !important;
            color: var(--dropdown-text) !important;
            font-size: 14px !important;
            padding: 10px 12px !important;
            border-radius: 6px !important;
            margin: 2px 4px !important;
            transition: all 0.15s ease !important;
        }

        .stMultiSelect [role="option"]:hover {
            background-color: var(--dropdown-hover-bg) !important;
        }

        .stMultiSelect [role="option"][aria-selected="true"] {
            background-color: var(--dropdown-selected-bg) !important;
            color: var(--dropdown-selected-text) !important;
        }

        /* ==================== DROPDOWN SCROLLBAR ==================== */

        /* Custom scrollbar for dropdown menus */
        .stSelectbox [data-baseweb="popover"]::-webkit-scrollbar,
        .stMultiSelect [data-baseweb="popover"]::-webkit-scrollbar {
            width: 8px;
        }

        .stSelectbox [data-baseweb="popover"]::-webkit-scrollbar-track,
        .stMultiSelect [data-baseweb="popover"]::-webkit-scrollbar-track {
            background: var(--bg-surface-alt);
            border-radius: 4px;
        }

        .stSelectbox [data-baseweb="popover"]::-webkit-scrollbar-thumb,
        .stMultiSelect [data-baseweb="popover"]::-webkit-scrollbar-thumb {
            background: var(--dropdown-border);
            border-radius: 4px;
        }

        .stSelectbox [data-baseweb="popover"]::-webkit-scrollbar-thumb:hover,
        .stMultiSelect [data-baseweb="popover"]::-webkit-scrollbar-thumb:hover {
            background: var(--dropdown-border-hover);
        }

        /* ==================== BUTTON OVERRIDES FOR LIGHT MODE ==================== */

        /* Fix secondary buttons - give them white background instead of transparent */
        .stButton > button[kind="secondary"] {
            background-color: #FFFFFF !important;
            color: #0891B2 !important;
            border: 1.5px solid #0891B2 !important;
        }

        .stButton > button[kind="secondary"]:hover {
            background-color: #F0F9FF !important;
            border-color: #06B6D4 !important;
            color: #06B6D4 !important;
        }

        /* Fix primary buttons text color */
        .stButton > button[kind="primary"] {
            background-color: #0891B2 !important;
            color: #FFFFFF !important;
            border: none !important;
        }

        .stButton > button[kind="primary"]:hover {
            background-color: #0E7490 !important;
        }

        /* Fix default/tertiary buttons */
        .stButton > button {
            background-color: #F3F4F6 !important;
            color: #1F2937 !important;
            border: 1px solid #E5E7EB !important;
        }

        .stButton > button:hover {
            background-color: #E5E7EB !important;
            color: #111827 !important;
            border-color: #D1D5DB !important;
        }

        /* ==================== STREAMLIT COMPONENT BACKGROUNDS ==================== */

        /* Fix main app background */
        .main .block-container {
            padding-top: 1rem !important;
            background-color: #F5F7FA !important;
        }

        /* Fix sidebar background */
        section[data-testid="stSidebar"] {
            background-color: #FFFFFF !important;
        }

        section[data-testid="stSidebar"] > div {
            background-color: #FFFFFF !important;
        }

        /* Fix expander backgrounds */
        .streamlit-expanderHeader {
            background-color: #FFFFFF !important;
            border: 1px solid #E5E7EB !important;
        }

        .streamlit-expanderContent {
            background-color: #FFFFFF !important;
            border: 1px solid #E5E7EB !important;
            border-top: none !important;
        }

        /* Fix text areas */
        .stTextArea textarea {
            background-color: #FFFFFF !important;
            color: #1F2937 !important;
            border: 1.5px solid #D1D5DB !important;
        }

        .stTextArea textarea:focus {
            border-color: #0891B2 !important;
        }

        /* Fix text inputs */
        .stTextInput input {
            background-color: #FFFFFF !important;
            color: #1F2937 !important;
            border: 1.5px solid #D1D5DB !important;
        }

        .stTextInput input:focus {
            border-color: #0891B2 !important;
        }

        /* Fix number inputs */
        .stNumberInput input {
            background-color: #FFFFFF !important;
            color: #1F2937 !important;
            border: 1.5px solid #D1D5DB !important;
        }

        /* Fix code blocks */
        .stCodeBlock, pre {
            background-color: #F9FAFB !important;
            border: 1px solid #E5E7EB !important;
        }

        .stCodeBlock code {
            color: #1F2937 !important;
        }

        /* Fix dataframes */
        .stDataFrame {
            background-color: #FFFFFF !important;
        }

        /* Fix tabs */
        .stTabs [data-baseweb="tab-list"] {
            background-color: transparent !important;
        }

        .stTabs [data-baseweb="tab-panel"] {
            background-color: transparent !important;
        }

        /* Fix radio buttons */
        .stRadio > label {
            background-color: transparent !important;
        }

        /* Fix checkboxes */
        .stCheckbox > label {
            color: #1F2937 !important;
        }

        /* Fix all container backgrounds */
        [data-testid="stVerticalBlock"],
        [data-testid="stHorizontalBlock"],
        [data-testid="column"] {
            background-color: transparent !important;
        }

        /* Fix modal/dialog backgrounds if any */
        [data-baseweb="modal"] {
            background-color: #FFFFFF !important;
        }

        /* Fix any remaining baseweb components */
        [data-baseweb="input"],
        [data-baseweb="textarea"] {
            background-color: #FFFFFF !important;
            color: #1F2937 !important;
        }

        /* Buttons in light mode */
        .stButton > button {
            background-color: #FFFFFF !important;
            color: #1F2937 !important;
            border: 1px solid #E5E7EB !important;
        }

        .stButton > button:hover {
            background-color: #F9FAFB !important;
            border-color: #D1D5DB !important;
        }

        .stButton > button[kind="secondary"] {
            background-color: #FFFFFF !important;
            color: #0891B2 !important;
            border: 1.5px solid #0891B2 !important;
        }

        .stButton > button[kind="secondary"]:hover {
            background-color: #F0F9FF !important;
            border-color: #06B6D4 !important;
        }

        .stButton > button[kind="primary"] {
            background-color: #0891B2 !important;
            color: #FFFFFF !important;
        }

        /* Streamlit emotion cache button selectors for light mode */
        button[kind="secondary"] {
            background-color: #FFFFFF !important;
            color: #0891B2 !important;
            border: 1.5px solid #0891B2 !important;
        }

        button[kind="secondary"]:hover {
            background-color: #F0F9FF !important;
            border-color: #06B6D4 !important;
        }

        /* BaseWeb button styling */
        [data-testid="stBaseButton-secondary"] {
            background-color: #FFFFFF !important;
            color: #0891B2 !important;
            border: 1.5px solid #0891B2 !important;
        }

        [data-testid="stBaseButton-secondary"]:hover {
            background-color: #F0F9FF !important;
            border-color: #06B6D4 !important;
        }

    </style>
    """, unsafe_allow_html=True)


def apply_dark_theme():
    """Apply dark theme overrides."""
    st.markdown("""
    <style>
        :root {
            /* Dark Theme Variables */
            --bg-canvas: #020617;
            --bg-surface: #0B1120;
            --bg-surface-alt: #1E293B;
            --accent-teal: #22D3EE;
            --accent-teal-soft: #06B6D4;
            --accent-muted: #0891B2;
            --text-primary: #E5E7EB;
            --text-muted: #9CA3AF;
            --text-subtle: #6B7280;
            --success: #22C55E;
            --warning: #FACC15;
            --error: #EF4444;
            --border-default: #1E293B;
            --border-subtle: #0F172A;
        }

        /* Override any light mode remnants */
        body, .main, .stApp {
            background-color: #020617 !important;
            color: #E5E7EB !important;
        }

        /* Buttons in dark mode */
        .stButton > button {
            background-color: #1E293B !important;
            color: #E5E7EB !important;
            border: 1px solid #334155 !important;
        }

        .stButton > button:hover {
            background-color: #334155 !important;
            border-color: #475569 !important;
        }

        .stButton > button[kind="secondary"] {
            background-color: transparent !important;
            color: #22D3EE !important;
            border: 1.5px solid #22D3EE !important;
        }

        .stButton > button[kind="secondary"]:hover {
            background-color: rgba(34, 211, 238, 0.1) !important;
            border-color: #06B6D4 !important;
        }

        .stButton > button[kind="primary"] {
            background-color: #22D3EE !important;
            color: #020617 !important;
        }

        /* Streamlit emotion cache button selectors for dark mode */
        button[kind="secondary"] {
            background-color: #1E293B !important;
            color: #22D3EE !important;
            border: 1.5px solid #22D3EE !important;
        }

        button[kind="secondary"]:hover {
            background-color: rgba(34, 211, 238, 0.15) !important;
            border-color: #06B6D4 !important;
        }

        /* BaseWeb button styling for dark mode */
        [data-testid="stBaseButton-secondary"] {
            background-color: #1E293B !important;
            color: #22D3EE !important;
            border: 1.5px solid #22D3EE !important;
        }

        [data-testid="stBaseButton-secondary"]:hover {
            background-color: rgba(34, 211, 238, 0.15) !important;
            border-color: #06B6D4 !important;
        }

        /* Dropdowns in dark mode */
        .stSelectbox [data-baseweb="select"] > div {
            background-color: #0B1120 !important;
            border: 1.5px solid #334155 !important;
            color: #E5E7EB !important;
        }

        .stSelectbox [data-baseweb="popover"] {
            background-color: #0B1120 !important;
            border: 1px solid #334155 !important;
        }

        .stSelectbox [role="option"] {
            background-color: transparent !important;
            color: #E5E7EB !important;
        }

        .stSelectbox [role="option"]:hover {
            background-color: #1E293B !important;
        }

        .stSelectbox [role="option"][aria-selected="true"] {
            background-color: #164E63 !important;
            color: #22D3EE !important;
        }

        /* Inputs in dark mode */
        .stTextInput input,
        .stNumberInput input,
        .stTextArea textarea {
            background-color: #0B1120 !important;
            color: #E5E7EB !important;
            border: 1.5px solid #334155 !important;
        }

        /* Sidebar in dark mode */
        section[data-testid="stSidebar"] {
            background-color: #0B1120 !important;
        }

        /* Tables in dark mode */
        .stDataFrame {
            background-color: #0B1120 !important;
        }

        .stDataFrame th {
            background-color: #1E293B !important;
            color: #E5E7EB !important;
        }

        .stDataFrame td {
            background-color: #0B1120 !important;
            color: #E5E7EB !important;
            border-bottom: 1px solid #1E293B !important;
        }

        /* Code blocks in dark mode */
        .stCodeBlock, pre {
            background-color: #0B1120 !important;
            border: 1px solid #1E293B !important;
        }

        /* Expanders in dark mode */
        .streamlit-expanderHeader {
            background-color: #0B1120 !important;
            border: 1px solid #1E293B !important;
            color: #E5E7EB !important;
        }

        .streamlit-expanderContent {
            background-color: #0B1120 !important;
            border: 1px solid #1E293B !important;
        }
    </style>
    """, unsafe_allow_html=True)


def apply_login_page_styles():
    """Apply login page-specific styles."""
    st.markdown("""
    <style>
        /* Hide sidebar on login page */
        [data-testid="stSidebar"] {
            display: none !important;
        }

        /* Force the entire app container to center the login block vertically/horizontally */
        section.main {
            display: flex !important;
            justify-content: center !important;
            align-items: center !important;
            min-height: 80vh !important;
            padding-top: 0rem !important;
            padding-bottom: 0rem !important;
            background-color: var(--background-color) !important;
        }

        /* Constrain the login block width and reduce outer padding so layout is tighter */
        section.main > div {
            max-width: 420px !important;
            width: 100% !important;
            margin: 0 auto !important;
            padding: 0 1rem !important;
        }
    </style>
    """, unsafe_allow_html=True)

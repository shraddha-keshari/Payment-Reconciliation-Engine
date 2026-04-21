"""
============================================================
dashboard.py — Interactive Streamlit Dashboard

A web-based interface to monitor reconciliation health, 
visualize matching statistics, and audit discrepancies.
============================================================
"""

import streamlit as st
import polars as pl
from datetime import datetime

from src.persistence.db_manager import DatabaseManager
from src.persistence.repository import ReconciliationRepository
from src.ingestion.loader import DataLoader
from src.config import DATA_DIR

# ============================================================
# PAGE CONFIGURATION
# ============================================================

st.set_page_config(
    page_title="Payment Reconciliation Dashboard",
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ============================================================
# CUSTOM CSS — UI/UX Enhancements
# ============================================================

st.markdown("""
<style>
    .stMetric {
        background: linear-gradient(135deg, #1e293b 0%, #334155 100%);
        padding: 1rem;
        border-radius: 12px;
        border: 1px solid #475569;
    }
    
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        background: linear-gradient(90deg, #60a5fa, #a78bfa, #f472b6);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        text-align: center;
        margin-bottom: 0.5rem;
    }
    
    .sub-header {
        text-align: center;
        color: #94a3b8;
        margin-bottom: 2rem;
    }
</style>
""", unsafe_allow_html=True)


# ============================================================
# DATA FETCHING (CACHED)
# ============================================================

@st.cache_resource
def get_db():
    """Initializes the database connection once."""
    db = DatabaseManager()
    db.create_tables()
    return db


@st.cache_data(ttl=30)
def load_run_data():
    """Fetches run summaries from the repository with a 30s cache TTL."""
    db = get_db()
    repo = ReconciliationRepository(db)
    
    latest = repo.get_latest_run()
    all_runs = repo.get_all_runs()
    
    return latest, all_runs


# ============================================================
# MAIN DASHBOARD RENDERER
# ============================================================

def main():
    # --- Header ---
    st.markdown('<h1 class="main-header">🏦 Payment Reconciliation Dashboard</h1>', 
                unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Real-time reconciliation monitoring & analytics</p>', 
                unsafe_allow_html=True)
    
    # --- Data Loading ---
    latest_run, all_runs = load_run_data()
    
    if latest_run is None:
        st.warning("⚠️ No reconciliation runs found. Run `python main.py` first!")
        return
    
    # --- Sidebar Controls & History ---
    with st.sidebar:
        st.header("🔧 Controls")
        if st.button("🔄 Refresh Data", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
        
        st.divider()
        st.header("📜 Run History")
        for run in all_runs[:10]:
            with st.expander(f"Run #{run['id']} — {run['match_rate']:.1f}%"):
                st.write(f"🕐 {run['timestamp']}")
                st.write(f"✅ Matched: {run['total_matched']:,}")
                st.write(f"❌ Discrepancies: {run['total_discrepancies']:,}")
    
    # --- Key Performance Indicators (KPIs) ---
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric(label="Match Rate", value=f"{latest_run['match_rate']:.1f}%", delta="Target: 95%")
    with col2:
        st.metric(label="Total Matched", value=f"{latest_run['total_matched']:,}")
    with col3:
        st.metric(label="Discrepancies", value=f"{latest_run['total_discrepancies']:,}", delta_color="inverse")
    with col4:
        st.metric(label="Total Records", value=f"{latest_run['total_ledger'] + latest_run['total_gateway']:,}")
    
    st.divider()
    
    # --- Visual Analytics ---
    st.subheader("📈 Reconciliation Breakdown")
    chart_col1, chart_col2 = st.columns(2)
    
    with chart_col1:
        st.markdown("**Matching Methods**")
        method_data = {
            "Method": ["Exact", "Fuzzy", "Rule-Based", "Unmatched"],
            "Count": [latest_run['exact_matches'], latest_run['fuzzy_matches'], 
                      latest_run.get('rule_based_matches', 0), latest_run['total_discrepancies']]
        }
        st.bar_chart(pl.DataFrame(method_data).to_pandas().set_index("Method"))
    
    with chart_col2:
        st.markdown("**Discrepancy Types**")
        type_data = {
            "Type": ["Missing Gateway", "Missing Ledger", "Amount Mismatch", "Duplicates"],
            "Count": [latest_run['missing_in_gateway'], latest_run['missing_in_ledger'], 
                      latest_run['amount_mismatches'], latest_run['duplicates_found']]
        }
        st.bar_chart(pl.DataFrame(type_data).to_pandas().set_index("Type"))
    
    st.divider()
    
    # --- Detailed Audit Logs ---
    st.subheader("🔍 Discrepancy Details")
    db = get_db()
    repo = ReconciliationRepository(db)
    discrepancies = repo.get_discrepancies_for_run(latest_run['id'])
    
    if discrepancies:
        disc_df = pl.DataFrame(discrepancies)
        
        f_col1, f_col2 = st.columns(2)
        with f_col1:
            types = st.multiselect("Filter Type", options=disc_df["type"].unique().to_list(), default=disc_df["type"].unique().to_list())
        with f_col2:
            severities = st.multiselect("Filter Severity", options=disc_df["severity"].unique().to_list(), default=disc_df["severity"].unique().to_list())
            
        filtered = disc_df.filter(pl.col("type").is_in(types) & pl.col("severity").is_in(severities))
        st.dataframe(filtered.to_pandas(), use_container_width=True, height=400)
    else:
        st.success("🎉 Zero discrepancies found!")

    # --- Footer ---
    st.markdown("<div style='text-align: center; color: #64748b; font-size: 0.8rem;'>Payment Recon Engine v1.0</div>", unsafe_allow_html=True)


if __name__ == "__main__":
    main()
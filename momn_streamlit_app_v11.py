"""
momn_streamlit_app_v11.py
=========================
Momentum Screener + Portfolio Rebalancer — Integrated Single App
Steps:
  1. NSE Universe Setup   (EQUITY_L.csv upload)
  2. Run Screener          (data fetch + ranking)
  3. Plan Rebalance        (GSheet compare + orders)
  4. Apply & Export        (Excel export + Apps Script links)
"""

import io
import time
import datetime
import warnings
import threading

import numpy as np
import pandas as pd
import streamlit as st
import yfinance as yf
import requests

warnings.filterwarnings("ignore")

# ─── Try importing local modules ──────────────────────────────
try:
    from calculations import build_dfStats, apply_filters
    _CALCS_AVAILABLE = True
except ImportError:
    _CALCS_AVAILABLE = False

# ═══════════════════════════════════════════════════════════════
# PAGE CONFIG
# ═══════════════════════════════════════════════════════════════
st.set_page_config(
    page_title="Momn Screener + Rebalancer",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── Custom CSS ───────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@500&display=swap');

html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }

/* ── Header banner ── */
.app-header {
    background: linear-gradient(135deg, #0f172a 0%, #1e3a5f 50%, #0f172a 100%);
    border-bottom: 2px solid #334155;
    padding: 18px 28px;
    margin: -1rem -1rem 1.5rem -1rem;
    display: flex; align-items: center; justify-content: space-between;
}
.app-title { color: #f8fafc; font-size: 22px; font-weight: 700; letter-spacing: -0.3px; }
.app-title span { color: #38bdf8; }
.app-subtitle { color: #94a3b8; font-size: 12px; margin-top: 2px; }

/* ── Step progress bar ── */
.step-bar {
    display: flex; align-items: center; gap: 0;
    background: #f8fafc; border: 1px solid #e2e8f0;
    border-radius: 12px; padding: 12px 20px;
    margin-bottom: 1.5rem; overflow-x: auto;
}
.step-item {
    display: flex; align-items: center; gap: 10px;
    padding: 8px 16px; border-radius: 8px;
    font-size: 13px; font-weight: 600; white-space: nowrap;
    transition: all 0.2s;
}
.step-item.done { background: #dcfce7; color: #15803d; }
.step-item.active { background: #dbeafe; color: #1d4ed8; box-shadow: 0 0 0 2px #3b82f6; }
.step-item.pending { color: #94a3b8; }
.step-circle {
    width: 26px; height: 26px; border-radius: 50%;
    display: flex; align-items: center; justify-content: center;
    font-size: 11px; font-weight: 700; flex-shrink: 0;
}
.done .step-circle { background: #16a34a; color: #fff; }
.active .step-circle { background: #2563eb; color: #fff; }
.pending .step-circle { background: #e2e8f0; color: #94a3b8; }
.step-connector { width: 32px; height: 2px; background: #e2e8f0; flex-shrink: 0; }
.step-connector.done-line { background: #16a34a; }

/* ── Metric cards ── */
.metric-row { display: flex; gap: 12px; flex-wrap: wrap; margin: 12px 0; }
.metric-card {
    background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 10px;
    padding: 12px 18px; min-width: 140px;
}
.metric-label { font-size: 10px; color: #64748b; text-transform: uppercase; letter-spacing: 0.5px; font-weight: 600; }
.metric-value { font-size: 22px; font-weight: 700; color: #0f172a; margin-top: 2px; }
.metric-value.green { color: #16a34a; }
.metric-value.red   { color: #dc2626; }
.metric-value.blue  { color: #2563eb; }
.metric-value.amber { color: #d97706; }

/* ── Section headers ── */
.section-hdr {
    font-size: 14px; font-weight: 700; color: #0f172a;
    border-bottom: 2px solid #3b82f6; padding-bottom: 6px;
    margin: 1.2rem 0 0.8rem;
}

/* ── NSE link ── */
.nse-link-box {
    background: #eff6ff; border: 1px solid #bfdbfe; border-radius: 10px;
    padding: 14px 18px; display: flex; align-items: center; gap: 12px; margin: 12px 0;
}
.nse-link-box .icon { font-size: 22px; }
.nse-link-box a { color: #2563eb; font-weight: 600; font-size: 13px; text-decoration: none; }
.nse-link-box .hint { font-size: 11px; color: #64748b; margin-top: 3px; }

/* ── Sell/Buy chips ── */
.chip { display: inline-block; padding: 3px 10px; border-radius: 20px; font-size: 11px; font-weight: 700; margin: 2px; }
.chip-sell { background: #fee2e2; color: #dc2626; }
.chip-buy  { background: #dcfce7; color: #16a34a; }
.chip-hold { background: #f1f5f9; color: #475569; }

/* ── Rebalance summary strip ── */
.reb-strip {
    display: flex; gap: 14px; flex-wrap: wrap;
    background: #f8fafc; border: 1px solid #e2e8f0;
    border-radius: 10px; padding: 12px 18px; margin: 10px 0;
}
.reb-stat { }
.reb-stat .label { font-size: 9px; color: #64748b; text-transform: uppercase; font-weight: 600; }
.reb-stat .val   { font-size: 16px; font-weight: 700; }
.reb-stat .val.r { color: #dc2626; }
.reb-stat .val.g { color: #16a34a; }
.reb-stat .val.b { color: #2563eb; }
.reb-stat .val.p { color: #7c3aed; }

/* ── Stock table improvements ── */
.stDataFrame { border-radius: 10px; overflow: hidden; }

/* ── Sidebar step buttons ── */
[data-testid="stSidebar"] .step-btn-active {
    background: #2563eb !important; color: white !important;
}
</style>
""", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════
# SESSION STATE INIT
# ═══════════════════════════════════════════════════════════════
_defaults = {
    "current_step":   1,         # 1-4
    "eq_symbols":     None,      # list of symbols from EQUITY_L.csv
    "eq_df":          None,      # full EQUITY_L dataframe
    "dfStats":        None,      # screener results (unfiltered)
    "dfFiltered":     None,      # screener results (filtered, ranked)
    "reb_portfolio":  None,      # current portfolio from GSheet/CSV
    "reb_worst":      None,      # worst-rank stocks from GSheet/CSV
    "sell_list":      None,      # computed sell list
    "buy_list":       None,      # computed buy list
    "lookback_date":  datetime.date.today(),
    "ranking_method": "avgZScore12_6_3",
    "data_source":    "YFinance",
    "top_n_rank":     100,
    "universe_count": 0,
    "screener_done":  False,
    "rebalance_done": False,
}
for k, v in _defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ═══════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════
def fmt_inr(v):
    if pd.isna(v): return "—"
    v = int(round(v))
    if abs(v) >= 10_000_000: return f"₹{v/10_000_000:.1f}Cr"
    if abs(v) >= 100_000:    return f"₹{v/100_000:.1f}L"
    return f"₹{v:,}"

def step_html(current):
    steps = [
        (1, "NSE Universe"),
        (2, "Run Screener"),
        (3, "Plan Rebalance"),
        (4, "Apply & Export"),
    ]
    html = '<div class="step-bar">'
    for i, (n, label) in enumerate(steps):
        if n < current:
            cls, sym = "done",    "✓"
        elif n == current:
            cls, sym = "active",  str(n)
        else:
            cls, sym = "pending", str(n)
        html += f'<div class="step-item {cls}"><div class="step-circle">{sym}</div>{label}</div>'
        if i < len(steps) - 1:
            line_cls = "done-line" if n < current else ""
            html += f'<div class="step-connector {line_cls}"></div>'
    html += '</div>'
    return html

def metric_card(label, value, color=""):
    return f'''<div class="metric-card">
        <div class="metric-label">{label}</div>
        <div class="metric-value {color}">{value}</div>
    </div>'''

def parse_equity_csv(uploaded_file) -> pd.DataFrame:
    """Parse EQUITY_L.csv → return dataframe with EQ series only."""
    df = pd.read_csv(uploaded_file, skipinitialspace=True)
    df.columns = [c.strip() for c in df.columns]
    # Keep EQ series only
    if 'SERIES' in df.columns:
        df = df[df['SERIES'].str.strip() == 'EQ'].copy()
    df['SYMBOL'] = df['SYMBOL'].str.strip().str.upper()
    return df.reset_index(drop=True)

def symbol_to_ns(symbol: str) -> str:
    return f"{symbol}.NS"

def fetch_gsheet_rebalance():
    """
    Read published Google Sheet rebalance data.
    Returns dict {portfolio: [list], worst_rank: [list]}
    """
    sheet_id = "1xb8xoW91HWeXBW8Zd99TobULSgwxcvfPaaYPlMLZmHI"
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid=0"
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        df = pd.read_csv(io.StringIO(resp.text), header=0)
        df.columns = [c.strip() for c in df.columns]
        col_worst = df.columns[0]
        col_port  = df.columns[1]
        worst_rank = [str(x).strip().upper() for x in df[col_worst].dropna() if str(x).strip() and str(x).strip().lower() not in ('nan','worst rank held','')]
        portfolio  = [str(x).strip().upper() for x in df[col_port].dropna()  if str(x).strip() and str(x).strip().lower() not in ('nan','current portfolio','')]
        return {"portfolio": portfolio, "worst_rank": worst_rank, "ok": True}
    except Exception as e:
        return {"portfolio": [], "worst_rank": [], "ok": False, "error": str(e)}

def compute_rebalance(portfolio: list, worst_rank: list, top_screener: list, top_n: int = 100):
    """
    Core rebalance logic:
    - SELL = stocks in portfolio that are NOT in top_n screener OR appear in worst_rank list
    - BUY  = top_n screener stocks NOT currently in portfolio
    - HOLD = portfolio stocks in top_n screener
    """
    port_set    = set(portfolio)
    screener_set = set(top_screener[:top_n])
    worst_set   = set(worst_rank)

    sell = sorted([s for s in portfolio if s not in screener_set or s in worst_set])
    hold = sorted([s for s in portfolio if s in screener_set and s not in worst_set])
    buy  = [s for s in top_screener[:top_n] if s not in port_set][:len(sell)]

    return sell, hold, buy

def run_yfinance_screener(symbols_ns: list, start_date: datetime.date, end_date: datetime.date,
                           ranking_method: str, filter_params: dict,
                           progress_cb, status_cb, chunk_size: int = 15):
    """Fetch data via YFinance in chunks and compute dfStats."""
    close_chunks, high_chunks, volume_chunks = [], [], []
    failed = []
    total  = len(symbols_ns)

    for k in range(0, total, chunk_size):
        chunk = symbols_ns[k:k + chunk_size]
        pct   = min((k + chunk_size) / total, 1.0)
        status_cb(f"Fetching {k+1}–{min(k+chunk_size, total)} / {total}")
        progress_cb(pct * 0.85)
        try:
            raw = yf.download(chunk, start=start_date, progress=False,
                              auto_adjust=True, threads=True, multi_level_index=False)
            if not raw.empty:
                close_chunks.append(raw["Close"])
                high_chunks.append(raw["High"])
                volval = raw["Close"].multiply(raw["Volume"] if "Volume" in raw.columns else 1)
                volume_chunks.append(volval)
        except Exception as e:
            failed.extend(chunk)
        time.sleep(0.8)

    if not close_chunks:
        return None, None, None, failed

    close  = pd.concat(close_chunks,  axis=1)
    high   = pd.concat(high_chunks,   axis=1)
    volume = pd.concat(volume_chunks, axis=1)

    # Deduplicate columns
    close  = close.loc[:,  ~close.columns.duplicated()]
    high   = high.loc[:,   ~high.columns.duplicated()]
    volume = volume.loc[:, ~volume.columns.duplicated()]

    return close, high, volume, failed

def build_dates(end_date: datetime.date):
    end = datetime.datetime.combine(end_date, datetime.time())
    return {
        "endDate": end,
        "date12M": end - datetime.timedelta(days=365),
        "date9M":  end - datetime.timedelta(days=274),
        "date6M":  end - datetime.timedelta(days=182),
        "date3M":  end - datetime.timedelta(days=91),
        "date1M":  end - datetime.timedelta(days=30),
    }

# ═══════════════════════════════════════════════════════════════
# HEADER
# ═══════════════════════════════════════════════════════════════
st.markdown("""
<div class="app-header">
  <div>
    <div class="app-title">📈 <span>Momn</span> Screener + Rebalancer</div>
    <div class="app-subtitle">NSE Momentum Strategy — All-in-One Workflow</div>
  </div>
  <div style="color:#94a3b8;font-size:12px;font-family:'JetBrains Mono',monospace;">
    v11 &nbsp;|&nbsp; prayan2702
  </div>
</div>
""", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════
# SIDEBAR — STEP NAVIGATION & SETTINGS
# ═══════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown("### ⚙️ Workflow Steps")
    step_labels = {
        1: "1️⃣  NSE Universe",
        2: "2️⃣  Run Screener",
        3: "3️⃣  Plan Rebalance",
        4: "4️⃣  Apply & Export",
    }
    for s, lbl in step_labels.items():
        is_active = (st.session_state.current_step == s)
        is_done   = (s == 1 and st.session_state.eq_symbols is not None) or \
                    (s == 2 and st.session_state.screener_done) or \
                    (s == 3 and st.session_state.rebalance_done)
        indicator = "✅" if is_done else ("▶" if is_active else "○")
        if st.button(f"{indicator} {lbl}", key=f"nav_{s}", use_container_width=True,
                     type="primary" if is_active else "secondary"):
            st.session_state.current_step = s
            st.rerun()

    st.divider()
    st.markdown("### 🔧 Screener Settings")

    st.session_state.ranking_method = st.selectbox(
        "Ranking Method",
        ["avgZScore12_6_3", "avgZScore12_9_6_3", "avgSharpe12_6_3",
         "avgSharpe9_6_3", "avg_All"],
        index=0,
    )
    st.session_state.data_source = st.selectbox(
        "Data Source",
        ["YFinance", "Upstox", "Angel One"],
        index=0,
    )
    st.session_state.lookback_date = st.date_input(
        "Lookback Date",
        value=st.session_state.lookback_date,
        max_value=datetime.date.today(),
    )
    st.session_state.top_n_rank = st.number_input(
        "Top-N Rank for Universe",
        min_value=20, max_value=200, value=100, step=10,
    )

    st.divider()
    st.markdown("### 🔗 Quick Links")
    st.markdown("""
    <div style="font-size:12px;line-height:2;">
    <a href="https://www.nseindia.com/static/market-data/securities-available-for-trading" target="_blank">📥 NSE EQUITY_L.csv</a><br>
    <a href="https://docs.google.com/spreadsheets/d/1xb8xoW91HWeXBW8Zd99TobULSgwxcvfPaaYPlMLZmHI/edit?gid=0" target="_blank">📊 Rebalance Sheet</a>
    </div>
    """, unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════
# STEP PROGRESS BAR
# ═══════════════════════════════════════════════════════════════
st.markdown(step_html(st.session_state.current_step), unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════
# STEP 1 — NSE UNIVERSE SETUP
# ═══════════════════════════════════════════════════════════════
if st.session_state.current_step == 1:
    st.markdown('<div class="section-hdr">Step 1 — NSE Universe Setup</div>', unsafe_allow_html=True)

    # NSE website link box
    st.markdown("""
    <div class="nse-link-box">
      <div class="icon">📥</div>
      <div>
        <div><a href="https://www.nseindia.com/static/market-data/securities-available-for-trading" target="_blank">
          NSE — Securities Available for Trading</a></div>
        <div class="hint">Wahan se EQUITY_L.csv download karo → phir neeche browse karo</div>
      </div>
    </div>
    """, unsafe_allow_html=True)

    col1, col2 = st.columns([2, 1])
    with col1:
        uploaded = st.file_uploader(
            "📂 Browse EQUITY_L.csv (NSE EQ Segment)",
            type=["csv"],
            key="equity_csv_upload",
            help="NSE website se download kiya EQUITY_L.csv yahaan browse karo",
        )

    if uploaded:
        try:
            eq_df = parse_equity_csv(uploaded)
            st.session_state.eq_df      = eq_df
            st.session_state.eq_symbols = eq_df["SYMBOL"].tolist()
            st.session_state.universe_count = len(eq_df)

            st.markdown(f"""
            <div class="metric-row">
                {metric_card("Total EQ Stocks", f"{len(eq_df):,}", "green")}
                {metric_card("Series", "EQ Only", "blue")}
            </div>
            """, unsafe_allow_html=True)

            st.markdown('<div class="section-hdr">Preview — First 20 Rows</div>', unsafe_allow_html=True)
            st.dataframe(
                eq_df[["SYMBOL", "NAME OF COMPANY"]].head(20),
                use_container_width=True, height=320,
            )
        except Exception as e:
            st.error(f"CSV parse error: {e}")

    # Already loaded check
    if st.session_state.eq_symbols:
        n = len(st.session_state.eq_symbols)
        st.success(f"✅ Universe loaded: **{n:,} EQ stocks**")
        if st.button("▶ Next: Run Screener →", type="primary", use_container_width=False):
            st.session_state.current_step = 2
            st.rerun()
    else:
        st.info("⬆️ EQUITY_L.csv upload karo (ya sidebar se date/settings set karo), phir screener run hoga.")

# ═══════════════════════════════════════════════════════════════
# STEP 2 — RUN SCREENER
# ═══════════════════════════════════════════════════════════════
elif st.session_state.current_step == 2:
    st.markdown('<div class="section-hdr">Step 2 — Run Momentum Screener</div>', unsafe_allow_html=True)

    if not st.session_state.eq_symbols:
        st.warning("⚠️ Step 1 pehle complete karo — EQUITY_L.csv upload nahi hua.")
        if st.button("← Step 1 par jao"):
            st.session_state.current_step = 1
            st.rerun()
        st.stop()

    # ── Filter settings ──────────────────────────────────────
    with st.expander("🔧 Filter Settings", expanded=True):
        fc1, fc2, fc3 = st.columns(3)
        with fc1:
            use_dma200 = st.checkbox("Close > 200-day DMA", value=True)
            use_roc12  = st.checkbox("12M ROC > 5.5%",       value=True)
            use_roc_cap= st.checkbox("12M return < 1000x",   value=True)
        with fc2:
            volm_min   = st.slider("Avg Vol (Cr) >", 0.0, 10.0, 1.0, 0.1)
            circuit_max= st.slider("Circuit hits/yr <", 1, 100, 20, 1)
            circuit5   = st.slider("5% circuit in 3M ≤", 0, 30, 10, 1)
        with fc3:
            use_ath    = st.checkbox("Within 25% of ATH",    value=True)
            close_min  = st.slider("Min CMP ₹", 0.0, 500.0, 30.0, 5.0)

    filter_params = {
        "use_dma200":   use_dma200,
        "use_roc12":    use_roc12,
        "use_roc_cap":  use_roc_cap,
        "volm_cr_min":  volm_min,
        "circuit_max":  circuit_max,
        "circuit5_max": circuit5,
        "use_away_ath": use_ath,
        "close_min":    close_min,
    }

    # ── Run button ────────────────────────────────────────────
    col_run, col_info = st.columns([1, 2])
    with col_run:
        run_clicked = st.button("▶ Start Data Download", type="primary", use_container_width=True)
    with col_info:
        n_syms = len(st.session_state.eq_symbols)
        end_dt = st.session_state.lookback_date
        start_dt = datetime.date(2000, 1, 1)
        st.markdown(f"""
        <div style="font-size:12px;color:#64748b;padding-top:10px;">
        📊 Universe: <b>{n_syms:,}</b> stocks &nbsp;|&nbsp;
        📅 End date: <b>{end_dt.strftime('%d-%m-%Y')}</b> &nbsp;|&nbsp;
        🔢 Method: <b>{st.session_state.ranking_method}</b> &nbsp;|&nbsp;
        📡 Source: <b>{st.session_state.data_source}</b>
        </div>
        """, unsafe_allow_html=True)

    # ── Screener execution ────────────────────────────────────
    if run_clicked:
        if not _CALCS_AVAILABLE:
            st.error("❌ `calculations.py` not found. Isse project folder mein rakh kar dobara run karo.")
            st.stop()

        symbols = st.session_state.eq_symbols
        symbols_ns = [symbol_to_ns(s) for s in symbols]
        end_date   = st.session_state.lookback_date

        prog_bar  = st.progress(0)
        status_tx = st.empty()
        log_tx    = st.empty()
        counter   = st.empty()

        def progress_cb(v):
            prog_bar.progress(min(float(v), 1.0))

        def status_cb(msg):
            status_tx.markdown(f"⏳ **{msg}**")

        status_cb("Downloading market data...")

        if st.session_state.data_source == "YFinance":
            try:
                close, high, volume, failed = run_yfinance_screener(
                    symbols_ns, start_dt, end_date,
                    st.session_state.ranking_method, filter_params,
                    progress_cb, status_cb, chunk_size=15,
                )
            except Exception as e:
                st.error(f"Data fetch error: {e}")
                st.stop()
        else:
            st.warning(f"⚠️ {st.session_state.data_source} ke liye live token required hai. YFinance use karo ya main window se token set karo.")
            st.stop()

        if close is None or close.empty:
            st.error("❌ Data fetch hua nahi. Internet connection aur symbols check karo.")
            st.stop()

        status_cb("Calculating momentum metrics...")
        progress_cb(0.9)

        try:
            dates    = build_dates(end_date)
            dfStats  = build_dfStats(close, high, volume, dates, st.session_state.ranking_method)
            dfFiltered = apply_filters(dfStats, filter_params)

            st.session_state.dfStats    = dfStats
            st.session_state.dfFiltered = dfFiltered
            st.session_state.screener_done = True

            prog_bar.progress(1.0)
            status_tx.markdown("✅ **Screener complete!**")
        except Exception as e:
            st.error(f"Calculation error: {e}")
            st.stop()

    # ── Display results ───────────────────────────────────────
    if st.session_state.screener_done and st.session_state.dfFiltered is not None:
        dfF = st.session_state.dfFiltered
        dfU = st.session_state.dfStats

        n_filtered = len(dfF)
        n_total    = len(dfU) if dfU is not None else 0
        rank_col   = st.session_state.ranking_method

        st.markdown(f"""
        <div class="metric-row">
            {metric_card("Total Screened",   f"{n_total:,}")}
            {metric_card("Passed Filters",   f"{n_filtered:,}", "green")}
            {metric_card("Ranking Method",   rank_col.replace('avg','Avg ').replace('ZScore','Z ').replace('Sharpe','Sharpe '), "blue")}
            {metric_card("End Date",         st.session_state.lookback_date.strftime('%d %b %Y'))}
        </div>
        """, unsafe_allow_html=True)

        tab1, tab2 = st.tabs(["✅ Filtered (Top Ranked)", "📊 All Stocks"])

        with tab1:
            top100 = dfF.head(st.session_state.top_n_rank).reset_index()
            display_cols = ["Rank", "Ticker", "Close", rank_col, "roc12M", "roc6M",
                            "roc3M", "vol12M", "volm_cr", "AWAY_ATH", "circuit"]
            display_cols = [c for c in display_cols if c in top100.columns]
            st.dataframe(
                top100[display_cols].style.format(precision=2),
                use_container_width=True, height=450,
            )

            # Download button
            csv_buf = io.BytesIO()
            with pd.ExcelWriter(csv_buf, engine="openpyxl") as writer:
                dfF.reset_index().to_excel(writer, sheet_name="Filtered", index=False)
                if dfU is not None:
                    dfU.reset_index().to_excel(writer, sheet_name="Unfiltered", index=False)
            csv_buf.seek(0)
            st.download_button(
                "💾 Excel Download (Filtered + Unfiltered)",
                data=csv_buf.getvalue(),
                file_name=f"{datetime.date.today()}_AllNSE_{rank_col}_YFinance_lookback.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

        with tab2:
            if dfU is not None:
                st.dataframe(dfU.reset_index().head(200).style.format(precision=2),
                             use_container_width=True, height=450)

        st.divider()
        if st.button("▶ Next: Plan Rebalance →", type="primary"):
            st.session_state.current_step = 3
            st.rerun()

# ═══════════════════════════════════════════════════════════════
# STEP 3 — PLAN REBALANCE
# ═══════════════════════════════════════════════════════════════
elif st.session_state.current_step == 3:
    st.markdown('<div class="section-hdr">Step 3 — Plan Rebalance</div>', unsafe_allow_html=True)

    # ── Portfolio source ──────────────────────────────────────
    port_source = st.radio(
        "Portfolio data source",
        ["📊 Load from Google Sheet (auto)", "📂 Upload CSV manually"],
        horizontal=True,
    )

    portfolio, worst_rank = [], []

    if "📊" in port_source:
        col_load, _ = st.columns([1, 2])
        with col_load:
            if st.button("🔄 Fetch from Google Sheet", type="primary"):
                with st.spinner("Fetching from published sheet..."):
                    data = fetch_gsheet_rebalance()
                if data["ok"]:
                    st.session_state.reb_portfolio = data["portfolio"]
                    st.session_state.reb_worst     = data["worst_rank"]
                    st.success(f"✅ Loaded — Portfolio: {len(data['portfolio'])} stocks, Worst Rank: {len(data['worst_rank'])} stocks")
                else:
                    st.error(f"Sheet fetch failed: {data.get('error')}")
                    st.info("CSV manually upload karo neeche se.")

    else:
        up_reb = st.file_uploader("📂 Upload Rebalance CSV", type=["csv"], key="reb_csv")
        if up_reb:
            try:
                df_reb = pd.read_csv(up_reb)
                df_reb.columns = [c.strip() for c in df_reb.columns]
                col_a = df_reb.columns[0]
                col_b = df_reb.columns[1]
                st.session_state.reb_worst     = [str(x).strip().upper() for x in df_reb[col_a].dropna() if str(x).strip() and len(str(x).strip()) > 1]
                st.session_state.reb_portfolio = [str(x).strip().upper() for x in df_reb[col_b].dropna() if str(x).strip() and len(str(x).strip()) > 1]
                st.success(f"Loaded — Portfolio: {len(st.session_state.reb_portfolio)}, Worst: {len(st.session_state.reb_worst)}")
            except Exception as e:
                st.error(f"CSV parse error: {e}")

    portfolio  = st.session_state.reb_portfolio or []
    worst_rank = st.session_state.reb_worst or []

    # ── Manual override ───────────────────────────────────────
    with st.expander("✏️ Manual Edit (comma-separated)", expanded=False):
        mc1, mc2 = st.columns(2)
        with mc1:
            port_text = st.text_area(
                "Current Portfolio (symbols)",
                value=", ".join(portfolio),
                height=120,
                help="Portfolio ke stocks comma-separated"
            )
        with mc2:
            worst_text = st.text_area(
                "Worst Rank Held (to exit)",
                value=", ".join(worst_rank),
                height=120,
                help="Worst rank stocks jo exit karne hain"
            )
        if st.button("Apply Manual Edit"):
            st.session_state.reb_portfolio = [s.strip().upper() for s in port_text.split(",") if s.strip()]
            st.session_state.reb_worst     = [s.strip().upper() for s in worst_text.split(",") if s.strip()]
            portfolio  = st.session_state.reb_portfolio
            worst_rank = st.session_state.reb_worst
            st.success("Updated!")

    # ── Compute rebalance ─────────────────────────────────────
    if portfolio and st.session_state.screener_done and st.session_state.dfFiltered is not None:
        top_screener = st.session_state.dfFiltered.reset_index()["Ticker"].tolist()
        sell, hold, buy = compute_rebalance(
            portfolio, worst_rank, top_screener, st.session_state.top_n_rank
        )
        st.session_state.sell_list = sell
        st.session_state.buy_list  = buy
        st.session_state.rebalance_done = True

        st.markdown(f"""
        <div class="reb-strip">
          <div class="reb-stat"><div class="label">Portfolio</div><div class="val b">{len(portfolio)}</div></div>
          <div class="reb-stat"><div class="label">Top-{st.session_state.top_n_rank} Screener</div><div class="val b">{len(top_screener[:st.session_state.top_n_rank])}</div></div>
          <div class="reb-stat"><div class="label">SELL</div><div class="val r">{len(sell)}</div></div>
          <div class="reb-stat"><div class="label">BUY (New Entry)</div><div class="val g">{len(buy)}</div></div>
          <div class="reb-stat"><div class="label">HOLD (Retain)</div><div class="val p">{len(hold)}</div></div>
        </div>
        """, unsafe_allow_html=True)

        col_sell, col_buy, col_hold = st.columns(3)

        with col_sell:
            st.markdown("#### 🔴 SELL List")
            if sell:
                chips = " ".join([f'<span class="chip chip-sell">{s}</span>' for s in sell])
                st.markdown(chips, unsafe_allow_html=True)
                sell_df = pd.DataFrame({"Stock": sell, "Action": "SELL"})

                # Add screener rank if available
                rank_map = {}
                if st.session_state.dfFiltered is not None:
                    df_tmp = st.session_state.dfFiltered.reset_index()
                    if "Ticker" in df_tmp.columns:
                        rank_map = dict(zip(df_tmp["Ticker"], df_tmp.index))
                sell_df["Screener Rank"] = sell_df["Stock"].map(lambda x: rank_map.get(x, "—"))
                st.dataframe(sell_df, hide_index=True, use_container_width=True)
            else:
                st.success("Koi sell nahi hai!")

        with col_buy:
            st.markdown("#### 🟢 BUY List (New Entry)")
            if buy:
                chips = " ".join([f'<span class="chip chip-buy">{s}</span>' for s in buy])
                st.markdown(chips, unsafe_allow_html=True)
                buy_df = pd.DataFrame({"Stock": buy, "Action": "BUY"})
                rank_map = {}
                if st.session_state.dfFiltered is not None:
                    df_tmp = st.session_state.dfFiltered.reset_index()
                    if "Ticker" in df_tmp.columns:
                        rank_map = dict(zip(df_tmp["Ticker"], df_tmp.index))
                buy_df["Screener Rank"] = buy_df["Stock"].map(lambda x: rank_map.get(x, "—"))
                st.dataframe(buy_df, hide_index=True, use_container_width=True)
            else:
                st.info("Koi buy nahi hai ya portfolio already updated hai.")

        with col_hold:
            st.markdown("#### 🔵 HOLD (Retain)")
            if hold:
                chips = " ".join([f'<span class="chip chip-hold">{s}</span>' for s in hold])
                st.markdown(chips, unsafe_allow_html=True)

        st.divider()

        # ── Rebalance Panel (HTML page logic embedded) ────────
        st.markdown('<div class="section-hdr">💹 Quick Rebalance Inputs</div>', unsafe_allow_html=True)

        q1, q2, q3 = st.columns(3)
        with q1:
            capital_add = st.number_input("+ Capital Addition ₹", min_value=0, value=0, step=5000)
        with q2:
            brokerage   = st.number_input("🏦 Brokerage/Stock ₹", min_value=0, value=0, step=10)
        with q3:
            net_per_stock = st.empty()

        if st.session_state.dfFiltered is not None:
            df_cmp = st.session_state.dfFiltered.reset_index()
            if "Ticker" in df_cmp.columns and "Close" in df_cmp.columns:
                cmp_map = dict(zip(df_cmp["Ticker"], df_cmp["Close"]))

                # Sell values — approximate using CMP for held stocks
                sell_val = sum([cmp_map.get(s, 0) for s in sell]) * 10  # approx qty 10
                sell_brk = len(sell) * brokerage
                buy_brk  = len(buy)  * brokerage
                pool     = sell_val + capital_add - sell_brk - buy_brk
                per_stock = pool / len(buy) if buy else 0

                net_per_stock.markdown(f"""
                <div class="metric-card">
                  <div class="metric-label">Est. Pool / Stock</div>
                  <div class="metric-value green">{fmt_inr(per_stock)}</div>
                </div>
                """, unsafe_allow_html=True)

                if buy:
                    st.markdown('<div class="section-hdr">📋 Buy Orders (Estimated)</div>', unsafe_allow_html=True)
                    orders = []
                    for stock in buy:
                        cmp = cmp_map.get(stock, 0)
                        if cmp > 0:
                            qty = int(per_stock / cmp) if per_stock > 0 else 0
                            orders.append({
                                "Stock":    stock,
                                "CMP ₹":   round(cmp, 2),
                                "Alloc ₹": round(per_stock),
                                "Brok ₹":  brokerage,
                                "Net ₹":   round(per_stock - brokerage),
                                "Qty":     qty,
                                "Val ₹":   qty * cmp,
                            })
                    if orders:
                        orders_df = pd.DataFrame(orders)
                        st.dataframe(
                            orders_df.style.format({
                                "CMP ₹": "{:.2f}", "Alloc ₹": "{:,.0f}",
                                "Net ₹": "{:,.0f}", "Val ₹": "{:,.0f}"
                            }),
                            use_container_width=True, hide_index=True
                        )

        if st.button("▶ Next: Apply & Export →", type="primary"):
            st.session_state.current_step = 4
            st.rerun()

    elif not portfolio:
        st.info("⬆️ Upar se portfolio data load karo.")
    elif not st.session_state.screener_done:
        st.warning("⚠️ Pehle Step 2 mein screener run karo.")
        if st.button("← Step 2 par jao"):
            st.session_state.current_step = 2
            st.rerun()

# ═══════════════════════════════════════════════════════════════
# STEP 4 — APPLY & EXPORT
# ═══════════════════════════════════════════════════════════════
elif st.session_state.current_step == 4:
    st.markdown('<div class="section-hdr">Step 4 — Apply & Export</div>', unsafe_allow_html=True)

    sell = st.session_state.sell_list or []
    buy  = st.session_state.buy_list  or []
    portfolio = st.session_state.reb_portfolio or []

    # ── Summary ───────────────────────────────────────────────
    st.markdown(f"""
    <div class="reb-strip">
      <div class="reb-stat"><div class="label">Exits (SELL)</div><div class="val r">{len(sell)}</div></div>
      <div class="reb-stat"><div class="label">New Entries (BUY)</div><div class="val g">{len(buy)}</div></div>
      <div class="reb-stat"><div class="label">Retained</div><div class="val p">{len(portfolio) - len(sell)}</div></div>
      <div class="reb-stat"><div class="label">New Portfolio Size</div><div class="val b">{len(portfolio) - len(sell) + len(buy)}</div></div>
    </div>
    """, unsafe_allow_html=True)

    # ── Excel export ──────────────────────────────────────────
    if st.session_state.dfFiltered is not None:
        st.markdown('<div class="section-hdr">💾 Excel Export</div>', unsafe_allow_html=True)

        export_buf = io.BytesIO()
        with pd.ExcelWriter(export_buf, engine="openpyxl") as writer:
            # Sheet 1: Filtered screener
            dfF = st.session_state.dfFiltered.reset_index()
            dfF.to_excel(writer, sheet_name="Filtered", index=False)

            # Sheet 2: Unfiltered
            if st.session_state.dfStats is not None:
                st.session_state.dfStats.reset_index().to_excel(writer, sheet_name="Unfiltered", index=False)

            # Sheet 3: Rebalance plan
            reb_plan = []
            for s in sell:
                reb_plan.append({"Stock": s, "Action": "SELL", "Type": "Exit"})
            for b in buy:
                reb_plan.append({"Stock": b, "Action": "BUY",  "Type": "New Entry"})
            for p in portfolio:
                if p not in sell:
                    reb_plan.append({"Stock": p, "Action": "HOLD", "Type": "Retain"})
            pd.DataFrame(reb_plan).to_excel(writer, sheet_name="Rebalance Plan", index=False)

        export_buf.seek(0)
        fname = f"{datetime.date.today()}_Momn_Rebalance_v11.xlsx"
        st.download_button(
            "📥 Download Rebalance Excel",
            data=export_buf.getvalue(),
            file_name=fname,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            type="primary",
        )

    # ── Apps Script integration ───────────────────────────────
    st.markdown('<div class="section-hdr">📊 Google Sheets — Apps Script Flow</div>', unsafe_allow_html=True)

    st.markdown("""
    <div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:10px;padding:14px 18px;font-size:13px;line-height:2;">
    <b>Zerodha trade ke baad — Apps Script workflow:</b><br>
    1. 📂 Zerodha → Positions → CSV download karo<br>
    2. 📋 Google Sheet → <b>🔄 Rebalance → Process Positions CSV</b> menu<br>
    3. CSV paste karo → Preview check karo → ✅ Process<br>
    4. Automatic update: <b>Portfolio</b> + <b>Exits</b> + <b>XIRR</b> sheets
    </div>
    """, unsafe_allow_html=True)

    col_links = st.columns(3)
    with col_links[0]:
        st.markdown("""
        <a href="https://docs.google.com/spreadsheets/d/1xb8xoW91HWeXBW8Zd99TobULSgwxcvfPaaYPlMLZmHI" target="_blank">
        <div style="background:#3b82f6;color:white;padding:10px 16px;border-radius:8px;text-align:center;font-weight:700;font-size:13px;text-decoration:none;">
        📊 Open Google Sheet
        </div></a>
        """, unsafe_allow_html=True)
    with col_links[1]:
        st.markdown("""
        <a href="https://prayan2702.github.io/momn-dashboard/" target="_blank">
        <div style="background:#7c3aed;color:white;padding:10px 16px;border-radius:8px;text-align:center;font-weight:700;font-size:13px;">
        📈 Portfolio Dashboard
        </div></a>
        """, unsafe_allow_html=True)

    # ── Rebalance checklist ───────────────────────────────────
    st.markdown('<div class="section-hdr">✅ Monthly Rebalance Checklist</div>', unsafe_allow_html=True)

    checklist = [
        ("1", "📥 NSE se EQUITY_L.csv download kiya",   "Step 1"),
        ("2", "▶ Screener run kiya (YFinance/Upstox)",  "Step 2"),
        ("3", "📋 Top-100 list se sell/buy nikala",     "Step 3"),
        ("4", "📊 Rebalance Sheet update kiya (manually)", "Manual"),
        ("5", "🤝 Zerodha se trades execute kiye",       "Manual"),
        ("6", "📂 Positions CSV se Apps Script run kiya","Apps Script"),
        ("7", "💾 Backup sheet copy kiya (monthly)",    "Manual"),
        ("8", "📈 Dashboard refresh / XIRR check kiya", "Dashboard"),
    ]

    for num, task, where in checklist:
        c1, c2, c3 = st.columns([0.3, 3, 1])
        with c1:
            st.checkbox("", key=f"chk_{num}")
        with c2:
            st.markdown(f"<div style='padding-top:4px;font-size:13px;'>{task}</div>", unsafe_allow_html=True)
        with c3:
            st.markdown(f"<div style='padding-top:4px;font-size:11px;color:#64748b;'>{where}</div>", unsafe_allow_html=True)

    st.divider()
    col_r1, col_r2 = st.columns(2)
    with col_r1:
        if st.button("🔄 New Month — Restart from Step 1", use_container_width=True):
            for k in list(st.session_state.keys()):
                del st.session_state[k]
            st.rerun()
    with col_r2:
        if st.button("← Step 3 — Edit Rebalance", use_container_width=False):
            st.session_state.current_step = 3
            st.rerun()

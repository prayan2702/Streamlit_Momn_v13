# Momn Screener + Rebalancer — v12

NSE Momentum Strategy — 4-Step Workflow App

---

## Files Required (same folder mein rakhna)

```
momn_streamlit_app_v12.py   ← main app
calculations.py              ← momentum metrics
data_service.py              ← data fetch (YFinance / Upstox / Angel One)
upstox_auth.py               ← Upstox OAuth manager
angelone_auth.py             ← Angel One TOTP login manager
requirements.txt             ← dependencies
```

---

## Installation & Run

```bash
pip install -r requirements.txt
streamlit run momn_streamlit_app_v12.py
```


## requirements.txt

```
streamlit>=1.32.0
pandas>=2.0.0
numpy>=1.26.0
yfinance>=0.2.40
openpyxl>=3.1.2
requests>=2.31.0
python-dateutil>=2.8.2
pyxirr>=0.9.0
pyotp>=2.8.0

# Angel One ke liye uncomment karo:
# smartapi-python>=1.3.4
```

---


## 4-Step Workflow

---

### Step 1 — Universe Setup

| Universe | Source |
|----------|--------|
| Nifty50 / Nifty100 / Nifty200 / Nifty250 / Nifty500 | GitHub se auto-load |
| N750 (Nifty Total Market) | GitHub se auto-load |
| AllNSE | EQUITY_L.csv upload karo ya GitHub fallback |

**AllNSE ke liye:**
1. [NSE website](https://www.nseindia.com/static/market-data/securities-available-for-trading) pe jao
2. `EQUITY_L.csv` download karo
3. App mein upload karo (ya bina upload ke bhi GitHub fallback kaam karta hai)

**Nifty50–N750 ke liye:**
- "Load Symbol List" button dabao → GitHub se auto-fetch
- Ya seedha "Next: Run Screener" dabao — screener run pe auto-load hoga

---

### Step 2 — Run Momentum Screener

**Sidebar Settings:**
- **Ranking Method:** AvgZScore 12M/6M/3M (recommended) ya koi bhi
- **Data Source:** YFinance / Upstox / Angel One
- **Lookback Date:** Aaj ki ya koi recent trading date
- **Top-N Rank:** 75 (Nifty indices) ya 100 (AllNSE)

**Filter Settings (expandable):**
| Filter | Default |
|--------|---------|
| Close > 200-day DMA | ✅ ON |
| 12M ROC > 5.5% | ✅ ON |
| 12M return < 1000x | ✅ ON |
| Avg Vol (Cr) > | 1.0 Cr |
| Circuit hits/yr < | 20 |
| 5% circuit in 3M ≤ | 10 |
| Within 25% of ATH | ✅ ON |
| Min CMP ₹ | 30 |

**"▶ Start Data Download" dabao:**
- Data download hoga → momentum metrics calculate honge
- Filtered + Unfiltered tables dikhenge
- Failed downloads (blank volume) list alag dikhegi

---

### Step 3 — Plan Rebalance

**Portfolio Load karo:**
- **Google Sheet (auto):** "Fetch from Google Sheet" dabao → `Current Portfolio` column se load
- **CSV manually:** apna portfolio CSV upload karo (Column B = Current Portfolio)
- **Manual Edit:** text area mein comma-separated tickers type karo

**Auto-compute:**
- SELL list = portfolio stocks jo Top-N mein nahi hain
- BUY list = Top-N mein naye stocks jo portfolio mein nahi hain
- HOLD list = portfolio stocks jo Top-N mein bhi hain
- Har exit stock ka **Reason for Exit** bhi dikhta hai

**Rebalancer Workflow (sell value ke liye):**

```
1. "Sell Stocks" text area se stocks copy karo (📋 Copy button)
   ↓
2. Google Sheet ke "Worst Rank Held" column mein paste karo
   ↓
3. "⚖️ Open Portfolio Rebalancer" button dabao
   → Apps Script page khulega
   → Wahan sell stocks select karo → actual total sell value note karo
   ↓
4. "💸 Sell Value ₹" field mein woh actual value enter karo
   ↓
5. Buy Orders table auto-calculate hoga:
   - Per-stock allocation
   - Quantity (floor division)
   - Total invested + Leftover
```

**Order Calculator fields:**
| Field | Kya enter karo |
|-------|----------------|
| 💰 Capital Addition ₹ | Agar extra paisa add kar rahe ho |
| 🏦 Brokerage/Stock ₹ | Zerodha = ₹20, Angel = ₹0, etc. |
| 💸 Sell Value ₹ | Portfolio Rebalancer se actual sell value |

---

### Step 4 — Apply & Export

**Excel Download (v10 Format — 4 Sheets):**

| Sheet | Content |
|-------|---------|
| Unfiltered Stocks | Saare stocks + momentum metrics |
| Filtered Stocks | Filter pass stocks + rank summary |
| Failed Downloads | Blank volume wale stocks |
| Portfolio Rebalancing | Sell → Buy mapping + Reason for Exit |

**Excel Formatting:**
- Header: Dark blue, white bold font
- Unfiltered: Purple highlight = filter fail cells, Green = top-rank cells
- Filtered: AWAY_ATH% suffix, Rank green highlight, Summary row at bottom
- Rebalancing: Red = Sell column, Green = Buy column
- Failed: Orange highlight

**Monthly Checklist (in-app):**
1. NSE se EQUITY_L.csv download kiya
2. Screener run kiya
3. Top-N list se sell/buy nikala
4. Google Sheet update kiya (manually)
5. Broker se trades execute kiye
6. Portfolio Rebalancer (Apps Script) run kiya
7. Sheet backup kiya (monthly)
8. Dashboard refresh / XIRR check kiya

---

## Google Sheet Structure

**Published CSV (auto-fetch):**
- Column A: `Worst Rank Held` — exit candidates (Step 3 mein copy karo)
- Column B: `Current Portfolio` — currently held stocks

**Portfolio Rebalancer (Apps Script):**
URL: `https://script.google.com/macros/...exec`
- Sell stocks select karo → sell value nikalo
- Quick / Annual / Lumpsum rebalance tabs

---

## Troubleshooting

| Problem | Solution |
|---------|----------|
| `data_service.py import failed` | `smartapi-python` install nahi — YFinance auto-use hoga |
| Angel One error sidebar mein | `smartapi-python>=1.3.4` requirements.txt mein add karo |
| Sell Value = ₹27 (too low) | Portfolio Rebalancer se actual value enter karo (CMP estimate mat use karo) |
| Symbol list load failed | Internet check karo ya CSV manually upload karo |
| Future date error | Aaj ki ya recent past trading date select karo |
| All qty = 0 | Sell Value field mein actual value enter karo (0 nahi) |

---

## Changelog

### v12
- Universe dropdown: Nifty50/100/200/250/500/N750/AllNSE
- Nifty universes GitHub se auto-load (CSV upload zaroorat nahi)
- data_service.py integration (YFinance / Upstox / Angel One)
- Import errors graceful handling — YFinance fallback automatic
- Step 3: Rebalancer Workflow panel — sell list copy button + Rebalancer link
- Sell Value field = 0 default (actual value enter karo, estimate nahi)
- Excel — v10 exact format: 4 sheets, color highlights, rank summary

### v11
- 4-step workflow UI
- EQUITY_L.csv upload (AllNSE)
- calculations.py modular separation
- Apps Script integration

### v10
- Multi-API: YFinance / Upstox / Angel One
- data_service.py extracted
- Excel 4-sheet export
- Portfolio rebalancing with reasons for exit

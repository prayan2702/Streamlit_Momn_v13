# Momn Screener + Rebalancer — v11

## Files needed in same folder
```
momn_streamlit_app_v11.py   ← main app (this file)
calculations.py              ← momentum metrics (existing)
data_service.py              ← data fetch (existing, YFinance/Upstox/Angel One)
requirements_v11.txt         ← pip install
```

## Run locally
```bash
pip install -r requirements_v11.txt
streamlit run momn_streamlit_app_v11.py
```

## Deploy on Streamlit Cloud / Hugging Face Spaces
- `requirements.txt` mein v11 ka content paste karo
- `app.py` = `momn_streamlit_app_v11.py` rename karo (HF Spaces ke liye)
- `calculations.py` bhi same repo mein hona chahiye

## 4-Step Workflow
| Step | Kya hota hai |
|------|--------------|
| 1 | NSE EQUITY_L.csv upload (website se download kar ke) |
| 2 | Momentum screener run (YFinance / Upstox / Angel One) |
| 3 | Google Sheet se portfolio load → sell/buy nikalo |
| 4 | Excel export + Apps Script checklist |

## Google Sheet
Rebalance published sheet: https://docs.google.com/spreadsheets/d/1xb8xoW91HWeXBW8Zd99TobULSgwxcvfPaaYPlMLZmHI
- Column A: Worst Rank Held (exit candidates)  
- Column B: Current Portfolio (held stocks)

## Notes
- `calculations.py` aur `data_service.py` pehle se existing files hain
- data_service.py ke Upstox/Angel One ke liye token setup required hai
- YFinance by default kaam karta hai bina token ke

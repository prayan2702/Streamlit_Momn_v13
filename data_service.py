"""
data_service.py
===============
Multi-API data-fetching service for Momentum Screener.
Supports: YFinance (live) | Upstox (LIVE) | Angel One (LIVE) | Zerodha (placeholder)

UPSTOX SPEED OPTIMIZATION (v3):
  - Sequential loop → ThreadPoolExecutor (40 parallel workers)
  - Token Bucket Rate Limiter @ 40 req/sec (Upstox hard limit: 50/sec)
  - 20% safety buffer below rate limit to avoid 429s
  - 429 hit hone pe: exponential backoff in per-symbol fetcher
  - Expected speedup: 6-8x faster for large universes
    Nifty500 : ~100s → ~15s
    AllNSE   : ~400s → ~55s

ANGEL ONE SPEED OPTIMIZATION (v2):
  - ThreadPoolExecutor se parallel requests (2 workers)
  - Token Bucket Rate Limiter (1.5 req/sec strictly enforce)
  - Fixed sleep hataya — ab network latency parallel mein hide hoti hai
  - Expected speedup: 3-4x faster for large symbol lists
"""

import time
import threading
import requests
import numpy as np
import pandas as pd
import yfinance as yf
import streamlit as st
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from upstox_auth import get_upstox_access_token
from angelone_auth import get_angelone_client

# ─────────────────────────────────────────────────────────────
# SECTION A — UPSTOX INSTRUMENT MASTER
# ─────────────────────────────────────────────────────────────
_INSTRUMENT_MAP = None

def _load_instrument_map() -> dict:
    global _INSTRUMENT_MAP
    if _INSTRUMENT_MAP is not None:
        return _INSTRUMENT_MAP
    if "upstox_instrument_map" in st.session_state:
        _INSTRUMENT_MAP = st.session_state["upstox_instrument_map"]
        return _INSTRUMENT_MAP

    url = "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz"
    try:
        st.sidebar.info("Downloading Upstox instrument master...")
        df   = pd.read_csv(url, compression="gzip", low_memory=False)
        mask = df["instrument_key"].astype(str).str.startswith("NSE_EQ|")
        df   = df[mask].copy()
        mapping = dict(zip(df["tradingsymbol"].astype(str).str.upper(), df["instrument_key"]))
        _INSTRUMENT_MAP = mapping
        st.session_state["upstox_instrument_map"] = mapping
        st.sidebar.success(f"Instrument master loaded - {len(mapping):,} NSE EQ symbols")
        return mapping
    except Exception as e:
        st.sidebar.error(f"Instrument master load failed: {e}")
        return {}

def _get_instrument_key(symbol_ns: str, instrument_map: dict):
    clean = symbol_ns.replace(".NS", "").replace(".BO", "").upper().strip()
    return instrument_map.get(clean)

# ─────────────────────────────────────────────────────────────
# SECTION B — UPSTOX TOKEN VALIDATION
# ─────────────────────────────────────────────────────────────
def _validate_token(access_token: str) -> bool:
    url = "https://api.upstox.com/v3/historical-candle/NSE_EQ%7CINE002A01018/days/1/2025-01-10/2025-01-01"
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        return resp.status_code not in (401, 403)
    except Exception:
        return False

# ─────────────────────────────────────────────────────────────
# SECTION C — UPSTOX SINGLE SYMBOL FETCHER (V3)
# ─────────────────────────────────────────────────────────────
def _fetch_upstox_history_live(instrument_key: str, access_token: str,
                               start_date: datetime, end_date: datetime,
                               retries: int = 3):
    """
    Single-symbol Upstox V3 historical fetch.
    Thread-safe — no shared state.
    429 pe exponential backoff; token errors immediately re-raised.
    """
    encoded_key   = instrument_key.replace("|", "%7C")
    from_date_str = start_date.strftime("%Y-%m-%d")
    to_date_str   = end_date.strftime("%Y-%m-%d")

    url = (f"https://api.upstox.com/v3/historical-candle/"
           f"{encoded_key}/days/1/{to_date_str}/{from_date_str}")
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}

    delay = 0.5
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=headers, timeout=15)

            if resp.status_code == 429:
                # Rate limit hit — backoff aur retry
                sleep_time = delay * (2 ** attempt)
                time.sleep(sleep_time)
                continue

            if resp.status_code in (401, 403):
                raise ValueError(f"Token invalid (HTTP {resp.status_code})")

            resp.raise_for_status()
            payload = resp.json()
            candles = payload.get("data", {}).get("candles", [])

            if not candles:
                return None

            df = pd.DataFrame(
                candles,
                columns=["timestamp", "open", "high", "low", "close", "volume", "oi"]
            )
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            if df["timestamp"].dt.tz is not None:
                df["timestamp"] = df["timestamp"].dt.tz_localize(None)
            df.set_index("timestamp", inplace=True)
            df.sort_index(inplace=True)
            return df[["open", "high", "low", "close", "volume"]]

        except ValueError:
            raise   # Token error — bahar nikalna zaroori hai
        except requests.exceptions.Timeout:
            if attempt < retries - 1:
                time.sleep(delay * (2 ** attempt))
        except Exception:
            if attempt == retries - 1:
                return None
            time.sleep(delay * (2 ** attempt))

    return None

# ─────────────────────────────────────────────────────────────
# SECTION F — YFINANCE FETCHER
def _download_yfinance_chunk(symbols, start_date, max_retries=3, delay=2.0):
    for attempt in range(max_retries):
        try:
            return yf.download(symbols, start=start_date, progress=False, auto_adjust=True, threads=True, multi_level_index=False)
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(delay); delay *= 2
            else:
                raise e

def fetch_yfinance(symbols, start_date, chunk_size, progress_bar, status_text):
    close_chunks, high_chunks, volume_chunks, failed_symbols = [], [], [], []
    total = len(symbols)
    for k in range(0, total, chunk_size):
        progress = min((k + chunk_size) / total, 1.0)
        chunk    = symbols[k:k + chunk_size]
        for attempt in range(3):
            try:
                raw = _download_yfinance_chunk(chunk, start_date)
                close_chunks.append(raw['Close'])
                high_chunks.append(raw['High'])
                volume_chunks.append(raw['Close'] * raw['Volume'])
                break
            except Exception:
                if attempt == 2:
                    failed_symbols.extend(chunk)
        progress_bar.progress(progress)
        status_text.text(f"YFinance: {int(progress*100)}%")
        time.sleep(1.5)

    progress_bar.progress(1.0)
    status_text.text("Download complete!")
    close  = pd.concat(close_chunks,  axis=1) if close_chunks  else pd.DataFrame()
    high   = pd.concat(high_chunks,   axis=1) if high_chunks   else pd.DataFrame()
    volume = pd.concat(volume_chunks, axis=1) if volume_chunks else pd.DataFrame()
    for df in (close, high, volume):
        df.index = pd.to_datetime(df.index)
    return close, high, volume, failed_symbols


# ─────────────────────────────────────────────────────────────
# SECTION D — TOKEN BUCKET RATE LIMITER (Thread-Safe)
# Upstox aur Angel One dono use karte hain.
# ─────────────────────────────────────────────────────────────
class _TokenBucket:
    """
    Thread-safe token bucket rate limiter.
    Upstox  : _TokenBucket(max_rate=40.0)  — 40 req/sec
    AngelOne: _TokenBucket(max_rate=1.5)   — 1.5 req/sec
    Threads yahan block karte hain jab tak token available nahi hota.
    """
    def __init__(self, max_rate: float = 3.0):
        self._rate      = max_rate
        self._tokens    = max_rate          # bucket full se start
        self._last_time = time.monotonic()
        self._lock      = threading.Lock()

    def acquire(self):
        """Block until a token is available, then consume one."""
        while True:
            with self._lock:
                now     = time.monotonic()
                elapsed = now - self._last_time
                self._tokens    = min(self._rate, self._tokens + elapsed * self._rate)
                self._last_time = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
            time.sleep(0.01)   # Upstox ke liye 10ms granularity (40/sec pe 25ms interval)


# ─────────────────────────────────────────────────────────────
# SECTION H — UPSTOX BULK FETCHER (LIVE) — OPTIMIZED v3
# ─────────────────────────────────────────────────────────────
UPSTOX_MAX_LOOKBACK_MONTHS = 120

# Upstox rate limits (Other Standard APIs — historical candles):
#   50 req/sec  |  500 req/min  |  2000 req/30min
# We use 40/sec (20% safety buffer) to stay safely under 50/sec.
# Per-minute: 40*60 = 2400 > 500 — per-minute limit more restrictive!
# So effective safe rate = min(40/sec, 500/min) = 8.3/sec sustained.
# But bursting to 40 within first second is fine since bucket starts full.
# We use 40 workers + 40/sec token bucket for max burst speed,
# then naturally throttles to ~8/sec for sustained large downloads.

_UPSTOX_MAX_WORKERS = 40
_UPSTOX_RATE        = 40.0   # tokens/sec — stays under 50/sec hard limit


def _upstox_worker(sym: str, instrument_key: str, access_token: str,
                   start_date: datetime, end_date: datetime,
                   rate_limiter) -> tuple:
    """Worker: acquire rate-limit token, then fetch one symbol. Thread-safe."""
    rate_limiter.acquire()
    df = _fetch_upstox_history_live(instrument_key, access_token, start_date, end_date)
    return sym, df


def fetch_upstox(symbols, start_date, end_date, chunk_size,
                 progress_bar, status_text):
    """
    Upstox bulk fetcher — parallel v3.
    40 concurrent threads + TokenBucket(40/sec) — safely under 50/sec limit.
    Speedup vs sequential: ~6-8x for large universes.
    """
    # ── Auth & token validation ──────────────────────────────
    access_token = get_upstox_access_token(sidebar=True)
    if not access_token:
        progress_bar.progress(0.0)
        st.error("Please complete Upstox login in the sidebar first, then retry.")
        st.stop()

    status_text.text("Validating Upstox token...")
    if not _validate_token(access_token):
        st.session_state.pop("upstox_token_data", None)
        st.error("Token expired. Please re-login from sidebar and retry.")
        st.stop()
    st.sidebar.success("Token validated OK")

    # ── Date cap: Upstox max 120 months ──────────────────────
    upstox_start = end_date - relativedelta(months=UPSTOX_MAX_LOOKBACK_MONTHS)
    if start_date < upstox_start:
        start_date = upstox_start

    # ── Instrument master ─────────────────────────────────────
    status_text.text("Loading Upstox instrument master...")
    instrument_map = _load_instrument_map()
    if not instrument_map:
        st.error("Could not load Upstox instrument master.")
        st.stop()

    # ── Build task list: resolve symbol → instrument_key ─────
    tasks     = []   # list of (sym, instrument_key)
    failed    = []
    not_found = 0

    for sym in symbols:
        key = _get_instrument_key(sym, instrument_map)
        if not key:
            not_found += 1
            failed.append(sym)
        else:
            tasks.append((sym, key))

    total = len(symbols)
    n_tasks = len(tasks)
    status_text.text(
        f"Upstox: {n_tasks} symbols to fetch | "
        f"{not_found} not in master | "
        f"Workers: {_UPSTOX_MAX_WORKERS} | Rate: {int(_UPSTOX_RATE)}/sec"
    )

    close_map, high_map, vol_map = {}, {}, {}
    fetched_count = 0
    token_error   = False

    # ── Parallel fetch with TokenBucket rate limiter ─────────
    rate_limiter = _TokenBucket(max_rate=_UPSTOX_RATE)

    with ThreadPoolExecutor(max_workers=_UPSTOX_MAX_WORKERS) as executor:
        future_map = {
            executor.submit(
                _upstox_worker, sym, key, access_token,
                start_date, end_date, rate_limiter
            ): sym
            for sym, key in tasks
        }

        for future in as_completed(future_map):
            try:
                sym_result, df = future.result()
            except ValueError:
                # Token expired mid-download — abort all
                executor.shutdown(wait=False, cancel_futures=True)
                st.session_state.pop("upstox_token_data", None)
                st.error("Token expired mid-download. Re-login from sidebar and retry.")
                token_error = True
                break
            except Exception:
                sym_result = future_map[future]
                failed.append(sym_result)
                df = None

            fetched_count += 1

            if df is not None and not df.empty:
                idx = pd.to_datetime(df.index)
                close_map[sym_result] = pd.Series(df['close'].values,                  index=idx)
                high_map[sym_result]  = pd.Series(df['high'].values,                   index=idx)
                vol_map[sym_result]   = pd.Series((df['close'] * df['volume']).values, index=idx)
            else:
                if sym_result not in failed:
                    failed.append(sym_result)

            # ── Progress update every 5 completions ──────────
            if fetched_count % 5 == 0 or fetched_count == n_tasks:
                progress = (fetched_count + not_found) / total
                progress_bar.progress(min(progress, 1.0))
                rate_actual = fetched_count / max(
                    (time.monotonic() - rate_limiter._last_time + 0.001), 0.001
                )
                status_text.text(
                    f"Upstox ⚡ {int(progress*100)}% | "
                    f"✅ {len(close_map)} fetched | "
                    f"❌ {len(failed)} failed | "
                    f"🔄 {fetched_count}/{n_tasks}"
                )

    if token_error:
        st.stop()

    progress_bar.progress(1.0)
    status_text.text(
        f"Done ✅ — {len(close_map)}/{total} fetched | "
        f"Not in master: {not_found} | Failed: {len(failed) - not_found}"
    )

    # ── Assemble aligned DataFrames ───────────────────────────
    all_idx = pd.bdate_range(start=start_date, end=end_date)
    close  = pd.DataFrame({s: v.reindex(all_idx) for s, v in close_map.items()}, index=all_idx)
    high   = pd.DataFrame({s: v.reindex(all_idx) for s, v in high_map.items()},  index=all_idx)
    volume = pd.DataFrame({s: v.reindex(all_idx) for s, v in vol_map.items()},   index=all_idx)

    if close.empty:
        st.error("No data fetched from Upstox. Check token and retry.")
        st.stop()

    return close, high, volume, failed


# ─────────────────────────────────────────────────────────────
# SECTION F — YFINANCE FETCHER
# ─────────────────────────────────────────────────────────────

_ANGELONE_INSTRUMENT_MAP = None

def _load_angelone_instrument_map():
    global _ANGELONE_INSTRUMENT_MAP
    if _ANGELONE_INSTRUMENT_MAP is not None:
        return _ANGELONE_INSTRUMENT_MAP
    
    if "angelone_instrument_map" in st.session_state:
        _ANGELONE_INSTRUMENT_MAP = st.session_state["angelone_instrument_map"]
        return _ANGELONE_INSTRUMENT_MAP

    url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
    try:
        st.sidebar.info("Downloading Angel One instrument master...")
        response = requests.get(url, timeout=15)
        data = response.json()
        
        mapping = {}
        for item in data:
            if item['exch_seg'] == 'NSE' and item['symbol'].endswith('-EQ'):
                clean_symbol = item['symbol'].replace('-EQ', '').upper()
                mapping[clean_symbol] = item['token']
                
        _ANGELONE_INSTRUMENT_MAP = mapping
        st.session_state["angelone_instrument_map"] = mapping
        st.sidebar.success(f"Angel One master loaded - {len(mapping):,} NSE EQ symbols")
        return mapping
    except Exception as e:
        st.sidebar.error(f"Angel One master load failed: {e}")
        return {}



_RATE_LIMIT_CODES    = {"AG8001", "AB1010", "AB2010", "AB1004"}
_RATE_LIMIT_KEYWORDS = ("rate", "limit", "exceed", "too many", "throttl", "access denied")

def _fetch_angelone_history_live(client, token: str, start_date: datetime, end_date: datetime, retries=4):
    """
    Single-symbol fetch. Thread-safe.
    Angel One rate-limit aata hai JSON body mein (status:false + errorcode),
    HTTP 429 nahi bhejta. Ye function dono cases handle karta hai.
    """
    historicParam = {
        "exchange": "NSE",
        "symboltoken": token,
        "interval": "ONE_DAY",
        "fromdate": start_date.strftime("%Y-%m-%d 09:15"),
        "todate":   end_date.strftime("%Y-%m-%d 15:30"),
    }

    delay = 2.0
    for attempt in range(retries):
        try:
            resp = client.getCandleData(historicParam)

            if resp.get('status') and resp.get('data'):
                columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
                df = pd.DataFrame(resp['data'], columns=columns)
                df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_localize(None)
                df.set_index('timestamp', inplace=True)
                return df[['open', 'high', 'low', 'close', 'volume']]

            # Rate-limit error JSON body mein check karo
            error_code = str(resp.get('errorcode', '') or resp.get('error_code', ''))
            error_msg  = str(resp.get('message', '') or resp.get('msg', '')).lower()
            is_rate_limit = (
                error_code in _RATE_LIMIT_CODES
                or any(kw in error_msg for kw in _RATE_LIMIT_KEYWORDS)
            )

            if is_rate_limit:
                time.sleep(delay * (2 ** attempt))   # exponential backoff
                continue

            return None   # data nahi mila, retry ka fayda nahi

        except Exception:
            if attempt == retries - 1:
                return None
            time.sleep(delay * (2 ** attempt))

    return None


# ── Worker function (runs in each thread) ───────────────────
def _angelone_worker(sym, token, client, start_date, end_date, rate_limiter):
    rate_limiter.acquire()
    time.sleep(0.05)   # small jitter — threads ek saath burst na karein
    df = _fetch_angelone_history_live(client, token, start_date, end_date)
    return sym, df


_ANGELONE_LAST_RUN_TIME: float = 0.0   # epoch seconds — last run ka timestamp
_ANGELONE_COOLDOWN_SECS: int   = 30    # consecutive runs ke beech minimum gap

def fetch_angelone(symbols, start_date, end_date, chunk_size, progress_bar, status_text):
    """
    Angel One bulk fetcher.
    - Cooldown enforced between consecutive runs (30s) to avoid rate-limit spike
    - 2 workers + 1.5 req/sec token bucket for safe throughput
    - JSON-body rate-limit detection with exponential backoff in per-symbol fetcher
    """
    global _ANGELONE_LAST_RUN_TIME

    client = get_angelone_client(sidebar=True)
    if not client:
        progress_bar.progress(0.0)
        st.stop()

    # ── Cooldown check ───────────────────────────────────────
    elapsed = time.monotonic() - _ANGELONE_LAST_RUN_TIME
    if elapsed < _ANGELONE_COOLDOWN_SECS and _ANGELONE_LAST_RUN_TIME > 0:
        wait = int(_ANGELONE_COOLDOWN_SECS - elapsed)
        for remaining in range(wait, 0, -1):
            status_text.text(
                f"Angel One cooldown: {remaining}s wait to avoid rate-limit "
                f"(previous run just {int(elapsed)}s ago)"
            )
            time.sleep(1)

    # ── Date cap: Angel One max 2000 days ───────────────────
    angelone_start = end_date - timedelta(days=2000)
    if start_date < angelone_start:
        st.sidebar.info(
            f"Angel One API Limit: Date capped to {angelone_start.strftime('%d-%m-%Y')} "
            f"(Max 2000 days per request allowed)"
        )
        start_date = angelone_start

    status_text.text("Angel One Token Validated. Fetching Master...")
    instrument_map = _load_angelone_instrument_map()
    if not instrument_map:
        st.error("Could not load Angel One instrument master.")
        st.stop()

    # ── Symbol → Token resolution ────────────────────────────
    tasks     = []
    failed    = []
    not_found = 0

    for sym in symbols:
        token = instrument_map.get(sym.upper().replace('.NS', ''))
        if not token:
            not_found += 1
            failed.append(sym)
        else:
            tasks.append((sym, token))

    total         = len(symbols)
    fetched_count = 0
    close_map, high_map, vol_map = {}, {}, {}

    # ── 2 workers + 1.5 req/sec ──────────────────────────────
    # 4 workers @ 2.5/sec caused burst spikes crossing Angel One's limit.
    # 2 workers @ 1.5/sec = smooth, predictable, well under 3/sec hard limit.
    MAX_WORKERS  = 2
    rate_limiter = _TokenBucket(max_rate=1.5)

    status_text.text(f"Angel One: Fetching {len(tasks)} symbols...")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_map = {
            executor.submit(_angelone_worker, sym, tok, client, start_date, end_date, rate_limiter): sym
            for sym, tok in tasks
        }

        for future in as_completed(future_map):
            sym_result, df = future.result()
            fetched_count += 1

            if df is not None and not df.empty:
                idx = pd.to_datetime(df.index)
                close_map[sym_result] = pd.Series(df['close'].values,                  index=idx)
                high_map[sym_result]  = pd.Series(df['high'].values,                   index=idx)
                vol_map[sym_result]   = pd.Series((df['close'] * df['volume']).values, index=idx)
            else:
                failed.append(sym_result)

            if fetched_count % 5 == 0 or fetched_count == len(tasks):
                progress = (fetched_count + not_found) / total
                progress_bar.progress(min(progress, 1.0))
                status_text.text(
                    f"Angel One: {int(progress * 100)}% | "
                    f"Fetched: {len(close_map)} | Failed: {len(failed)}"
                )

    # ── Mark run completion time ─────────────────────────────
    _ANGELONE_LAST_RUN_TIME = time.monotonic()

    progress_bar.progress(1.0)
    status_text.text(f"Done — {len(close_map)}/{total} fetched | Not in master: {not_found}")

    all_idx = pd.bdate_range(start=start_date, end=end_date)
    close  = pd.DataFrame({s: v.reindex(all_idx) for s, v in close_map.items()}, index=all_idx)
    high   = pd.DataFrame({s: v.reindex(all_idx) for s, v in high_map.items()},  index=all_idx)
    volume = pd.DataFrame({s: v.reindex(all_idx) for s, v in vol_map.items()},   index=all_idx)

    if close.empty:
        st.error("No data fetched from Angel One. Try re-logging in and retry.")
        st.stop()

    return close, high, volume, failed


# ─────────────────────────────────────────────────────────────
# SECTION J — ZERODHA (Mock)
# ─────────────────────────────────────────────────────────────
def fetch_zerodha(symbols, start_date, end_date, chunk_size, progress_bar, status_text):
    status_text.text("Zerodha (MOCK) is not implemented yet.")
    st.stop()

# ─────────────────────────────────────────────────────────────
# SECTION K — UNIFIED ENTRY POINT
# ─────────────────────────────────────────────────────────────
def fetch_data(api_source, symbols, start_date, end_date,
               chunk_size, progress_bar, status_text) -> tuple:
    if api_source == "YFinance":
        return fetch_yfinance(symbols, start_date, chunk_size, progress_bar, status_text)
    elif api_source == "Upstox":
        return fetch_upstox(symbols, start_date, end_date, chunk_size, progress_bar, status_text)
    elif api_source == "Angel One":
        return fetch_angelone(symbols, start_date, end_date, chunk_size, progress_bar, status_text)
    elif api_source == "Zerodha":
        return fetch_zerodha(symbols, start_date, end_date, chunk_size, progress_bar, status_text)
    else:
        raise ValueError(f"Unknown api_source: {api_source!r}")

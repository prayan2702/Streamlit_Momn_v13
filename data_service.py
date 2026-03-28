"""
data_service.py
===============
Multi-API data-fetching service for Momentum Screener.
Supports: YFinance (live) | Upstox (LIVE) | Angel One (LIVE) | Zerodha (placeholder)

UPSTOX SPEED OPTIMIZATION (v4 — MultiWindowRateLimiter):
  - Sequential loop → 20 parallel ThreadPoolExecutor workers
  - _MultiWindowRateLimiter respects ALL 3 Upstox limits simultaneously:
      Window A:    1 sec  →  45 req  (hard limit: 50)
      Window B:   60 sec  → 450 req  (hard limit: 500)
      Window C: 1800 sec  →1800 req  (hard limit: 2000)
  - Auto-adapts speed to universe size — no manual tuning needed:
      Nifty50   : bursts at ~45/sec → done in ~2 sec
      Nifty500  : ~7-8/sec sustained → ~65 sec
      AllNSE    : ~7/sec first 450 symbols, auto-slows to ~1/sec after
  - Live rate display: req/sec · req/min · req/30min in status bar
  - ETA estimate shown before start + updated every 10 fetches

ANGEL ONE SPEED OPTIMIZATION (v2):
  - 2 workers + TokenBucket(1.5/sec) — safely under 3/sec Angel One limit
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
# SECTION D — RATE LIMITERS (Thread-Safe)
# ─────────────────────────────────────────────────────────────

class _TokenBucket:
    """
    Simple token-bucket rate limiter.
    Angel One: _TokenBucket(max_rate=1.5)
    """
    def __init__(self, max_rate: float = 3.0):
        self._rate      = max_rate
        self._tokens    = max_rate
        self._last_time = time.monotonic()
        self._lock      = threading.Lock()

    def acquire(self):
        while True:
            with self._lock:
                now     = time.monotonic()
                elapsed = now - self._last_time
                self._tokens    = min(self._rate, self._tokens + elapsed * self._rate)
                self._last_time = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
            time.sleep(0.05)


class _MultiWindowRateLimiter:
    """
    Thread-safe multi-window rate limiter for Upstox.

    Upstox imposes THREE simultaneous limits on historical candle API:
        Window A :   1 second  →   50 requests  (hard burst cap)
        Window B :  60 seconds →  500 requests  (per-minute cap)
        Window C : 1800 seconds→ 2000 requests  (per-30-min cap)

    We apply 10% safety buffers:
        Window A :  45 req /  1 sec
        Window B : 450 req / 60 sec
        Window C :1800 req / 1800 sec

    Logic: maintain a deque of timestamps of past requests.
    Before each new request, count how many timestamps fall inside
    each window. If ANY window is full → block (sleep 50ms, retry).

    This means:
      - Small universes (Nifty50/100): bursts at ~45/sec ✅
      - Medium (Nifty500): ~7-8/sec sustained ✅
      - Large (AllNSE 2100+): auto-slows to ~1 req/sec after
        first 1800 requests, respecting 30-min cap ✅
    """

    # (window_seconds, max_requests_with_buffer)
    WINDOWS = [
        (1,    45),    # 50/sec  → 45 safe
        (60,   450),   # 500/min → 450 safe
        (1800, 1800),  # 2000/30min → 1800 safe
    ]

    def __init__(self):
        from collections import deque
        self._timestamps = deque()   # monotonic times of completed acquires
        self._lock       = threading.Lock()

    def acquire(self):
        """Block until ALL three windows have capacity, then record request."""
        from collections import deque
        while True:
            with self._lock:
                now = time.monotonic()
                # Prune timestamps older than the largest window (1800s)
                cutoff = now - self.WINDOWS[-1][0]
                while self._timestamps and self._timestamps[0] < cutoff:
                    self._timestamps.popleft()

                # Check every window
                can_proceed = True
                for window_sec, max_req in self.WINDOWS:
                    window_cutoff = now - window_sec
                    count = sum(1 for t in self._timestamps if t >= window_cutoff)
                    if count >= max_req:
                        can_proceed = False
                        break

                if can_proceed:
                    self._timestamps.append(now)
                    return

            time.sleep(0.05)   # 50ms poll interval

    def current_rates(self):
        """Returns (req/sec, req/min, req/30min) for status display."""
        from collections import deque
        now = time.monotonic()
        with self._lock:
            r1  = sum(1 for t in self._timestamps if t >= now - 1)
            r60 = sum(1 for t in self._timestamps if t >= now - 60)
            r30m= len(self._timestamps)
        return r1, r60, r30m


# ─────────────────────────────────────────────────────────────
# SECTION H — UPSTOX BULK FETCHER (LIVE) — OPTIMIZED v4
# ─────────────────────────────────────────────────────────────
UPSTOX_MAX_LOOKBACK_MONTHS = 120

# Workers: 20 concurrent threads.
# Rate is controlled purely by _MultiWindowRateLimiter — workers just
# block at acquire() until a slot is available in all three windows.
# 20 workers >> max throughput (45/sec) so the limiter is always
# the bottleneck, never worker starvation.
_UPSTOX_MAX_WORKERS = 20


def _upstox_worker(sym: str, instrument_key: str, access_token: str,
                   start_date: datetime, end_date: datetime,
                   rate_limiter: _MultiWindowRateLimiter) -> tuple:
    """Worker: block on rate limiter, then fetch. Fully thread-safe."""
    rate_limiter.acquire()
    df = _fetch_upstox_history_live(instrument_key, access_token,
                                    start_date, end_date)
    return sym, df


def fetch_upstox(symbols, start_date, end_date, chunk_size,
                 progress_bar, status_text):
    """
    Upstox bulk fetcher — parallel v4 with MultiWindowRateLimiter.

    Respects ALL three Upstox API limits simultaneously:
      50/sec | 500/min | 2000/30min  (10% buffers applied)

    Speed auto-adapts to universe size:
      Nifty50   →  ~45/sec burst   → ~2 sec total
      Nifty500  →  ~7-8/sec        → ~65 sec total
      AllNSE    →  ~7-8/sec first 450, then ~1/sec → ~35 min total
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

    # ── Date cap ─────────────────────────────────────────────
    upstox_start = end_date - relativedelta(months=UPSTOX_MAX_LOOKBACK_MONTHS)
    if start_date < upstox_start:
        start_date = upstox_start

    # ── Instrument master ─────────────────────────────────────
    status_text.text("Loading Upstox instrument master...")
    instrument_map = _load_instrument_map()
    if not instrument_map:
        st.error("Could not load Upstox instrument master.")
        st.stop()

    # ── Symbol → instrument_key resolution ───────────────────
    tasks     = []
    failed    = []
    not_found = 0
    for sym in symbols:
        key = _get_instrument_key(sym, instrument_map)
        if not key:
            not_found += 1
            failed.append(sym)
        else:
            tasks.append((sym, key))

    total   = len(symbols)
    n_tasks = len(tasks)

    # ── ETA estimate for user ─────────────────────────────────
    # First 450 requests: ~7/sec.  Remaining: ~1/sec (30-min window)
    if n_tasks <= 450:
        eta_sec = max(n_tasks / 7, 1)
    else:
        eta_sec = (450 / 7) + (n_tasks - 450) * 1.0
    eta_min = eta_sec / 60

    st.sidebar.info(
        f"⏱ Upstox: {n_tasks} symbols | "
        f"ETA ~{eta_min:.1f} min\n"
        f"Limits: 45/sec · 450/min · 1800/30min"
    )
    status_text.text(
        f"Upstox: {n_tasks} symbols | Not in master: {not_found} | "
        f"ETA ~{eta_min:.1f} min"
    )

    close_map, high_map, vol_map = {}, {}, {}
    fetched_count = 0
    token_error   = False
    _t_start      = time.monotonic()

    # ── Parallel fetch — MultiWindowRateLimiter ───────────────
    rate_limiter = _MultiWindowRateLimiter()

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
                # Token expired — abort immediately
                executor.shutdown(wait=False, cancel_futures=True)
                st.session_state.pop("upstox_token_data", None)
                st.error("Token expired mid-download. Re-login from sidebar and retry.")
                token_error = True
                break
            except Exception:
                sym_result = future_map[future]
                if sym_result not in failed:
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

            # ── Progress + live rate display every 10 fetches ─
            if fetched_count % 10 == 0 or fetched_count == n_tasks:
                progress      = (fetched_count + not_found) / total
                elapsed       = time.monotonic() - _t_start
                r1, r60, r30m = rate_limiter.current_rates()
                remaining     = n_tasks - fetched_count
                eta_remaining = (remaining / max(r60 / 60, 0.1)) if r60 > 0 else 0

                progress_bar.progress(min(progress, 1.0))
                status_text.text(
                    f"Upstox ⚡ {int(progress*100)}%  |  "
                    f"✅ {len(close_map)}  ❌ {len(failed) - not_found}  "
                    f"🔄 {fetched_count}/{n_tasks}  |  "
                    f"Rate: {r1}/s · {r60}/min · {r30m}/30min  |  "
                    f"ETA: {eta_remaining/60:.1f}min"
                )

    if token_error:
        st.stop()

    progress_bar.progress(1.0)
    status_text.text(
        f"Done ✅ — {len(close_map)}/{total} fetched | "
        f"Not in master: {not_found} | "
        f"Failed: {len(failed) - not_found} | "
        f"Time: {(time.monotonic()-_t_start)/60:.1f}min"
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

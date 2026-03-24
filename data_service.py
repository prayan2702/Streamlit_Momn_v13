"""
core/data_service.py
====================
Multi-API data-fetching service — desktop version.
Identical logic to the original; all st.* calls replaced:
  - st.error() + st.stop()        → raise RuntimeError(msg)
  - st.session_state              → module-level globals (already existed)
  - st.sidebar.info/success/error → log_cb(msg) callback
  - progress_bar.progress(x)      → progress_cb(x)  where x is float 0.0–1.0
  - status_text.text(x)           → status_cb(x)    where x is str

Caller passes progress_cb and status_cb when calling fetch_data().
"""

import time
import threading
import requests
import numpy as np
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from core.upstox_auth  import get_cached_token
from core.angelone_auth import get_angelone_client

# ─────────────────────────────────────────────────────────────
# SECTION A — UPSTOX INSTRUMENT MASTER
# ─────────────────────────────────────────────────────────────
_INSTRUMENT_MAP = None

def _load_instrument_map(log_cb=print) -> dict:
    global _INSTRUMENT_MAP
    if _INSTRUMENT_MAP is not None:
        return _INSTRUMENT_MAP

    url = "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz"
    try:
        log_cb("Downloading Upstox instrument master...")
        df   = pd.read_csv(url, compression="gzip", low_memory=False)
        mask = df["instrument_key"].astype(str).str.startswith("NSE_EQ|")
        df   = df[mask].copy()
        mapping = dict(zip(
            df["tradingsymbol"].astype(str).str.upper(),
            df["instrument_key"]
        ))
        _INSTRUMENT_MAP = mapping
        log_cb(f"Upstox instrument master loaded — {len(mapping):,} NSE EQ symbols")
        return mapping
    except Exception as e:
        log_cb(f"Instrument master load failed: {e}")
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
                                retries: int = 2):
    encoded_key   = instrument_key.replace("|", "%7C")
    from_date_str = start_date.strftime("%Y-%m-%d")
    to_date_str   = end_date.strftime("%Y-%m-%d")
    url = (f"https://api.upstox.com/v3/historical-candle/"
           f"{encoded_key}/days/1/{to_date_str}/{from_date_str}")
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}

    delay = 1.0
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            if resp.status_code == 429:
                time.sleep(delay * 2); delay *= 2; continue
            if resp.status_code in (401, 403):
                raise ValueError(f"Token invalid (HTTP {resp.status_code})")
            resp.raise_for_status()
            candles = resp.json().get("data", {}).get("candles", [])
            if not candles:
                return None
            df = pd.DataFrame(candles, columns=["timestamp","open","high","low","close","volume","oi"])
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            if df["timestamp"].dt.tz is not None:
                df["timestamp"] = df["timestamp"].dt.tz_localize(None)
            df.set_index("timestamp", inplace=True)
            df.sort_index(inplace=True)
            return df[["open","high","low","close","volume"]]
        except ValueError:
            raise
        except requests.exceptions.Timeout:
            if attempt < retries - 1:
                time.sleep(delay); delay *= 2
        except Exception:
            if attempt == retries - 1:
                return None
            time.sleep(delay); delay *= 2
    return None


# ─────────────────────────────────────────────────────────────
# SECTION F — YFINANCE FETCHER
# ─────────────────────────────────────────────────────────────
def _download_yfinance_chunk(symbols, start_date, max_retries=3, delay=2.0):
    for attempt in range(max_retries):
        try:
            return yf.download(symbols, start=start_date, progress=False,
                               auto_adjust=True, threads=True, multi_level_index=False)
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(delay); delay *= 2
            else:
                raise e


def fetch_yfinance(symbols, start_date, chunk_size, progress_cb, status_cb):
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
        progress_cb(progress)
        status_cb(f"YFinance: {int(progress * 100)}%")
        time.sleep(1.5)

    progress_cb(1.0)
    status_cb("Download complete!")
    close  = pd.concat(close_chunks,  axis=1) if close_chunks  else pd.DataFrame()
    high   = pd.concat(high_chunks,   axis=1) if high_chunks   else pd.DataFrame()
    volume = pd.concat(volume_chunks, axis=1) if volume_chunks else pd.DataFrame()
    for df in (close, high, volume):
        df.index = pd.to_datetime(df.index)
    return close, high, volume, failed_symbols


# ─────────────────────────────────────────────────────────────
# SECTION H — UPSTOX BULK FETCHER (LIVE)
# ─────────────────────────────────────────────────────────────
UPSTOX_MAX_LOOKBACK_MONTHS = 120

def fetch_upstox(symbols, start_date, end_date, chunk_size, progress_cb, status_cb,
                 access_token: str = None, log_cb=print):
    if access_token is None:
        access_token = get_cached_token()
    if not access_token:
        raise RuntimeError("Upstox: No valid token. Please authenticate first via Settings → Upstox Login.")

    status_cb("Validating Upstox token...")
    if not _validate_token(access_token):
        raise RuntimeError("Upstox token expired. Please re-authenticate via Settings → Upstox Login.")
    log_cb("Upstox token validated OK")

    upstox_start = end_date - relativedelta(months=UPSTOX_MAX_LOOKBACK_MONTHS)
    if start_date < upstox_start:
        start_date = upstox_start

    instrument_map = _load_instrument_map(log_cb)
    if not instrument_map:
        raise RuntimeError("Could not load Upstox instrument master.")

    close_map, high_map, vol_map = {}, {}, {}
    failed, not_found = [], 0
    total = len(symbols)

    for i, sym in enumerate(symbols):
        progress        = (i + 1) / total
        instrument_key  = _get_instrument_key(sym, instrument_map)
        if not instrument_key:
            not_found += 1
            failed.append(sym)
        else:
            try:
                df = _fetch_upstox_history_live(instrument_key, access_token, start_date, end_date)
                if df is not None and not df.empty:
                    idx = pd.to_datetime(df.index)
                    close_map[sym] = pd.Series(df['close'].values,                  index=idx)
                    high_map[sym]  = pd.Series(df['high'].values,                   index=idx)
                    vol_map[sym]   = pd.Series((df['close'] * df['volume']).values, index=idx)
                else:
                    failed.append(sym)
            except ValueError as e:
                raise RuntimeError(f"Upstox token expired mid-download: {e}")
            except Exception:
                failed.append(sym)

        if i % 10 == 0 or i == total - 1:
            progress_cb(progress)
            status_cb(f"Upstox: {int(progress*100)}% | Fetched: {len(close_map)} | Failed: {len(failed)}")
        time.sleep(0.05)

    progress_cb(1.0)
    status_cb(f"Done — {len(close_map)}/{total} fetched | Not in master: {not_found}")

    all_idx = pd.bdate_range(start=start_date, end=end_date)
    close  = pd.DataFrame({s: v.reindex(all_idx) for s, v in close_map.items()},  index=all_idx)
    high   = pd.DataFrame({s: v.reindex(all_idx) for s, v in high_map.items()},   index=all_idx)
    volume = pd.DataFrame({s: v.reindex(all_idx) for s, v in vol_map.items()},    index=all_idx)
    return close, high, volume, failed


# ─────────────────────────────────────────────────────────────
# SECTION I — ANGEL ONE INSTRUMENT MASTER
# ─────────────────────────────────────────────────────────────
_ANGELONE_INSTRUMENT_MAP = None

def _load_angelone_instrument_map(log_cb=print) -> dict:
    global _ANGELONE_INSTRUMENT_MAP
    if _ANGELONE_INSTRUMENT_MAP is not None:
        return _ANGELONE_INSTRUMENT_MAP

    url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
    try:
        log_cb("Downloading Angel One instrument master...")
        response = requests.get(url, timeout=15)
        data     = response.json()
        mapping  = {}
        for item in data:
            if item['exch_seg'] == 'NSE' and item['symbol'].endswith('-EQ'):
                clean_symbol = item['symbol'].replace('-EQ', '').upper()
                mapping[clean_symbol] = item['token']
        _ANGELONE_INSTRUMENT_MAP = mapping
        log_cb(f"Angel One master loaded — {len(mapping):,} NSE EQ symbols")
        return mapping
    except Exception as e:
        log_cb(f"Angel One master load failed: {e}")
        return {}


# ─────────────────────────────────────────────────────────────
# SECTION I.2 — ANGEL ONE TOKEN BUCKET
# ─────────────────────────────────────────────────────────────
class _TokenBucket:
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


_RATE_LIMIT_CODES    = {"AG8001", "AB1010", "AB2010", "AB1004"}
_RATE_LIMIT_KEYWORDS = ("rate", "limit", "exceed", "too many", "throttl", "access denied")


def _fetch_angelone_history_live(client, token: str, start_date: datetime,
                                  end_date: datetime, retries=4):
    historicParam = {
        "exchange":    "NSE",
        "symboltoken": token,
        "interval":    "ONE_DAY",
        "fromdate":    start_date.strftime("%Y-%m-%d 09:15"),
        "todate":      end_date.strftime("%Y-%m-%d 15:30"),
    }
    delay = 2.0
    for attempt in range(retries):
        try:
            resp = client.getCandleData(historicParam)
            if resp.get('status') and resp.get('data'):
                columns = ['timestamp','open','high','low','close','volume']
                df = pd.DataFrame(resp['data'], columns=columns)
                df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_localize(None)
                df.set_index('timestamp', inplace=True)
                return df[['open','high','low','close','volume']]
            error_code = str(resp.get('errorcode', '') or resp.get('error_code', ''))
            error_msg  = str(resp.get('message', '')  or resp.get('msg', '')).lower()
            is_rate_limit = (
                error_code in _RATE_LIMIT_CODES
                or any(kw in error_msg for kw in _RATE_LIMIT_KEYWORDS)
            )
            if is_rate_limit:
                time.sleep(delay * (2 ** attempt)); continue
            return None
        except Exception:
            if attempt == retries - 1:
                return None
            time.sleep(delay * (2 ** attempt))
    return None


def _angelone_worker(sym, token, client, start_date, end_date, rate_limiter):
    rate_limiter.acquire()
    time.sleep(0.05)
    df = _fetch_angelone_history_live(client, token, start_date, end_date)
    return sym, df


_ANGELONE_LAST_RUN_TIME: float = 0.0
_ANGELONE_COOLDOWN_SECS: int   = 30


def fetch_angelone(symbols, start_date, end_date, chunk_size, progress_cb, status_cb,
                   client=None, log_cb=print):
    global _ANGELONE_LAST_RUN_TIME

    if client is None:
        client = get_angelone_client()
    if not client:
        raise RuntimeError("Angel One: Not authenticated. Please check config.toml [angelone] credentials.")

    # Cooldown check
    elapsed = time.monotonic() - _ANGELONE_LAST_RUN_TIME
    if elapsed < _ANGELONE_COOLDOWN_SECS and _ANGELONE_LAST_RUN_TIME > 0:
        wait = int(_ANGELONE_COOLDOWN_SECS - elapsed)
        for remaining in range(wait, 0, -1):
            status_cb(
                f"Angel One cooldown: {remaining}s wait to avoid rate-limit "
                f"(previous run {int(elapsed)}s ago)"
            )
            time.sleep(1)

    angelone_start = end_date - timedelta(days=2000)
    if start_date < angelone_start:
        log_cb(f"Angel One date capped to {angelone_start.strftime('%d-%m-%Y')} (max 2000 days)")
        start_date = angelone_start

    status_cb("Angel One: Loading instrument master...")
    instrument_map = _load_angelone_instrument_map(log_cb)
    if not instrument_map:
        raise RuntimeError("Could not load Angel One instrument master.")

    tasks, failed, not_found = [], [], 0
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

    MAX_WORKERS  = 2
    rate_limiter = _TokenBucket(max_rate=1.5)
    status_cb(f"Angel One: Fetching {len(tasks)} symbols...")

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
                progress_cb(min(progress, 1.0))
                status_cb(
                    f"Angel One: {int(progress * 100)}% | "
                    f"Fetched: {len(close_map)} | Failed: {len(failed)}"
                )

    _ANGELONE_LAST_RUN_TIME = time.monotonic()
    progress_cb(1.0)
    status_cb(f"Done — {len(close_map)}/{total} | Not in master: {not_found}")

    if not close_map:
        raise RuntimeError("No data fetched from Angel One. Check credentials and retry.")

    all_idx = pd.bdate_range(start=start_date, end=end_date)
    close  = pd.DataFrame({s: v.reindex(all_idx) for s, v in close_map.items()}, index=all_idx)
    high   = pd.DataFrame({s: v.reindex(all_idx) for s, v in high_map.items()},  index=all_idx)
    volume = pd.DataFrame({s: v.reindex(all_idx) for s, v in vol_map.items()},   index=all_idx)
    return close, high, volume, failed


# ─────────────────────────────────────────────────────────────
# SECTION J — ZERODHA (Mock)
# ─────────────────────────────────────────────────────────────
def fetch_zerodha(symbols, start_date, end_date, chunk_size, progress_cb, status_cb, **kw):
    raise RuntimeError("Zerodha is not implemented yet.")


# ─────────────────────────────────────────────────────────────
# SECTION K — UNIFIED ENTRY POINT
# ─────────────────────────────────────────────────────────────
def fetch_data(api_source: str, symbols, start_date, end_date,
               chunk_size: int, progress_cb, status_cb, **kwargs) -> tuple:
    """
    Unified entry point — identical interface to original.
    kwargs may include: access_token, client, log_cb
    """
    if api_source == "YFinance":
        return fetch_yfinance(symbols, start_date, chunk_size, progress_cb, status_cb)
    elif api_source == "Upstox":
        return fetch_upstox(symbols, start_date, end_date, chunk_size,
                            progress_cb, status_cb, **kwargs)
    elif api_source == "Angel One":
        return fetch_angelone(symbols, start_date, end_date, chunk_size,
                              progress_cb, status_cb, **kwargs)
    elif api_source == "Zerodha":
        return fetch_zerodha(symbols, start_date, end_date, chunk_size,
                             progress_cb, status_cb, **kwargs)
    else:
        raise ValueError(f"Unknown api_source: {api_source!r}")

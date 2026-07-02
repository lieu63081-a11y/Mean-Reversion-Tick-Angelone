#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
═══════════════════════════════════════════════════════════════════════════════
  ANGEL ONE TICK RECORDER — SELF-CONTAINED SINGLE FILE
═══════════════════════════════════════════════════════════════════════════════

Streams real-time NSE cash-market ticks (LTP + bid/ask depth + volume)
from Angel One SmartWebSocketV2 into compressed Parquet files
(one per date × ticker). No external files needed — just this script.

USAGE:
    # Set 4 environment variables (see README.md for how to get them)
    export ANGEL_API_KEY='...'
    export ANGEL_CLIENT_CODE='A123456'
    export ANGEL_PIN='1234'
    export ANGEL_TOTP_SECRET='JBSWY3DPEHPK3PXP'

    # Record full Nifty 50 in SNAP_QUOTE mode (default)
    python3 tick_recorder.py

    # Record specific tickers only
    python3 tick_recorder.py --tickers TCS.NS INFY.NS EICHERMOT.NS

    # LTP-only mode (smaller files, no bid/ask)
    python3 tick_recorder.py --mode ltp --tickers TCS.NS

    # CSV instead of Parquet (human-readable, larger)
    python3 tick_recorder.py --format csv --tickers TCS.NS

    # Custom output directory
    python3 tick_recorder.py --out /data/ticks

FILE LAYOUT:
    tick_data/
    └── 2026-07-02/
        ├── TCS.parquet          (all ticks for TCS on this date)
        ├── INFY.parquet
        └── _metadata.json       (recorder stats: ticks, gaps, reconnects)

STOPPING:
    Press Ctrl+C — pending ticks are flushed, session terminated cleanly.

DEPENDENCIES:
    pip install smartapi-python pyotp websocket-client pandas pyarrow

═══════════════════════════════════════════════════════════════════════════════
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional

# Suppress noisy loggers
logging.getLogger("SmartApi").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)


# ═══════════════════════════════════════════════════════════════════════ #
#                              CONFIG                                      #
# ═══════════════════════════════════════════════════════════════════════ #

IST = timezone(timedelta(hours=5, minutes=30))


class RecorderConfig:
    OUTPUT_DIR      = "tick_data"     # base dir for stored files
    FORMAT          = "parquet"       # 'parquet' | 'csv'
    MODE            = "snap"          # 'ltp' | 'quote' | 'snap'
    FLUSH_EVERY_S   = 5.0             # write to disk every N seconds
    FLUSH_MAX_TICKS = 500             # or when buffer hits this many ticks
    HEARTBEAT_TIMEOUT_S = 30          # warn if no message for this long
    ROTATE_ON_NEW_DAY = True          # start a fresh file each new IST date
    LOG_LEVEL       = logging.INFO


logging.basicConfig(
    level=RecorderConfig.LOG_LEVEL,
    format="%(asctime)s  %(levelname)-5s  %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("tick_recorder.log"),
    ],
)
log = logging.getLogger("recorder")


# ═══════════════════════════════════════════════════════════════════════ #
#                       NIFTY 50 SYMBOL TOKENS                             #
# ═══════════════════════════════════════════════════════════════════════ #
# yfinance-style ticker → Angel One numeric symbol_token.
# Verified against Angel One master (2025). If a token is invalid,
# uncomment fetch_instrument_master() below to refresh from Angel.

NIFTY_50_TOKENS: Dict[str, str] = {
    "RELIANCE.NS":     "2885",
    "TCS.NS":          "11536",
    "HDFCBANK.NS":     "1333",
    "ICICIBANK.NS":    "4963",
    "INFY.NS":         "1594",
    "HINDUNILVR.NS":   "1394",
    "ITC.NS":          "1660",
    "LT.NS":           "11483",
    "SBIN.NS":         "3045",
    "BHARTIARTL.NS":   "10604",
    "KOTAKBANK.NS":    "1922",
    "AXISBANK.NS":     "5900",
    "BAJFINANCE.NS":   "317",
    "ASIANPAINT.NS":   "236",
    "MARUTI.NS":       "10999",
    "HCLTECH.NS":      "7229",
    "SUNPHARMA.NS":    "3351",
    "TITAN.NS":        "3506",
    "ULTRACEMCO.NS":   "11532",
    "WIPRO.NS":        "3787",
    "NESTLEIND.NS":    "17963",
    "ONGC.NS":         "2475",
    "NTPC.NS":         "11630",
    "POWERGRID.NS":    "14977",
    "M&M.NS":          "2031",
    "TATAMOTORS.NS":   "3456",
    "TATASTEEL.NS":    "3499",
    "JSWSTEEL.NS":     "11723",
    "COALINDIA.NS":    "20374",
    "GRASIM.NS":       "1232",
    "BAJAJFINSV.NS":   "16675",
    "HDFCLIFE.NS":     "467",
    "SBILIFE.NS":      "21808",
    "BRITANNIA.NS":    "547",
    "DIVISLAB.NS":     "10940",
    "DRREDDY.NS":      "881",
    "CIPLA.NS":        "694",
    "EICHERMOT.NS":    "910",
    "HEROMOTOCO.NS":   "1348",
    "BAJAJ-AUTO.NS":   "16669",
    "TECHM.NS":        "13538",
    "ADANIENT.NS":     "25",
    "ADANIPORTS.NS":   "15083",
    "APOLLOHOSP.NS":   "157",
    "INDUSINDBK.NS":   "5258",
    "HINDALCO.NS":     "1363",
    "TATACONSUM.NS":   "3432",
    "BPCL.NS":         "526",
    "LTIM.NS":         "17818",
    "SHRIRAMFIN.NS":   "4306",
}


_MASTER_URL = ("https://margincalculator.angelbroking.com/"
               "OpenAPI_File/files/OpenAPIScripMaster.json")
_TOKEN_CACHE_PATH = Path.home() / ".angel_tokens.json"


def fetch_instrument_master(force: bool = False) -> Dict[str, str]:
    """Optional: download Angel One's full ScripMaster (~30 MB) and cache
    it. Only needed if the hardcoded NIFTY_50_TOKENS map is stale for
    the ticker you want. Returns dict {yf_ticker: symbol_token}."""
    if _TOKEN_CACHE_PATH.exists() and not force:
        try:
            with open(_TOKEN_CACHE_PATH) as f:
                return json.load(f)
        except Exception:
            pass

    import requests
    log.info("Downloading Angel One instrument master (~30 MB)...")
    r = requests.get(_MASTER_URL, timeout=60)
    r.raise_for_status()
    data = r.json()
    lookup = {}
    for row in data:
        sym = row.get("symbol", "")
        if row.get("exch_seg") != "NSE" or not sym.endswith("-EQ"):
            continue
        # Angel "INFY-EQ" → yfinance "INFY.NS"
        base = sym[:-3].replace("&", "&")   # keep '&' as-is
        yf_ticker = f"{base}.NS"
        lookup[yf_ticker] = row.get("token")
    try:
        with open(_TOKEN_CACHE_PATH, "w") as f:
            json.dump(lookup, f, indent=2)
    except Exception as e:
        log.warning(f"Could not cache master: {e}")
    return lookup


def yf_to_angel_symbol(yf_ticker: str) -> str:
    """'INFY.NS' → 'INFY-EQ' (Angel One NSE cash equity naming)."""
    return yf_ticker.replace(".NS", "") + "-EQ"


# ═══════════════════════════════════════════════════════════════════════ #
#                       ANGEL ONE LOGIN                                    #
# ═══════════════════════════════════════════════════════════════════════ #

class AngelSession:
    """
    Minimal Angel One SmartAPI session — TOTP auto-login only.
    Exposes jwt_token, feed_token, api_key, client_code for the
    WebSocket subscription. Also handles graceful terminate.
    """

    def __init__(self):
        self.api_key: str        = ""
        self.client_code: str    = ""
        self.jwt_token: Optional[str]  = None
        self.refresh_token: Optional[str] = None
        self.feed_token: Optional[str] = None
        self.smart = None
        self._login()

    def _login(self):
        try:
            from SmartApi import SmartConnect
            import pyotp
        except ImportError:
            log.error("Install: pip install smartapi-python pyotp websocket-client")
            sys.exit(1)

        api_key      = os.environ.get("ANGEL_API_KEY")
        client_code  = os.environ.get("ANGEL_CLIENT_CODE")
        pin          = os.environ.get("ANGEL_PIN")
        totp_secret  = os.environ.get("ANGEL_TOTP_SECRET")

        missing = [n for n, v in [
            ("ANGEL_API_KEY", api_key),
            ("ANGEL_CLIENT_CODE", client_code),
            ("ANGEL_PIN", pin),
            ("ANGEL_TOTP_SECRET", totp_secret),
        ] if not v]
        if missing:
            log.error(f"Missing env vars: {', '.join(missing)}")
            log.error("See README.md for how to obtain each of these.")
            sys.exit(1)

        self.api_key     = api_key
        self.client_code = client_code

        totp_code = pyotp.TOTP(totp_secret).now()
        self.smart = SmartConnect(api_key=api_key)
        session = self.smart.generateSession(client_code, pin, totp_code)

        if not session or session.get("status") is False:
            log.error(f"Angel One login FAILED: {session.get('message', session)}")
            sys.exit(1)

        data = session.get("data", {})
        self.jwt_token     = data.get("jwtToken")
        self.refresh_token = data.get("refreshToken")

        try:
            self.feed_token = self.smart.getfeedToken()
        except Exception as e:
            log.warning(f"Could not fetch feed token: {e}")

        try:
            prof = self.smart.getProfile(self.refresh_token)
            uid  = prof.get("data", {}).get("clientcode", client_code)
            name = prof.get("data", {}).get("name", "?")
            log.info(f"🟢 Angel One session — user: {uid} ({name})")
        except Exception:
            log.info(f"🟢 Angel One session — user: {client_code}")

    def terminate(self):
        if self.smart:
            try:
                self.smart.terminateSession(self.client_code)
                log.info("🔒 Angel One session terminated")
            except Exception as e:
                log.warning(f"terminateSession failed: {e}")


# ═══════════════════════════════════════════════════════════════════════ #
#                    SMART WEBSOCKET V2 MODE CONSTANTS                     #
# ═══════════════════════════════════════════════════════════════════════ #
MODE_LTP        = 1     # ltp only
MODE_QUOTE      = 2     # OHLCV + 5-level bid/ask
MODE_SNAP_QUOTE = 3     # QUOTE + last-traded-time, OI, circuit limits

EXCHANGE_NSE_CM = 1     # NSE cash market

MODE_STR_TO_INT = {"ltp": MODE_LTP, "quote": MODE_QUOTE, "snap": MODE_SNAP_QUOTE}


# ═══════════════════════════════════════════════════════════════════════ #
#                          TICK NORMALIZATION                              #
# ═══════════════════════════════════════════════════════════════════════ #

def normalize_tick(raw: dict, ticker: str) -> dict:
    """
    Convert SmartWebSocketV2's decoded dict into a clean record:
      - Prices from paise → rupees
      - Timestamps from ms epoch → ISO IST
      - 5-level bid/ask flattened
      - spread & mid computed
    """
    now_ist = datetime.now(IST)
    exch_ts_ms = raw.get("exchange_timestamp") or raw.get("last_traded_timestamp") or 0
    try:
        exch_dt = (
            datetime.fromtimestamp(int(exch_ts_ms) / 1000, tz=timezone.utc).astimezone(IST)
            if exch_ts_ms else None
        )
    except Exception:
        exch_dt = None

    rec = {
        "ticker":         ticker,
        "recv_ts_ist":    now_ist.isoformat(),
        "exch_ts_ist":    exch_dt.isoformat() if exch_dt else None,
        "sequence":       raw.get("sequence_number"),
        "ltp":            (raw.get("last_traded_price") or 0) / 100.0,
        "ltq":            raw.get("last_traded_quantity", 0),
        "avg_price":      (raw.get("average_traded_price") or 0) / 100.0,
        "volume_cum":     raw.get("volume_trade_for_the_day", 0),
        "total_buy_qty":  raw.get("total_buy_quantity", 0),
        "total_sell_qty": raw.get("total_sell_quantity", 0),
        "open_px":        (raw.get("open_price_of_the_day") or 0) / 100.0,
        "high_px":        (raw.get("high_price_of_the_day") or 0) / 100.0,
        "low_px":         (raw.get("low_price_of_the_day") or 0) / 100.0,
        "prev_close":     (raw.get("closed_price") or 0) / 100.0,
        "oi":             raw.get("open_interest", 0),
    }
    for i, bid in enumerate(raw.get("best_5_buy_data", [])[:5]):
        rec[f"bid_{i+1}_px"]  = (bid.get("price", 0) or 0) / 100.0
        rec[f"bid_{i+1}_qty"] = bid.get("quantity", 0)
    for i, ask in enumerate(raw.get("best_5_sell_data", [])[:5]):
        rec[f"ask_{i+1}_px"]  = (ask.get("price", 0) or 0) / 100.0
        rec[f"ask_{i+1}_qty"] = ask.get("quantity", 0)
    if rec.get("bid_1_px") and rec.get("ask_1_px"):
        rec["spread"] = rec["ask_1_px"] - rec["bid_1_px"]
        rec["mid"]    = (rec["ask_1_px"] + rec["bid_1_px"]) / 2.0
    return rec


# ═══════════════════════════════════════════════════════════════════════ #
#                          RECORDER STATE                                  #
# ═══════════════════════════════════════════════════════════════════════ #

class RecorderState:
    """Thread-safe tick buffer + statistics."""

    def __init__(self):
        self.buffer: Dict[str, list] = defaultdict(list)
        self.lock = threading.Lock()
        self.tick_counts: Dict[str, int]   = defaultdict(int)
        self.last_tick_ts: Dict[str, float] = defaultdict(float)
        self.max_gap_s: Dict[str, float]   = defaultdict(float)
        self.gaps_over_1s: Dict[str, int]   = defaultdict(int)
        self.reconnect_count = 0
        self.error_count     = 0
        self.start_time      = time.time()
        self.current_day     = datetime.now(IST).strftime("%Y-%m-%d")

    def add_tick(self, ticker: str, rec: dict):
        now = time.time()
        with self.lock:
            self.buffer[ticker].append(rec)
            self.tick_counts[ticker] += 1
            prev = self.last_tick_ts[ticker]
            if prev > 0:
                gap = now - prev
                if gap > self.max_gap_s[ticker]:
                    self.max_gap_s[ticker] = gap
                if gap > 1.0:
                    self.gaps_over_1s[ticker] += 1
            self.last_tick_ts[ticker] = now

    def drain(self) -> Dict[str, list]:
        with self.lock:
            out = dict(self.buffer)
            self.buffer = defaultdict(list)
        return out

    def snapshot_stats(self) -> dict:
        with self.lock:
            return {
                "uptime_sec":       round(time.time() - self.start_time, 1),
                "total_ticks":      sum(self.tick_counts.values()),
                "tickers_active":   len(self.tick_counts),
                "reconnect_count":  self.reconnect_count,
                "error_count":      self.error_count,
                "per_ticker": {
                    tk: {
                        "ticks":         self.tick_counts[tk],
                        "max_gap_s":     round(self.max_gap_s[tk], 3),
                        "gaps_over_1s":  self.gaps_over_1s[tk],
                    }
                    for tk in self.tick_counts
                },
            }


# ═══════════════════════════════════════════════════════════════════════ #
#                          DISK WRITER                                     #
# ═══════════════════════════════════════════════════════════════════════ #

class DiskWriter:
    """Append tick batches to per-(date, ticker) parquet/csv files."""

    def __init__(self, base_dir: str, fmt: str = "parquet"):
        assert fmt in ("parquet", "csv")
        self.base_dir = Path(base_dir)
        self.fmt      = fmt
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def _path(self, day_str: str, ticker: str) -> Path:
        day_dir = self.base_dir / day_str
        day_dir.mkdir(exist_ok=True)
        safe_tk = ticker.replace(".NS", "").replace("&", "AND")
        ext = "parquet" if self.fmt == "parquet" else "csv"
        return day_dir / f"{safe_tk}.{ext}"

    def append(self, day_str: str, ticker: str, records: list) -> int:
        if not records:
            return 0
        import pandas as pd
        df = pd.DataFrame(records)
        path = self._path(day_str, ticker)

        if self.fmt == "parquet":
            try:
                import pyarrow as pa
                import pyarrow.parquet as pq
                table = pa.Table.from_pandas(df, preserve_index=False)
                if path.exists():
                    old = pq.read_table(str(path))
                    merged = pa.concat_tables([old, table], promote_options="default")
                    pq.write_table(merged, str(path), compression="snappy")
                else:
                    pq.write_table(table, str(path), compression="snappy")
            except Exception as e:
                log.error(f"Parquet write failed for {ticker}: {e}")
                return 0
        else:
            mode = "a" if path.exists() else "w"
            df.to_csv(path, mode=mode, header=(mode == "w"), index=False)
        return len(records)

    def write_metadata(self, day_str: str, stats: dict):
        day_dir = self.base_dir / day_str
        day_dir.mkdir(exist_ok=True)
        path = day_dir / "_metadata.json"
        try:
            with open(path, "w") as f:
                json.dump(stats, f, indent=2, default=str)
        except Exception as e:
            log.warning(f"metadata write failed: {e}")


# ═══════════════════════════════════════════════════════════════════════ #
#                          FLUSHER THREAD                                  #
# ═══════════════════════════════════════════════════════════════════════ #

class FlusherThread(threading.Thread):
    """Background: every FLUSH_EVERY_S seconds, drain buffer and write."""

    def __init__(self, state: RecorderState, writer: DiskWriter):
        super().__init__(daemon=True, name="TickFlusher")
        self.state  = state
        self.writer = writer
        self._stop  = threading.Event()

    def stop(self):
        self._stop.set()

    def run(self):
        log.info("Flusher thread started")
        while not self._stop.is_set():
            self._stop.wait(RecorderConfig.FLUSH_EVERY_S)
            if self._stop.is_set():
                break
            self._flush_once()
        self._flush_once()   # final flush
        day = self.state.current_day
        self.writer.write_metadata(day, self.state.snapshot_stats())
        log.info("Flusher stopped: final flush + metadata written")

    def _flush_once(self):
        batches = self.state.drain()
        if not batches:
            return
        day = self.state.current_day
        total = 0
        for ticker, records in batches.items():
            total += self.writer.append(day, ticker, records)
        if total:
            log.info(f"Flushed {total} ticks across {len(batches)} tickers "
                     f"→ {self.writer.base_dir}/{day}/")


# ═══════════════════════════════════════════════════════════════════════ #
#                          MAIN RECORDER                                   #
# ═══════════════════════════════════════════════════════════════════════ #

class TickRecorder:
    def __init__(self, tickers: List[str], mode: str = "snap",
                 output_dir: str = "tick_data", fmt: str = "parquet"):
        self.tickers   = tickers
        self.mode_int  = MODE_STR_TO_INT[mode]
        self.mode_str  = mode
        self.state     = RecorderState()
        self.writer    = DiskWriter(output_dir, fmt=fmt)
        self.flusher   = FlusherThread(self.state, self.writer)
        self.sws       = None
        self.token_to_ticker: Dict[str, str] = {}
        self.session   = None
        self._running  = False

    def _resolve_tokens(self):
        """Login + build token→ticker map. Auto-fallback to master fetch."""
        # Start with hardcoded map; extend via master if needed
        for tk in self.tickers:
            tok = NIFTY_50_TOKENS.get(tk)
            if tok:
                self.token_to_ticker[str(tok)] = tk

        # If some tickers weren't in the hardcoded list, try master
        missing = [t for t in self.tickers if t not in NIFTY_50_TOKENS]
        if missing:
            log.info(f"Fetching Angel master for missing tickers: {missing}")
            try:
                master = fetch_instrument_master()
                for tk in missing:
                    tok = master.get(tk)
                    if tok:
                        self.token_to_ticker[str(tok)] = tk
                    else:
                        log.warning(f"  {tk} — no token found, skipping")
            except Exception as e:
                log.warning(f"master fetch failed: {e}; using hardcoded only")

        if not self.token_to_ticker:
            log.error("No valid tokens resolved; nothing to subscribe.")
            sys.exit(1)
        log.info(f"Resolved {len(self.token_to_ticker)} tokens")

    def _make_ws(self):
        from SmartApi.smartWebSocketV2 import SmartWebSocketV2
        return SmartWebSocketV2(
            auth_token=self.session.jwt_token,
            api_key=self.session.api_key,
            client_code=self.session.client_code,
            feed_token=self.session.feed_token,
            max_retry_attempt=3,
            retry_strategy=1,
            retry_delay=1,
            retry_multiplier=2,
            retry_duration=60,
        )

    # ────── Callbacks ──────
    def _on_data(self, wsapp, message):
        try:
            token = str(message.get("token", ""))
            ticker = self.token_to_ticker.get(token)
            if not ticker:
                return
            rec = normalize_tick(message, ticker)
            self.state.add_tick(ticker, rec)
        except Exception as e:
            self.state.error_count += 1
            log.debug(f"on_data error: {e}")

    def _on_open(self, wsapp):
        log.info(f"🟢 WebSocket connected — subscribing {len(self.token_to_ticker)} "
                 f"tickers in {self.mode_str.upper()} mode")
        tokens_list = [{
            "exchangeType": EXCHANGE_NSE_CM,
            "tokens":       list(self.token_to_ticker.keys()),
        }]
        self.sws.subscribe(
            correlation_id="tick_recorder",
            mode=self.mode_int,
            token_list=tokens_list,
        )
        log.info("Subscription request sent")

    def _on_error(self, wsapp, error):
        self.state.error_count += 1
        log.error(f"WS error: {error}")

    def _on_close(self, wsapp):
        log.warning("🔴 WebSocket disconnected")
        self.state.reconnect_count += 1

    def _on_control(self, wsapp, message):
        log.debug(f"Control message: {message}")

    # ────── Background threads ──────
    def _day_rotator(self):
        while self._running:
            time.sleep(60)
            today = datetime.now(IST).strftime("%Y-%m-%d")
            if today != self.state.current_day:
                log.info(f"🗓  Date rolled over: {self.state.current_day} → {today}")
                self.flusher._flush_once()
                self.writer.write_metadata(
                    self.state.current_day, self.state.snapshot_stats()
                )
                self.state = RecorderState()
                self.state.current_day = today
                self.flusher.state = self.state

    def _stats_printer(self):
        while self._running:
            time.sleep(60)
            s = self.state.snapshot_stats()
            log.info(f"📊 Stats: {s['total_ticks']} ticks across "
                     f"{s['tickers_active']} tickers  |  "
                     f"reconnects={s['reconnect_count']}, "
                     f"errors={s['error_count']}, uptime={s['uptime_sec']}s")

    # ────── Lifecycle ──────
    def start(self):
        self.session = AngelSession()   # TOTP login
        self._resolve_tokens()
        self.flusher.start()
        self._running = True

        self.sws = self._make_ws()
        self.sws.on_data            = self._on_data
        self.sws.on_open            = self._on_open
        self.sws.on_error           = self._on_error
        self.sws.on_close           = self._on_close
        self.sws.on_control_message = self._on_control

        def _sig(sig, _frame):
            log.info(f"Signal {sig} received → shutting down cleanly...")
            self.stop()
            sys.exit(0)
        signal.signal(signal.SIGINT,  _sig)
        signal.signal(signal.SIGTERM, _sig)

        threading.Thread(target=self._day_rotator, daemon=True,
                          name="DayRotator").start()
        threading.Thread(target=self._stats_printer, daemon=True,
                          name="StatsPrinter").start()

        log.info(f"Connecting to Angel One SmartWebSocketV2...")
        try:
            self.sws.connect()   # blocking
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        if not self._running:
            return
        self._running = False
        log.info("Stopping recorder...")
        try:
            if self.sws:
                self.sws.close_connection()
        except Exception as e:
            log.warning(f"WS close error: {e}")
        self.flusher.stop()
        self.flusher.join(timeout=10)
        if self.session:
            self.session.terminate()
        log.info("Final stats:")
        log.info(json.dumps(self.state.snapshot_stats(), indent=2, default=str))


# ═══════════════════════════════════════════════════════════════════════ #
#                                  CLI                                     #
# ═══════════════════════════════════════════════════════════════════════ #

DEFAULT_NIFTY50 = list(NIFTY_50_TOKENS.keys())


def main():
    p = argparse.ArgumentParser(
        description="Record real-time Angel One SmartAPI ticks to Parquet/CSV",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--tickers", nargs="+",
                   help="Custom ticker list (default: full Nifty 50)")
    p.add_argument("--mode", choices=["ltp", "quote", "snap"], default="snap",
                   help="Tick detail level (default: snap = LTP + 5-lvl depth + OI)")
    p.add_argument("--out", default="tick_data",
                   help="Output directory (default: tick_data/)")
    p.add_argument("--format", choices=["parquet", "csv"], default="parquet",
                   help="Storage format (default: parquet)")
    p.add_argument("--flush-every", type=float, default=5.0,
                   help="Flush buffer every N seconds (default: 5.0)")
    args = p.parse_args()

    tickers = args.tickers if args.tickers else DEFAULT_NIFTY50
    RecorderConfig.FLUSH_EVERY_S = args.flush_every

    log.info("═" * 72)
    log.info(f"  🎙  ANGEL ONE TICK RECORDER")
    log.info("═" * 72)
    log.info(f"  Tickers:  {len(tickers)}  ({', '.join(tickers[:5])}"
             f"{'...' if len(tickers) > 5 else ''})")
    log.info(f"  Mode:     {args.mode.upper()}")
    log.info(f"  Format:   {args.format}")
    log.info(f"  Output:   {args.out}/YYYY-MM-DD/<TICKER>.{args.format}")
    log.info(f"  Flush:    every {args.flush_every}s")
    log.info("═" * 72)

    required = ["ANGEL_API_KEY", "ANGEL_CLIENT_CODE",
                "ANGEL_PIN", "ANGEL_TOTP_SECRET"]
    missing = [v for v in required if not os.environ.get(v)]
    if missing:
        log.error(f"Missing env vars: {missing}")
        log.error("See README.md — Section 'Setup credentials'.")
        sys.exit(1)

    recorder = TickRecorder(
        tickers=tickers, mode=args.mode,
        output_dir=args.out, fmt=args.format,
    )
    recorder.start()   # blocks until Ctrl+C


if __name__ == "__main__":
    main()

# Mean-Reversion-Tick-Angelone

**Angel One SmartAPI real-time tick recorder — single-file tool.**

Streams NSE cash-market ticks (LTP + bid/ask depth + volume) from Angel One
SmartWebSocketV2 into compressed Parquet files. Purpose-built to give you
**real market microstructure data** to research mean-reversion (and any
other) intraday strategies with, in place of yfinance's 1-min OHLC (no
bid/ask, 15-min delay).

**One file. No external modules.** Everything lives in
[`tick_recorder.py`](tick_recorder.py).

---

## 🎯 Why This Exists

Public data sources like yfinance give you 1-min OHLC with a 15-min delay
and **no bid/ask information**. That is unusable for any serious retail
algo work.

Angel One's SmartAPI (via SmartWebSocketV2) gives you **real ticks** —
every LTP update, best-5 bid/ask levels, day volume, open interest — in
real time to your machine, and it's free with any Angel One account.

This script wraps that WebSocket into a robust recorder:

| Feature | Detail |
|:---|:---|
| Auto-login | TOTP-based, no manual OTP every session |
| Storage | Parquet (default, compressed) or CSV |
| File layout | `tick_data/YYYY-MM-DD/<TICKER>.parquet` |
| Rotation | New file per calendar day (IST) |
| Buffering | Batches every 5s so disk I/O doesn't block the WS |
| Data quality | Per-day `_metadata.json`: gap sizes, reconnects, errors |
| Graceful stop | Ctrl+C flushes pending ticks then terminates SmartAPI cleanly |
| Retry | Auto-reconnect with exponential backoff |

---

## 📋 Installation

```bash
pip install -r requirements.txt
```

That pulls in `smartapi-python`, `pyotp`, `websocket-client`, `pandas`, `pyarrow`.

---

## 🔑 Setup Credentials (One-time, ~10 minutes)

The easiest way: fill in a **`.env` file**. The recorder auto-loads it —
you don't need to type export commands every time.

### Step 1 — Copy the template

```bash
cp .env.example .env
```

Now open `.env` in any editor:
```bash
nano .env       # or vim, code, notepad, etc.
```

You'll see four lines to fill in:

```env
ANGEL_API_KEY=paste_your_smartapi_api_key_here
ANGEL_CLIENT_CODE=A123456
ANGEL_PIN=1234
ANGEL_TOTP_SECRET=JBSWY3DPEHPK3PXP
```

### Step 2 — How to get each value

#### `ANGEL_API_KEY`
1. Log in at [smartapi.angelbroking.com](https://smartapi.angelbroking.com/)
2. Click **Create App** → product type **Trading API**
3. Fill:
   - App name: anything (e.g. `tick_recorder`)
   - Redirect URL: `http://localhost` (not used, but required)
   - Postback URL: leave blank
4. Copy the API Key shown. Paste into `.env`.

#### `ANGEL_TOTP_SECRET` (Base32 secret, not the 6-digit code)
1. On the same SmartAPI page, click **Generate TOTP** → QR code appears.
2. Scan the QR with Google Authenticator (or Authy, 1Password) so you
   can also login manually if needed.
3. **CRITICAL:** click **"Can't scan QR?"** — the Base32 secret is
   revealed (looks like `JBSWY3DPEHPK3PXP...`). Copy this. Paste into `.env`.
4. Without this secret, the recorder cannot auto-login every session.

#### `ANGEL_CLIENT_CODE`
Your Angel One user ID (e.g. `A123456`), shown in your Angel profile.
Usually starts with a letter.

#### `ANGEL_PIN`
Your 4-digit **trading PIN** (for order placement).
This is NOT your login password.

### Step 3 — Verify

Once your `.env` is filled in, just run:
```bash
python3 tick_recorder.py --tickers TCS.NS
```

You should see:
```
Loaded credentials from: /path/to/.env
🟢 Angel One session — user: A123456 (Your Name)
🟢 WebSocket connected — subscribing 1 tickers in SNAP mode
```

If you see `Missing env vars: [...]`, your `.env` is not being found or
some values are empty. Re-check `cat .env` for typos.

### Security notes

- ✅ `.env` is already in `.gitignore` — safe from `git commit`
- ✅ `.env.example` **IS** committed as a template — it has only dummy values
- ⚠️ Don't share your `.env` file. Anyone with those creds can trade on your account.
- ⚠️ On shared machines, use file permissions: `chmod 600 .env`

### Alternative: shell env vars (no .env file)

If you prefer, you can still export in your shell profile:
```bash
export ANGEL_API_KEY='...'
export ANGEL_CLIENT_CODE='A123456'
export ANGEL_PIN='1234'
export ANGEL_TOTP_SECRET='JBSWY3DPEHPK3PXP'
```

The script checks `os.environ` first — shell vars take precedence over `.env`.

---

## 🚀 Usage

### Record full Nifty 50 (default)

```bash
python3 tick_recorder.py
```

Runs until Ctrl+C. Files land in `tick_data/YYYY-MM-DD/<TICKER>.parquet`.

### Record specific tickers only

```bash
python3 tick_recorder.py --tickers TCS.NS INFY.NS RELIANCE.NS
```

### Detail-level modes

```bash
python3 tick_recorder.py --mode ltp     # LTP only (~2 MB/stock/day)
python3 tick_recorder.py --mode quote   # + 5-lvl bid/ask (~7 MB/stock/day)
python3 tick_recorder.py --mode snap    # + OI + circuits (default, ~12 MB/stock/day)
```

### CSV instead of Parquet (readable but larger)

```bash
python3 tick_recorder.py --format csv --tickers TCS.NS
```

### Custom output directory

```bash
python3 tick_recorder.py --out /data/ticks
```

### Combine everything

```bash
python3 tick_recorder.py \
    --tickers TCS.NS INFY.NS EICHERMOT.NS \
    --mode snap \
    --format parquet \
    --out /data/ticks \
    --flush-every 5
```

Stop with **Ctrl+C** — the recorder flushes pending ticks, writes metadata,
and terminates the SmartAPI session cleanly.

---

## 📂 What Gets Stored

Every tick you receive becomes a row with these fields (SNAP mode):

| Field | Type | Description |
|:---|:---|:---|
| `ticker` | str | e.g. `TCS.NS` |
| `recv_ts_ist` | ISO datetime | when we received the tick (IST) |
| `exch_ts_ist` | ISO datetime | exchange-side timestamp (IST) |
| `sequence` | int | monotonic sequence number |
| `ltp` | float ₹ | last traded price |
| `ltq` | int | last traded quantity |
| `avg_price` | float ₹ | day-average traded price |
| `volume_cum` | int | cumulative day volume |
| `total_buy_qty` | int | total pending buy quantity |
| `total_sell_qty` | int | total pending sell quantity |
| `open_px, high_px, low_px, prev_close` | float ₹ | day OHLC |
| `oi` | int | open interest (0 for cash equity) |
| `bid_1_px … bid_5_px` | float ₹ | best-5 bid prices |
| `bid_1_qty … bid_5_qty` | int | best-5 bid quantities |
| `ask_1_px … ask_5_px` | float ₹ | best-5 ask prices |
| `ask_1_qty … ask_5_qty` | int | best-5 ask quantities |
| `spread` | float ₹ | `ask_1_px − bid_1_px` |
| `mid` | float ₹ | `(ask_1_px + bid_1_px) / 2` |

Prices are auto-converted from paise (SmartAPI wire format) to rupees.
Timestamps are auto-converted from ms epoch UTC to ISO IST.

### File layout

```
tick_data/
├── 2026-07-02/
│   ├── TCS.parquet         all ticks for TCS on that day
│   ├── INFY.parquet
│   ├── ...
│   └── _metadata.json      recorder stats (ticks, gaps, reconnects)
└── 2026-07-03/
    └── ...
```

### `_metadata.json` example

```json
{
  "uptime_sec": 22345.6,
  "total_ticks": 1245678,
  "tickers_active": 48,
  "reconnect_count": 2,
  "error_count": 0,
  "per_ticker": {
    "TCS.NS":  {"ticks": 34567, "max_gap_s": 2.1, "gaps_over_1s": 15},
    "INFY.NS": {"ticks": 41234, "max_gap_s": 1.4, "gaps_over_1s": 8}
  }
}
```

Check this before using the day's data for backtest. Warning signs:

- `max_gap_s > 5` → WebSocket had a hiccup, some ticks missing
- `gaps_over_1s > 100` → data quality suspect
- `reconnect_count > 5` → unstable connection
- `total_ticks < 1000` per liquid Nifty 50 stock → subscription failure

---

## 📊 Reading Stored Ticks

```python
import pandas as pd

# Load one day for one ticker
df = pd.read_parquet("tick_data/2026-07-02/TCS.parquet")

print(f"{len(df):,} ticks")
print(f"First: {df.recv_ts_ist.min()}")
print(f"Last:  {df.recv_ts_ist.max()}")
print(f"Median spread: ₹{df.spread.median():.4f}")

# Resample ticks to 1-min OHLC
df["recv_ts_ist"] = pd.to_datetime(df["recv_ts_ist"])
df = df.set_index("recv_ts_ist")
ohlc = df["ltp"].resample("1min").ohlc()
volume = df["volume_cum"].resample("1min").last().diff()
```

Load all tickers for one day:

```python
from pathlib import Path
folder = Path("tick_data/2026-07-02")
all_ticks = {p.stem: pd.read_parquet(p) for p in folder.glob("*.parquet")}
```

---

## 💾 Storage Estimates

Rough per-day usage in different modes (typical liquid Nifty 50 stock):

| Mode | Fields | Ticks/day | Bytes/tick | Size/stock/day |
|:---|---:|---:|---:|---:|
| `ltp` | ~5 | ~50,000 | ~40 | ~2 MB |
| `quote` | ~25 | ~50,000 | ~150 | ~7 MB |
| `snap` (default) | ~40 | ~50,000 | ~250 | ~12 MB |

**Full Nifty 50 in snap mode: ~600 MB/day → ~15 GB/month.**

For a laptop, use `--mode ltp` or a subset of tickers to keep it manageable.

---

## 🛑 Known Issues

### 1. TOTP time-drift
If your system clock is more than 30 seconds off, TOTP fails.
Fix: enable NTP (`sudo systemctl start systemd-timesyncd` on Linux).

### 2. Symbol tokens can go stale
Angel One's numeric token for a stock changes on splits/mergers.
The hardcoded map covers Nifty 50 as of 2025. If a token is invalid,
uncomment `fetch_instrument_master(force=True)` inside `tick_recorder.py`
to refresh from Angel's public master JSON (~30 MB).

### 3. Session expiry
SmartAPI JWT tokens expire after ~8 hours. Restart the recorder daily
(recommended anyway to reset for the new trading session).

### 4. Rate limits
Angel One: 20 requests/sec, 500/min. This recorder is nowhere near
that limit (~2 messages/sec typical). No action needed.

### 5. Only NSE cash equity supported out-of-box
The hardcoded token map is NSE-EQ only. For F&O or BSE, extend
`NIFTY_50_TOKENS` in the script or use `fetch_instrument_master()` to
pull the full universe.

---

## 🧾 Hinglish Quick Start

**चार env vars set करने हैं:**
```bash
export ANGEL_API_KEY='...'
export ANGEL_CLIENT_CODE='A123456'
export ANGEL_PIN='1234'
export ANGEL_TOTP_SECRET='...'
```

**चलाना (market hours में):**
```bash
python3 tick_recorder.py                        # full Nifty 50
python3 tick_recorder.py --tickers TCS.NS INFY.NS
python3 tick_recorder.py --mode ltp             # छोटी files
```

**बंद करने के लिए:** Ctrl+C

**Read करना:**
```python
import pandas as pd
df = pd.read_parquet('tick_data/2026-07-02/TCS.parquet')
```

**Space:** Full Nifty 50 snap mode = ~600 MB/day. Big pर SSD पर कुछ नहीं है।

---

## 📁 Repository Contents

```
Mean-Reversion-Tick-Angelone/
├── README.md          ← You are here
├── requirements.txt   ← 5 dependencies
├── .gitignore         ← Excludes tick_data/, .env, *.log
├── .env.example       ← Credential template — copy to .env and fill in
└── tick_recorder.py   ← The whole tool (self-contained)
```

**5 files total.** No external modules, no `scripts/`, no `docs/`.
One Python file, one credentials template, this README.

---

## ⚠️ Disclaimer

This tool only **records** ticks — it never places orders. But it uses
your Angel One credentials, so treat those with care. If your machine is
compromised, an attacker with those env vars could place trades under
your account. Standard security hygiene:

- Never commit env vars to git
- Use a dedicated Angel API app (separate from any other bots)
- Rotate the TOTP secret if you suspect compromise
- On shared machines, unset env vars after use: `unset ANGEL_API_KEY ...`

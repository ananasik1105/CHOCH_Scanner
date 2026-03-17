import time
import requests
import pandas as pd
import numpy as np
import datetime
from threading import Thread
from flask import Flask
import os
# ====== НАСТРОЙКИ ======
API_KEY    = "DeecQb17BmXDUJoDMJlSFrwqQA5fKmHEomLRFcOFRDUTPre6GsXNvtZqH7GA1u47wocRWdWW1q379KtWEg"
BOT_TOKEN  = "8504110255:AAHFQnxpm3kcqDQhsfluaetmjB0hgrs7j9U"
CHAT_ID    = "454082808"
SCAN_DELAY = 300      # секунд между проверками
TIMEFRAMES = ["1h", "4h", "1d"]
LEN        = 50      # CHoCH Detection Period (как в LuxAlgo по умолчанию)
FRESH_BARS = 3       # CHoCH считается свежим если случился в последних N барах
# ====== FLASK ======
app = Flask(__name__)
@app.route("/")
def home():
    return "Scanner is running!"
# ====== ПАМЯТЬ: 1 сигнал на монету+таймфрейм в день ======
# { "BTC-USDT_1h": "2026-03-16" }
signal_dates = {}
# ====== API ======
def send_telegram(msg):
    try:
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            data={"chat_id": CHAT_ID, "text": msg},
            timeout=10
        )
    except Exception as e:
        print(f"[ERROR] Telegram: {e}")
def get_symbols():
    try:
        r = requests.get(
            "https://open-api.bingx.com/openApi/swap/v2/quote/contracts",
            params={"apiKey": API_KEY}, timeout=10
        ).json()
        if "data" in r and r["data"]:
            symbols = [s["symbol"] for s in r["data"]
                       if "symbol" in s and s["symbol"].endswith("USDT")]
            print(f"[INFO] Получено символов: {len(symbols)}")
            return symbols
    except Exception as e:
        print(f"[ERROR] get_symbols: {e}")
    print("[WARN] Fallback список")
    return ["BTC-USDT", "ETH-USDT", "SOL-USDT", "XRP-USDT", "BNB-USDT"]
def get_klines(symbol, tf):
    for _ in range(3):
        try:
            r = requests.get(
                "https://open-api.bingx.com/openApi/swap/v2/quote/klines",
                params={"symbol": symbol, "interval": tf, "limit": 500, "apiKey": API_KEY},
                timeout=10
            ).json()
            if "data" not in r or not r["data"]:
                return None
            df = pd.DataFrame(r["data"])
            for col in ["high", "low", "close"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
            return df.dropna(subset=["high", "low", "close"]).reset_index(drop=True)
        except Exception as e:
            print(f"[ERROR] {symbol} {tf}: {e}")
            time.sleep(1)
    return None
# ====== СВИНГИ ПО АЛГОРИТМУ LUXALGO ======
def find_swings_luxalgo(highs, lows, length):
    n = len(highs)
    swing_high = {}
    swing_low  = {}
    os_state   = 0
    for bar in range(length, n):
        candidate_idx = bar - length
        upper = float(np.max(highs[bar - length + 1 : bar + 1]))
        lower = float(np.min(lows[bar  - length + 1 : bar + 1]))
        prev_os = os_state
        if highs[candidate_idx] > upper:
            os_state = 0
        elif lows[candidate_idx] < lower:
            os_state = 1
        if os_state == 0 and prev_os != 0:
            swing_high[candidate_idx] = float(highs[candidate_idx])
        if os_state == 1 and prev_os != 1:
            swing_low[candidate_idx] = float(lows[candidate_idx])
    return swing_high, swing_low
# ====== ДЕТЕКТИРОВАНИЕ CHOCH (точно как в LuxAlgo) ======
def detect_choch(df):
    n = len(df)
    if n < LEN * 2 + 10:
        return None, None, None, None
    highs  = df["high"].values
    lows   = df["low"].values
    closes = df["close"].values
    swing_high, swing_low = find_swings_luxalgo(highs, lows, LEN)
    topy        = None
    btmy        = None
    top_crossed = False
    btm_crossed = False
    os_dir      = 0
    last_choch_bar = -1
    last_choch_dir = None
    for bar in range(n):
        if bar in swing_high:
            topy        = swing_high[bar]
            top_crossed = False
        if bar in swing_low:
            btmy        = swing_low[bar]
            btm_crossed = False
        c        = closes[bar]
        prev_dir = os_dir
        if topy is not None and c > topy and not top_crossed:
            top_crossed = True
            os_dir      = 1
        if btmy is not None and c < btmy and not btm_crossed:
            btm_crossed = True
            os_dir      = 0
        if os_dir != prev_dir:
            last_choch_bar = bar
            last_choch_dir = "LONG" if os_dir == 1 else "SHORT"
    if last_choch_dir is not None and last_choch_bar >= n - FRESH_BARS - 1:
        return last_choch_dir, float(closes[-1]), topy, btmy
    return None, float(closes[-1]), topy, btmy
# ====== SCAN ======
def scan_symbol(symbol):
    today = datetime.date.today().isoformat()
    for tf in TIMEFRAMES:
        key = f"{symbol}_{tf}"
        # 1 сигнал на монету + таймфрейм в день
        if signal_dates.get(key) == today:
            continue
        df = get_klines(symbol, tf)
        if df is None:
            continue
        choch, last_close, topy, btmy = detect_choch(df)
        if choch:
            msg = (
                f"🚨 CHoCH {tf} | {symbol}\n"
                f"Сигнал:    {choch}\n"
                f"Close:     {last_close:.5f}\n"
                f"SwingHigh: {topy:.5f}\n"
                f"SwingLow:  {btmy:.5f}"
            )
            print(f"[SIGNAL] {symbol} {tf} -> {choch} | close={last_close:.5f} sh={topy:.5f} sl={btmy:.5f}")
            send_telegram(msg)
            signal_dates[key] = today
        else:
            print(f"[----]   {symbol} {tf} нет сигнала")
def scan_loop():
    symbols = get_symbols()
    print(f"[INFO] Будут проверены {len(symbols)} пар")
    while True:
        now = datetime.datetime.now()
        if 9 <= now.hour < 23:
            print("=== SCANNING MARKET ===")
            for symbol in symbols:
                scan_symbol(symbol)
            print(f"=== WAIT {SCAN_DELAY}s ===")
        else:
            print("=== Сканер вне активных часов ===")
        time.sleep(SCAN_DELAY)
# ====== MAIN ======
if __name__ == "__main__":
    t = Thread(target=scan_loop, daemon=True)
    t.start()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

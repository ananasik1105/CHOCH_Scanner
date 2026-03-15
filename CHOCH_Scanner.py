import requests
import pandas as pd
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# ====== TELEGRAM ======
BOT_TOKEN = "8504110255:AAHFQnxpm3kcqDQhsfluaetmjB0hgrs7j9U"
CHAT_ID = "454082808"
API_KEY = "DeecQb17BmXDUJoDMJlSFrwqQA5fKmHEomLRFcOFRDUTPre6GsXNvtZqH7GA1u47wocRWdWW1q379KtWEg"

# ====== TIMEFRAMES И SYMBOLS ======
timeframes = ["15m", "1h", "4h"]

# Топ-300 монет
symbols = [
    "BTC-USDT","ETH-USDT","SOL-USDT","XRP-USDT","BNB-USDT","ADA-USDT","DOGE-USDT","AVAX-USDT",
"LINK-USDT","POL-USDT","LTC-USDT","ATOM-USDT","TRX-USDT","APT-USDT","NEAR-USDT",
"FIL-USDT","ALGO-USDT","MANA-USDT","SAND-USDT","FTM-USDT","LRC-USDT","GRT-USDT","AXS-USDT",
"XTZ-USDT","KSM-USDT","VET-USDT","EOS-USDT","XLM-USDT","DASH-USDT","ZEC-USDT","UNI-USDT",
"COMP-USDT","AAVE-USDT","SNX-USDT","KNC-USDT","YFI-USDT","CRV-USDT","1INCH-USDT","CHZ-USDT",
"ENJ-USDT","SUSHI-USDT","MKR-USDT","BNT-USDT","STX-USDT","QTUM-USDT","NEO-USDT","RVN-USDT",
"ICP-USDT","FLOW-USDT","GALA-USDT","THETA-USDT","CEL-USDT","BAT-USDT","ZRX-USDT","WAVES-USDT",
"OMG-USDT","BTG-USDT","DOGE-USDT","SHIB-USDT","RVN-USDT","HNT-USDT","CELO-USDT","FTT-USDT",
"KAVA-USDT","ONE-USDT","ONT-USDT","IOST-USDT","DGB-USDT","SC-USDT","RVN-USDT","TFUEL-USDT",
"ICX-USDT","AR-USDT","STMX-USDT","CHSB-USDT","RVN-USDT","XEM-USDT","KDA-USDT","IOTA-USDT",
"NEAR-USDT","OCEAN-USDT","ANKR-USDT","GRT-USDT","COTI-USDT","NKN-USDT","LUNA-USDT","RUNE-USDT",
"CAKE-USDT","BAKE-USDT","ALPHA-USDT","BAND-USDT","REEF-USDT","HIVE-USDT","RAY-USDT","SRM-USDT",
"ORN-USDT","FIS-USDT","LPT-USDT","GLM-USDT","STORJ-USDT","SXP-USDT","LEND-USDT","REN-USDT",
"1INCH-USDT","OXT-USDT","CELR-USDT","MATH-USDT","KNC-USDT","BNT-USDT","RLC-USDT","ANT-USDT",
"TRIBE-USDT","API3-USDT","NFT-USDT","SPELL-USDT","RAD-USDT","GMX-USDT","OP-USDT","ARB-USDT",
"DYDX-USDT","ENS-USDT","LOOKS-USDT","PEOPLE-USDT","GODS-USDT","MAGIC-USDT","CVX-USDT","FXS-USDT",
"CRV-USDT","BAL-USDT","RENBTC-USDT","SUSHI-USDT","AAVE-USDT","UNI-USDT","MKR-USDT","COMP-USDT",
"YFI-USDT","SNX-USDT","LRC-USDT","1INCH-USDT","ALCX-USDT","FEI-USDT","FRAX-USDT","SPELL-USDT",
"GMX-USDT","OP-USDT","ARB-USDT","DYDX-USDT","ENS-USDT","LOOKS-USDT","PEOPLE-USDT","GODS-USDT",
"MAGIC-USDT","CVX-USDT","FXS-USDT","CRV-USDT","BAL-USDT","RENBTC-USDT","SUSHI-USDT","AAVE-USDT"
]


# ====== ФУНКЦИИ ======
def send_telegram(msg):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    requests.post(url, data={"chat_id": CHAT_ID, "text": msg})

def get_klines(symbol, tf):
    url = "https://open-api.bingx.com/openApi/swap/v2/quote/klines"
    params = {"symbol": symbol, "interval": tf, "limit": 200, "apiKey": API_KEY}
    for _ in range(3):
        try:
            r = requests.get(url, params=params, timeout=10).json()
            df = pd.DataFrame(r["data"])
            for col in ["high","low","close","volume","openInterest"]:
                df[col] = df[col].astype(float)
            return df
        except:
            time.sleep(1)
    return None

# ====== ИНДИКАТОРЫ ======
def calc_rsi(df, length=14):
    delta = df["close"].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(length).mean()
    avg_loss = loss.rolling(length).mean()
    rs = avg_gain / (avg_loss + 1e-9)
    return 100 - (100 / (1 + rs))

def calc_atr(df, length=14):
    tr = pd.concat([
        df["high"] - df["low"],
        (df["high"] - df["close"].shift()).abs(),
        (df["low"] - df["close"].shift()).abs()
    ], axis=1).max(axis=1)
    return tr.rolling(length).mean()

# ====== DETECT CHOCH ======
def detect_choch(df, swing_len=10):

    if len(df) < swing_len + 2:
        return None, None, None, None

    swing_high = df["high"].rolling(window=swing_len, center=True).max()
    swing_low = df["low"].rolling(window=swing_len, center=True).min()

    last_close = df["close"].iloc[-1]

    last_swing_high = swing_high.iloc[-2]
    last_swing_low = swing_low.iloc[-2]

    # защита от NaN
    if pd.isna(last_swing_high) or pd.isna(last_swing_low):
        return None, None, None, None

    choch = None

    if last_close > last_swing_high:
        choch = "LONG"
    elif last_close < last_swing_low:
        choch = "SHORT"

    return choch, last_close, last_swing_high, last_swing_low

# ====== СКАНЕР ======
def scan_symbol(symbol):
    for tf in timeframes:
        df = get_klines(symbol, tf)
        if df is None or df.empty:
            continue

        choch, last_close, swing_high, swing_low = detect_choch(df)
        if not choch:
            continue  # сигнал только при пробое

        rsi_series = calc_rsi(df)
        atr_series = calc_atr(df)
        rsi_prev = rsi_series.iloc[-2] if len(rsi_series) > 1 else rsi_series.iloc[-1]
        rsi = rsi_series.iloc[-1]
        atr = atr_series.iloc[-1]

        volume_change = (df["volume"].iloc[-1] / (df["volume"].iloc[-2] + 1e-9)) - 1
        oi_change = (df["openInterest"].iloc[-1] / (df["openInterest"].iloc[-2] + 1e-9)) - 1

        # ====== СИЛА СИГНАЛА ======
        if abs(volume_change) > 0.3:
            strength = "🟢🟢🟢🟢⚪️"
        elif abs(volume_change) > 0.1:
            strength = "🟢🟢🟢⚪️⚪️"
        else:
            strength = "🟢🟢⚪️⚪️⚪️"

        # ====== ФОРМАТ СООБЩЕНИЯ ======
        msg = (
            f"🟢 CHOCH {choch}\n\n"
            f"{symbol} | {tf.upper()}\n\n"
            f"Break: {last_close:.2f}\n"
            f"SwingHigh: {swing_high.iloc[-1]:.2f} SwingLow: {swing_low.iloc[-1]:.2f}\n"
            f"RSI: {rsi_prev:.1f} → {rsi:.1f}\n"
            f"Volume: {volume_change*100:+.0f}%\n"
            f"OI: {oi_change*100:+.1f}%\n"
            f"ATR: {atr:.2f}x\n\n"
            f"Strength:\n{strength}\n\n"
            f"{datetime.utcnow().strftime('%H:%M UTC')}"
        )

        print(msg)
        send_telegram(msg)
        time.sleep(0.5)

def scan():
    with ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(scan_symbol, symbols)

# ====== ЦИКЛ РАБОТЫ ======
from datetime import datetime, timezone, timedelta

# UTC+3
tz = timezone(timedelta(hours=3))

from flask import Flask
import threading
import os

app = Flask(__name__)

@app.route("/")
def home():
    return "CHOCH Scanner running"

def run_web():
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)

threading.Thread(target=run_web).start()

# основной цикл
while True:

    now = datetime.now(tz)
    hour = now.hour

    # рабочее время 08:00 – 01:00
    if hour >= 8 or hour <= 1:
        scan()
    else:
        print("Сейчас вне рабочего времени. Спим.")

    time.sleep(60)

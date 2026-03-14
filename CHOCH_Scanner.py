import requests
import pandas as pd
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# ====== TELEGRAM ======
BOT_TOKEN = "8504110255:AAHFQnxpm3kcqDQhsfluaetmjB0hgrs7j9U"
CHAT_ID = "454082808"

def send_telegram(msg):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = {"chat_id": CHAT_ID, "text": msg}
    try:
        r = requests.post(url, data=data, timeout=5)
        if r.status_code != 200:
            print("Ошибка Telegram:", r.text)
    except Exception as e:
        print("Ошибка при отправке в Telegram:", e)

# ====== НАСТРОЙКИ ======
timeframes = ["15m", "1h", "4h", "1d"]

symbols = [
    # здесь вставь топ-200 монет, например:
    "BTC-USDT","ETH-USDT","SOL-USDT","XRP-USDT","BNB-USDT","ADA-USDT","DOGE-USDT","AVAX-USDT",
    # ... полный список
]

# ====== ФУНКЦИИ ДЛЯ ДАННЫХ ======
def get_klines(symbol, tf):
    url = "https://open-api.bingx.com/openApi/swap/v2/quote/klines"
    params = {"symbol": symbol, "interval": tf, "limit": 200}
    for _ in range(3):
        try:
            r = requests.get(url, params=params, timeout=10).json()
            if "data" not in r:
                time.sleep(1)
                continue
            df = pd.DataFrame(r["data"])
            df["high"] = df["high"].astype(float)
            df["low"] = df["low"].astype(float)
            df["close"] = df["close"].astype(float)
            return df
        except:
            time.sleep(1)
    return None

# ====== SWING & CHOCH ======
def detect_swings(df, length=10):
    highs = df["high"]
    lows = df["low"]
    swing_high = []
    swing_low = []
    for i in range(length, len(df)-length):
        if highs[i] == max(highs[i-length:i+length]):
            swing_high.append((i, highs[i]))
        if lows[i] == min(lows[i-length:i+length]):
            swing_low.append((i, lows[i]))
    return swing_high, swing_low

def detect_choch_bos(df):
    swing_high, swing_low = detect_swings(df)
    if len(swing_high) < 2 or len(swing_low) < 2:
        return None, None, None

    last_close = df["close"].iloc[-1]
    last_high = swing_high[-1][1]
    last_low = swing_low[-1][1]
    prev_high = swing_high[-2][1]
    prev_low = swing_low[-2][1]

    choch_type = None
    bos_type = None
    sweep_type = None

    # CHOCH
    if last_close > prev_high:
        choch_type = "bullish"
    elif last_close < prev_low:
        choch_type = "bearish"

    # BOS
    if last_close > last_high:
        bos_type = "bullish"
    elif last_close < last_low:
        bos_type = "bearish"

    # Sweep
    if last_close < last_high and last_close > prev_high:
        sweep_type = "liquidity_bullish"
    elif last_close > last_low and last_close < prev_low:
        sweep_type = "liquidity_bearish"

    return choch_type, bos_type, sweep_type, swing_high, swing_low

# ====== ОПРЕДЕЛЕНИЕ ТРЕНДА 1D ======
def get_daily_trend(symbol):
    df = get_klines(symbol, "1d")
    if df is None:
        return "неизвестно"
    swing_high, swing_low = detect_swings(df)
    if len(swing_high) < 2 or len(swing_low) < 2:
        return "неизвестно"
    if swing_high[-1][1] > swing_high[-2][1] and swing_low[-1][1] > swing_low[-2][1]:
        return "восходящий"
    elif swing_high[-1][1] < swing_high[-2][1] and swing_low[-1][1] < swing_low[-2][1]:
        return "нисходящий"
    else:
        return "флэт"
        # ====== СКАНЕР ======
def scan_symbol(symbol):
    signals = []
    for tf in timeframes:
        df = get_klines(symbol, tf)
        if df is None or df.empty:
            continue
        choch, bos, sweep, _, _ = detect_choch_bos(df)
        if choch or bos or sweep:
            signals.append(f"""
TF: {tf}
CHOCH: {choch}
BOS: {bos}
Sweep: {sweep}
""")
        time.sleep(0.3)  # пауза между запросами
    if signals:
        trend_1d = get_daily_trend(symbol)
        msg = f"📈 Signal detected\nSymbol: {symbol}\nTrend 1D: {trend_1d}\n" + "\n".join(signals) + f"\nTime: {datetime.utcnow()}"
        print(msg)
        send_telegram(msg)

def scan():
    with ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(scan_symbol, symbols)

# ====== ЦИКЛ РАБОТЫ ======
while True:
    hour = (datetime.utcnow().hour + 3) % 24
    if 6 <= hour <= 22:
        scan()
    else:
        print("Сейчас вне рабочего времени. Спим.")
    time.sleep(300)

import time
import requests
import pandas as pd
import datetime

# ====== НАСТРОЙКИ ======
API_KEY = "DeecQb17BmXDUJoDMJlSFrwqQA5fKmHEomLRFcOFRDUTPre6GsXNvtZqH7GA1u47wocRWdWW1q379KtWEg"
BOT_TOKEN = "8504110255:AAHFQnxpm3kcqDQhsfluaetmjB0hgrs7j9U"
CHAT_ID = "454082808"
SCAN_DELAY = 60  # пауза между проверками в секундах
TIMEFRAME = "15m"  # 15-минутные свечи

# ====== ФУНКЦИИ ======
def send_telegram(msg):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    requests.post(url, data={"chat_id": CHAT_ID, "text": msg})

def get_symbols():
    """Получаем список фьючерсных пар (fallback на несколько популярных, если API не работает)"""
    url = "https://open-api.bingx.com/openApi/swap/v2/quote/symbols"
    try:
        r = requests.get(url, params={"apiKey": API_KEY}, timeout=10).json()
        if "data" in r:
            return [s["symbol"] for s in r["data"] if s["symbol"].endswith("USDT")]
    except:
        pass
    print("[WARN] Используем fallback список пар")
    return ["BTC-USDT", "ETH-USDT", "SOL-USDT", "XRP-USDT", "BNB-USDT"]

def get_klines(symbol, tf):
    url = "https://open-api.bingx.com/openApi/swap/v2/quote/klines"
    params = {"symbol": symbol, "interval": tf, "limit": 500, "apiKey": API_KEY}
    for _ in range(3):
        try:
            r = requests.get(url, params=params, timeout=10).json()
            if "data" not in r or not r["data"]:
                print(f"[API] Нет данных от BingX для {symbol} {tf}")
                return None
            df = pd.DataFrame(r["data"])
            for col in ["high","low","close"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
            return df
        except Exception as e:
            print(f"[ERROR] {symbol} {tf}: {e}")
            time.sleep(1)
    return None

# ====== DETECT CHOCH ======
def detect_choch(df):
    if len(df) < 5:
        return None, None, None, None
    swing_high = df["high"].max()
    swing_low = df["low"].min()
    last_close = df["close"].iloc[-1]

    choch = None
    if last_close > swing_high:
        choch = "LONG"
    elif last_close < swing_low:
        choch = "SHORT"

    return choch, last_close, swing_high, swing_low

# ====== SCAN SYMBOL ======
def scan_symbol(symbol):
    print(f"[SCAN] Проверяем {symbol}")
    df = get_klines(symbol, TIMEFRAME)
    if df is None:
        print(f"[SCAN] Нет данных для {symbol} на {TIMEFRAME}")
        return

    choch, last_close, swing_high, swing_low = detect_choch(df)
    if choch:
        msg = f"[SIGNAL] {symbol} {TIMEFRAME} CHoCH: {choch}, Close: {last_close:.2f}, SwingHigh: {swing_high:.2f}, SwingLow: {swing_low:.2f}"
        print(msg)
        send_telegram(msg)
    else:
        print(f"[SCAN] {symbol} без сигнала")

# ====== SCAN LOOP ======
def scan():
    symbols = get_symbols()
    print(f"[INFO] Будут проверены {len(symbols)} пар")

    while True:
        now = datetime.datetime.now()
        if 9 <= now.hour < 23:  # активные часы
            print("=== SCANNING MARKET ===")
            for symbol in symbols:
                scan_symbol(symbol)
            print(f"=== WAIT {SCAN_DELAY}s ===")
        else:
            print("=== Сканер вне активных часов ===")

        time.sleep(SCAN_DELAY)

# ====== START ======
if __name__ == "__main__":
    scan()

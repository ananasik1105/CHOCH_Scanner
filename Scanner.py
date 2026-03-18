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
SCAN_DELAY = 900
TIMEFRAMES = ["1h", "4h", "1d"]
LEN        = 50
LEN_SHORT  = max(5, LEN // 5)
FRESH_BARS = 3
MIN_PROB   = 60    # минимальная вероятность сигнала %
MIN_RR     = 2.0   # минимальный Risk/Reward
SL_BUFFER  = 0.15  # буфер за структурным уровнем (%)

# ====== FLASK ======
app = Flask(__name__)

@app.route("/")
def home():
    return "Scanner Pro is running!"

# ====== ПАМЯТЬ ======
signal_dates  = {}
daily_signals = []

# ====== ВСПОМОГАТЕЛЬНЫЕ ======
def fmt_vol(v):
    if v is None:
        return "N/A"
    if v >= 1_000_000:
        return f"{v/1_000_000:.2f}M"
    if v >= 1_000:
        return f"{v/1_000:.1f}K"
    return f"{v:,.0f}"

def fmt_pct(p):
    if p is None:
        return ""
    arrow = "↑" if p >= 0 else "↓"
    return f"({arrow}{abs(p):.1f}%)"

def get_htf_name(tf):
    return {"1h": "4h", "4h": "1d", "1d": "1d"}.get(tf, "1d")

# ====== API ======
def send_telegram(msg):
    try:
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            data={"chat_id": CHAT_ID, "text": msg, "parse_mode": ""},
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

def get_klines(symbol, tf, limit=500):
    for _ in range(3):
        try:
            r = requests.get(
                "https://open-api.bingx.com/openApi/swap/v2/quote/klines",
                params={"symbol": symbol, "interval": tf, "limit": limit, "apiKey": API_KEY},
                timeout=10
            ).json()
            if "data" not in r or not r["data"]:
                return None
            df = pd.DataFrame(r["data"])
            for col in ["open", "high", "low", "close", "volume"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
            return df.dropna(subset=["high", "low", "close"]).reset_index(drop=True)
        except Exception as e:
            print(f"[ERROR] {symbol} {tf}: {e}")
            time.sleep(1)
    return None

def get_open_interest(symbol):
    try:
        r = requests.get(
            "https://open-api.bingx.com/openApi/swap/v2/quote/openInterest",
            params={"symbol": symbol},
            timeout=10
        ).json()
        if "data" in r and r["data"]:
            oi = r["data"].get("openInterest")
            if oi is not None:
                return float(oi)
    except Exception:
        pass
    return None

# ====== ТЕХНИЧЕСКИЕ ИНДИКАТОРЫ ======
def calc_rsi(closes, period=14):
    if len(closes) < period + 1:
        return None
    s = pd.Series(closes, dtype=float)
    delta = s.diff()
    gain  = delta.clip(lower=0).rolling(period).mean()
    loss  = (-delta.clip(upper=0)).rolling(period).mean()
    rs    = gain / loss.replace(0, np.nan)
    rsi   = 100 - (100 / (1 + rs))
    val   = rsi.iloc[-1]
    return round(float(val), 1) if not np.isnan(val) else None

def calc_ema(values, period=20):
    s = pd.Series(values, dtype=float)
    return float(s.ewm(span=period, adjust=False).mean().iloc[-1])

def vol_ratio(df):
    if df is None or "volume" not in df.columns or len(df) < 3:
        return None
    last = float(df["volume"].iloc[-1])
    past = df["volume"].iloc[-21:-1] if len(df) >= 21 else df["volume"].iloc[:-1]
    avg  = float(past.mean())
    if avg == 0:
        return None
    return round(last / avg, 2)

def calc_volume_info(df):
    if df is None or "volume" not in df.columns or len(df) < 3:
        return None, None
    last = float(df["volume"].iloc[-1])
    past = df["volume"].iloc[-21:-1] if len(df) >= 21 else df["volume"].iloc[:-1]
    avg  = float(past.mean())
    if avg == 0:
        return last, None
    pct = (last / avg - 1) * 100
    return last, round(pct, 1)

def calc_delta_volume(df, lookback=20):
    if df is None or "volume" not in df.columns or "open" not in df.columns or len(df) < 3:
        return None, None, None
    recent = df.tail(lookback)
    buy_vol  = 0.0
    sell_vol = 0.0
    for _, row in recent.iterrows():
        o = float(row["open"])
        c = float(row["close"])
        v = float(row["volume"])
        wick = abs(float(row["high"]) - float(row["low"]))
        body = abs(c - o)
        body_ratio = body / wick if wick > 0 else 0.5
        weighted_v = v * max(0.5, body_ratio)
        if c >= o:
            buy_vol  += weighted_v
        else:
            sell_vol += weighted_v
    delta = buy_vol - sell_vol
    total = buy_vol + sell_vol
    delta_pct = round(delta / total * 100, 1) if total > 0 else 0
    return round(buy_vol, 0), round(sell_vol, 0), delta_pct

def calc_choch_candle_info(df, choch_bar, direction, level):
    if df is None or choch_bar < 0 or choch_bar >= len(df):
        return None, False, None
    bar    = df.iloc[choch_bar]
    open_p = float(bar.get("open", bar["close"])) if "open" in bar.index else float(bar["close"])
    cl     = float(bar["close"])
    hi     = float(bar["high"])
    lo     = float(bar["low"])
    wick   = hi - lo
    body   = abs(cl - open_p)
    body_pct = round(body / wick * 100, 1) if wick > 0 else 0
    if direction == "LONG" and level is not None:
        # Для LONG: тело должно закрыться ПОЛНОСТЬЮ выше уровня (min(open, close) > level)
        body_closed = cl > level and min(cl, open_p) > level
    elif direction == "SHORT" and level is not None:
        # Для SHORT: тело должно закрыться ПОЛНОСТЬЮ ниже уровня (max(open, close) < level)
        body_closed = cl < level and max(cl, open_p) < level
    else:
        body_closed = False
    return body_pct, body_closed, round(body, 6)

# ====== СВИНГИ ПО АЛГОРИТМУ LUXALGO ======
def find_swings_luxalgo(highs, lows, length):
    n          = len(highs)
    swing_high = {}
    swing_low  = {}
    os_state   = 0
    for bar in range(length, n):
        candidate_idx = bar - length
        upper = float(np.max(highs[bar - length + 1: bar + 1]))
        lower = float(np.min(lows[bar  - length + 1: bar + 1]))
        prev_os = os_state
        if highs[candidate_idx] > upper:
            os_state = 0
        elif lows[candidate_idx] < lower:
            os_state = 1
        if os_state == 0 and prev_os != 0:
            swing_high[candidate_idx] = float(highs[candidate_idx])
        if os_state == 1 and prev_os != 1:
            swing_low[candidate_idx]  = float(lows[candidate_idx])
    return swing_high, swing_low

# ====== ДЕТЕКТИРОВАНИЕ CHOCH ======
def detect_choch(df):
    n = len(df)
    if n < LEN * 2 + 10:
        return None, None, None, None, -1
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
        return last_choch_dir, float(closes[-1]), topy, btmy, last_choch_bar
    return None, float(closes[-1]), topy, btmy, -1

# ====== ЛИКВИДНОСТЬ ======
def detect_liquidity_sweep(df, direction, topy, btmy, choch_bar):
    if df is None or choch_bar <= 0:
        return False, None
    search_start = max(0, choch_bar - 15)
    for i in range(search_start, choch_bar):
        row = df.iloc[i]
        lo  = float(row["low"])
        hi  = float(row["high"])
        cl  = float(row["close"])
        if direction == "LONG" and btmy is not None:
            if lo < btmy and cl >= btmy:
                return True, round(btmy, 6)
        elif direction == "SHORT" and topy is not None:
            if hi > topy and cl <= topy:
                return True, round(topy, 6)
    return False, None

def get_close_confirmation(df, direction, level, choch_bar):
    if df is None or level is None or choch_bar < 0 or choch_bar >= len(df):
        return "Wick", None
    bar    = df.iloc[choch_bar]
    open_p = float(bar.get("open", bar["close"])) if "open" in bar.index else float(bar["close"])
    cl     = float(bar["close"])
    hi     = float(bar["high"])
    lo     = float(bar["low"])
    wick   = hi - lo
    if wick == 0:
        return "Wick", round(level, 6)
    body_ratio = abs(cl - open_p) / wick
    if direction == "LONG":
        if cl > level and body_ratio >= 0.4:
            return "Body", round(level, 6)
        return "Wick", round(level, 6)
    else:
        if cl < level and body_ratio >= 0.4:
            return "Body", round(level, 6)
        return "Wick", round(level, 6)

# ====== SL / TP / RR ======
def calc_stop_loss(direction, topy, btmy, buffer_pct=SL_BUFFER):
    if direction == "LONG":
        if btmy is None:
            return None
        return round(btmy * (1 - buffer_pct / 100), 6)
    else:
        if topy is None:
            return None
        return round(topy * (1 + buffer_pct / 100), 6)

def find_liquidity_targets(df, direction, entry_price, n_targets=2):
    if df is None or len(df) < LEN_SHORT * 2 + 5:
        return []
    highs = df["high"].values
    lows  = df["low"].values
    sh, sl = find_swings_luxalgo(highs, lows, LEN_SHORT)
    if direction == "LONG":
        levels = sorted([v for v in sh.values() if v > entry_price * 1.001])
    else:
        levels = sorted([v for v in sl.values() if v < entry_price * 0.999], reverse=True)
    return [round(l, 6) for l in levels[:n_targets]]

def calc_entry_zone(fvg, ob, topy, btmy, direction):
    if fvg is not None:
        entry = round((fvg[0] + fvg[1]) / 2, 6)
        poi_type = "FVG"
        poi_zone = f"{fvg[0]:.5f} — {fvg[1]:.5f}"
        return entry, poi_type, poi_zone
    if ob is not None:
        entry = round((ob[0] + ob[1]) / 2, 6)
        poi_type = "Order Block"
        poi_zone = f"{ob[1]:.5f} — {ob[0]:.5f}"
        return entry, poi_type, poi_zone
    if topy is not None and btmy is not None:
        if direction == "LONG":
            entry = round(btmy + (topy - btmy) * 0.382, 6)
        else:
            entry = round(topy - (topy - btmy) * 0.382, 6)
        poi_type = "Fib 0.618"
        poi_zone = f"{entry:.5f}"
        return entry, poi_type, poi_zone
    return None, None, None

def calc_rr(entry, sl, tp_levels):
    if entry is None or sl is None or not tp_levels:
        return None, []
    risk = abs(entry - sl)
    if risk == 0:
        return None, []
    rr_list = []
    for tp in tp_levels:
        reward = abs(tp - entry)
        rr_list.append(round(reward / risk, 2))
    min_rr = rr_list[0] if rr_list else None
    return min_rr, rr_list

# ====== СТРУКТУРНЫЕ ФИЛЬТРЫ ======
def detect_idm(df, direction):
    if df is None or len(df) < LEN_SHORT * 2 + 5:
        return False
    highs  = df["high"].values
    lows   = df["low"].values
    closes = df["close"].values
    ish, isl = find_swings_luxalgo(highs, lows, LEN_SHORT)
    last_close = closes[-1]
    if direction == "LONG":
        sh_sorted = sorted(ish.items())[-4:]
        if len(sh_sorted) >= 2:
            for i in range(len(sh_sorted) - 1, 0, -1):
                if sh_sorted[i][1] < sh_sorted[i - 1][1]:
                    if last_close > sh_sorted[i][1]:
                        return True
    else:
        sl_sorted = sorted(isl.items())[-4:]
        if len(sl_sorted) >= 2:
            for i in range(len(sl_sorted) - 1, 0, -1):
                if sl_sorted[i][1] > sl_sorted[i - 1][1]:
                    if last_close < sl_sorted[i][1]:
                        return True
    return False

def calc_htf_trend(df_htf):
    if df_htf is None or len(df_htf) < LEN + 5:
        return None
    highs = df_htf["high"].values
    lows  = df_htf["low"].values
    sh, sl = find_swings_luxalgo(highs, lows, LEN)
    sh_vals = [v for _, v in sorted(sh.items())][-4:]
    sl_vals = [v for _, v in sorted(sl.items())][-4:]
    if len(sh_vals) >= 2 and len(sl_vals) >= 2:
        sh_up = sh_vals[-1] > sh_vals[-2]
        sl_up = sl_vals[-1] > sl_vals[-2]
        if sh_up and sl_up:
            return "UP"
        elif not sh_up and not sl_up:
            return "DOWN"
        else:
            return "SIDEWAYS"
    if len(df_htf) >= 21:
        cl  = float(df_htf["close"].iloc[-1])
        ema = calc_ema(df_htf["close"].values, 20)
        return "UP" if cl > ema else "DOWN"
    return None

def is_at_htf_key_level(df_htf, price, threshold_pct=1.0):
    if df_htf is None or price is None:
        return False
    highs = df_htf["high"].values
    lows  = df_htf["low"].values
    sh, sl = find_swings_luxalgo(highs, lows, LEN)
    for level in list(sh.values()) + list(sl.values()):
        if level > 0 and abs(price - level) / level * 100 <= threshold_pct:
            return True
    return False

def calc_trend_signal(df):
    if df is None or len(df) < 21:
        return None
    cl  = float(df["close"].iloc[-1])
    ema = calc_ema(df["close"].values, 20)
    return "UP" if cl > ema else "DOWN"

# ====== ЗОНЫ ВХОДА ======
def calc_fvg(df, direction):
    if df is None or len(df) < 3:
        return None
    highs  = df["high"].values
    lows   = df["low"].values
    closes = df["close"].values
    last_close = closes[-1]
    best = None
    for i in range(max(2, len(df) - 30), len(df)):
        if direction == "LONG":
            if highs[i - 2] < lows[i]:
                fvg_lo = highs[i - 2]
                fvg_hi = lows[i]
                if fvg_hi < last_close:
                    best = (fvg_lo, fvg_hi)
        else:
            if lows[i - 2] > highs[i]:
                fvg_lo = highs[i]
                fvg_hi = lows[i - 2]
                if fvg_lo > last_close:
                    best = (fvg_lo, fvg_hi)
    return best

def find_order_block(df, direction):
    if df is None or "open" not in df.columns or len(df) < 5:
        return None
    search_end = len(df) - 1
    for i in range(search_end, max(0, search_end - 20), -1):
        bar    = df.iloc[i]
        open_p = float(bar.get("open", bar["close"])) if "open" in bar.index else float(bar["close"])
        cl     = float(bar["close"])
        if direction == "LONG" and cl < open_p:
            return (float(bar["high"]), float(bar["low"]))
        elif direction == "SHORT" and cl > open_p:
            return (float(bar["high"]), float(bar["low"]))
    return None

# ====== АНАЛИЗ КОНФЛИКТА ТФ ======
def analyze_tf_conflict(direction, trend_4h, trend_1d):
    conflicts = []
    if trend_4h and trend_4h != ("UP" if direction == "LONG" else "DOWN"):
        conflicts.append("4h")
    if trend_1d and trend_1d != ("UP" if direction == "LONG" else "DOWN"):
        conflicts.append("1d")
    n = len(conflicts)
    if n == 0:
        return {"status": "OK", "message": "Все ТФ подтверждают направление", "conflicts": []}
    elif n == 1:
        return {"status": "WARNING", "message": f"Тренд на {conflicts[0]} против сигнала", "conflicts": conflicts}
    else:
        return {"status": "DANGER", "message": f"Тренды {' и '.join(conflicts)} против сигнала", "conflicts": conflicts}

# ====== РАСЧЁТ ВЕРОЯТНОСТИ ======
def calc_probability(sweep, close_conf, vol_r, idm, bos_count, htf_trend, direction, rsi,
                     body_pct, delta_pct, conflicts_count):
    score = 20
    if close_conf == "Body":
        score += 15
    if sweep:
        score += 15
    if vol_r is not None:
        if vol_r >= 2.0:
            score += 15
        elif vol_r >= 1.5:
            score += 10
        elif vol_r >= 1.0:
            score += 5
        elif vol_r < 0.7:
            score -= 5
    if idm:
        score += 10
    if htf_trend is not None:
        htf_aligned = (direction == "LONG" and htf_trend == "UP") or \
                      (direction == "SHORT" and htf_trend == "DOWN")
        if htf_aligned:
            score += 15
        elif htf_trend == "SIDEWAYS":
            score += 2
        else:
            score -= 8
    if rsi is not None:
        if (direction == "LONG" and rsi > 70) or (direction == "SHORT" and rsi < 30):
            score -= 12
        elif (direction == "LONG" and rsi < 40) or (direction == "SHORT" and rsi > 60):
            score += 5
        elif 45 <= rsi <= 55:
            score += 3
    if body_pct is not None:
        if body_pct >= 60:
            score += 8
        elif body_pct >= 40:
            score += 4
        elif body_pct < 25:
            score -= 5
    if delta_pct is not None:
        if direction == "LONG" and delta_pct > 20:
            score += 7
        elif direction == "SHORT" and delta_pct < -20:
            score += 7
        elif direction == "LONG" and delta_pct < -10:
            score -= 5
        elif direction == "SHORT" and delta_pct > 10:
            score -= 5
    if conflicts_count == 1:
        score -= 5
    elif conflicts_count >= 2:
        score -= 15
    return max(5, min(95, score))

def strength_label(prob):
    if prob >= 75:
        return "СИЛЬНЫЙ"
    elif prob >= 60:
        return "СРЕДНИЙ"
    else:
        return "СЛАБЫЙ"

def build_verdict(direction, htf_trend, htf_tf, rr, prob):
    htf_aligned = htf_trend is not None and (
        (direction == "LONG" and htf_trend == "UP") or
        (direction == "SHORT" and htf_trend == "DOWN")
    )
    htf_opposite = htf_trend is not None and (
        (direction == "LONG" and htf_trend == "DOWN") or
        (direction == "SHORT" and htf_trend == "UP")
    )
    rr_str = f"R:R {rr:.1f}" if rr else "R:R N/A"
    if htf_aligned:
        return f"СИГНАЛ ПО ТРЕНДУ ({htf_tf}: совпадает) | {rr_str}"
    elif htf_opposite:
        return f"СИГНАЛ НА КОРРЕКЦИЮ (против {htf_tf} тренда) | {rr_str}"
    else:
        return f"НЕЙТРАЛЬНЫЙ КОНТЕКСТ | {rr_str}"

# ====== ФОРМАТИРОВАНИЕ СИГНАЛА ======
def build_signal_message(symbol, tf, direction, last_close, topy, btmy, choch_bar,
                          df_signal, df_htf, df_4h, df_1d,
                          sl, tp_levels, rr, entry, poi_type, poi_zone):
    now_str   = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    htf_tf    = get_htf_name(tf)

    sweep, sweep_lvl = detect_liquidity_sweep(df_signal, direction, topy, btmy, choch_bar)
    level    = topy if direction == "LONG" else btmy
    close_conf, _ = get_close_confirmation(df_signal, direction, level, choch_bar)

    vol_r      = vol_ratio(df_signal)
    idm        = detect_idm(df_signal, direction)
    htf_trend  = calc_htf_trend(df_htf)
    at_htf_lvl = is_at_htf_key_level(df_htf, last_close)
    rsi        = calc_rsi(df_signal["close"].values) if df_signal is not None else None
    fvg        = calc_fvg(df_signal, direction)
    ob         = find_order_block(df_signal, direction)

    trend_4h = calc_trend_signal(df_4h)
    trend_1d = calc_trend_signal(df_1d)
    tf_conflict = analyze_tf_conflict(direction, trend_4h, trend_1d)

    body_pct, body_closed, body_abs = calc_choch_candle_info(df_signal, choch_bar, direction, level)
    buy_vol, sell_vol, delta_pct    = calc_delta_volume(df_signal)

    prob     = calc_probability(sweep, close_conf, vol_r, idm, 0, htf_trend, direction, rsi,
                                body_pct, delta_pct, len(tf_conflict["conflicts"]))
    strength = strength_label(prob)
    verdict  = build_verdict(direction, htf_trend, htf_tf, rr, prob)

    body_pct_str  = f"{body_pct}%" if body_pct is not None else "N/A"
    body_abs_str  = f"{body_abs}" if body_abs is not None else "N/A"
    body_conf_str = "ТЕЛО" if body_closed else "ТЕНЬ"

    if sweep:
        sweep_str = f"ДА — ликвидность собрана у {sweep_lvl}"
    else:
        side = "Buy-Side" if direction == "LONG" else "Sell-Side"
        sweep_str = f"НЕТ — {side} не собрана"

    vol_r_str = f"{vol_r}x" if vol_r else "N/A"
    if vol_r and vol_r >= 1.5:
        vol_qual = "ВЫСОКИЙ — сильное подтверждение"
    elif vol_r and vol_r >= 1.0:
        vol_qual = "СРЕДНИЙ — наблюдать"
    elif vol_r:
        vol_qual = "НИЗКИЙ — риск манипуляции"
    else:
        vol_qual = "Данные недоступны"

    if delta_pct is not None:
        if direction == "LONG":
            delta_conf = "ПОДТВЕРЖДАЕТ (покупатели доминируют)" if delta_pct > 10 else "НЕЙТРАЛЬНО/ПРОТИВ"
        else:
            delta_conf = "ПОДТВЕРЖДАЕТ (продавцы доминируют)" if delta_pct < -10 else "НЕЙТРАЛЬНО/ПРОТИВ"
        delta_str = f"Buy: {fmt_vol(buy_vol)} | Sell: {fmt_vol(sell_vol)} | Delta: {delta_pct:+.1f}% — {delta_conf}"
    else:
        delta_str = "N/A"

    cf_status = tf_conflict["status"]
    if cf_status == "OK":
        conflict_str = "ВСЕ ТФ СОВПАДАЮТ"
    elif cf_status == "WARNING":
        conflict_str = f"ОСТОРОЖНО: {tf_conflict['message']}"
    else:
        conflict_str = f"ОПАСНО: {tf_conflict['message']}"

    idm_str = "ПРОБИТА" if idm else "НЕ ПРОБИТА"

    if htf_trend == "UP":
        htf_str = "Восходящий (UP)"
    elif htf_trend == "DOWN":
        htf_str = "Нисходящий (DOWN)"
    elif htf_trend == "SIDEWAYS":
        htf_str = "Боковой (SIDEWAYS)"
    else:
        htf_str = "N/A"

    rsi_str = f"{rsi}" if rsi is not None else "N/A"
    if rsi:
        if rsi > 70:
            rsi_note = " — ПЕРЕКУПЛЕННОСТЬ"
        elif rsi < 30:
            rsi_note = " — ПЕРЕПРОДАННОСТЬ"
        else:
            rsi_note = ""
    else:
        rsi_note = ""

    fvg_str = f"{fvg[0]:.5f} — {fvg[1]:.5f}" if fvg else "Не найдена"
    ob_str  = f"{ob[1]:.5f} — {ob[0]:.5f}" if ob else "Не найден"

    sl_str = f"{sl:.5f}" if sl is not None else "N/A"
    rr_str = f"{rr:.1f}:1" if rr is not None else "N/A"

    tp_lines = ""
    for i, tp in enumerate(tp_levels):
        pct_to_tp = abs(tp - last_close) / last_close * 100
        tp_lines += f"  TP{i+1}: {tp:.5f} (+{pct_to_tp:.2f}%)\n"
    if not tp_lines:
        tp_lines = "  N/A\n"

    sl_pct = abs(last_close - sl) / last_close * 100 if sl else 0
    entry_str = f"{entry:.5f}" if entry else "N/A"

    def tf_arrow(t):
        return {"UP": "UP", "DOWN": "DOWN", "SIDEWAYS": "SIDE"}.get(t, "N/A")
    trend_sig = calc_trend_signal(df_signal)

    msg = (
        f"{'[LONG]' if direction=='LONG' else '[SHORT]'} CHoCH {tf} | {symbol}\n"
        f"Время: {now_str}\n"
        f"\n"
        f"=== КАЧЕСТВО СИГНАЛА ===\n"
        f"Вероятность: {prob}% | {strength}\n"
        f"Вердикт: {verdict}\n"
        f"\n"
        f"=== ЦЕНА ВХОДА (POI — лимитный ордер) ===\n"
        f"Тип POI: {poi_type if poi_type else 'N/A'}\n"
        f"Зона: {poi_zone if poi_zone else 'N/A'}\n"
        f"Вход: {entry_str}\n"
        f"\n"
        f"=== УПРАВЛЕНИЕ РИСКОМ ===\n"
        f"Стоп-лосс: {sl_str} (-{sl_pct:.2f}% от Close)\n"
        f"[SL за структурным {'лоу' if direction=='LONG' else 'хаем'} + буфер {SL_BUFFER}%]\n"
        f"{tp_lines}"
        f"Risk:Reward: {rr_str}\n"
        f"\n"
        f"=== CHoCH СВЕЧА (BAR {choch_bar}) ===\n"
        f"Close: {last_close:.5f}\n"
        f"Закрытие: {body_conf_str} (тело {body_pct_str} от диапазона, {body_abs_str} ед.)\n"
        f"\n"
        f"=== ПОДТВЕРЖДЕНИЕ ===\n"
        f"Liquidity Sweep: {sweep_str}\n"
        f"IDM (внутр. структура): {idm_str}\n"
        f"HTF ({htf_tf}) тренд: {htf_str}\n"
        f"У ключевого уровня HTF: {'ДА' if at_htf_lvl else 'НЕТ'}\n"
        f"Конфликт ТФ: {conflict_str}\n"
        f"\n"
        f"=== ОБЪЁМ И МАНИПУЛЯЦИИ ===\n"
        f"Объём CHoCH бара: {vol_r_str} — {vol_qual}\n"
        f"Дельта (20 бар): {delta_str}\n"
        f"\n"
        f"=== ЗОНЫ POI ===\n"
        f"FVG: {fvg_str}\n"
        f"Order Block: {ob_str}\n"
        f"\n"
        f"=== МУЛЬТИТАЙМФРЕЙМ ===\n"
        f"Тренд: {tf}: {tf_arrow(trend_sig)} | 4h: {tf_arrow(trend_4h)} | 1d: {tf_arrow(trend_1d)}\n"
        f"RSI-14: {rsi_str}{rsi_note}\n"
    )
    return msg

# ====== ХРАНЕНИЕ И ПРОВЕРКА СИГНАЛОВ ======
def store_signal(symbol, tf, direction, close_price, topy, btmy,
                 prob, fvg, vol_r, htf_conflict, sl, tp_levels, rr, entry):
    today = datetime.date.today().isoformat()
    daily_signals.append({
        "date":        today,
        "time":        datetime.datetime.now(),
        "symbol":      symbol,
        "tf":          tf,
        "direction":   direction,
        "close":       close_price,
        "entry":       entry,
        "sl":          sl,
        "tp_levels":   tp_levels,
        "rr":          rr,
        "topy":        topy,
        "btmy":        btmy,
        "prob":        prob,
        "fvg":         fvg,
        "vol_r":       vol_r,
        "htf_conflict": htf_conflict,
        "outcome":     None,
        "outcome_price": None,
    })

def check_signal_outcome(sig):
    if sig.get("outcome") in ("win", "loss"):
        return sig["outcome"]
    df = get_klines(sig["symbol"], sig["tf"], limit=50)
    if df is None:
        return "open"
    highs  = df["high"].values
    lows   = df["low"].values
    sl     = sig.get("sl")
    tp1    = sig["tp_levels"][0] if sig.get("tp_levels") else None
    if sl is None or tp1 is None:
        return "open"
    direction = sig["direction"]
    for i in range(len(df)):
        hi = highs[i]
        lo = lows[i]
        if direction == "LONG":
            if lo <= sl:
                sig["outcome"] = "loss"
                sig["outcome_price"] = sl
                return "loss"
            if hi >= tp1:
                sig["outcome"] = "win"
                sig["outcome_price"] = tp1
                return "win"
        else:
            if hi >= sl:
                sig["outcome"] = "loss"
                sig["outcome_price"] = sl
                return "loss"
            if lo <= tp1:
                sig["outcome"] = "win"
                sig["outcome_price"] = tp1
                return "win"
    return "open"

# ====== ДНЕВНОЙ ОТЧЁТ ======
def build_daily_report():
    today      = datetime.date.today().isoformat()
    today_sigs = [s for s in daily_signals if s["date"] == today]
    if not today_sigs:
        return None

    for s in today_sigs:
        if s["outcome"] is None:
            s["outcome"] = check_signal_outcome(s)

    total      = len(today_sigs)
    long_sigs  = [s for s in today_sigs if s["direction"] == "LONG"]
    short_sigs = [s for s in today_sigs if s["direction"] == "SHORT"]
    entered    = [s for s in today_sigs if s["outcome"] in ("win", "loss")]
    wins       = [s for s in entered if s["outcome"] == "win"]
    losses     = [s for s in entered if s["outcome"] == "loss"]
    open_sigs  = [s for s in today_sigs if s["outcome"] not in ("win", "loss")]

    risk_per_trade = 1.0
    total_earned = 0.0
    total_lost   = 0.0
    for s in entered:
        rr = s.get("rr") or 2.0
        if s["outcome"] == "win":
            total_earned += risk_per_trade * rr
        else:
            total_lost += risk_per_trade
    net_pnl = total_earned - total_lost
    pnl_str    = f"+{net_pnl:.1f}%" if net_pnl >= 0 else f"{net_pnl:.1f}%"
    earned_str = f"+{total_earned:.1f}%"
    lost_str   = f"-{total_lost:.1f}%"
    win_rate   = round(len(wins) / len(entered) * 100, 1) if entered else 0

    tf_blocks = ""
    for tf in TIMEFRAMES:
        sigs = [s for s in today_sigs if s["tf"] == tf]
        if not sigs:
            continue
        ent  = [s for s in sigs if s["outcome"] in ("win", "loss")]
        w    = [s for s in ent if s["outcome"] == "win"]
        wr   = round(len(w) / len(ent) * 100, 1) if ent else 0
        tf_blocks += f"  {tf}: {len(sigs)} сигналов | {len(w)}/{len(ent)} прибыльных ({wr}%)\n"

    errors = []
    low_vol_err  = sum(1 for s in losses if s.get("vol_r") and s["vol_r"] < 1.0)
    htf_conf_err = sum(1 for s in losses if s.get("htf_conflict") and s["htf_conflict"].get("conflicts"))
    fvg_err      = sum(1 for s in losses if s.get("fvg") is None)
    if low_vol_err:
        errors.append(f"Низкий объём при входе: {low_vol_err} сделок")
    if htf_conf_err:
        errors.append(f"Вход против старшего ТФ: {htf_conf_err} сделок")
    if fvg_err:
        errors.append(f"Отсутствие FVG/OB зоны: {fvg_err} сделок")
    if not errors:
        errors.append("Ошибки не выявлены")
    errors_str = "\n  ".join(errors)

    pair_stats = {}
    for s in entered:
        sym = s["symbol"]
        if sym not in pair_stats:
            pair_stats[sym] = {"w": 0, "l": 0}
        if s["outcome"] == "win":
            pair_stats[sym]["w"] += 1
        else:
            pair_stats[sym]["l"] += 1
    sorted_pairs = sorted(pair_stats.items(), key=lambda x: x[1]["w"]/(x[1]["w"]+x[1]["l"]), reverse=True)
    top3_good = sorted_pairs[:3]
    top3_bad  = sorted(pair_stats.items(), key=lambda x: x[1]["l"]/(x[1]["w"]+x[1]["l"]+0.001), reverse=True)[:3]

    def pair_line(sym, res):
        cnt = res["w"] + res["l"]
        return f"  {sym}: {res['w']}/{cnt} ({round(res['w']/cnt*100)}%)\n"

    good_block = "".join(pair_line(s, r) for s, r in top3_good) or "  Нет данных\n"
    bad_block  = "".join(pair_line(s, r) for s, r in top3_bad)  or "  Нет данных\n"

    date_fmt    = datetime.date.today().strftime("%d.%m.%Y")
    report_time = datetime.datetime.now().strftime("%H:%M")

    report = (
        f"==================================================\n"
        f"ДНЕВНОЙ ОТЧЁТ — Smart Money Scanner\n"
        f"Дата: {date_fmt} | Время: {report_time}\n"
        f"==================================================\n"
        f"\n"
        f"ОБЩАЯ СТАТИСТИКА\n"
        f"Сигналов за день: {total}\n"
        f"  LONG: {len(long_sigs)} | SHORT: {len(short_sigs)}\n"
        f"{tf_blocks}"
        f"\n"
        f"РЕЗУЛЬТАТЫ СДЕЛОК\n"
        f"Отработано: {len(entered)} из {total}\n"
        f"  Прибыльных (TP): {len(wins)}\n"
        f"  Убыточных (SL):  {len(losses)}\n"
        f"  Открытых:        {len(open_sigs)}\n"
        f"Win Rate: {win_rate}%\n"
        f"\n"
        f"P&L (при риске {risk_per_trade}% на сделку)\n"
        f"  Заработано: {earned_str}\n"
        f"  Потеряно:   {lost_str}\n"
        f"  ИТОГО:      {pnl_str}\n"
        f"\n"
        f"ЛУЧШИЕ ПАРЫ\n"
        f"{good_block}"
        f"\n"
        f"УБЫТОЧНЫЕ ПАРЫ\n"
        f"{bad_block}"
        f"\n"
        f"АНАЛИЗ ОШИБОК (причины убытков)\n"
        f"  {errors_str}\n"
        f"\n"
        f"==================================================\n"
        f"ИТОГ: {pnl_str} | {len(wins)}/{len(entered)} прибыльных ({win_rate}%)\n"
        f"=================================================="
    )
    return report

def daily_report_loop():
    sent_today = None
    while True:
        now   = datetime.datetime.now()
        today = datetime.date.today().isoformat()
        if now.hour == 23 and now.minute >= 55 and sent_today != today:
            print("[INFO] Генерация дневного отчёта...")
            report = build_daily_report()
            if report:
                send_telegram(report)
                sent_today = today
                print("[INFO] Дневной отчёт отправлен.")
            else:
                print("[INFO] Нет сигналов за день.")
        time.sleep(60)

# ====== ГЛАВНЫЙ СКАНЕР ======
def scan_symbol(symbol, df_cache, oi):
    today = datetime.date.today().isoformat()
    for tf in TIMEFRAMES:
        key = f"{symbol}_{tf}"
        if signal_dates.get(key) == today:
            continue
        df = df_cache.get(tf)
        if df is None:
            continue

        choch, last_close, topy, btmy, choch_bar = detect_choch(df)
        if not choch:
            continue

        level = topy if choch == "LONG" else btmy

        close_conf, _ = get_close_confirmation(df, choch, level, choch_bar)
        if close_conf != "Body":
            print(f"[SKIP] {symbol} {tf} {choch} — закрытие тенью")
            continue

        htf_name = get_htf_name(tf)
        df_htf   = df_cache.get(htf_name, df)
        df_4h    = df_cache.get("4h") if tf != "4h" else df
        df_1d    = df_cache.get("1d") if tf != "1d" else df

        sweep, _    = detect_liquidity_sweep(df, choch, topy, btmy, choch_bar)
        vol_r       = vol_ratio(df)
        idm         = detect_idm(df, choch)
        htf_trend   = calc_htf_trend(df_htf)
        rsi         = calc_rsi(df["close"].values) if len(df) > 14 else None
        fvg         = calc_fvg(df, choch)
        ob          = find_order_block(df, choch)
        trend_4h    = calc_trend_signal(df_4h)
        trend_1d    = calc_trend_signal(df_1d)
        tf_conflict = analyze_tf_conflict(choch, trend_4h, trend_1d)

        body_pct, body_closed, body_abs = calc_choch_candle_info(df, choch_bar, choch, level)
        _, _, delta_pct = calc_delta_volume(df)

        if not body_closed:
            print(f"[SKIP] {symbol} {tf} {choch} — тело не закрылось за уровнем")
            continue

        if tf_conflict["status"] == "DANGER":
            print(f"[SKIP] {symbol} {tf} {choch} — конфликт всех ТФ")
            continue

        sl = calc_stop_loss(choch, topy, btmy)
        if sl is None:
            print(f"[SKIP] {symbol} {tf} {choch} — нет структурного уровня для SL")
            continue

        entry, poi_type, poi_zone = calc_entry_zone(fvg, ob, topy, btmy, choch)
        if entry is None:
            entry = last_close

        tp_levels = find_liquidity_targets(df, choch, entry, n_targets=2)
        if not tp_levels:
            risk_dist = abs(entry - sl)
            if choch == "LONG":
                tp_levels = [round(entry + 2 * risk_dist, 6), round(entry + 3 * risk_dist, 6)]
            else:
                tp_levels = [round(entry - 2 * risk_dist, 6), round(entry - 3 * risk_dist, 6)]

        min_rr, rr_list = calc_rr(entry, sl, tp_levels)

        if min_rr is None or min_rr < MIN_RR:
            print(f"[SKIP] {symbol} {tf} {choch} — R:R {min_rr} < {MIN_RR}")
            continue

        prob = calc_probability(sweep, close_conf, vol_r, idm, 0, htf_trend, choch, rsi,
                                body_pct, delta_pct, len(tf_conflict["conflicts"]))

        if prob < MIN_PROB:
            print(f"[SKIP] {symbol} {tf} {choch} — вероятность {prob}% < {MIN_PROB}%")
            continue

        msg = build_signal_message(
            symbol, tf, choch, last_close, topy, btmy, choch_bar,
            df, df_htf, df_4h, df_1d,
            sl, tp_levels, min_rr, entry, poi_type, poi_zone
        )

        send_telegram(msg)
        store_signal(symbol, tf, choch, last_close, topy, btmy,
                     prob, fvg, vol_r, tf_conflict, sl, tp_levels, min_rr, entry)
        signal_dates[key] = today
        print(f"[SIGNAL] {symbol} {tf} {choch} prob={prob}% RR={min_rr} entry={entry} SL={sl}")


def main_loop():
    print(f"[INFO] Сканер запущен. Мин. вероятность: {MIN_PROB}%, мин. R:R: {MIN_RR}:1", flush=True)
    time.sleep(3)  # Даём Flask время стартовать
    while True:
        print("[INFO] === Новый цикл сканирования ===", flush=True)
        try:
            symbols = get_symbols()
            for sig in list(daily_signals):
                if sig.get("outcome") not in ("win", "loss"):
                    check_signal_outcome(sig)
            for symbol in symbols:
                try:
                    df_cache = {}
                    oi = get_open_interest(symbol)
                    for tf in list(set(TIMEFRAMES + ["4h", "1d"])):
                        df = get_klines(symbol, tf)
                        if df is not None:
                            df_cache[tf] = df
                    scan_symbol(symbol, df_cache, oi)
                    time.sleep(0.25)
                except Exception as e:
                    print(f"[ERROR] {symbol}: {e}")
        except Exception as e:
            print(f"[ERROR] main_loop: {e}")
        print(f"[INFO] Цикл завершён. Ожидание {SCAN_DELAY} сек...")
        time.sleep(SCAN_DELAY)


if __name__ == "__main__":
    import sys
    print("[START] Scanner starting...", flush=True)
    Thread(target=main_loop, daemon=True).start()
    Thread(target=daily_report_loop, daemon=True).start()
    print("[START] Background threads started", flush=True)
    port = int(os.environ.get("PORT", 5000))
    print(f"[START] Flask starting on port {port}", flush=True)
    app.run(host="0.0.0.0", port=port, threaded=True)

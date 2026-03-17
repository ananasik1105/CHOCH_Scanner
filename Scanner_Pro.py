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
SCAN_DELAY = 900      # 15 минут между проверками
TIMEFRAMES = ["1h", "4h", "1d"]
LEN        = 50       # CHoCH Detection Period (LuxAlgo default)
LEN_SHORT  = max(5, LEN // 5)   # для IDM и внутренней структуры
FRESH_BARS = 3        # CHoCH считается свежим если в последних N барах

# ====== FLASK ======
app = Flask(__name__)

@app.route("/")
def home():
    return "Scanner is running!"

# ====== ПАМЯТЬ: 1 сигнал на монету+таймфрейм в день ======
signal_dates  = {}
daily_signals = []  # хранит все сигналы текущего дня для отчёта

# ====== ВСПОМОГАТЕЛЬНЫЕ ======
def fmt_vol(v):
    if v is None:
        return "N/A"
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

def vol_ratio(df):
    if df is None or "volume" not in df.columns or len(df) < 3:
        return None
    last = float(df["volume"].iloc[-1])
    past = df["volume"].iloc[-21:-1] if len(df) >= 21 else df["volume"].iloc[:-1]
    avg  = float(past.mean())
    if avg == 0:
        return None
    return round(last / avg, 2)

def candle_body_pct(df, bar_idx=-1):
    if df is None or "open" not in df.columns or len(df) < 1:
        return None
    bar  = df.iloc[bar_idx]
    wick = abs(float(bar["high"]) - float(bar["low"]))
    if wick == 0:
        return None
    body = abs(float(bar["close"]) - float(bar["open"]))
    return round(body / wick * 100, 1)

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

# ====== ЭТАП 1: БАЗОВЫЕ ФИЛЬТРЫ ======

def detect_liquidity_sweep(df, direction, topy, btmy, choch_bar):
    """
    LONG: до CHOCH цена кратко пробила btmy (wick) и закрылась выше → Buy-Side Liquidity swept
    SHORT: до CHOCH цена кратко пробила topy (wick) и закрылась ниже → Sell-Side Liquidity swept
    """
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
    """
    Body: тело свечи пересекло уровень (body_ratio >= 40% и close за уровнем)
    Wick: только тень пробила, тело слабое
    """
    if df is None or level is None or choch_bar < 0 or choch_bar >= len(df):
        return "Body", None
    bar    = df.iloc[choch_bar]
    open_p = float(bar.get("open", bar["close"])) if "open" in bar.index else float(bar["close"])
    cl     = float(bar["close"])
    hi     = float(bar["high"])
    lo     = float(bar["low"])
    wick   = hi - lo
    if wick == 0:
        return "Body", round(level, 6)
    body_ratio = abs(cl - open_p) / wick
    if direction == "LONG":
        if cl > level and body_ratio >= 0.4:
            return "Body", round(level, 6)
        return "Wick", round(level, 6)
    else:
        if cl < level and body_ratio >= 0.4:
            return "Body", round(level, 6)
        return "Wick", round(level, 6)

# ====== ЭТАП 2: ПРОДВИНУТЫЕ ФИЛЬТРЫ ======

def detect_idm(df, direction):
    """
    IDM — внутренняя структура рынка пробита.
    LONG: цена сломала локальный Lower High (внутренний) → IDM True
    SHORT: цена сломала локальный Higher Low (внутренний) → IDM True
    """
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

def count_bos(df, direction):
    """
    Считает последнюю серию последовательных BOS в направлении предыдущего тренда
    (противоположного к CHoCH). LONG CHoCH → считаем bearish BOS (lower lows).
    """
    if df is None or len(df) < LEN_SHORT * 2 + 5:
        return 0
    highs = df["high"].values
    lows  = df["low"].values
    sh, sl = find_swings_luxalgo(highs, lows, LEN_SHORT)
    if direction == "LONG":
        vals = [v for _, v in sorted(sl.items())]
    else:
        vals = [v for _, v in sorted(sh.items())]
    if len(vals) < 2:
        return 0
    count = 0
    for i in range(len(vals) - 1, 0, -1):
        if direction == "LONG" and vals[i] < vals[i - 1]:
            count += 1
        elif direction == "SHORT" and vals[i] > vals[i - 1]:
            count += 1
        else:
            break
    return count

def calc_htf_trend(df_htf):
    """
    Тренд на старшем ТФ по рыночной структуре (HH/HL или LH/LL).
    Fallback: сравнение цены с EMA(20).
    """
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
        cl = float(df_htf["close"].iloc[-1])
        ema = calc_ema(df_htf["close"].values, 20)
        return "UP" if cl > ema else "DOWN"
    return None

def is_at_htf_key_level(df_htf, price, threshold_pct=1.0):
    """Цена находится рядом с ключевым уровнем свинга на старшем ТФ (в пределах threshold_pct%)"""
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
    """Тренд на рабочем ТФ по EMA(20)"""
    if df is None or len(df) < 21:
        return None
    cl  = float(df["close"].iloc[-1])
    ema = calc_ema(df["close"].values, 20)
    return "UP" if cl > ema else "DOWN"

# ====== ЭТАП 3: ЗОНЫ ВХОДА ======

def calc_fvg(df, direction):
    """
    Bullish FVG (LONG): highs[i-2] < lows[i] → gap
    Bearish FVG (SHORT): lows[i-2] > highs[i] → gap
    Ищем последний FVG ближайший к текущей цене.
    """
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

def calc_fibonacci(topy, btmy, direction):
    if topy is None or btmy is None:
        return None
    if direction == "LONG":
        fib_0, fib_100 = btmy, topy
    else:
        fib_0, fib_100 = topy, btmy
    fib_50 = (fib_0 + fib_100) / 2
    return fib_100, fib_50, fib_0

def find_order_block(df, direction):
    """
    Order Block: последняя противоположная свеча перед импульсом CHOCH.
    LONG → последняя медвежья свеча (close < open) в последних 20 барах.
    SHORT → последняя бычья свеча (close > open).
    """
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

def analyze_tf_conflict(direction, trend_sig, trend_4h, trend_1d, tf):
    """
    Проверяет конфликт между направлением сигнала и трендами на других ТФ.
    Возвращает dict: status, message, action, conflicts_count
    """
    conflicts = []
    if trend_4h and trend_4h != ("UP" if direction == "LONG" else "DOWN"):
        conflicts.append("4h")
    if trend_1d and trend_1d != ("UP" if direction == "LONG" else "DOWN"):
        conflicts.append("1d")

    n = len(conflicts)
    if n == 0:
        return {
            "status": "OK",
            "message": "Все ТФ подтверждают направление",
            "action": "СТАНДАРТНЫЙ РАЗМЕР ПОЗИЦИИ",
            "conflicts": []
        }
    elif n == 1:
        return {
            "status": "WARNING",
            "message": f"Тренд на {conflicts[0]} против сигнала",
            "action": "УМЕНЬШИТЬ ПОЗИЦИЮ",
            "conflicts": conflicts
        }
    else:
        return {
            "status": "DANGER",
            "message": f"Тренды {' и '.join(conflicts)} против сигнала",
            "action": "МИНИМАЛЬНЫЙ РАЗМЕР / ПРОПУСТИТЬ",
            "conflicts": conflicts
        }

# ====== АНАЛИЗ ОБЪЁМА С КОНТЕКСТОМ ======

def volume_context(vol_r):
    """Возвращает смысловую интерпретацию объёма."""
    if vol_r is None:
        return "N/A", "данные недоступны"
    if vol_r >= 2.0:
        return "очень высокий", "крупный игрок входит — сильное подтверждение"
    elif vol_r >= 1.5:
        return "высокий", "движение подтверждено, интерес есть"
    elif vol_r >= 1.0:
        return "средний", "нейтрально, наблюдать за развитием"
    elif vol_r >= 0.7:
        return "низкий", "движение слабо подтверждено, риск фейка"
    else:
        return "очень низкий", "движение не подтверждено — высокий риск ловушки"

# ====== РАЗМЕР ПОЗИЦИИ ======

def calc_position_sizing(sweep, vol_r, rsi, direction, conflicts_count, close_conf, idm):
    """
    Рекомендует % риска от депозита.
    База: 1.0%. Каждый негативный фактор снижает размер.
    """
    risk = 1.0
    reasons = []

    if conflicts_count == 1:
        risk -= 0.25
        reasons.append("конфликт ТФ")
    elif conflicts_count >= 2:
        risk -= 0.5
        reasons.append("конфликт нескольких ТФ")

    if vol_r is not None and vol_r < 1.0:
        risk -= 0.25
        reasons.append("низкий объём")

    if rsi is not None:
        if (direction == "LONG" and rsi > 70) or (direction == "SHORT" and rsi < 30):
            risk -= 0.25
            reasons.append("RSI в экстремуме")

    if not sweep:
        risk -= 0.1
        reasons.append("нет ликвидационного свипа")

    if close_conf == "Wick":
        risk -= 0.1
        reasons.append("закрытие тенью")

    risk = max(0.25, round(risk, 2))
    reason_str = " + ".join(reasons) if reasons else "все факторы позитивны"
    return risk, reason_str

# ====== СЦЕНАРИИ РАЗВИТИЯ ======

def build_scenarios(direction, last_close, topy, btmy, fvg, ob):
    """
    Генерирует три сценария: бычий, медвежий, наблюдение.
    """
    if fvg:
        wait_zone = f"зона FVG {fvg[0]:.5f}–{fvg[1]:.5f}"
    elif ob:
        wait_zone = f"зона OB {ob[1]:.5f}–{ob[0]:.5f}"
    else:
        mid = round((topy + btmy) / 2, 6) if topy and btmy else None
        wait_zone = f"уровень 0.5 Fib ({mid})" if mid else "ближайший FVG/OB"

    if direction == "LONG":
        bull = f"объём >1.5x + закрепление выше {round(topy, 5) if topy else '?'} → продолжение роста"
        bear = f"возврат и закрытие ниже {round(btmy, 5) if btmy else '?'} = фейк, сигнал аннулирован"
        watch = f"ждать отката в {wait_zone} для входа в лонг"
    else:
        bull = f"возврат и закрытие выше {round(topy, 5) if topy else '?'} = фейк, сигнал аннулирован"
        bear = f"объём >1.5x + закрепление ниже {round(btmy, 5) if btmy else '?'} → продолжение падения"
        watch = f"ждать отскока в {wait_zone} для входа в шорт"

    return bull, bear, watch

# ====== РАСЧЕТ ВЕРОЯТНОСТИ ======

def calc_probability(sweep, close_conf, vol_r, idm, bos_count,
                     htf_trend, direction, rsi):
    score = 25

    if sweep:
        score += 15
    close_body = close_conf == "Body"
    if close_body:
        score += 10
    if vol_r is not None:
        if vol_r >= 1.5:
            score += 12
        elif vol_r >= 1.0:
            score += 6
        elif vol_r >= 0.8:
            score += 2

    if idm:
        score += 12
    score += min(bos_count * 2, 10)

    if htf_trend is not None:
        htf_aligned = (direction == "LONG" and htf_trend == "UP") or \
                      (direction == "SHORT" and htf_trend == "DOWN")
        if htf_aligned:
            score += 15
        elif htf_trend == "SIDEWAYS":
            score += 3
        else:
            score -= 5

    if rsi is not None:
        overbought = rsi > 70
        oversold   = rsi < 30
        if (direction == "LONG" and overbought) or (direction == "SHORT" and oversold):
            score -= 12
        elif (direction == "LONG" and oversold) or (direction == "SHORT" and overbought):
            score += 5
        elif 40 <= rsi <= 60:
            score += 3

    return max(5, min(95, score))

def strength_label(prob):
    if prob >= 65:
        return "🟢 СИЛЬНЫЙ"
    elif prob >= 40:
        return "🟡 СРЕДНИЙ"
    else:
        return "🔴 СЛАБЫЙ"

def build_verdict(direction, htf_trend, htf_tf, prob):
    htf_aligned = htf_trend is not None and (
        (direction == "LONG" and htf_trend == "UP") or
        (direction == "SHORT" and htf_trend == "DOWN")
    )
    htf_opposite = htf_trend is not None and (
        (direction == "LONG" and htf_trend == "DOWN") or
        (direction == "SHORT" and htf_trend == "UP")
    )
    if htf_aligned:
        return f"СИГНАЛ ПО ТРЕНДУ 🔥 (тренд на {htf_tf} совпадает). Высокий приоритет."
    elif htf_opposite:
        return f"СИГНАЛ НА КОРРЕКЦИЮ ⚠️ (тренд на {htf_tf} противоположный). Ждать тест FVG/OB."
    else:
        return f"НЕЙТРАЛЬНЫЙ КОНТЕКСТ (тренд на {htf_tf} боковой). Осторожность."

# ====== ФОРМАТИРОВАНИЕ СИГНАЛА ======

def build_signal_message(symbol, tf, direction, last_close, topy, btmy, choch_bar,
                          df_signal, df_htf, df_4h, df_1d):
    now_str    = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    htf_tf     = get_htf_name(tf)
    dir_emoji  = "🟢" if direction == "LONG" else "🔴"

    sweep, sweep_lvl = detect_liquidity_sweep(df_signal, direction, topy, btmy, choch_bar)
    close_conf, conf_lvl = get_close_confirmation(df_signal, direction,
                                                   topy if direction == "LONG" else btmy,
                                                   choch_bar)
    vol_r = vol_ratio(df_signal)

    idm       = detect_idm(df_signal, direction)
    bos_count = count_bos(df_htf, direction)
    htf_trend = calc_htf_trend(df_htf)
    at_htf_level = is_at_htf_key_level(df_htf, last_close)

    fvg = calc_fvg(df_signal, direction)
    ob  = find_order_block(df_signal, direction)
    in_fvg = (fvg is not None and fvg[0] <= last_close <= fvg[1])
    in_ob  = (ob  is not None and ob[1]  <= last_close <= ob[0])

    rsi  = calc_rsi(df_signal["close"].values) if df_signal is not None else None
    fib  = calc_fibonacci(topy, btmy, direction)

    trend_sig = calc_trend_signal(df_signal)
    trend_4h  = calc_trend_signal(df_4h)
    trend_1d  = calc_trend_signal(df_1d)

    vol_sig, vol_sig_pct = calc_volume_info(df_signal)
    vol_4h,  vol_4h_pct  = calc_volume_info(df_4h)
    vol_1d,  vol_1d_pct  = calc_volume_info(df_1d)

    prob     = calc_probability(sweep, close_conf, vol_r, idm, bos_count,
                                htf_trend, direction, rsi)
    strength = strength_label(prob)
    verdict  = build_verdict(direction, htf_trend, htf_tf, prob)

    tf_conflict  = analyze_tf_conflict(direction, trend_sig, trend_4h, trend_1d, tf)
    vol_lvl, vol_impl = volume_context(vol_r)
    pos_risk, pos_reason = calc_position_sizing(
        sweep, vol_r, rsi, direction, len(tf_conflict["conflicts"]), close_conf, idm
    )
    scen_bull, scen_bear, scen_watch = build_scenarios(direction, last_close, topy, btmy, fvg, ob)

    def tf_arrow(t):
        return {"UP": "UP ↑", "DOWN": "DOWN ↓", "SIDEWAYS": "SIDEWAYS →"}.get(t, "N/A")

    rsi_str = f"{rsi}" if rsi is not None else "N/A"

    if sweep:
        sweep_str = f"✅ Да (ликвидность собрана у {sweep_lvl})"
    else:
        side = "Buy-Side" if direction == "LONG" else "Sell-Side"
        sweep_str = f"❌ Нет ({side} ликвидность не собрана)"

    level_val = conf_lvl if conf_lvl else (topy if direction == "LONG" else btmy)
    if close_conf == "Body":
        cc_emoji = "✅"
        cc_dir   = "выше" if direction == "LONG" else "ниже"
        cc_str   = f"{cc_emoji} Закрытие ТЕЛОМ {cc_dir} уровня {level_val}"
    else:
        cc_str   = f"⚠️ Закрытие только ТЕНЬЮ выше уровня {level_val}"

    if vol_r is not None:
        if vol_r >= 1.5:
            vol_conf_str = f"✅ Выше среднего в {vol_r}x — {vol_impl}"
        elif vol_r >= 1.0:
            vol_conf_str = f"⚠️ {vol_r}x среднего — {vol_impl}"
        else:
            vol_conf_str = f"❌ {vol_r}x среднего — {vol_impl}"
    else:
        vol_conf_str = f"⚠️ Данные объёма недоступны"

    cf_status = tf_conflict["status"]
    if cf_status == "OK":
        conflict_str = f"✅ {tf_conflict['message']}"
    elif cf_status == "WARNING":
        conflict_str = f"⚠️ {tf_conflict['message']}"
    else:
        conflict_str = f"🚫 {tf_conflict['message']}"
    conflict_action = tf_conflict["action"]

    pos_str = f"{pos_risk}% от депозита"
    if pos_risk < 1.0:
        pos_str += f" (вместо стандартных 1%)"
    pos_reason_str = pos_reason if pos_reason else "все факторы позитивны"

    idm_str = "✅ ПРОБИТА (глобальный фильтр пройден)" if idm else "❌ Не пробита"

    if htf_trend == "UP":
        htf_trend_str = "Восходящий ↑"
    elif htf_trend == "DOWN":
        htf_trend_str = "Нисходящий ↓"
    elif htf_trend == "SIDEWAYS":
        htf_trend_str = "Боковой →"
    else:
        htf_trend_str = "N/A"

    at_htf_str = "✅ Да" if at_htf_level else "❌ Нет"

    if fvg:
        fvg_zone = f"{fvg[0]:.5f} - {fvg[1]:.5f}"
        fvg_in   = "✅ Да (цена в зоне)" if in_fvg else "❌ Нет"
    else:
        fvg_zone = "Не найдена"
        fvg_in   = "—"

    if ob:
        ob_zone = f"{ob[1]:.5f} - {ob[0]:.5f}"
        ob_in   = "✅ Да (цена в OB)" if in_ob else "❌ Нет"
    else:
        ob_zone = "Не найден"
        ob_in   = "—"

    if fib:
        fib_100, fib_50, fib_0 = fib
        fib_block = (
            f"  1.0: {fib_100:.5f}  |  0.5: {fib_50:.5f}  |  0.0: {fib_0:.5f}\n"
            f"  Зона входа ({'Дисконт' if direction == 'LONG' else 'Премиум'}): "
            f"{'< ' + str(round(fib_50, 5)) if direction == 'LONG' else '> ' + str(round(fib_50, 5))}"
        )
    else:
        fib_block = "  N/A"

    vol_sig_str = f"{fmt_vol(vol_sig)} {fmt_pct(vol_sig_pct)}" if vol_sig else "N/A"
    vol_4h_str  = f"{fmt_vol(vol_4h)} {fmt_pct(vol_4h_pct)}"   if vol_4h  else "N/A"
    vol_1d_str  = f"{fmt_vol(vol_1d)} {fmt_pct(vol_1d_pct)}"   if vol_1d  else "N/A"

    msg = (
        f"{dir_emoji} CHoCH {tf} | {symbol}\n"
        f"⏰ Время: {now_str}\n"
        f"Направление: {direction}\n"
        f"Close: {last_close:.5f}\n"
        f"\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{strength}  |  Вероятность: {prob}%\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"\n"
        f"🔍 Подтверждение:\n"
        f"- Liquidity Sweep: {sweep_str}\n"
        f"- Close Confirmation: {cc_str}\n"
        f"- Объём: {vol_conf_str}\n"
        f"\n"
        f"🛡 Фильтры:\n"
        f"- IDM (внутренняя структура): {idm_str}\n"
        f"- Сила тренда: {bos_count} последовательных BOS перед сломом\n"
        f"- Старший ТФ ({htf_tf}): {htf_trend_str}\n"
        f"- CHOCH у ключевого уровня HTF: {at_htf_str}\n"
        f"\n"
        f"⚡ Конфликт ТФ:\n"
        f"- Статус: {conflict_str}\n"
        f"- Действие: {conflict_action}\n"
        f"\n"
        f"🎯 Зоны входа (Retrace):\n"
        f"- FVG (Зона дисбаланса): {fvg_zone}\n"
        f"  Price in FVG: {fvg_in}\n"
        f"- Order Block: {ob_zone}\n"
        f"  Price in OB: {ob_in}\n"
        f"\n"
        f"📊 Тренд по ТФ:\n"
        f"  {tf}: {tf_arrow(trend_sig)}  |  4h: {tf_arrow(trend_4h)}  |  1d: {tf_arrow(trend_1d)}\n"
        f"\n"
        f"📈 RSI-14 ({tf}): {rsi_str}\n"
        f"\n"
        f"📐 Фибоначчи:\n"
        f"{fib_block}\n"
        f"\n"
        f"📦 Объём:\n"
        f"  {tf}: {vol_sig_str}  |  4h: {vol_4h_str}  |  24h: {vol_1d_str}\n"
        f"\n"
        f"💰 Размер позиции:\n"
        f"- Рекомендуемый риск: {pos_str}\n"
        f"- Причина: {pos_reason_str}\n"
        f"\n"
        f"🔭 Сценарии:\n"
        f"- Бычий:    {scen_bull}\n"
        f"- Медвежий: {scen_bear}\n"
        f"- Наблюдать: {scen_watch}\n"
        f"\n"
        f"📊 Вердикт: {verdict}"
    )
    return msg

# ====== ДНЕВНОЙ ОТЧЕТ ======

def store_signal(symbol, tf, direction, close_price, topy, btmy,
                 prob, fvg, vol_r, htf_conflict):
    today = datetime.date.today().isoformat()
    daily_signals.append({
        "date":         today,
        "time":         datetime.datetime.now(),
        "symbol":       symbol,
        "tf":           tf,
        "direction":    direction,
        "close":        close_price,
        "topy":         topy,
        "btmy":         btmy,
        "prob":         prob,
        "fvg":          fvg,
        "vol_r":        vol_r,
        "htf_conflict": htf_conflict,
        "outcome":      None,
    })

def check_signal_outcome(sig):
    """
    Определяет исход сигнала по текущей цене.
    win: цена прошла ≥1% в сторону сигнала
    loss: цена прошла ≥1% против сигнала
    open: цена ещё не определилась
    """
    df = get_klines(sig["symbol"], sig["tf"], limit=10)
    if df is None or sig["close"] == 0:
        return "unknown"
    current = float(df["close"].iloc[-1])
    pct     = (current - sig["close"]) / sig["close"] * 100
    if sig["direction"] == "LONG":
        return "win" if pct >= 1.0 else ("loss" if pct <= -1.0 else "open")
    else:
        return "win" if pct <= -1.0 else ("loss" if pct >= 1.0 else "open")

def _win_rate_for(sigs):
    ent = [s for s in sigs if s["outcome"] in ("win", "loss")]
    if not ent:
        return 0, 0, 0.0
    w  = sum(1 for s in ent if s["outcome"] == "win")
    pf = round(w / (len(ent) - w), 1) if (len(ent) - w) > 0 else (99.0 if w > 0 else 0.0)
    return len(ent), w, pf

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

    by_tf  = {tf: [s for s in today_sigs if s["tf"] == tf] for tf in TIMEFRAMES}
    entered = [s for s in today_sigs if s["outcome"] in ("win", "loss")]
    wins    = [s for s in entered    if s["outcome"] == "win"]
    losses  = [s for s in entered    if s["outcome"] == "loss"]

    entered_pct = round(len(entered) / total * 100, 1) if total else 0
    pnl         = len(wins) * 1.0 - len(losses) * 0.5
    pnl_str     = f"+{pnl:.1f}%" if pnl >= 0 else f"{pnl:.1f}%"

    high_q = [s for s in today_sigs if s["prob"] > 80]
    mid_q  = [s for s in today_sigs if 50 <= s["prob"] <= 80]

    tf_blocks = ""
    for tf in TIMEFRAMES:
        sigs = by_tf[tf]
        if not sigs:
            continue
        ent  = [s for s in sigs if s["outcome"] in ("win", "loss")]
        w    = sum(1 for s in ent if s["outcome"] == "win")
        wr   = round(w / len(ent) * 100, 1) if ent else 0
        ent_pct = round(len(ent) / len(sigs) * 100, 1) if sigs else 0
        pair_res = {}
        for s in sigs:
            sym = s["symbol"]
            if sym not in pair_res:
                pair_res[sym] = {"w": 0, "l": 0}
            if s["outcome"] == "win":    pair_res[sym]["w"] += 1
            elif s["outcome"] == "loss": pair_res[sym]["l"] += 1
        active = {k: v for k, v in pair_res.items() if v["w"] + v["l"] > 0}
        best_pair_str = ""
        if active:
            bp = max(active.items(), key=lambda x: x[1]["w"] / (x[1]["w"] + x[1]["l"]))
            cnt = bp[1]["w"] + bp[1]["l"]
            best_pair_str = f"\n   Лучшая пара: {bp[0]} ({bp[1]['w']}/{cnt} прибыльных)"
        tf_blocks += (
            f"{tf} ТФ:\n"
            f"   - Сигналов: {len(sigs)}\n"
            f"   - Отработано: {len(ent)} ({ent_pct}%)\n"
            f"   - Win Rate: {wr}%"
            f"{best_pair_str}\n\n"
        )

    pair_stats = {}
    for s in today_sigs:
        sym = s["symbol"]
        if sym not in pair_stats:
            pair_stats[sym] = {"w": 0, "l": 0}
        if s["outcome"] == "win":    pair_stats[sym]["w"] += 1
        elif s["outcome"] == "loss": pair_stats[sym]["l"] += 1
    active_pairs  = {k: v for k, v in pair_stats.items() if v["w"] + v["l"] > 0}
    sorted_pairs  = sorted(active_pairs.items(),
                           key=lambda x: x[1]["w"] / (x[1]["w"] + x[1]["l"]), reverse=True)
    top3_good = sorted_pairs[:3]
    top3_bad  = [p for p in reversed(sorted_pairs) if p[1]["l"] > 0][:3]

    def pair_line(rank, sym, res):
        cnt = res["w"] + res["l"]
        return f"   {rank}. {sym}: {res['w']}/{cnt} ({round(res['w']/cnt*100)}% прибыльных)\n"

    good_block = "".join(pair_line(i+1, s, r) for i, (s, r) in enumerate(top3_good)) or "   Нет данных\n"
    bad_block  = "".join(pair_line(i+1, s, r) for i, (s, r) in enumerate(top3_bad))  or "   Нет данных\n"

    hour_stats = {}
    for s in today_sigs:
        h = s["time"].hour
        if h not in hour_stats:
            hour_stats[h] = {"w": 0, "l": 0, "long": 0, "short": 0}
        if s["direction"] == "LONG":  hour_stats[h]["long"]  += 1
        else:                         hour_stats[h]["short"] += 1
        if s["outcome"] == "win":    hour_stats[h]["w"] += 1
        elif s["outcome"] == "loss": hour_stats[h]["l"] += 1

    def best_hour_str(dir_key):
        filtered = {h: v for h, v in hour_stats.items()
                    if v[dir_key] > 0 and v["w"] + v["l"] > 0}
        if not filtered:
            return "Нет данных"
        best = max(filtered.items(), key=lambda x: x[1]["w"] / (x[1]["w"] + x[1]["l"] + 0.01))
        h, v = best
        cnt  = v["w"] + v["l"]
        wr   = round(v["w"] / cnt * 100) if cnt else 0
        return f"{h:02d}:00 - {(h+4)%24:02d}:00 UTC ({cnt} сигналов, {wr}% прибыльных)"

    worst_h_str = "Нет данных"
    worst_cands = {h: v for h, v in hour_stats.items() if v["w"] + v["l"] > 0 and v["l"] > v["w"]}
    if worst_cands:
        wh, wv = min(worst_cands.items(), key=lambda x: x[1]["w"] / (x[1]["w"] + x[1]["l"] + 0.01))
        cnt = wv["w"] + wv["l"]
        worst_h_str = f"{wh:02d}:00 - {(wh+2)%24:02d}:00 UTC ({cnt} сигналов, 0% прибыльных)"

    low_vol_err  = sum(1 for s in losses if s["vol_r"] is not None and s["vol_r"] < 1.0)
    htf_conf_err = sum(1 for s in losses if s["htf_conflict"])
    fvg_err      = sum(1 for s in losses if s["fvg"] is None)

    hq_ent, hq_w, hq_pf = _win_rate_for(high_q)
    mq_ent, mq_w, mq_pf = _win_rate_for(mid_q)

    good_pairs_str = ", ".join(s.replace("-USDT", "") for s, _ in top3_good) if top3_good else "Нет данных"
    bad_pairs_str  = ", ".join(s.replace("-USDT", "") for s, _ in top3_bad)  if top3_bad  else "Нет данных"

    date_fmt    = datetime.date.today().strftime("%d.%m.%Y")
    report_time = datetime.datetime.now().strftime("%H:%M")
    tf_distr    = "\n".join(
        f"   - {tf} ТФ: {len(by_tf[tf])} сигналов"
        for tf in TIMEFRAMES if by_tf[tf]
    )

    report = (
        f"==================================================\n"
        f"📊 ДНЕВНОЙ ОТЧЕТ ПО СИГНАЛАМ SMART MONEY\n"
        f"📅 Дата: {date_fmt}\n"
        f"⏰ Время отчета: {report_time}\n"
        f"==================================================\n"
        f"\n"
        f"🔥 ОБЩАЯ СТАТИСТИКА\n"
        f"----------------------------------------\n"
        f"✅ Всего сигналов за день: {total}\n"
        f"{tf_distr}\n"
        f"\n"
        f"📊 Распределение по направлению:\n"
        f"   - LONG:  {len(long_sigs)} ({round(len(long_sigs)/total*100,1) if total else 0}%)\n"
        f"   - SHORT: {len(short_sigs)} ({round(len(short_sigs)/total*100,1) if total else 0}%)\n"
        f"\n"
        f"📈 ЭФФЕКТИВНОСТЬ СИГНАЛОВ\n"
        f"----------------------------------------\n"
        f"🎯 Отработано сделок (были входы): {len(entered)} из {total} ({entered_pct}%)\n"
        f"\n"
        f"📊 Результаты по отработанным сделкам:\n"
        f"   ✅ Прибыльных: {len(wins)} ({round(len(wins)/len(entered)*100) if entered else 0}%)\n"
        f"   ❌ Убыточных:  {len(losses)} ({round(len(losses)/len(entered)*100) if entered else 0}%)\n"
        f"   💰 Общий P&L за день: {pnl_str}\n"
        f"\n"
        f"📊 Распределение по качеству сигналов:\n"
        f"   - 🟢 Высокий (>80%):   {len(high_q)} сигналов ({round(len(high_q)/total*100,1) if total else 0}%)\n"
        f"   - 🟡 Средний (50-80%): {len(mid_q)} сигналов ({round(len(mid_q)/total*100,1) if total else 0}%)\n"
        f"\n"
        f"🎯 КАЧЕСТВО СИГНАЛОВ (по вероятности)\n"
        f"----------------------------------------\n"
        f"🟢 Высокая вероятность (>80%):\n"
        f"   - Всего: {len(high_q)}\n"
        f"   - Отработано: {hq_ent}\n"
        f"   - Прибыльных: {hq_w}\n"
        f"   - Профит-фактор: {hq_pf}\n"
        f"\n"
        f"🟡 Средняя вероятность (50-80%):\n"
        f"   - Всего: {len(mid_q)}\n"
        f"   - Отработано: {mq_ent}\n"
        f"   - Прибыльных: {mq_w}\n"
        f"   - Профит-фактор: {mq_pf}\n"
        f"\n"
        f"⏱️ ВРЕМЕННОЙ АНАЛИЗ\n"
        f"----------------------------------------\n"
        f"🕐 Лучшее время для LONG:\n"
        f"   - {best_hour_str('long')}\n"
        f"\n"
        f"🕐 Лучшее время для SHORT:\n"
        f"   - {best_hour_str('short')}\n"
        f"\n"
        f"⚠️ Худшее время (избегать):\n"
        f"   - {worst_h_str}\n"
        f"\n"
        f"📈 ДИНАМИКА ПО ТФ\n"
        f"----------------------------------------\n"
        f"{tf_blocks}"
        f"🔍 ЛУЧШИЕ И ХУДШИЕ ПАРЫ\n"
        f"----------------------------------------\n"
        f"🏆 Топ-3 прибыльных пар:\n"
        f"{good_block}"
        f"\n"
        f"📉 Топ-3 убыточных пар:\n"
        f"{bad_block}"
        f"\n"
        f"⚠️ АНАЛИЗ ОШИБОК\n"
        f"----------------------------------------\n"
        f"❌ Причины убыточных сделок:\n"
        f"   - Низкий объем при входе: {low_vol_err}\n"
        f"   - Вход против старшего ТФ: {htf_conf_err}\n"
        f"   - Без зоны FVG/OB: {fvg_err}\n"
        f"\n"
        f"💡 РЕКОМЕНДАЦИИ НА ЗАВТРА\n"
        f"----------------------------------------\n"
        f"✅ Фокус на пары: {good_pairs_str}\n"
        f"⚠️ Избегать: {bad_pairs_str}\n"
        f"🕐 Лучшее время для торговли: {best_hour_str('long')}\n"
        f"📊 Размер позиции: 1% (высокая вероятность), 0.5% (средняя)\n"
        f"\n"
        f"==================================================\n"
        f"🏆 ИТОГО: {pnl_str} | Прибыльных: {len(wins)}/{len(entered)} "
        f"({round(len(wins)/len(entered)*100) if entered else 0}%)\n"
        f"=================================================="
    )
    return report

def daily_report_loop():
    """Отправляет дневной отчет в 23:55 каждый день."""
    sent_today = None
    while True:
        now   = datetime.datetime.now()
        today = datetime.date.today().isoformat()
        if now.hour == 23 and now.minute >= 55 and sent_today != today:
            print("[INFO] Генерация дневного отчета...")
            report = build_daily_report()
            if report:
                send_telegram(report)
                sent_today = today
                print("[INFO] Дневной отчет отправлен.")
            else:
                print("[INFO] Нет сигналов за день — отчет не отправлен.")
        time.sleep(60)

# ====== SCAN ======
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
        if choch:
            htf_name = get_htf_name(tf)
            df_htf   = df_cache.get(htf_name, df)
            df_4h    = df_cache.get("4h")
            df_1d    = df_cache.get("1d")
            if tf == "4h":
                df_4h = df
            elif tf == "1d":
                df_1d = df

            sweep, _     = detect_liquidity_sweep(df, choch, topy, btmy, choch_bar)
            vol_r_quick  = vol_ratio(df)
            idm_quick    = detect_idm(df, choch)
            bos_quick    = count_bos(df_htf, choch)
            htf_quick    = calc_htf_trend(df_htf)
            rsi_quick    = calc_rsi(df["close"].values) if df **...**

_This response is too long to display in full._

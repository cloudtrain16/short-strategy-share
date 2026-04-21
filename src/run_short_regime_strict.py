#!/usr/bin/env python3
import argparse
import csv
import json
import math
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

BINANCE_BAR_MAP = {
    "1H": "1h",
    "4H": "4h",
    "1D": "1d",
}

MA_FAST_LIST = [7, 10]
MA_MID_LIST = [20, 30]
MA_SLOW_LIST = [52, 60]
RSI_SHORT_LIST = [58, 62, 66]
RSI_COVER_LIST = [38, 42, 46]
VOL_MIN_PCT_LIST = [0.4, 0.8, 1.2]
MAX_HOLD_BARS_LIST = [24, 48, 96]


def api_get(path: str, params: dict):
    base = "https://www.okx.com"
    url = f"{base}{path}?{urlencode(params)}"
    req = Request(url, headers={"User-Agent": "quant-short-regime/1.0"})
    with urlopen(req, timeout=20) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    if payload.get("code") not in ("0", 0, None):
        raise RuntimeError(f"OKX API error: {payload}")
    return payload


def fetch_okx_candles(inst: str, bar: str, total_bars: int):
    rows_desc = []
    after = None
    while len(rows_desc) < total_bars:
        batch = min(300, total_bars - len(rows_desc))
        params = {"instId": inst, "bar": bar, "limit": batch}
        if after is not None:
            params["after"] = after
        payload = api_get("/api/v5/market/candles", params)
        data = payload.get("data", [])
        if not data:
            break
        rows_desc.extend(data)
        after = data[-1][0]
        if len(data) < batch:
            break
        time.sleep(0.05)

    uniq = {}
    for r in rows_desc:
        uniq[int(r[0])] = r
    merged_desc = [uniq[k] for k in sorted(uniq.keys(), reverse=True)]
    merged_desc = merged_desc[:total_bars]
    rows = list(reversed(merged_desc))

    if not rows:
        raise RuntimeError(f"No OKX candle data returned for {inst}")

    return [
        {
            "ts": int(x[0]),
            "o": float(x[1]),
            "h": float(x[2]),
            "l": float(x[3]),
            "c": float(x[4]),
            "v": float(x[5]),
        }
        for x in rows
    ]


def fetch_binance_candles(inst: str, bar: str, total_bars: int):
    interval = BINANCE_BAR_MAP.get(bar.upper())
    if interval is None:
        raise ValueError(f"Unsupported Binance bar: {bar}")

    symbol = inst.replace("-", "").upper()
    base = "https://api.binance.com/api/v3/klines"

    uniq = {}
    end_time = None
    while len(uniq) < total_bars:
        batch = min(1000, total_bars - len(uniq))
        params = {"symbol": symbol, "interval": interval, "limit": batch}
        if end_time is not None:
            params["endTime"] = end_time

        req = Request(f"{base}?{urlencode(params)}", headers={"User-Agent": "quant-short-regime/1.0"})
        with urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read().decode("utf-8"))

        if not data:
            break

        for x in data:
            ts = int(x[0])
            uniq[ts] = {
                "ts": ts,
                "o": float(x[1]),
                "h": float(x[2]),
                "l": float(x[3]),
                "c": float(x[4]),
                "v": float(x[5]),
            }

        oldest = int(data[0][0])
        if oldest <= 0:
            break
        end_time = oldest - 1

        if len(data) < batch:
            break
        time.sleep(0.03)

    rows = [uniq[k] for k in sorted(uniq.keys())]
    if not rows:
        raise RuntimeError(f"No Binance candle data returned for {inst}")
    return rows[-total_bars:]


def fetch_candles(inst: str, bar: str, total_bars: int, data_source: str):
    source = data_source.lower()
    if source == "okx":
        return fetch_okx_candles(inst, bar, total_bars)
    if source == "binance":
        return fetch_binance_candles(inst, bar, total_bars)
    raise ValueError(f"Unsupported data source: {data_source}")


def sma(values, n):
    out = []
    s = 0.0
    for i, v in enumerate(values):
        s += v
        if i >= n:
            s -= values[i - n]
        out.append(s / min(i + 1, n))
    return out


def rsi(values, n=14):
    if len(values) < n + 2:
        return [50.0] * len(values)

    gains = []
    losses = []
    for i in range(1, len(values)):
        d = values[i] - values[i - 1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))

    avg_gain = sum(gains[:n]) / n
    avg_loss = sum(losses[:n]) / n

    out = [50.0] * n
    for i in range(n, len(gains)):
        avg_gain = (avg_gain * (n - 1) + gains[i]) / n
        avg_loss = (avg_loss * (n - 1) + losses[i]) / n
        rs = avg_gain / (avg_loss if avg_loss else 1e-9)
        out.append(100 - 100 / (1 + rs))

    out.append(out[-1])
    return out[: len(values)]


def rolling_std(values, n):
    out = []
    for i in range(len(values)):
        start = max(0, i - n + 1)
        w = values[start : i + 1]
        m = sum(w) / len(w)
        var = sum((x - m) ** 2 for x in w) / len(w)
        out.append(math.sqrt(var))
    return out


def parse_utc_ts(date_str: str):
    dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def ts_to_iso(ts_ms):
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()


def build_exec_cfg(args):
    return {
        "entry_order_type": args.entry_order_type,
        "exit_order_type": args.exit_order_type,
        "maker_fee_bps": args.maker_fee_bps,
        "taker_fee_bps": args.taker_fee_bps,
        "market_slippage_bps": args.market_slippage_bps,
        "half_spread_bps": args.half_spread_bps,
        "limit_offset_bps": args.limit_offset_bps,
        "limit_slippage_bps": args.limit_slippage_bps,
        "limit_ttl_bars": args.limit_ttl_bars,
        "limit_fallback_market": args.limit_fallback_market,
        "limit_fill_model": args.limit_fill_model,
        "queue_bps": args.queue_bps,
        "stop_loss_pct": args.stop_loss_pct,
        "take_profit_pct": args.take_profit_pct,
        "starting_capital_usd": args.starting_capital_usd,
        "max_participation_rate": args.max_participation_rate,
        "min_capacity_ratio": args.min_capacity_ratio,
        "funding_bps_per_8h": args.funding_bps_per_8h,
        "seed": args.exec_seed,
    }


def market_fill_price(side, ref_px, cfg):
    impact = (cfg["half_spread_bps"] + cfg["market_slippage_bps"]) / 10000.0
    if side == "buy":
        return ref_px * (1.0 + impact)
    return ref_px * (1.0 - impact)


def limit_fill_price(side, limit_px, cfg):
    slip = cfg["limit_slippage_bps"] / 10000.0
    if side == "buy":
        return limit_px * (1.0 + slip)
    return limit_px * (1.0 - slip)


def place_order(side, kind, decision_px, signal_bar_idx, cfg):
    activate = signal_bar_idx + 1
    limit_px = None
    if kind == "limit":
        off = cfg["limit_offset_bps"] / 10000.0
        if side == "sell":
            limit_px = decision_px * (1.0 + off)
        else:
            limit_px = decision_px * (1.0 - off)
    return {
        "side": side,
        "kind": kind,
        "decision_px": decision_px,
        "activate": activate,
        "expire": activate + max(1, cfg["limit_ttl_bars"]) - 1,
        "limit_px": limit_px,
    }


def capacity_ratio(bar, equity_multiple, desired_fraction, cfg):
    desired_notional = max(1e-9, equity_multiple * cfg["starting_capital_usd"] * desired_fraction)
    bar_quote_notional = max(0.0, bar["v"] * bar["c"])
    allowed_notional = bar_quote_notional * cfg["max_participation_rate"]
    return allowed_notional / desired_notional


def calc_order_fill_probability(order, bar, vol_ratio, cfg):
    if cfg["limit_fill_model"] == "deterministic":
        return 1.0

    if order["side"] == "buy":
        if bar["l"] > order["limit_px"]:
            return 0.0
        penetration_bps = max(0.0, (order["limit_px"] - bar["l"]) / order["limit_px"] * 10000.0)
    else:
        if bar["h"] < order["limit_px"]:
            return 0.0
        penetration_bps = max(0.0, (bar["h"] - order["limit_px"]) / order["limit_px"] * 10000.0)

    depth_factor = min(1.0, penetration_bps / max(1.0, cfg["queue_bps"]))
    volume_factor = min(0.25, max(0.0, vol_ratio - 1.0) * 0.1)
    p = 0.35 + 0.45 * depth_factor + volume_factor
    return max(0.0, min(0.98, p))


def try_execute_order(order, bar, bar_idx, vol_ratio, cfg, rng):
    if bar_idx < order["activate"]:
        return {"status": "pending"}

    if order["kind"] == "market":
        px = market_fill_price(order["side"], bar["o"], cfg)
        return {
            "status": "filled",
            "fill_px": px,
            "fee_bps": cfg["taker_fee_bps"],
            "is_maker": 0,
            "reason": "market",
        }

    touched = (bar["h"] >= order["limit_px"]) if order["side"] == "sell" else (bar["l"] <= order["limit_px"])
    if touched:
        p = calc_order_fill_probability(order, bar, vol_ratio, cfg)
        if rng.random() <= p:
            px = limit_fill_price(order["side"], order["limit_px"], cfg)
            return {
                "status": "filled",
                "fill_px": px,
                "fee_bps": cfg["maker_fee_bps"],
                "is_maker": 1,
                "reason": "limit_touch",
            }

    if bar_idx >= order["expire"]:
        if cfg["limit_fallback_market"]:
            px = market_fill_price(order["side"], bar["c"], cfg)
            return {
                "status": "filled",
                "fill_px": px,
                "fee_bps": cfg["taker_fee_bps"],
                "is_maker": 0,
                "reason": "limit_expire_fallback_market",
            }
        return {"status": "expired"}

    return {"status": "pending"}


def short_return_multiple(entry_net_sell_px, exit_cost_buy_px):
    raw = 1.0 + (entry_net_sell_px - exit_cost_buy_px) / max(1e-12, entry_net_sell_px)
    return max(0.01, raw)


def backtest_short_strict(candles, p, exec_cfg, bear_start_ts, collect_marks=False):
    ma_fast_n, ma_mid_n, ma_slow_n, rsi_short, rsi_cover, vol_min_pct, max_hold_bars = p

    close = [x["c"] for x in candles]
    ma_fast = sma(close, ma_fast_n)
    ma_mid = sma(close, ma_mid_n)
    ma_slow = sma(close, ma_slow_n)
    r = rsi(close, 14)

    rets = [0.0]
    for i in range(1, len(close)):
        rets.append((close[i] / close[i - 1]) - 1.0)
    vol_pct = [x * 100.0 for x in rolling_std(rets, 20)]

    vol20 = sma([x["v"] for x in candles], 20)
    rng = random.Random(exec_cfg["seed"] + ma_fast_n * 100000 + ma_mid_n * 1000 + ma_slow_n)

    equity = 1.0
    peak = 1.0
    max_dd = 0.0

    in_pos = False
    pending_entry = None
    pending_exit = None
    entry_raw_px = 0.0
    entry_net_sell = 0.0
    entry_i = -10**9

    entry_signals = 0
    exit_signals = 0
    entries = 0
    trades = 0
    wins = 0
    maker_fills = 0
    taker_fills = 0
    limit_expired = 0
    missed_entries = 0
    missed_exits = 0
    capacity_reject_entries = 0
    capacity_reject_exits = 0
    funding_bars = 0
    funding_carry_multiple = 1.0

    bar_hours = 0.0
    if len(candles) >= 2:
        bar_hours = max(0.0, (candles[1]["ts"] - candles[0]["ts"]) / 3600000.0)
    if bar_hours <= 0.0:
        bar_hours = 4.0
    funding_rate_per_bar = (exec_cfg["funding_bps_per_8h"] / 10000.0) * (bar_hours / 8.0)

    marks = [1.0] if collect_marks else None

    for i in range(1, len(candles)):
        bar = candles[i]
        vol_ratio = bar["v"] / max(1e-9, vol20[i])

        if pending_entry is not None and (not in_pos):
            res = try_execute_order(pending_entry, bar, i, vol_ratio, exec_cfg, rng)
            if res["status"] == "filled":
                cap = capacity_ratio(bar, equity, 1.0, exec_cfg)
                if cap >= exec_cfg["min_capacity_ratio"]:
                    fee = res["fee_bps"] / 10000.0
                    entry_raw_px = res["fill_px"]
                    entry_net_sell = entry_raw_px * (1.0 - fee)
                    in_pos = True
                    entry_i = i
                    entries += 1
                    maker_fills += res["is_maker"]
                    taker_fills += 1 - res["is_maker"]
                    pending_entry = None
                else:
                    capacity_reject_entries += 1
                    if i >= pending_entry["expire"]:
                        limit_expired += 1
                        missed_entries += 1
                        pending_entry = None
            elif res["status"] == "expired":
                limit_expired += 1
                missed_entries += 1
                pending_entry = None

        if pending_exit is not None and in_pos:
            res = try_execute_order(pending_exit, bar, i, vol_ratio, exec_cfg, rng)
            if res["status"] == "filled":
                cap = capacity_ratio(bar, equity, 1.0, exec_cfg)
                if cap >= exec_cfg["min_capacity_ratio"]:
                    fee = res["fee_bps"] / 10000.0
                    exit_cost = res["fill_px"] * (1.0 + fee)
                    ret = short_return_multiple(entry_net_sell, exit_cost)
                    equity *= ret
                    trades += 1
                    if ret > 1.0:
                        wins += 1
                    maker_fills += res["is_maker"]
                    taker_fills += 1 - res["is_maker"]
                    in_pos = False
                    pending_exit = None
                    entry_raw_px = 0.0
                    entry_net_sell = 0.0
                else:
                    capacity_reject_exits += 1
                    if i >= pending_exit["expire"]:
                        limit_expired += 1
                        missed_exits += 1
                        pending_exit = None
            elif res["status"] == "expired":
                limit_expired += 1
                missed_exits += 1
                pending_exit = None

        # Hard risk exits for short
        if in_pos and pending_exit is None:
            stop_hit = False
            tp_hit = False
            stop_px = None
            tp_px = None

            if exec_cfg["stop_loss_pct"] > 0:
                stop_px = entry_raw_px * (1.0 + exec_cfg["stop_loss_pct"] / 100.0)
                stop_hit = bar["h"] >= stop_px
            if exec_cfg["take_profit_pct"] > 0:
                tp_px = entry_raw_px * (1.0 - exec_cfg["take_profit_pct"] / 100.0)
                tp_hit = bar["l"] <= tp_px

            if stop_hit or tp_hit:
                trigger = stop_px if stop_hit else tp_px
                cover_px = market_fill_price("buy", trigger, exec_cfg)
                fee = exec_cfg["taker_fee_bps"] / 10000.0
                exit_cost = cover_px * (1.0 + fee)
                ret = short_return_multiple(entry_net_sell, exit_cost)
                equity *= ret
                trades += 1
                if ret > 1.0:
                    wins += 1
                taker_fills += 1
                in_pos = False
                entry_raw_px = 0.0
                entry_net_sell = 0.0

        # Funding carry for short perp approximation:
        # positive funding_bps_per_8h => short side pays carry each bar.
        if in_pos and funding_rate_per_bar != 0.0:
            carry = max(0.01, 1.0 - funding_rate_per_bar)
            equity *= carry
            funding_carry_multiple *= carry
            funding_bars += 1

        mark = equity
        if in_pos:
            est_cover_px = market_fill_price("buy", bar["c"], exec_cfg)
            est_exit = est_cover_px * (1.0 + exec_cfg["taker_fee_bps"] / 10000.0)
            mark = equity * short_return_multiple(entry_net_sell, est_exit)

        if mark > peak:
            peak = mark
        dd = (peak - mark) / max(1e-12, peak)
        if dd > max_dd:
            max_dd = dd
        if collect_marks:
            marks.append(mark)

        if i >= len(candles) - 1:
            continue

        in_bear = candles[i]["ts"] >= bear_start_ts
        downtrend = ma_mid[i] < ma_slow[i]
        rebound = close[i] > ma_fast[i] and close[i] > ma_mid[i] and close[i] > ma_slow[i]
        rsi_ok = r[i] >= rsi_short
        vol_ok = vol_pct[i] >= vol_min_pct

        short_signal = in_bear and downtrend and rebound and rsi_ok and vol_ok
        cover_signal = (r[i] <= rsi_cover and close[i] < ma_mid[i]) or ((i - entry_i) >= max_hold_bars)

        if (not in_pos) and pending_entry is None and short_signal:
            entry_signals += 1
            pending_entry = place_order("sell", exec_cfg["entry_order_type"], candles[i]["c"], i, exec_cfg)

        if in_pos and pending_exit is None and cover_signal:
            exit_signals += 1
            pending_exit = place_order("buy", exec_cfg["exit_order_type"], candles[i]["c"], i, exec_cfg)

    if in_pos:
        last = candles[-1]
        cover_px = market_fill_price("buy", last["c"], exec_cfg)
        fee = exec_cfg["taker_fee_bps"] / 10000.0
        exit_cost = cover_px * (1.0 + fee)
        ret = short_return_multiple(entry_net_sell, exit_cost)
        equity *= ret
        trades += 1
        if ret > 1.0:
            wins += 1
        taker_fills += 1
        if collect_marks:
            marks.append(equity)

    win_rate = (wins / trades * 100.0) if trades else 0.0
    total_ret = (equity - 1.0) * 100.0
    max_dd_pct = max_dd * 100.0
    entry_fill_rate = (entries / entry_signals * 100.0) if entry_signals else 0.0
    exit_fill_rate = (trades / exit_signals * 100.0) if exit_signals else 0.0

    fill_penalty = (100.0 - entry_fill_rate) * 0.15 + (100.0 - exit_fill_rate) * 0.05
    score = total_ret - 1.2 * max_dd_pct - fill_penalty

    return {
        "ma_fast": ma_fast_n,
        "ma_mid": ma_mid_n,
        "ma_slow": ma_slow_n,
        "rsi_short": rsi_short,
        "rsi_cover": rsi_cover,
        "vol_min_pct": vol_min_pct,
        "max_hold_bars": max_hold_bars,
        "entry_signals": entry_signals,
        "exit_signals": exit_signals,
        "entries": entries,
        "trades": trades,
        "wins": wins,
        "win_rate": round(win_rate, 2),
        "entry_fill_rate": round(entry_fill_rate, 2),
        "exit_fill_rate": round(exit_fill_rate, 2),
        "maker_fills": maker_fills,
        "taker_fills": taker_fills,
        "limit_expired": limit_expired,
        "missed_entries": missed_entries,
        "missed_exits": missed_exits,
        "capacity_reject_entries": capacity_reject_entries,
        "capacity_reject_exits": capacity_reject_exits,
        "funding_bars": funding_bars,
        "funding_carry_pct": round((funding_carry_multiple - 1.0) * 100.0, 4),
        "return_pct": round(total_ret, 2),
        "max_dd_pct": round(max_dd_pct, 2),
        "score": round(score, 2),
        "equity_multiple": equity,
        "equity_marks": marks,
    }


def build_params():
    out = []
    for a in MA_FAST_LIST:
        for b in MA_MID_LIST:
            for c in MA_SLOW_LIST:
                if not (a < b < c):
                    continue
                for rs in RSI_SHORT_LIST:
                    for rc in RSI_COVER_LIST:
                        if rs <= rc:
                            continue
                        for v in VOL_MIN_PCT_LIST:
                            for h in MAX_HOLD_BARS_LIST:
                                out.append((a, b, c, rs, rc, v, h))
    return out


def calc_max_dd_from_curve(curve):
    peak = 1.0
    mdd = 0.0
    for x in curve:
        if x > peak:
            peak = x
        dd = (peak - x) / max(1e-12, peak)
        if dd > mdd:
            mdd = dd
    return mdd * 100.0


def select_best_grid(train, params, exec_cfg, bear_start_ts):
    best = None
    for p in params:
        r = backtest_short_strict(train, p, exec_cfg, bear_start_ts, collect_marks=False)
        if best is None or r["score"] > best["score"]:
            best = r
    return best


def select_best_ga(train, params, exec_cfg, bear_start_ts, generations, pop_size, mutation_rate, seed):
    rng = random.Random(seed)
    valid = set(params)
    cache = {}

    def evalp(p):
        if p not in cache:
            cache[p] = backtest_short_strict(train, p, exec_cfg, bear_start_ts, collect_marks=False)
        return cache[p]

    def repair(p):
        if p in valid:
            return p
        return rng.choice(params)

    def tournament(pop, k=3):
        cand = rng.sample(pop, k=min(k, len(pop)))
        cand.sort(key=lambda x: evalp(x)["score"], reverse=True)
        return cand[0]

    def crossover(a, b):
        c = tuple(a[i] if rng.random() < 0.5 else b[i] for i in range(len(a)))
        return repair(c)

    def mutate(p):
        a, b, c, rs, rc, v, h = p
        if rng.random() < mutation_rate:
            a = rng.choice(MA_FAST_LIST)
        if rng.random() < mutation_rate:
            b = rng.choice(MA_MID_LIST)
        if rng.random() < mutation_rate:
            c = rng.choice(MA_SLOW_LIST)
        if rng.random() < mutation_rate:
            rs = rng.choice(RSI_SHORT_LIST)
        if rng.random() < mutation_rate:
            rc = rng.choice(RSI_COVER_LIST)
        if rng.random() < mutation_rate:
            v = rng.choice(VOL_MIN_PCT_LIST)
        if rng.random() < mutation_rate:
            h = rng.choice(MAX_HOLD_BARS_LIST)
        if not (a < b < c and rs > rc):
            return rng.choice(params)
        return repair((a, b, c, rs, rc, v, h))

    pop = [rng.choice(params) for _ in range(max(8, pop_size))]
    best_p = pop[0]
    best_s = evalp(best_p)["score"]

    for _ in range(max(1, generations)):
        pop.sort(key=lambda x: evalp(x)["score"], reverse=True)
        if evalp(pop[0])["score"] > best_s:
            best_p = pop[0]
            best_s = evalp(pop[0])["score"]

        new_pop = pop[:2]
        while len(new_pop) < len(pop):
            p1 = tournament(pop)
            p2 = tournament(pop)
            child = crossover(p1, p2)
            child = mutate(child)
            new_pop.append(child)
        pop = new_pop

    final_best = max(pop, key=lambda x: evalp(x)["score"])
    if evalp(final_best)["score"] > best_s:
        best_p = final_best
    return evalp(best_p)


def walk_forward(candles, params, train_bars, test_bars, selector, exec_cfg, bear_start_ts, ga_cfg=None, symbol=""):
    n = len(candles)
    folds = []
    oos_equity = 1.0
    oos_curve = [1.0]

    fold = 0
    start = 0
    while start + train_bars + test_bars <= n:
        tr = candles[start : start + train_bars]
        te = candles[start + train_bars : start + train_bars + test_bars]

        fold_exec = dict(exec_cfg)
        fold_exec["seed"] = exec_cfg["seed"] + fold + sum(ord(c) for c in symbol)

        if selector == "grid":
            best_train = select_best_grid(tr, params, fold_exec, bear_start_ts)
        elif selector == "ga":
            best_train = select_best_ga(
                tr,
                params,
                fold_exec,
                bear_start_ts,
                generations=ga_cfg["generations"],
                pop_size=ga_cfg["pop_size"],
                mutation_rate=ga_cfg["mutation_rate"],
                seed=ga_cfg["seed"] + fold + sum(ord(c) for c in symbol),
            )
        else:
            raise ValueError(f"Unknown selector: {selector}")

        p = (
            best_train["ma_fast"],
            best_train["ma_mid"],
            best_train["ma_slow"],
            best_train["rsi_short"],
            best_train["rsi_cover"],
            best_train["vol_min_pct"],
            best_train["max_hold_bars"],
        )
        test_r = backtest_short_strict(te, p, fold_exec, bear_start_ts, collect_marks=True)

        fold_start = oos_equity
        oos_equity *= test_r["equity_multiple"]
        marks = test_r.get("equity_marks") or [1.0, test_r["equity_multiple"]]
        for m in marks[1:]:
            oos_curve.append(fold_start * m)

        folds.append(
            {
                "fold": fold,
                "train_start_ts": tr[0]["ts"],
                "train_end_ts": tr[-1]["ts"],
                "test_start_ts": te[0]["ts"],
                "test_end_ts": te[-1]["ts"],
                "best_ma_fast": best_train["ma_fast"],
                "best_ma_mid": best_train["ma_mid"],
                "best_ma_slow": best_train["ma_slow"],
                "best_rsi_short": best_train["rsi_short"],
                "best_rsi_cover": best_train["rsi_cover"],
                "best_vol_min_pct": best_train["vol_min_pct"],
                "best_max_hold_bars": best_train["max_hold_bars"],
                "train_score": best_train["score"],
                "train_return_pct": best_train["return_pct"],
                "test_return_pct": test_r["return_pct"],
                "test_max_dd_pct": test_r["max_dd_pct"],
                "test_trades": test_r["trades"],
                "test_win_rate": test_r["win_rate"],
                "test_entry_fill_rate": test_r["entry_fill_rate"],
                "test_exit_fill_rate": test_r["exit_fill_rate"],
                "test_limit_expired": test_r["limit_expired"],
                "test_capacity_reject_entries": test_r["capacity_reject_entries"],
                "test_capacity_reject_exits": test_r["capacity_reject_exits"],
                "test_funding_bars": test_r["funding_bars"],
                "test_funding_carry_pct": test_r["funding_carry_pct"],
                "oos_equity_after_fold": round(oos_equity, 6),
            }
        )

        fold += 1
        start += test_bars

    if not folds:
        raise RuntimeError("Not enough bars for walk-forward")

    total_trades = sum(int(x["test_trades"]) for x in folds)
    weighted_wins = sum(float(x["test_trades"]) * float(x["test_win_rate"]) / 100.0 for x in folds)
    win_rate = (weighted_wins / total_trades * 100.0) if total_trades else 0.0

    avg_entry_fill = sum(float(x["test_entry_fill_rate"]) for x in folds) / len(folds)
    avg_exit_fill = sum(float(x["test_exit_fill_rate"]) for x in folds) / len(folds)

    summary = {
        "method": selector,
        "folds": len(folds),
        "train_bars": train_bars,
        "test_bars": test_bars,
        "oos_return_pct": round((oos_equity - 1.0) * 100.0, 2),
        "oos_max_dd_pct": round(calc_max_dd_from_curve(oos_curve), 2),
        "oos_trades": total_trades,
        "oos_win_rate": round(win_rate, 2),
        "oos_avg_entry_fill_rate": round(avg_entry_fill, 2),
        "oos_avg_exit_fill_rate": round(avg_exit_fill, 2),
        "oos_total_limit_expired": sum(int(x["test_limit_expired"]) for x in folds),
        "oos_total_capacity_reject_entries": sum(int(x["test_capacity_reject_entries"]) for x in folds),
        "oos_total_capacity_reject_exits": sum(int(x["test_capacity_reject_exits"]) for x in folds),
        "oos_total_funding_carry_pct": round(sum(float(x["test_funding_carry_pct"]) for x in folds), 4),
        "oos_equity_multiple": round(oos_equity, 6),
    }

    return folds, summary


def write_csv(path: Path, rows, fieldnames):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def run_symbol(inst, args, params, out_dir, bear_start_ts):
    candles = fetch_candles(inst, args.bar, args.bars, args.data_source)
    exec_cfg = build_exec_cfg(args)

    grid_results = []
    for p in params:
        cfg = dict(exec_cfg)
        cfg["seed"] = exec_cfg["seed"] + sum(ord(c) for c in inst)
        grid_results.append(backtest_short_strict(candles, p, cfg, bear_start_ts, collect_marks=False))
    grid_results.sort(key=lambda x: x["score"], reverse=True)

    ga_full = select_best_ga(
        candles,
        params,
        exec_cfg,
        bear_start_ts,
        generations=args.ga_generations,
        pop_size=args.ga_pop_size,
        mutation_rate=args.ga_mutation,
        seed=args.ga_seed + sum(ord(c) for c in inst),
    )

    ga_cfg = {
        "seed": args.ga_seed,
        "generations": args.ga_generations,
        "pop_size": args.ga_pop_size,
        "mutation_rate": args.ga_mutation,
    }

    grid_folds, grid_wf = walk_forward(
        candles,
        params,
        args.train_bars,
        args.test_bars,
        "grid",
        exec_cfg,
        bear_start_ts,
        symbol=inst,
    )
    ga_folds, ga_wf = walk_forward(
        candles,
        params,
        args.train_bars,
        args.test_bars,
        "ga",
        exec_cfg,
        bear_start_ts,
        ga_cfg=ga_cfg,
        symbol=inst,
    )

    safe = inst.replace("-", "_")

    write_csv(
        out_dir / f"{safe}_grid_backtest_results.csv",
        grid_results,
        [
            "ma_fast",
            "ma_mid",
            "ma_slow",
            "rsi_short",
            "rsi_cover",
            "vol_min_pct",
            "max_hold_bars",
            "entry_signals",
            "exit_signals",
            "entries",
            "trades",
            "wins",
            "win_rate",
            "entry_fill_rate",
            "exit_fill_rate",
            "maker_fills",
            "taker_fills",
            "limit_expired",
            "missed_entries",
            "missed_exits",
            "capacity_reject_entries",
            "capacity_reject_exits",
            "funding_bars",
            "funding_carry_pct",
            "return_pct",
            "max_dd_pct",
            "score",
            "equity_multiple",
        ],
    )

    fold_fields = [
        "fold",
        "train_start_ts",
        "train_end_ts",
        "test_start_ts",
        "test_end_ts",
        "best_ma_fast",
        "best_ma_mid",
        "best_ma_slow",
        "best_rsi_short",
        "best_rsi_cover",
        "best_vol_min_pct",
        "best_max_hold_bars",
        "train_score",
        "train_return_pct",
        "test_return_pct",
        "test_max_dd_pct",
        "test_trades",
        "test_win_rate",
        "test_entry_fill_rate",
        "test_exit_fill_rate",
        "test_limit_expired",
        "test_capacity_reject_entries",
        "test_capacity_reject_exits",
        "test_funding_bars",
        "test_funding_carry_pct",
        "oos_equity_after_fold",
    ]
    write_csv(out_dir / f"{safe}_grid_walkforward_folds.csv", grid_folds, fold_fields)
    write_csv(out_dir / f"{safe}_ga_walkforward_folds.csv", ga_folds, fold_fields)

    with (out_dir / f"{safe}_grid_walkforward_summary.json").open("w") as f:
        json.dump(grid_wf, f, indent=2)
    with (out_dir / f"{safe}_ga_walkforward_summary.json").open("w") as f:
        json.dump(ga_wf, f, indent=2)
    with (out_dir / f"{safe}_ga_fullsample_best.json").open("w") as f:
        json.dump(ga_full, f, indent=2)

    score_grid = grid_wf["oos_return_pct"] - 1.2 * grid_wf["oos_max_dd_pct"] + 0.2 * grid_wf["oos_avg_entry_fill_rate"]
    score_ga = ga_wf["oos_return_pct"] - 1.2 * ga_wf["oos_max_dd_pct"] + 0.2 * ga_wf["oos_avg_entry_fill_rate"]
    pref = "ga" if score_ga > score_grid else "grid"

    return {
        "symbol": inst,
        "data_source": args.data_source,
        "bars": len(candles),
        "start_ts": candles[0]["ts"],
        "end_ts": candles[-1]["ts"],
        "start_iso_utc": ts_to_iso(candles[0]["ts"]),
        "end_iso_utc": ts_to_iso(candles[-1]["ts"]),
        "wf_folds": grid_wf["folds"],
        "wf_grid_oos_return_pct": grid_wf["oos_return_pct"],
        "wf_grid_oos_max_dd_pct": grid_wf["oos_max_dd_pct"],
        "wf_grid_oos_trades": grid_wf["oos_trades"],
        "wf_grid_oos_win_rate": grid_wf["oos_win_rate"],
        "wf_grid_oos_avg_entry_fill_rate": grid_wf["oos_avg_entry_fill_rate"],
        "wf_grid_oos_total_capacity_reject_entries": grid_wf["oos_total_capacity_reject_entries"],
        "wf_grid_oos_total_capacity_reject_exits": grid_wf["oos_total_capacity_reject_exits"],
        "wf_grid_oos_total_funding_carry_pct": grid_wf["oos_total_funding_carry_pct"],
        "wf_ga_oos_return_pct": ga_wf["oos_return_pct"],
        "wf_ga_oos_max_dd_pct": ga_wf["oos_max_dd_pct"],
        "wf_ga_oos_trades": ga_wf["oos_trades"],
        "wf_ga_oos_win_rate": ga_wf["oos_win_rate"],
        "wf_ga_oos_avg_entry_fill_rate": ga_wf["oos_avg_entry_fill_rate"],
        "wf_ga_oos_total_capacity_reject_entries": ga_wf["oos_total_capacity_reject_entries"],
        "wf_ga_oos_total_capacity_reject_exits": ga_wf["oos_total_capacity_reject_exits"],
        "wf_ga_oos_total_funding_carry_pct": ga_wf["oos_total_funding_carry_pct"],
        "wf_ga_minus_grid_return_pct": round(ga_wf["oos_return_pct"] - grid_wf["oos_return_pct"], 2),
        "preferred_method": pref,
        "bear_start_utc": args.bear_start,
    }


def main():
    p = argparse.ArgumentParser(description="Short Regime Strict MVP: MA short + filters + walk-forward + GA")
    p.add_argument("--symbols", nargs="+", default=["BTC-USDT", "ETH-USDT", "SOL-USDT", "BNB-USDT", "UNI-USDT", "STRK-USDT"])
    p.add_argument("--data-source", choices=["binance", "okx"], default="binance")
    p.add_argument("--bar", default="4H")
    p.add_argument("--bars", type=int, default=2190, help="4H one-year ~= 2190")
    p.add_argument("--train-bars", type=int, default=700)
    p.add_argument("--test-bars", type=int, default=240)
    p.add_argument("--workers", type=int, default=3)

    p.add_argument("--ga-generations", type=int, default=6)
    p.add_argument("--ga-pop-size", type=int, default=18)
    p.add_argument("--ga-mutation", type=float, default=0.25)
    p.add_argument("--ga-seed", type=int, default=42)

    p.add_argument("--bear-start", default="2025-10-01", help="UTC date. before this date: no short entries")

    p.add_argument("--entry-order-type", choices=["market", "limit"], default="limit")
    p.add_argument("--exit-order-type", choices=["market", "limit"], default="market")
    p.add_argument("--maker-fee-bps", type=float, default=5.0)
    p.add_argument("--taker-fee-bps", type=float, default=7.0)
    p.add_argument("--market-slippage-bps", type=float, default=4.0)
    p.add_argument("--half-spread-bps", type=float, default=2.0)
    p.add_argument("--limit-offset-bps", type=float, default=8.0)
    p.add_argument("--limit-slippage-bps", type=float, default=0.5)
    p.add_argument("--limit-ttl-bars", type=int, default=2)
    p.add_argument("--limit-fill-model", choices=["probabilistic", "deterministic"], default="probabilistic")
    p.add_argument("--queue-bps", type=float, default=8.0)
    p.add_argument("--limit-fallback-market", action="store_true")

    p.add_argument("--stop-loss-pct", type=float, default=30.0)
    p.add_argument("--take-profit-pct", type=float, default=10.0)
    p.add_argument("--exec-seed", type=int, default=123)

    p.add_argument("--starting-capital-usd", type=float, default=100000.0)
    p.add_argument("--max-participation-rate", type=float, default=0.001)
    p.add_argument("--min-capacity-ratio", type=float, default=1.0)
    p.add_argument("--funding-bps-per-8h", type=float, default=0.0, help="Short-side carry cost per 8h in bps.")

    p.add_argument("--out-dir", default=str((Path(__file__).resolve().parents[1] / "results")))
    args = p.parse_args()

    bear_start_ts = parse_utc_ts(args.bear_start)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    params = build_params()
    rows = []

    workers = max(1, min(args.workers, len(args.symbols)))
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(run_symbol, s, args, params, out_dir, bear_start_ts): s for s in args.symbols}
        for fut in as_completed(futs):
            s = futs[fut]
            r = fut.result()
            rows.append(r)
            print(
                f"{s}: bars={r['bars']} {r['start_iso_utc']} -> {r['end_iso_utc']} "
                f"WF(grid)={r['wf_grid_oos_return_pct']}% DD={r['wf_grid_oos_max_dd_pct']}% Fill={r['wf_grid_oos_avg_entry_fill_rate']}% | "
                f"WF(ga)={r['wf_ga_oos_return_pct']}% DD={r['wf_ga_oos_max_dd_pct']}% Fill={r['wf_ga_oos_avg_entry_fill_rate']}% "
                f"pref={r['preferred_method']}"
            )

    rows.sort(key=lambda x: x["wf_grid_oos_return_pct"], reverse=True)
    summary_csv = out_dir / "multi_symbol_summary.csv"
    write_csv(
        summary_csv,
        rows,
        [
            "symbol",
            "data_source",
            "bars",
            "start_ts",
            "end_ts",
            "start_iso_utc",
            "end_iso_utc",
            "bear_start_utc",
            "wf_folds",
            "wf_grid_oos_return_pct",
            "wf_grid_oos_max_dd_pct",
            "wf_grid_oos_trades",
            "wf_grid_oos_win_rate",
            "wf_grid_oos_avg_entry_fill_rate",
            "wf_grid_oos_total_capacity_reject_entries",
            "wf_grid_oos_total_capacity_reject_exits",
            "wf_grid_oos_total_funding_carry_pct",
            "wf_ga_oos_return_pct",
            "wf_ga_oos_max_dd_pct",
            "wf_ga_oos_trades",
            "wf_ga_oos_win_rate",
            "wf_ga_oos_avg_entry_fill_rate",
            "wf_ga_oos_total_capacity_reject_entries",
            "wf_ga_oos_total_capacity_reject_exits",
            "wf_ga_oos_total_funding_carry_pct",
            "wf_ga_minus_grid_return_pct",
            "preferred_method",
        ],
    )

    with (out_dir / "run_config.json").open("w") as f:
        json.dump(vars(args), f, indent=2)

    print("\nSaved files:")
    print(f"- {summary_csv}")
    print(f"- {out_dir}/run_config.json")
    print(f"- {out_dir}/*_grid_backtest_results.csv")
    print(f"- {out_dir}/*_grid_walkforward_folds.csv")
    print(f"- {out_dir}/*_grid_walkforward_summary.json")
    print(f"- {out_dir}/*_ga_walkforward_folds.csv")
    print(f"- {out_dir}/*_ga_walkforward_summary.json")
    print(f"- {out_dir}/*_ga_fullsample_best.json")


if __name__ == "__main__":
    main()

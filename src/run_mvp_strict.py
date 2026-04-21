#!/usr/bin/env python3
import argparse
import csv
import json
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

FAST_LIST = [5, 8, 10, 12, 15]
SLOW_LIST = [20, 30, 40, 50, 60]
RSI_BUY_LIST = [52, 55, 58]
RSI_SELL_LIST = [42, 45, 48]
BINANCE_BAR_MAP = {
    "1H": "1h",
    "4H": "4h",
    "1D": "1d",
}


# -------------------------
# Data fetch
# -------------------------
def api_get(path: str, params: dict):
    base = "https://www.okx.com"
    url = f"{base}{path}?{urlencode(params)}"
    req = Request(url, headers={"User-Agent": "quant-mvp-strict/1.0"})
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
        raise RuntimeError(f"No candle data returned for {inst}")

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
        raise ValueError(f"Unsupported Binance bar: {bar}. Supported: {', '.join(BINANCE_BAR_MAP.keys())}")

    symbol = inst.replace("-", "").upper()
    base = "https://api.binance.com/api/v3/klines"

    uniq = {}
    end_time = None

    while len(uniq) < total_bars:
        batch = min(1000, total_bars - len(uniq))
        params = {"symbol": symbol, "interval": interval, "limit": batch}
        if end_time is not None:
            params["endTime"] = end_time

        url = f"{base}?{urlencode(params)}"
        req = Request(url, headers={"User-Agent": "quant-mvp-strict/1.0"})
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

        oldest_ts = int(data[0][0])
        if oldest_ts <= 0:
            break
        end_time = oldest_ts - 1

        if len(data) < batch:
            break
        time.sleep(0.03)

    rows = [uniq[k] for k in sorted(uniq.keys())]
    if not rows:
        raise RuntimeError(f"No candle data returned for {inst} from Binance")
    return rows[-total_bars:]


def fetch_candles(inst: str, bar: str, total_bars: int, data_source: str = "okx"):
    source = data_source.lower()
    if source == "okx":
        return fetch_okx_candles(inst, bar, total_bars)
    if source == "binance":
        return fetch_binance_candles(inst, bar, total_bars)
    raise ValueError(f"Unsupported data source: {data_source}")


# -------------------------
# Indicators
# -------------------------
def ema(values, n):
    k = 2 / (n + 1)
    out = []
    e = values[0]
    for v in values:
        e = v * k + e * (1 - k)
        out.append(e)
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


def rolling_avg(values, n):
    out = []
    s = 0.0
    for i, v in enumerate(values):
        s += v
        if i >= n:
            s -= values[i - n]
        out.append(s / min(i + 1, n))
    return out


# -------------------------
# Execution model
# -------------------------
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
        "seed": args.exec_seed,
        "starting_capital_usd": args.starting_capital_usd,
        "max_participation_rate": args.max_participation_rate,
        "min_capacity_ratio": args.min_capacity_ratio,
    }


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
        if side == "buy":
            limit_px = decision_px * (1.0 - off)
        else:
            limit_px = decision_px * (1.0 + off)

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

    touched = (bar["l"] <= order["limit_px"]) if order["side"] == "buy" else (bar["h"] >= order["limit_px"])

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


def backtest_strict(candles, fast, slow, rsi_buy, rsi_sell, exec_cfg, collect_marks=False):
    close = [x["c"] for x in candles]
    efast = ema(close, fast)
    eslow = ema(close, slow)
    r = rsi(close, 14)
    avg_vol20 = rolling_avg([x["v"] for x in candles], 20)

    rng = random.Random(exec_cfg["seed"] + fast * 10000 + slow * 100 + rsi_buy * 10 + rsi_sell)

    equity = 1.0
    peak = 1.0
    max_dd = 0.0

    in_pos = False
    entry_basis = 0.0  # filled buy price including entry fee
    entry_px_raw = 0.0
    pending_entry = None
    pending_exit = None

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
    mark_curve = [1.0] if collect_marks else None

    for i in range(1, len(candles)):
        bar = candles[i]
        vol_ratio = bar["v"] / max(1e-9, avg_vol20[i])

        # 1) Pending entry execution
        if pending_entry is not None and (not in_pos):
            res = try_execute_order(pending_entry, bar, i, vol_ratio, exec_cfg, rng)
            if res["status"] == "filled":
                cap = capacity_ratio(bar, equity, 1.0, exec_cfg)
                if cap >= exec_cfg["min_capacity_ratio"]:
                    fill_px = res["fill_px"]
                    fee = res["fee_bps"] / 10000.0
                    entry_px_raw = fill_px
                    entry_basis = fill_px * (1.0 + fee)
                    in_pos = True
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

        # 2) Pending exit execution
        if pending_exit is not None and in_pos:
            res = try_execute_order(pending_exit, bar, i, vol_ratio, exec_cfg, rng)
            if res["status"] == "filled":
                cap = capacity_ratio(bar, equity, 1.0, exec_cfg)
                if cap >= exec_cfg["min_capacity_ratio"]:
                    fill_px = res["fill_px"]
                    fee = res["fee_bps"] / 10000.0
                    exit_net = fill_px * (1.0 - fee)
                    ret = exit_net / entry_basis
                    equity *= ret
                    trades += 1
                    if ret > 1.0:
                        wins += 1
                    maker_fills += res["is_maker"]
                    taker_fills += 1 - res["is_maker"]
                    in_pos = False
                    pending_exit = None
                    entry_basis = 0.0
                    entry_px_raw = 0.0
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

        # 3) Hard risk controls (intrabar)
        if in_pos and pending_exit is None:
            stop_hit = False
            tp_hit = False
            stop_px = None
            tp_px = None

            if exec_cfg["stop_loss_pct"] > 0:
                stop_px = entry_px_raw * (1.0 - exec_cfg["stop_loss_pct"] / 100.0)
                stop_hit = bar["l"] <= stop_px
            if exec_cfg["take_profit_pct"] > 0:
                tp_px = entry_px_raw * (1.0 + exec_cfg["take_profit_pct"] / 100.0)
                tp_hit = bar["h"] >= tp_px

            if stop_hit or tp_hit:
                if stop_hit:
                    trigger_px = stop_px
                else:
                    trigger_px = tp_px
                fill_px = market_fill_price("sell", trigger_px, exec_cfg)
                fee = exec_cfg["taker_fee_bps"] / 10000.0
                exit_net = fill_px * (1.0 - fee)
                ret = exit_net / entry_basis
                equity *= ret
                trades += 1
                if ret > 1.0:
                    wins += 1
                taker_fills += 1
                in_pos = False
                entry_basis = 0.0
                entry_px_raw = 0.0

        # 4) Drawdown with mark-to-market equity
        mark = equity
        if in_pos:
            est_exit = market_fill_price("sell", bar["c"], exec_cfg)
            est_exit_net = est_exit * (1.0 - exec_cfg["taker_fee_bps"] / 10000.0)
            mark = equity * (est_exit_net / entry_basis)

        if mark > peak:
            peak = mark
        dd = (peak - mark) / max(1e-12, peak)
        if dd > max_dd:
            max_dd = dd
        if collect_marks:
            mark_curve.append(mark)

        # 5) End-of-bar signals -> place order for next bar
        if i >= len(candles) - 1:
            continue

        long_signal = efast[i] > eslow[i] and r[i] >= rsi_buy
        flat_signal = efast[i] < eslow[i] or r[i] <= rsi_sell

        if (not in_pos) and pending_entry is None and long_signal:
            entry_signals += 1
            pending_entry = place_order("buy", exec_cfg["entry_order_type"], candles[i]["c"], i, exec_cfg)

        if in_pos and pending_exit is None and flat_signal:
            exit_signals += 1
            pending_exit = place_order("sell", exec_cfg["exit_order_type"], candles[i]["c"], i, exec_cfg)

    # Forced close on last candle if still in position
    if in_pos:
        last = candles[-1]
        fill_px = market_fill_price("sell", last["c"], exec_cfg)
        fee = exec_cfg["taker_fee_bps"] / 10000.0
        exit_net = fill_px * (1.0 - fee)
        ret = exit_net / entry_basis
        equity *= ret
        trades += 1
        if ret > 1.0:
            wins += 1
        taker_fills += 1
        if collect_marks:
            mark_curve.append(equity)

    win_rate = (wins / trades * 100.0) if trades else 0.0
    total_ret = (equity - 1.0) * 100.0
    max_dd_pct = max_dd * 100.0
    entry_fill_rate = (entries / entry_signals * 100.0) if entry_signals else 0.0
    exit_fill_rate = (trades / exit_signals * 100.0) if exit_signals else 0.0

    # Penalize poor fill quality to avoid paper alpha with impossible fills
    fill_penalty = (100.0 - entry_fill_rate) * 0.15 + (100.0 - exit_fill_rate) * 0.05
    score = total_ret - max_dd_pct * 1.2 - fill_penalty

    return {
        "fast": fast,
        "slow": slow,
        "rsi_buy": rsi_buy,
        "rsi_sell": rsi_sell,
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
        "return_pct": round(total_ret, 2),
        "max_dd_pct": round(max_dd_pct, 2),
        "score": round(score, 2),
        "equity_multiple": equity,
        "equity_marks": mark_curve,
    }


# -------------------------
# Search + walk-forward
# -------------------------
def build_params():
    params = []
    for f in FAST_LIST:
        for s in SLOW_LIST:
            if f >= s:
                continue
            for rb in RSI_BUY_LIST:
                for rs in RSI_SELL_LIST:
                    if rb <= rs:
                        continue
                    params.append((f, s, rb, rs))
    return params


def calc_fold_max_dd(equity_path):
    peak = 1.0
    max_dd = 0.0
    for e in equity_path:
        if e > peak:
            peak = e
        dd = (peak - e) / peak
        if dd > max_dd:
            max_dd = dd
    return max_dd * 100.0


def select_best_grid(train_candles, params, exec_cfg):
    best = None
    for f, s, rb, rs in params:
        r = backtest_strict(train_candles, f, s, rb, rs, exec_cfg)
        if best is None or r["score"] > best["score"]:
            best = r
    return best


def select_best_ga(train_candles, params, exec_cfg, generations, pop_size, mutation_rate, seed):
    rng = random.Random(seed)
    valid_param_set = set(params)
    fast_slow_pairs = [(f, s) for f in FAST_LIST for s in SLOW_LIST if f < s]
    rsi_pairs = [(rb, rs) for rb in RSI_BUY_LIST for rs in RSI_SELL_LIST if rb > rs]

    cache = {}

    def eval_param(p):
        if p not in cache:
            cache[p] = backtest_strict(train_candles, p[0], p[1], p[2], p[3], exec_cfg)
        return cache[p]

    def repair(p):
        f, s, rb, rs = p
        if (f, s) not in fast_slow_pairs:
            f, s = rng.choice(fast_slow_pairs)
        if (rb, rs) not in rsi_pairs:
            rb, rs = rng.choice(rsi_pairs)
        fixed = (f, s, rb, rs)
        if fixed not in valid_param_set:
            fixed = rng.choice(params)
        return fixed

    def tournament(pop, k=3):
        cands = rng.sample(pop, k=min(k, len(pop)))
        cands.sort(key=lambda x: eval_param(x)["score"], reverse=True)
        return cands[0]

    def crossover(a, b):
        child = (
            a[0] if rng.random() < 0.5 else b[0],
            a[1] if rng.random() < 0.5 else b[1],
            a[2] if rng.random() < 0.5 else b[2],
            a[3] if rng.random() < 0.5 else b[3],
        )
        return repair(child)

    def mutate(p):
        f, s, rb, rs = p
        if rng.random() < mutation_rate:
            f = rng.choice(FAST_LIST)
        if rng.random() < mutation_rate:
            s = rng.choice(SLOW_LIST)
        if rng.random() < mutation_rate:
            rb = rng.choice(RSI_BUY_LIST)
        if rng.random() < mutation_rate:
            rs = rng.choice(RSI_SELL_LIST)
        return repair((f, s, rb, rs))

    pop = [rng.choice(params) for _ in range(max(6, pop_size))]
    best_param = pop[0]
    best_score = eval_param(best_param)["score"]

    for _ in range(max(1, generations)):
        pop.sort(key=lambda x: eval_param(x)["score"], reverse=True)
        if eval_param(pop[0])["score"] > best_score:
            best_param = pop[0]
            best_score = eval_param(pop[0])["score"]

        elites = pop[:2]
        new_pop = elites[:]

        while len(new_pop) < len(pop):
            p1 = tournament(pop)
            p2 = tournament(pop)
            child = crossover(p1, p2)
            child = mutate(child)
            new_pop.append(child)

        pop = new_pop

    final_best = max(pop, key=lambda x: eval_param(x)["score"])
    if eval_param(final_best)["score"] > best_score:
        best_param = final_best

    return eval_param(best_param)


def walk_forward(candles, params, train_bars, test_bars, selector, exec_cfg, ga_cfg=None, symbol=""):
    n = len(candles)
    folds = []
    oos_equity = 1.0
    oos_path = [1.0]

    fold_id = 0
    start = 0
    while start + train_bars + test_bars <= n:
        train = candles[start : start + train_bars]
        test = candles[start + train_bars : start + train_bars + test_bars]

        fold_exec_cfg = dict(exec_cfg)
        fold_exec_cfg["seed"] = exec_cfg["seed"] + fold_id + sum(ord(c) for c in symbol)

        if selector == "grid":
            best_train = select_best_grid(train, params, fold_exec_cfg)
        elif selector == "ga":
            seed = ga_cfg["seed"] + fold_id + sum(ord(c) for c in symbol)
            best_train = select_best_ga(
                train,
                params,
                fold_exec_cfg,
                generations=ga_cfg["generations"],
                pop_size=ga_cfg["pop_size"],
                mutation_rate=ga_cfg["mutation_rate"],
                seed=seed,
            )
        else:
            raise ValueError(f"Unknown selector: {selector}")

        test_result = backtest_strict(
            test,
            best_train["fast"],
            best_train["slow"],
            best_train["rsi_buy"],
            best_train["rsi_sell"],
            fold_exec_cfg,
            collect_marks=True,
        )

        fold_start_equity = oos_equity
        oos_equity *= test_result["equity_multiple"]
        marks = test_result.get("equity_marks") or [1.0, test_result["equity_multiple"]]
        for m in marks[1:]:
            oos_path.append(fold_start_equity * m)

        folds.append(
            {
                "fold": fold_id,
                "train_start_ts": train[0]["ts"],
                "train_end_ts": train[-1]["ts"],
                "test_start_ts": test[0]["ts"],
                "test_end_ts": test[-1]["ts"],
                "best_fast": best_train["fast"],
                "best_slow": best_train["slow"],
                "best_rsi_buy": best_train["rsi_buy"],
                "best_rsi_sell": best_train["rsi_sell"],
                "train_score": best_train["score"],
                "train_return_pct": best_train["return_pct"],
                "test_return_pct": test_result["return_pct"],
                "test_max_dd_pct": test_result["max_dd_pct"],
                "test_trades": test_result["trades"],
                "test_win_rate": test_result["win_rate"],
                "test_entry_fill_rate": test_result["entry_fill_rate"],
                "test_exit_fill_rate": test_result["exit_fill_rate"],
                "test_limit_expired": test_result["limit_expired"],
                "test_capacity_reject_entries": test_result["capacity_reject_entries"],
                "test_capacity_reject_exits": test_result["capacity_reject_exits"],
                "oos_equity_after_fold": round(oos_equity, 6),
            }
        )

        fold_id += 1
        start += test_bars

    if not folds:
        raise RuntimeError("Not enough bars for walk-forward. Increase --bars or reduce train/test bars.")

    total_trades = sum(int(x["test_trades"]) for x in folds)
    weighted_wins = sum(float(x["test_trades"]) * float(x["test_win_rate"]) / 100.0 for x in folds)
    oos_win_rate = (weighted_wins / total_trades * 100.0) if total_trades else 0.0

    total_expired = sum(int(x["test_limit_expired"]) for x in folds)
    total_cap_reject_entries = sum(int(x["test_capacity_reject_entries"]) for x in folds)
    total_cap_reject_exits = sum(int(x["test_capacity_reject_exits"]) for x in folds)
    avg_entry_fill = sum(float(x["test_entry_fill_rate"]) for x in folds) / len(folds)
    avg_exit_fill = sum(float(x["test_exit_fill_rate"]) for x in folds) / len(folds)

    summary = {
        "method": selector,
        "folds": len(folds),
        "train_bars": train_bars,
        "test_bars": test_bars,
        "oos_return_pct": round((oos_equity - 1.0) * 100.0, 2),
        "oos_max_dd_pct": round(calc_fold_max_dd(oos_path), 2),
        "oos_trades": total_trades,
        "oos_win_rate": round(oos_win_rate, 2),
        "oos_avg_entry_fill_rate": round(avg_entry_fill, 2),
        "oos_avg_exit_fill_rate": round(avg_exit_fill, 2),
        "oos_total_limit_expired": total_expired,
        "oos_total_capacity_reject_entries": total_cap_reject_entries,
        "oos_total_capacity_reject_exits": total_cap_reject_exits,
        "oos_equity_multiple": round(oos_equity, 6),
    }

    return folds, summary


# -------------------------
# IO helpers
# -------------------------
def write_csv(path: Path, rows, fieldnames):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def ts_to_iso(ts_ms):
    from datetime import datetime, timezone

    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()


def run_symbol(inst: str, args, params, out_dir: Path):
    candles = fetch_candles(inst=inst, bar=args.bar, total_bars=args.bars, data_source=args.data_source)
    exec_cfg = build_exec_cfg(args)

    grid_results = []
    for f, s, rb, rs in params:
        cfg = dict(exec_cfg)
        cfg["seed"] = exec_cfg["seed"] + sum(ord(c) for c in inst)
        grid_results.append(backtest_strict(candles, f, s, rb, rs, cfg))
    grid_results.sort(key=lambda x: x["score"], reverse=True)

    ga_full = select_best_ga(
        candles,
        params,
        exec_cfg,
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
        train_bars=args.train_bars,
        test_bars=args.test_bars,
        selector="grid",
        exec_cfg=exec_cfg,
        symbol=inst,
    )

    ga_folds, ga_wf = walk_forward(
        candles,
        params,
        train_bars=args.train_bars,
        test_bars=args.test_bars,
        selector="ga",
        exec_cfg=exec_cfg,
        ga_cfg=ga_cfg,
        symbol=inst,
    )

    safe = inst.replace("-", "_")

    grid_rank_csv = out_dir / f"{safe}_grid_backtest_results.csv"
    write_csv(
        grid_rank_csv,
        grid_results,
        [
            "fast",
            "slow",
            "rsi_buy",
            "rsi_sell",
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
            "return_pct",
            "max_dd_pct",
            "score",
            "equity_multiple",
        ],
    )

    grid_folds_csv = out_dir / f"{safe}_grid_walkforward_folds.csv"
    write_csv(
        grid_folds_csv,
        grid_folds,
        [
            "fold",
            "train_start_ts",
            "train_end_ts",
            "test_start_ts",
            "test_end_ts",
            "best_fast",
            "best_slow",
            "best_rsi_buy",
            "best_rsi_sell",
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
            "oos_equity_after_fold",
        ],
    )

    ga_folds_csv = out_dir / f"{safe}_ga_walkforward_folds.csv"
    write_csv(
        ga_folds_csv,
        ga_folds,
        [
            "fold",
            "train_start_ts",
            "train_end_ts",
            "test_start_ts",
            "test_end_ts",
            "best_fast",
            "best_slow",
            "best_rsi_buy",
            "best_rsi_sell",
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
            "oos_equity_after_fold",
        ],
    )

    grid_json = out_dir / f"{safe}_grid_walkforward_summary.json"
    ga_json = out_dir / f"{safe}_ga_walkforward_summary.json"
    ga_full_json = out_dir / f"{safe}_ga_fullsample_best.json"
    with grid_json.open("w") as f:
        json.dump(grid_wf, f, indent=2)
    with ga_json.open("w") as f:
        json.dump(ga_wf, f, indent=2)
    with ga_full_json.open("w") as f:
        json.dump(ga_full, f, indent=2)

    score_grid = grid_wf["oos_return_pct"] - 1.2 * grid_wf["oos_max_dd_pct"] + 0.2 * grid_wf["oos_avg_entry_fill_rate"]
    score_ga = ga_wf["oos_return_pct"] - 1.2 * ga_wf["oos_max_dd_pct"] + 0.2 * ga_wf["oos_avg_entry_fill_rate"]
    preferred = "ga" if score_ga > score_grid else "grid"

    return {
        "symbol": inst,
        "data_source": args.data_source,
        "bars": len(candles),
        "start_ts": candles[0]["ts"],
        "end_ts": candles[-1]["ts"],
        "start_iso_utc": ts_to_iso(candles[0]["ts"]),
        "end_iso_utc": ts_to_iso(candles[-1]["ts"]),
        "grid_top_fast": grid_results[0]["fast"],
        "grid_top_slow": grid_results[0]["slow"],
        "grid_top_rsi_buy": grid_results[0]["rsi_buy"],
        "grid_top_rsi_sell": grid_results[0]["rsi_sell"],
        "grid_top_return_pct": grid_results[0]["return_pct"],
        "grid_top_max_dd_pct": grid_results[0]["max_dd_pct"],
        "grid_top_entry_fill_rate": grid_results[0]["entry_fill_rate"],
        "ga_full_fast": ga_full["fast"],
        "ga_full_slow": ga_full["slow"],
        "ga_full_rsi_buy": ga_full["rsi_buy"],
        "ga_full_rsi_sell": ga_full["rsi_sell"],
        "ga_full_return_pct": ga_full["return_pct"],
        "ga_full_max_dd_pct": ga_full["max_dd_pct"],
        "ga_full_entry_fill_rate": ga_full["entry_fill_rate"],
        "wf_folds": grid_wf["folds"],
        "wf_grid_oos_return_pct": grid_wf["oos_return_pct"],
        "wf_grid_oos_max_dd_pct": grid_wf["oos_max_dd_pct"],
        "wf_grid_oos_trades": grid_wf["oos_trades"],
        "wf_grid_oos_win_rate": grid_wf["oos_win_rate"],
        "wf_grid_oos_avg_entry_fill_rate": grid_wf["oos_avg_entry_fill_rate"],
        "wf_grid_oos_avg_exit_fill_rate": grid_wf["oos_avg_exit_fill_rate"],
        "wf_grid_oos_total_limit_expired": grid_wf["oos_total_limit_expired"],
        "wf_grid_oos_total_capacity_reject_entries": grid_wf["oos_total_capacity_reject_entries"],
        "wf_grid_oos_total_capacity_reject_exits": grid_wf["oos_total_capacity_reject_exits"],
        "wf_ga_oos_return_pct": ga_wf["oos_return_pct"],
        "wf_ga_oos_max_dd_pct": ga_wf["oos_max_dd_pct"],
        "wf_ga_oos_trades": ga_wf["oos_trades"],
        "wf_ga_oos_win_rate": ga_wf["oos_win_rate"],
        "wf_ga_oos_avg_entry_fill_rate": ga_wf["oos_avg_entry_fill_rate"],
        "wf_ga_oos_avg_exit_fill_rate": ga_wf["oos_avg_exit_fill_rate"],
        "wf_ga_oos_total_limit_expired": ga_wf["oos_total_limit_expired"],
        "wf_ga_oos_total_capacity_reject_entries": ga_wf["oos_total_capacity_reject_entries"],
        "wf_ga_oos_total_capacity_reject_exits": ga_wf["oos_total_capacity_reject_exits"],
        "wf_ga_minus_grid_return_pct": round(ga_wf["oos_return_pct"] - grid_wf["oos_return_pct"], 2),
        "preferred_method": preferred,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Strict Quant MVP: walk-forward + GA + execution realism (market/limit fill model)"
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=["BTC-USDT", "ETH-USDT", "SOL-USDT", "BNB-USDT", "UNI-USDT", "STRK-USDT"],
    )
    parser.add_argument("--bar", default="4H")
    parser.add_argument("--data-source", choices=["okx", "binance"], default="okx")
    parser.add_argument("--bars", type=int, default=1440)
    parser.add_argument("--train-bars", type=int, default=500)
    parser.add_argument("--test-bars", type=int, default=200)
    parser.add_argument("--workers", type=int, default=4)

    parser.add_argument("--ga-generations", type=int, default=8)
    parser.add_argument("--ga-pop-size", type=int, default=24)
    parser.add_argument("--ga-mutation", type=float, default=0.25)
    parser.add_argument("--ga-seed", type=int, default=42)

    parser.add_argument("--entry-order-type", choices=["market", "limit"], default="limit")
    parser.add_argument("--exit-order-type", choices=["market", "limit"], default="market")
    parser.add_argument("--maker-fee-bps", type=float, default=5.0)
    parser.add_argument("--taker-fee-bps", type=float, default=7.0)
    parser.add_argument("--market-slippage-bps", type=float, default=4.0)
    parser.add_argument("--half-spread-bps", type=float, default=2.0)
    parser.add_argument("--limit-offset-bps", type=float, default=6.0)
    parser.add_argument("--limit-slippage-bps", type=float, default=0.5)
    parser.add_argument("--limit-ttl-bars", type=int, default=2)
    parser.add_argument("--limit-fill-model", choices=["probabilistic", "deterministic"], default="probabilistic")
    parser.add_argument("--queue-bps", type=float, default=8.0)
    parser.add_argument("--limit-fallback-market", action="store_true")
    parser.add_argument("--starting-capital-usd", type=float, default=100000.0)
    parser.add_argument("--max-participation-rate", type=float, default=0.001)
    parser.add_argument("--min-capacity-ratio", type=float, default=1.0)

    parser.add_argument("--stop-loss-pct", type=float, default=30.0)
    parser.add_argument("--take-profit-pct", type=float, default=0.0)
    parser.add_argument("--exec-seed", type=int, default=123)

    parser.add_argument(
        "--out-dir",
        default=str((Path(__file__).resolve().parents[1] / "results")),
    )
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    params = build_params()
    summary_rows = []

    workers = max(1, min(args.workers, len(args.symbols)))
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(run_symbol, inst, args, params, out_dir): inst for inst in args.symbols}
        for fut in as_completed(futures):
            inst = futures[fut]
            row = fut.result()
            summary_rows.append(row)
            print(
                f"{inst}: bars={row['bars']} {row['start_iso_utc']} -> {row['end_iso_utc']} "
                f"WF(grid)={row['wf_grid_oos_return_pct']}% DD={row['wf_grid_oos_max_dd_pct']}% "
                f"Fill={row['wf_grid_oos_avg_entry_fill_rate']}% | "
                f"WF(ga)={row['wf_ga_oos_return_pct']}% DD={row['wf_ga_oos_max_dd_pct']}% "
                f"Fill={row['wf_ga_oos_avg_entry_fill_rate']}% pref={row['preferred_method']}"
            )

    summary_rows.sort(key=lambda x: x["wf_grid_oos_return_pct"], reverse=True)
    summary_csv = out_dir / "multi_symbol_summary.csv"

    write_csv(
        summary_csv,
        summary_rows,
        [
            "symbol",
            "data_source",
            "bars",
            "start_ts",
            "end_ts",
            "start_iso_utc",
            "end_iso_utc",
            "grid_top_fast",
            "grid_top_slow",
            "grid_top_rsi_buy",
            "grid_top_rsi_sell",
            "grid_top_return_pct",
            "grid_top_max_dd_pct",
            "grid_top_entry_fill_rate",
            "ga_full_fast",
            "ga_full_slow",
            "ga_full_rsi_buy",
            "ga_full_rsi_sell",
            "ga_full_return_pct",
            "ga_full_max_dd_pct",
            "ga_full_entry_fill_rate",
            "wf_folds",
            "wf_grid_oos_return_pct",
            "wf_grid_oos_max_dd_pct",
            "wf_grid_oos_trades",
            "wf_grid_oos_win_rate",
            "wf_grid_oos_avg_entry_fill_rate",
            "wf_grid_oos_avg_exit_fill_rate",
            "wf_grid_oos_total_limit_expired",
            "wf_grid_oos_total_capacity_reject_entries",
            "wf_grid_oos_total_capacity_reject_exits",
            "wf_ga_oos_return_pct",
            "wf_ga_oos_max_dd_pct",
            "wf_ga_oos_trades",
            "wf_ga_oos_win_rate",
            "wf_ga_oos_avg_entry_fill_rate",
            "wf_ga_oos_avg_exit_fill_rate",
            "wf_ga_oos_total_limit_expired",
            "wf_ga_oos_total_capacity_reject_entries",
            "wf_ga_oos_total_capacity_reject_exits",
            "wf_ga_minus_grid_return_pct",
            "preferred_method",
        ],
    )

    run_cfg_json = out_dir / "run_config.json"
    with run_cfg_json.open("w") as f:
        json.dump(vars(args), f, indent=2)

    print("\nSaved files:")
    print(f"- {summary_csv}")
    print(f"- {run_cfg_json}")
    print(f"- {out_dir}/*_grid_backtest_results.csv")
    print(f"- {out_dir}/*_grid_walkforward_folds.csv")
    print(f"- {out_dir}/*_grid_walkforward_summary.json")
    print(f"- {out_dir}/*_ga_walkforward_folds.csv")
    print(f"- {out_dir}/*_ga_walkforward_summary.json")
    print(f"- {out_dir}/*_ga_fullsample_best.json")


if __name__ == "__main__":
    main()

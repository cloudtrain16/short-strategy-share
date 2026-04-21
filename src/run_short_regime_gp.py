#!/usr/bin/env python3
import argparse
import csv
import json
import math
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

BINANCE_BAR_MAP = {"1H": "1h", "4H": "4h", "1D": "1d"}

UNARY_OPS = ["neg", "abs", "tanh"]
BINARY_OPS = ["add", "sub", "mul", "div", "max", "min"]
CONSTS = [-2.0, -1.0, -0.5, 0.5, 1.0, 2.0]

FEATURE_NAMES = [
    "dist_ma7_x50",
    "dist_ma20_x50",
    "dist_ma52_x50",
    "rsi_n_x4",
    "vol20_x20",
    "trend_gap_x30",
    "ret1_x50",
    "regime_flag",
]


def clamp(x, lo=-20.0, hi=20.0):
    if math.isnan(x) or math.isinf(x):
        return 0.0
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


def parse_utc_ts(date_str: str):
    dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def ts_to_iso(ts_ms):
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()


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

        req = Request(f"{base}?{urlencode(params)}", headers={"User-Agent": "quant-short-gp/1.0"})
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


def sma(values, n):
    out = []
    s = 0.0
    for i, v in enumerate(values):
        s += v
        if i >= n:
            s -= values[i - n]
        out.append(s / min(i + 1, n))
    return out


def rolling_std(values, n):
    out = []
    for i in range(len(values)):
        st = max(0, i - n + 1)
        win = values[st : i + 1]
        m = sum(win) / len(win)
        var = sum((x - m) ** 2 for x in win) / len(win)
        out.append(math.sqrt(var))
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

    ag = sum(gains[:n]) / n
    al = sum(losses[:n]) / n
    out = [50.0] * n

    for i in range(n, len(gains)):
        ag = (ag * (n - 1) + gains[i]) / n
        al = (al * (n - 1) + losses[i]) / n
        rs = ag / (al if al else 1e-9)
        out.append(100 - 100 / (1 + rs))

    out.append(out[-1])
    return out[: len(values)]


def build_features(candles, bear_start_ts):
    close = [x["c"] for x in candles]
    high = [x["h"] for x in candles]
    low = [x["l"] for x in candles]

    ma7 = sma(close, 7)
    ma20 = sma(close, 20)
    ma52 = sma(close, 52)
    r = rsi(close, 14)

    ret = [0.0]
    for i in range(1, len(close)):
        ret.append((close[i] / close[i - 1]) - 1.0)
    vol20 = rolling_std(ret, 20)

    feats = []
    for i in range(len(close)):
        c = close[i]
        m7 = ma7[i]
        m20 = ma20[i]
        m52 = ma52[i]
        regime = 1.0 if candles[i]["ts"] >= bear_start_ts else -1.0

        dist7 = (c - m7) / c if c > 0 else 0.0
        dist20 = (c - m20) / c if c > 0 else 0.0
        dist52 = (c - m52) / c if c > 0 else 0.0
        rsi_n = (r[i] - 50.0) / 50.0
        vg = vol20[i]
        trend_gap = (m52 - m20) / c if c > 0 else 0.0
        re1 = ret[i]

        feats.append([
            clamp(dist7 * 50),
            clamp(dist20 * 50),
            clamp(dist52 * 50),
            clamp(rsi_n * 4),
            clamp(vg * 20),
            clamp(trend_gap * 30),
            clamp(re1 * 50),
            clamp(regime, -2.0, 2.0),
        ])

    return feats, ma7, ma20, ma52, r


def random_terminal(rng, n_vars):
    if rng.random() < 0.7:
        return ("var", rng.randrange(n_vars))
    return ("const", rng.choice(CONSTS))


def random_tree(rng, max_depth, n_vars, grow=True):
    if max_depth <= 0:
        return random_terminal(rng, n_vars)
    if grow and rng.random() < 0.35:
        return random_terminal(rng, n_vars)
    if rng.random() < 0.3:
        op = rng.choice(UNARY_OPS)
        return ("u", op, random_tree(rng, max_depth - 1, n_vars, grow))
    op = rng.choice(BINARY_OPS)
    return (
        "b",
        op,
        random_tree(rng, max_depth - 1, n_vars, grow),
        random_tree(rng, max_depth - 1, n_vars, grow),
    )


def eval_node(node, x):
    t = node[0]
    if t == "var":
        return x[node[1]]
    if t == "const":
        return node[1]
    if t == "u":
        op = node[1]
        a = eval_node(node[2], x)
        if op == "neg":
            return clamp(-a)
        if op == "abs":
            return clamp(abs(a))
        if op == "tanh":
            return clamp(math.tanh(a))
        return 0.0

    op = node[1]
    a = eval_node(node[2], x)
    b = eval_node(node[3], x)
    if op == "add":
        return clamp(a + b)
    if op == "sub":
        return clamp(a - b)
    if op == "mul":
        return clamp(a * b)
    if op == "div":
        return clamp(a / b) if abs(b) > 1e-9 else clamp(a)
    if op == "max":
        return clamp(max(a, b))
    if op == "min":
        return clamp(min(a, b))
    return 0.0


def tree_to_str(node):
    t = node[0]
    if t == "var":
        idx = node[1]
        return FEATURE_NAMES[idx] if 0 <= idx < len(FEATURE_NAMES) else f"x{idx}"
    if t == "const":
        return f"{node[1]:.2f}"
    if t == "u":
        return f"{node[1]}({tree_to_str(node[2])})"
    if t == "b":
        return f"({tree_to_str(node[2])} {node[1]} {tree_to_str(node[3])})"
    return "?"


def collect_paths(node, path=()):
    out = [path]
    if node[0] == "u":
        out += collect_paths(node[2], path + (2,))
    elif node[0] == "b":
        out += collect_paths(node[2], path + (2,))
        out += collect_paths(node[3], path + (3,))
    return out


def get_subtree(node, path):
    cur = node
    for idx in path:
        cur = cur[idx]
    return cur


def set_subtree(node, path, new_sub):
    if not path:
        return deepcopy(new_sub)
    node = list(node)
    idx = path[0]
    node[idx] = set_subtree(node[idx], path[1:], new_sub)
    return tuple(node)


def crossover(rng, a, b):
    pa = rng.choice(collect_paths(a))
    pb = rng.choice(collect_paths(b))
    sub_b = deepcopy(get_subtree(b, pb))
    return set_subtree(a, pa, sub_b)


def mutate(rng, node, n_vars, max_depth):
    p = rng.choice(collect_paths(node))
    new_sub = random_tree(rng, rng.randint(0, max_depth), n_vars, grow=True)
    return set_subtree(node, p, new_sub)


def capacity_ratio(bar, equity_multiple, starting_capital_usd, max_participation_rate):
    desired = max(1e-9, equity_multiple * starting_capital_usd)
    bar_quote = max(0.0, bar["v"] * bar["c"])
    allow = bar_quote * max_participation_rate
    return allow / desired


def short_ret_multiple(entry_net_sell_px, exit_cost_buy_px):
    raw = 1.0 + (entry_net_sell_px - exit_cost_buy_px) / max(1e-12, entry_net_sell_px)
    return max(0.01, raw)


def backtest_gp_short(candles, feats, ma7, ma20, ma52, rsi14, tree, cfg, bear_start_ts):
    close = [x["c"] for x in candles]

    equity = 1.0
    peak = 1.0
    mdd = 0.0

    in_pos = False
    entry_raw = 0.0
    entry_net_sell = 0.0
    entry_i = -10**9

    trades = 0
    wins = 0
    entry_signals = 0
    entries = 0
    cap_reject_entries = 0

    for i in range(1, len(close)):
        bar = candles[i]
        score = eval_node(tree, feats[i])

        # hard exits when in short
        if in_pos:
            stop_px = entry_raw * (1.0 + cfg["stop_loss_pct"] / 100.0)
            tp_px = entry_raw * (1.0 - cfg["take_profit_pct"] / 100.0)
            stop_hit = bar["h"] >= stop_px
            tp_hit = bar["l"] <= tp_px
            if stop_hit or tp_hit:
                trigger = stop_px if stop_hit else tp_px
                cover = trigger * (1.0 + cfg["slippage"])
                exit_cost = cover * (1.0 + cfg["fee"])
                ret = short_ret_multiple(entry_net_sell, exit_cost)
                equity *= ret
                trades += 1
                if ret > 1.0:
                    wins += 1
                in_pos = False
                entry_raw = 0.0
                entry_net_sell = 0.0

        # mark-to-market dd
        mark = equity
        if in_pos:
            cover = bar["c"] * (1.0 + cfg["slippage"])
            exit_cost = cover * (1.0 + cfg["fee"])
            mark = equity * short_ret_multiple(entry_net_sell, exit_cost)

        if mark > peak:
            peak = mark
        dd = (peak - mark) / max(1e-12, peak)
        if dd > mdd:
            mdd = dd

        if i >= len(close) - 1:
            continue

        in_bear = candles[i]["ts"] >= bear_start_ts
        downtrend = ma20[i] < ma52[i]
        rebound = close[i] > ma7[i] and close[i] > ma20[i] and close[i] > ma52[i]
        base_short = in_bear and downtrend and rebound

        if (not in_pos) and base_short and score > cfg["entry_thr"]:
            entry_signals += 1
            cap = capacity_ratio(bar, equity, cfg["starting_capital_usd"], cfg["max_participation_rate"])
            if cap >= cfg["min_capacity_ratio"]:
                sell = bar["c"] * (1.0 - cfg["slippage"])
                entry_raw = sell
                entry_net_sell = sell * (1.0 - cfg["fee"])
                in_pos = True
                entries += 1
                entry_i = i
            else:
                cap_reject_entries += 1

        if in_pos:
            cover_sig = score < cfg["exit_thr"] or rsi14[i] < cfg["rsi_cover"] or (i - entry_i) >= cfg["max_hold_bars"]
            if cover_sig:
                cover = bar["c"] * (1.0 + cfg["slippage"])
                exit_cost = cover * (1.0 + cfg["fee"])
                ret = short_ret_multiple(entry_net_sell, exit_cost)
                equity *= ret
                trades += 1
                if ret > 1.0:
                    wins += 1
                in_pos = False
                entry_raw = 0.0
                entry_net_sell = 0.0

    if in_pos:
        cover = close[-1] * (1.0 + cfg["slippage"])
        exit_cost = cover * (1.0 + cfg["fee"])
        ret = short_ret_multiple(entry_net_sell, exit_cost)
        equity *= ret
        trades += 1
        if ret > 1.0:
            wins += 1

    win_rate = (wins / trades * 100.0) if trades else 0.0
    fill_rate = (entries / entry_signals * 100.0) if entry_signals else 0.0
    ret_pct = (equity - 1.0) * 100.0
    dd_pct = mdd * 100.0
    score = ret_pct - 1.2 * dd_pct - (100.0 - fill_rate) * 0.1 - (0.5 if trades < 3 else 0.0)

    return {
        "return_pct": round(ret_pct, 2),
        "max_dd_pct": round(dd_pct, 2),
        "trades": trades,
        "win_rate": round(win_rate, 2),
        "entry_fill_rate": round(fill_rate, 2),
        "cap_reject_entries": cap_reject_entries,
        "score": round(score, 2),
        "equity_multiple": equity,
    }


def evolve_gp(train_c, train_f, ma7, ma20, ma52, rsi14, cfg, bear_start_ts, n_vars, generations, pop_size, max_depth, mut_prob, cx_prob, seed):
    rng = random.Random(seed)
    pop = [random_tree(rng, max_depth, n_vars, grow=True) for _ in range(pop_size)]

    def fitness(tree):
        return backtest_gp_short(train_c, train_f, ma7, ma20, ma52, rsi14, tree, cfg, bear_start_ts)

    scored = [(t, fitness(t)) for t in pop]

    for _ in range(generations):
        scored.sort(key=lambda x: x[1]["score"], reverse=True)
        elites = [scored[0][0], scored[1][0]]
        new_pop = [deepcopy(elites[0]), deepcopy(elites[1])]

        def tournament(k=3):
            c = rng.sample(scored, k=min(k, len(scored)))
            c.sort(key=lambda x: x[1]["score"], reverse=True)
            return deepcopy(c[0][0])

        while len(new_pop) < pop_size:
            a = tournament()
            b = tournament()
            child = a
            if rng.random() < cx_prob:
                child = crossover(rng, a, b)
            if rng.random() < mut_prob:
                child = mutate(rng, child, n_vars, max_depth)
            new_pop.append(child)

        pop = new_pop
        scored = [(t, fitness(t)) for t in pop]

    scored.sort(key=lambda x: x[1]["score"], reverse=True)
    return scored[0][0], scored[0][1]


def walk_forward_gp(candles, feats, ma7, ma20, ma52, rsi14, cfg, bear_start_ts, args, sym_seed):
    n = len(candles)
    folds = []
    oos_eq = 1.0
    oos_curve = [1.0]

    fold = 0
    start = 0
    while start + args.train_bars + args.test_bars <= n:
        tr_c = candles[start : start + args.train_bars]
        te_c = candles[start + args.train_bars : start + args.train_bars + args.test_bars]

        tr_f = feats[start : start + args.train_bars]
        te_f = feats[start + args.train_bars : start + args.train_bars + args.test_bars]

        tr_ma7 = ma7[start : start + args.train_bars]
        te_ma7 = ma7[start + args.train_bars : start + args.train_bars + args.test_bars]
        tr_ma20 = ma20[start : start + args.train_bars]
        te_ma20 = ma20[start + args.train_bars : start + args.train_bars + args.test_bars]
        tr_ma52 = ma52[start : start + args.train_bars]
        te_ma52 = ma52[start + args.train_bars : start + args.train_bars + args.test_bars]
        tr_rsi = rsi14[start : start + args.train_bars]
        te_rsi = rsi14[start + args.train_bars : start + args.train_bars + args.test_bars]

        best_tree, train_res = evolve_gp(
            tr_c,
            tr_f,
            tr_ma7,
            tr_ma20,
            tr_ma52,
            tr_rsi,
            cfg,
            bear_start_ts,
            n_vars=len(feats[0]),
            generations=args.generations,
            pop_size=args.pop_size,
            max_depth=args.max_depth,
            mut_prob=args.mutation,
            cx_prob=args.crossover,
            seed=args.seed + sym_seed + fold,
        )

        test_res = backtest_gp_short(te_c, te_f, te_ma7, te_ma20, te_ma52, te_rsi, best_tree, cfg, bear_start_ts)
        prev = oos_eq
        oos_eq *= test_res["equity_multiple"]
        oos_curve.append(oos_eq)

        folds.append(
            {
                "fold": fold,
                "train_start_ts": tr_c[0]["ts"],
                "train_end_ts": tr_c[-1]["ts"],
                "test_start_ts": te_c[0]["ts"],
                "test_end_ts": te_c[-1]["ts"],
                "best_expr": tree_to_str(best_tree),
                "train_score": train_res["score"],
                "train_return_pct": train_res["return_pct"],
                "test_score": test_res["score"],
                "test_return_pct": test_res["return_pct"],
                "test_max_dd_pct": test_res["max_dd_pct"],
                "test_trades": test_res["trades"],
                "test_win_rate": test_res["win_rate"],
                "test_entry_fill_rate": test_res["entry_fill_rate"],
                "test_cap_reject_entries": test_res["cap_reject_entries"],
                "oos_equity_after_fold": round(oos_eq, 6),
            }
        )

        fold += 1
        start += args.test_bars

    if not folds:
        raise RuntimeError("Not enough bars for walk-forward")

    peak = 1.0
    mdd = 0.0
    for x in oos_curve:
        if x > peak:
            peak = x
        dd = (peak - x) / max(1e-12, peak)
        if dd > mdd:
            mdd = dd

    total_trades = sum(int(x["test_trades"]) for x in folds)
    weighted_wins = sum(float(x["test_trades"]) * float(x["test_win_rate"]) / 100.0 for x in folds)
    avg_fill = sum(float(x["test_entry_fill_rate"]) for x in folds) / len(folds)

    summary = {
        "folds": len(folds),
        "oos_return_pct": round((oos_eq - 1.0) * 100.0, 2),
        "oos_max_dd_pct": round(mdd * 100.0, 2),
        "oos_trades": total_trades,
        "oos_win_rate": round((weighted_wins / total_trades * 100.0) if total_trades else 0.0, 2),
        "oos_avg_entry_fill_rate": round(avg_fill, 2),
        "oos_total_cap_reject_entries": sum(int(x["test_cap_reject_entries"]) for x in folds),
        "oos_equity_multiple": round(oos_eq, 6),
    }
    return folds, summary


def write_csv(path: Path, rows, fields):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def run_symbol(sym, args, out_dir):
    candles = fetch_binance_candles(sym, args.bar, args.bars)
    bear_start_ts = parse_utc_ts(args.bear_start)
    feats, ma7, ma20, ma52, rsi14 = build_features(candles, bear_start_ts)

    cfg = {
        "fee": args.fee_bps / 10000.0,
        "slippage": args.slippage_bps / 10000.0,
        "entry_thr": args.entry_thr,
        "exit_thr": args.exit_thr,
        "rsi_cover": args.rsi_cover,
        "max_hold_bars": args.max_hold_bars,
        "stop_loss_pct": args.stop_loss_pct,
        "take_profit_pct": args.take_profit_pct,
        "starting_capital_usd": args.starting_capital_usd,
        "max_participation_rate": args.max_participation_rate,
        "min_capacity_ratio": args.min_capacity_ratio,
    }

    seed_shift = sum(ord(c) for c in sym)
    folds, wf = walk_forward_gp(candles, feats, ma7, ma20, ma52, rsi14, cfg, bear_start_ts, args, seed_shift)

    # full sample tree for reference
    best_tree, best_train = evolve_gp(
        candles,
        feats,
        ma7,
        ma20,
        ma52,
        rsi14,
        cfg,
        bear_start_ts,
        n_vars=len(feats[0]),
        generations=args.generations,
        pop_size=args.pop_size,
        max_depth=args.max_depth,
        mut_prob=args.mutation,
        cx_prob=args.crossover,
        seed=args.seed + seed_shift,
    )

    safe = sym.replace("-", "_")
    write_csv(
        out_dir / f"{safe}_gp_walkforward_folds.csv",
        folds,
        [
            "fold",
            "train_start_ts",
            "train_end_ts",
            "test_start_ts",
            "test_end_ts",
            "best_expr",
            "train_score",
            "train_return_pct",
            "test_score",
            "test_return_pct",
            "test_max_dd_pct",
            "test_trades",
            "test_win_rate",
            "test_entry_fill_rate",
            "test_cap_reject_entries",
            "oos_equity_after_fold",
        ],
    )

    with (out_dir / f"{safe}_gp_walkforward_summary.json").open("w") as f:
        json.dump(wf, f, indent=2)
    with (out_dir / f"{safe}_gp_fullsample_best.json").open("w") as f:
        json.dump(
            {
                "symbol": sym,
                "bar": args.bar,
                "bars": len(candles),
                "start_iso_utc": ts_to_iso(candles[0]["ts"]),
                "end_iso_utc": ts_to_iso(candles[-1]["ts"]),
                "bear_start_utc": args.bear_start,
                "feature_names": FEATURE_NAMES,
                "best_expression": tree_to_str(best_tree),
                "metrics": best_train,
            },
            f,
            indent=2,
        )

    return {
        "symbol": sym,
        "bars": len(candles),
        "start_iso_utc": ts_to_iso(candles[0]["ts"]),
        "end_iso_utc": ts_to_iso(candles[-1]["ts"]),
        "bear_start_utc": args.bear_start,
        "wf_folds": wf["folds"],
        "gp_oos_return_pct": wf["oos_return_pct"],
        "gp_oos_max_dd_pct": wf["oos_max_dd_pct"],
        "gp_oos_trades": wf["oos_trades"],
        "gp_oos_win_rate": wf["oos_win_rate"],
        "gp_oos_avg_entry_fill_rate": wf["oos_avg_entry_fill_rate"],
        "gp_oos_total_cap_reject_entries": wf["oos_total_cap_reject_entries"],
        "gp_best_expr": tree_to_str(best_tree),
    }


def main():
    p = argparse.ArgumentParser(description="Short Regime GP tuner (1y/4H default)")
    p.add_argument("--symbols", nargs="+", default=["BTC-USDT", "ETH-USDT", "SOL-USDT", "BNB-USDT", "UNI-USDT", "STRK-USDT"])
    p.add_argument("--bar", default="4H")
    p.add_argument("--bars", type=int, default=2190)
    p.add_argument("--train-bars", type=int, default=700)
    p.add_argument("--test-bars", type=int, default=240)
    p.add_argument("--workers", type=int, default=3)

    p.add_argument("--generations", type=int, default=10)
    p.add_argument("--pop-size", type=int, default=36)
    p.add_argument("--max-depth", type=int, default=4)
    p.add_argument("--mutation", type=float, default=0.35)
    p.add_argument("--crossover", type=float, default=0.75)
    p.add_argument("--seed", type=int, default=42)

    p.add_argument("--bear-start", default="2025-10-01")
    p.add_argument("--entry-thr", type=float, default=0.35)
    p.add_argument("--exit-thr", type=float, default=-0.15)
    p.add_argument("--rsi-cover", type=float, default=42.0)
    p.add_argument("--max-hold-bars", type=int, default=72)
    p.add_argument("--stop-loss-pct", type=float, default=30.0)
    p.add_argument("--take-profit-pct", type=float, default=10.0)
    p.add_argument("--fee-bps", type=float, default=7.0)
    p.add_argument("--slippage-bps", type=float, default=4.0)
    p.add_argument("--starting-capital-usd", type=float, default=100000.0)
    p.add_argument("--max-participation-rate", type=float, default=0.001)
    p.add_argument("--min-capacity-ratio", type=float, default=1.0)

    p.add_argument("--out-dir", default=str((Path(__file__).resolve().parents[1] / "results")))
    args = p.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    rows = []
    workers = max(1, min(args.workers, len(args.symbols)))
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(run_symbol, s, args, out_dir): s for s in args.symbols}
        for fut in as_completed(futs):
            s = futs[fut]
            r = fut.result()
            rows.append(r)
            print(
                f"{s}: bars={r['bars']} {r['start_iso_utc']} -> {r['end_iso_utc']} "
                f"WF(GP)={r['gp_oos_return_pct']}% DD={r['gp_oos_max_dd_pct']}% "
                f"Fill={r['gp_oos_avg_entry_fill_rate']}%"
            )

    rows.sort(key=lambda x: x["gp_oos_return_pct"], reverse=True)
    summary_csv = out_dir / "multi_symbol_gp_summary.csv"
    write_csv(
        summary_csv,
        rows,
        [
            "symbol",
            "bars",
            "start_iso_utc",
            "end_iso_utc",
            "bear_start_utc",
            "wf_folds",
            "gp_oos_return_pct",
            "gp_oos_max_dd_pct",
            "gp_oos_trades",
            "gp_oos_win_rate",
            "gp_oos_avg_entry_fill_rate",
            "gp_oos_total_cap_reject_entries",
            "gp_best_expr",
        ],
    )

    with (out_dir / "run_config.json").open("w") as f:
        json.dump(vars(args), f, indent=2)

    print("\nSaved:")
    print(f"- {summary_csv}")
    print(f"- {out_dir}/run_config.json")
    print(f"- {out_dir}/*_gp_walkforward_folds.csv")
    print(f"- {out_dir}/*_gp_walkforward_summary.json")
    print(f"- {out_dir}/*_gp_fullsample_best.json")


if __name__ == "__main__":
    main()

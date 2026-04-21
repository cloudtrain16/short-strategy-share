#!/usr/bin/env python3
import argparse
import csv
import json
import random
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from run_short_regime_strict import (
    calc_order_fill_probability,
    capacity_ratio,
    fetch_candles,
    limit_fill_price,
    market_fill_price,
    parse_utc_ts,
    rolling_std,
    rsi,
    short_return_multiple,
    sma,
    ts_to_iso,
)


STOP = False


def on_signal(_sig, _frame):
    global STOP
    STOP = True


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def read_json(path: Path, default):
    if not path.exists():
        return default
    with path.open() as f:
        return json.load(f)


def write_json(path: Path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w") as f:
        json.dump(payload, f, indent=2)
    tmp.replace(path)


def ensure_event_writer(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    is_new = not path.exists()
    f = path.open("a", newline="")
    fields = [
        "ts_utc",
        "symbol",
        "event",
        "bar_ts",
        "bar_iso_utc",
        "price",
        "detail",
        "in_pos_after",
        "pending_entry_after",
        "pending_exit_after",
        "equity_multiple_after",
        "max_dd_pct_after",
    ]
    writer = csv.DictWriter(f, fieldnames=fields)
    if is_new:
        writer.writeheader()
    return f, writer


def build_order(side, kind, decision_px, signal_ts, bar_ms, cfg):
    activate_ts = signal_ts + bar_ms
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
        "activate_ts": activate_ts,
        "expire_ts": activate_ts + (max(1, int(cfg["limit_ttl_bars"])) - 1) * bar_ms,
        "limit_px": limit_px,
    }


def try_execute_order_ts(order, bar, vol_ratio, cfg, rng):
    if bar["ts"] < order["activate_ts"]:
        return {"status": "pending"}

    if order["kind"] == "market":
        px = market_fill_price(order["side"], bar["o"], cfg)
        return {"status": "filled", "fill_px": px, "fee_bps": cfg["taker_fee_bps"], "is_maker": 0, "reason": "market"}

    touched = (bar["h"] >= order["limit_px"]) if order["side"] == "sell" else (bar["l"] <= order["limit_px"])
    if touched:
        p = calc_order_fill_probability(order, bar, vol_ratio, cfg)
        if rng.random() <= p:
            px = limit_fill_price(order["side"], order["limit_px"], cfg)
            return {"status": "filled", "fill_px": px, "fee_bps": cfg["maker_fee_bps"], "is_maker": 1, "reason": "limit_touch"}

    if bar["ts"] >= order["expire_ts"]:
        if cfg.get("limit_fallback_market", False):
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


def log_event(writer, sym_state, symbol, event, bar, price=0.0, detail=""):
    writer.writerow(
        {
            "ts_utc": now_iso(),
            "symbol": symbol,
            "event": event,
            "bar_ts": bar["ts"],
            "bar_iso_utc": ts_to_iso(bar["ts"]),
            "price": round(float(price), 8),
            "detail": detail,
            "in_pos_after": int(bool(sym_state["in_pos"])),
            "pending_entry_after": int(sym_state["pending_entry"] is not None),
            "pending_exit_after": int(sym_state["pending_exit"] is not None),
            "equity_multiple_after": round(float(sym_state["equity_multiple"]), 8),
            "max_dd_pct_after": round(float(sym_state["max_dd"]) * 100.0, 4),
        }
    )


def process_symbol(candles, sym_cfg, sym_state, global_cfg, writer):
    if len(candles) < 120:
        return

    close = [x["c"] for x in candles]
    ma_fast = sma(close, int(sym_cfg["ma_fast"]))
    ma_mid = sma(close, int(sym_cfg["ma_mid"]))
    ma_slow = sma(close, int(sym_cfg["ma_slow"]))
    r = rsi(close, 14)
    rets = [0.0]
    for i in range(1, len(close)):
        rets.append((close[i] / close[i - 1]) - 1.0)
    vol_pct = [x * 100.0 for x in rolling_std(rets, 20)]
    vol20 = sma([x["v"] for x in candles], 20)

    bar_ms = max(1, int(candles[-1]["ts"] - candles[-2]["ts"]))
    closed_ts = candles[-2]["ts"]
    if sym_state["last_processed_ts"] == 0:
        # Fresh bootstrap: start from latest closed bar to avoid replaying stale bars.
        sym_state["last_processed_ts"] = closed_ts
        return

    pending = [i for i, b in enumerate(candles) if sym_state["last_processed_ts"] < b["ts"] <= closed_ts]
    if not pending:
        return

    funding_rate_per_bar = (global_cfg["funding_bps_per_8h"] / 10000.0) * ((bar_ms / 3600000.0) / 8.0)
    rng = random.Random(int(sym_state["rng_seed"]))

    for i in pending:
        bar = candles[i]
        vol_ratio = bar["v"] / max(1e-9, vol20[i])

        if sym_state["pending_entry"] is not None and (not sym_state["in_pos"]):
            res = try_execute_order_ts(sym_state["pending_entry"], bar, vol_ratio, global_cfg, rng)
            if res["status"] == "filled":
                cap = capacity_ratio(bar, sym_state["equity_multiple"], sym_cfg["target_weight"], global_cfg)
                if cap >= global_cfg["min_capacity_ratio"]:
                    fee = res["fee_bps"] / 10000.0
                    sym_state["entry_raw_px"] = res["fill_px"]
                    sym_state["entry_net_sell"] = res["fill_px"] * (1.0 - fee)
                    sym_state["entry_ts"] = bar["ts"]
                    sym_state["in_pos"] = True
                    sym_state["pending_entry"] = None
                    log_event(writer, sym_state, sym_cfg["symbol"], "ENTRY_FILLED", bar, res["fill_px"], res["reason"])
                else:
                    sym_state["capacity_reject_entries"] += 1
                    if bar["ts"] >= sym_state["pending_entry"]["expire_ts"]:
                        sym_state["pending_entry"] = None
                        log_event(writer, sym_state, sym_cfg["symbol"], "ENTRY_EXPIRED_CAPACITY", bar, 0.0, "capacity_reject")
            elif res["status"] == "expired":
                sym_state["pending_entry"] = None
                log_event(writer, sym_state, sym_cfg["symbol"], "ENTRY_EXPIRED", bar, 0.0, "limit_expired")

        if sym_state["pending_exit"] is not None and sym_state["in_pos"]:
            res = try_execute_order_ts(sym_state["pending_exit"], bar, vol_ratio, global_cfg, rng)
            if res["status"] == "filled":
                cap = capacity_ratio(bar, sym_state["equity_multiple"], sym_cfg["target_weight"], global_cfg)
                if cap >= global_cfg["min_capacity_ratio"]:
                    fee = res["fee_bps"] / 10000.0
                    exit_cost = res["fill_px"] * (1.0 + fee)
                    ret = short_return_multiple(sym_state["entry_net_sell"], exit_cost)
                    sym_state["equity_multiple"] *= ret
                    sym_state["in_pos"] = False
                    sym_state["pending_exit"] = None
                    sym_state["entry_raw_px"] = 0.0
                    sym_state["entry_net_sell"] = 0.0
                    sym_state["entry_ts"] = 0
                    log_event(writer, sym_state, sym_cfg["symbol"], "EXIT_FILLED", bar, res["fill_px"], res["reason"])
                else:
                    sym_state["capacity_reject_exits"] += 1
                    if bar["ts"] >= sym_state["pending_exit"]["expire_ts"]:
                        sym_state["pending_exit"] = None
                        log_event(writer, sym_state, sym_cfg["symbol"], "EXIT_EXPIRED_CAPACITY", bar, 0.0, "capacity_reject")
            elif res["status"] == "expired":
                sym_state["pending_exit"] = None
                log_event(writer, sym_state, sym_cfg["symbol"], "EXIT_EXPIRED", bar, 0.0, "limit_expired")

        if sym_state["in_pos"] and sym_state["pending_exit"] is None:
            stop_px = sym_state["entry_raw_px"] * (1.0 + global_cfg["stop_loss_pct"] / 100.0) if global_cfg["stop_loss_pct"] > 0 else None
            tp_px = sym_state["entry_raw_px"] * (1.0 - global_cfg["take_profit_pct"] / 100.0) if global_cfg["take_profit_pct"] > 0 else None
            stop_hit = (stop_px is not None) and (bar["h"] >= stop_px)
            tp_hit = (tp_px is not None) and (bar["l"] <= tp_px)

            if stop_hit or tp_hit:
                trigger = stop_px if stop_hit else tp_px
                cover_px = market_fill_price("buy", trigger, global_cfg)
                fee = global_cfg["taker_fee_bps"] / 10000.0
                exit_cost = cover_px * (1.0 + fee)
                ret = short_return_multiple(sym_state["entry_net_sell"], exit_cost)
                sym_state["equity_multiple"] *= ret
                sym_state["in_pos"] = False
                sym_state["entry_raw_px"] = 0.0
                sym_state["entry_net_sell"] = 0.0
                sym_state["entry_ts"] = 0
                log_event(writer, sym_state, sym_cfg["symbol"], "EXIT_RISK", bar, cover_px, "stop" if stop_hit else "take_profit")

        if sym_state["in_pos"] and funding_rate_per_bar != 0.0:
            carry = max(0.01, 1.0 - funding_rate_per_bar)
            sym_state["equity_multiple"] *= carry
            sym_state["funding_carry_multiple"] *= carry
            sym_state["funding_bars"] += 1

        mark = sym_state["equity_multiple"]
        if sym_state["in_pos"]:
            est_cover_px = market_fill_price("buy", bar["c"], global_cfg)
            est_exit = est_cover_px * (1.0 + global_cfg["taker_fee_bps"] / 10000.0)
            mark = sym_state["equity_multiple"] * short_return_multiple(sym_state["entry_net_sell"], est_exit)
        sym_state["peak"] = max(sym_state["peak"], mark)
        dd = (sym_state["peak"] - mark) / max(1e-12, sym_state["peak"])
        sym_state["max_dd"] = max(sym_state["max_dd"], dd)

        if i >= len(candles) - 1:
            continue

        in_bear = bar["ts"] >= global_cfg["bear_start_ts"]
        downtrend = ma_mid[i] < ma_slow[i]
        rebound = close[i] > ma_fast[i] and close[i] > ma_mid[i] and close[i] > ma_slow[i]
        rsi_ok = r[i] >= sym_cfg["rsi_short"]
        vol_ok = vol_pct[i] >= sym_cfg["vol_min_pct"]
        short_signal = in_bear and downtrend and rebound and rsi_ok and vol_ok

        hold_bars = int((bar["ts"] - sym_state["entry_ts"]) / bar_ms) if sym_state["entry_ts"] else 0
        cover_signal = (r[i] <= sym_cfg["rsi_cover"] and close[i] < ma_mid[i]) or (hold_bars >= sym_cfg["max_hold_bars"])

        if (not sym_state["in_pos"]) and sym_state["pending_entry"] is None and short_signal:
            sym_state["pending_entry"] = build_order(
                "sell",
                global_cfg["entry_order_type"],
                bar["c"],
                bar["ts"],
                bar_ms,
                global_cfg,
            )
            log_event(writer, sym_state, sym_cfg["symbol"], "ENTRY_SIGNAL", bar, bar["c"], "scheduled_next_bar")

        if sym_state["in_pos"] and sym_state["pending_exit"] is None and cover_signal:
            sym_state["pending_exit"] = build_order(
                "buy",
                global_cfg["exit_order_type"],
                bar["c"],
                bar["ts"],
                bar_ms,
                global_cfg,
            )
            log_event(writer, sym_state, sym_cfg["symbol"], "EXIT_SIGNAL", bar, bar["c"], "scheduled_next_bar")

        sym_state["last_processed_ts"] = bar["ts"]

    sym_state["rng_seed"] = rng.randint(1, 2**31 - 1)


def make_symbol_cfg(pack):
    out = []
    for s in pack["symbols"]:
        p = s.get("frozen_params_from_wf_median", {})
        out.append(
            {
                "symbol": s["symbol"],
                "target_weight": float(s.get("target_weight", 0.0)),
                "ma_fast": int(round(float(p.get("best_ma_fast", 7)))),
                "ma_mid": int(round(float(p.get("best_ma_mid", 20)))),
                "ma_slow": int(round(float(p.get("best_ma_slow", 52)))),
                "rsi_short": float(p.get("best_rsi_short", 58)),
                "rsi_cover": float(p.get("best_rsi_cover", 38)),
                "vol_min_pct": float(p.get("best_vol_min_pct", 0.4)),
                "max_hold_bars": int(round(float(p.get("best_max_hold_bars", 24)))),
            }
        )
    return out


def default_symbol_state(seed):
    return {
        "in_pos": False,
        "pending_entry": None,
        "pending_exit": None,
        "entry_raw_px": 0.0,
        "entry_net_sell": 0.0,
        "entry_ts": 0,
        "equity_multiple": 1.0,
        "peak": 1.0,
        "max_dd": 0.0,
        "last_processed_ts": 0,
        "funding_bars": 0,
        "funding_carry_multiple": 1.0,
        "capacity_reject_entries": 0,
        "capacity_reject_exits": 0,
        "rng_seed": int(seed),
    }


def main():
    parser = argparse.ArgumentParser(description="Shadow runner for short-only strategy (paper mode, no live orders).")
    parser.add_argument(
        "--pack-json",
        default=str(Path(__file__).resolve().parents[1] / "results" / "short_portfolio_pack_1y.json"),
    )
    parser.add_argument("--data-source", default="binance", choices=["binance", "okx"])
    parser.add_argument("--bar", default="4H")
    parser.add_argument("--lookback-bars", type=int, default=500)
    parser.add_argument("--poll-seconds", type=int, default=60)
    parser.add_argument("--once", action="store_true")
    parser.add_argument(
        "--state-json",
        default=str(Path(__file__).resolve().parents[1] / "results" / "shadow_state.json"),
    )
    parser.add_argument(
        "--events-csv",
        default=str(Path(__file__).resolve().parents[1] / "results" / "shadow_events.csv"),
    )
    parser.add_argument(
        "--snapshot-json",
        default=str(Path(__file__).resolve().parents[1] / "results" / "shadow_snapshot.json"),
    )
    parser.add_argument("--seed", type=int, default=20260421)
    args = parser.parse_args()

    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)

    pack = read_json(Path(args.pack_json), None)
    if pack is None:
        raise RuntimeError(f"Portfolio pack not found: {args.pack_json}")

    exec_cfg = pack["execution_and_risk"]
    global_cfg = {
        "entry_order_type": exec_cfg["entry_order_type"],
        "exit_order_type": exec_cfg["exit_order_type"],
        "maker_fee_bps": float(exec_cfg["maker_fee_bps"]),
        "taker_fee_bps": float(exec_cfg["taker_fee_bps"]),
        "market_slippage_bps": float(exec_cfg["market_slippage_bps"]),
        "half_spread_bps": float(exec_cfg["half_spread_bps"]),
        "limit_offset_bps": float(exec_cfg["limit_offset_bps"]),
        "limit_slippage_bps": float(exec_cfg["limit_slippage_bps"]),
        "limit_ttl_bars": int(exec_cfg["limit_ttl_bars"]),
        "limit_fill_model": exec_cfg["limit_fill_model"],
        "queue_bps": float(exec_cfg["queue_bps"]),
        "limit_fallback_market": bool(exec_cfg["limit_fallback_market"]),
        "starting_capital_usd": float(pack["portfolio"]["capital_usd"]),
        "max_participation_rate": float(exec_cfg["max_participation_rate"]),
        "min_capacity_ratio": float(exec_cfg["min_capacity_ratio"]),
        "stop_loss_pct": float(exec_cfg["stop_loss_pct"]),
        "take_profit_pct": float(exec_cfg["take_profit_pct"]),
        "funding_bps_per_8h": float(exec_cfg.get("funding_bps_per_8h", 0.0)),
        "bear_start_ts": parse_utc_ts(pack["data_scope"]["bear_start_utc"]),
    }
    symbols_cfg = make_symbol_cfg(pack)

    state_path = Path(args.state_json)
    state = read_json(
        state_path,
        {
            "meta": {"created_at_utc": now_iso(), "pack_json": str(Path(args.pack_json).resolve())},
            "symbols": {},
        },
    )
    state.setdefault("symbols", {})
    for idx, s in enumerate(symbols_cfg):
        state["symbols"].setdefault(s["symbol"], default_symbol_state(args.seed + idx * 131))

    events_f, events_writer = ensure_event_writer(Path(args.events_csv))
    try:
        while not STOP:
            cycle_start = now_iso()
            for s in symbols_cfg:
                symbol = s["symbol"]
                candles = fetch_candles(symbol, args.bar, args.lookback_bars, args.data_source)
                process_symbol(candles, s, state["symbols"][symbol], global_cfg, events_writer)
                events_f.flush()

            snapshot = {
                "updated_at_utc": now_iso(),
                "data_source": args.data_source,
                "bar": args.bar,
                "symbols": {},
            }
            for s in symbols_cfg:
                sym = s["symbol"]
                st = state["symbols"][sym]
                snapshot["symbols"][sym] = {
                    "in_pos": st["in_pos"],
                    "pending_entry": st["pending_entry"] is not None,
                    "pending_exit": st["pending_exit"] is not None,
                    "last_processed_ts": st["last_processed_ts"],
                    "last_processed_iso_utc": ts_to_iso(st["last_processed_ts"]) if st["last_processed_ts"] else "",
                    "equity_multiple": round(float(st["equity_multiple"]), 8),
                    "max_dd_pct": round(float(st["max_dd"]) * 100.0, 4),
                    "funding_carry_pct": round((float(st["funding_carry_multiple"]) - 1.0) * 100.0, 4),
                }

            state["meta"]["last_cycle_utc"] = cycle_start
            write_json(state_path, state)
            write_json(Path(args.snapshot_json), snapshot)

            if args.once:
                break
            for _ in range(max(1, int(args.poll_seconds))):
                if STOP:
                    break
                time.sleep(1)
    finally:
        events_f.close()

    print("shadow runner stopped")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from statistics import median


def load_csv_rows(path: Path):
    with path.open() as f:
        return list(csv.DictReader(f))


def load_csv_map(path: Path, key: str = "symbol"):
    return {r[key]: r for r in load_csv_rows(path)}


def f(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return default


def i(x, default=0):
    try:
        return int(float(x))
    except Exception:
        return default


def normalize_with_cap(raw_weights, cap):
    w = dict(raw_weights)
    total = sum(w.values())
    if total <= 0:
        n = len(w)
        return {k: round(1.0 / n, 6) for k in w}
    w = {k: v / total for k, v in w.items()}

    # Small iterative projection to handle caps then renormalize leftover.
    for _ in range(5):
        over = {k: v for k, v in w.items() if v > cap}
        if not over:
            break
        pinned_total = sum(min(v, cap) for v in w.values())
        free_keys = [k for k, v in w.items() if v <= cap]
        free_total = sum(w[k] for k in free_keys)
        if free_total <= 0:
            break
        remain = max(0.0, 1.0 - sum(cap for _ in over))
        for k in w:
            if k in over:
                w[k] = cap
            else:
                w[k] = w[k] / free_total * remain
    total = sum(w.values()) or 1.0
    return {k: round(v / total, 6) for k, v in w.items()}


def get_method_vote(symbol, base, funding, high, ultra):
    votes = [
        (base.get(symbol, {}).get("preferred_method") or "grid").lower(),
        (funding.get(symbol, {}).get("preferred_method") or "grid").lower(),
        (high.get(symbol, {}).get("preferred_method") or "grid").lower(),
        (ultra.get(symbol, {}).get("preferred_method") or "grid").lower(),
    ]
    ga_votes = sum(1 for v in votes if v == "ga")
    return "ga" if ga_votes >= 2 else "grid"


def median_fold_params(results_dir: Path, symbol: str, method: str):
    safe = symbol.replace("-", "_")
    fold_csv = results_dir / f"{safe}_{method}_walkforward_folds.csv"
    if not fold_csv.exists():
        return {}
    rows = load_csv_rows(fold_csv)
    if not rows:
        return {}
    keys = [
        "best_ma_fast",
        "best_ma_mid",
        "best_ma_slow",
        "best_rsi_short",
        "best_rsi_cover",
        "best_vol_min_pct",
        "best_max_hold_bars",
    ]
    out = {}
    for k in keys:
        vals = [f(r.get(k)) for r in rows if r.get(k) not in (None, "")]
        if vals:
            out[k] = round(median(vals), 2)
    return out


def main():
    p = argparse.ArgumentParser(description="Build short-only portfolio pack from robustness outputs.")
    p.add_argument(
        "--results-root",
        default=str(Path(__file__).resolve().parents[1] / "results"),
    )
    p.add_argument("--top-n", type=int, default=3)
    p.add_argument("--min-tradeable-scenarios", type=int, default=3)
    p.add_argument("--max-symbol-weight", type=float, default=0.5)
    p.add_argument("--capital-usd", type=float, default=100000.0)
    p.add_argument("--out-json", default="")
    p.add_argument("--out-md", default="")
    args = p.parse_args()

    root = Path(args.results_root)
    robust_csv = root / "short_1y_robustness_matrix_5scenarios.csv"
    base_csv = root / "short_ma_filters_1y_wf12/multi_symbol_summary.csv"
    funding_csv = root / "short_ma_filters_1y_wf12_funding1bps/multi_symbol_summary.csv"
    high_csv = root / "short_ma_filters_1y_wf12_stress_highcost/multi_symbol_summary.csv"
    ultra_csv = root / "short_ma_filters_1y_wf12_stress_ultra/multi_symbol_summary.csv"

    robust_rows = load_csv_rows(robust_csv)
    base = load_csv_map(base_csv)
    funding = load_csv_map(funding_csv)
    high = load_csv_map(high_csv)
    ultra = load_csv_map(ultra_csv)

    ranked = sorted(
        robust_rows,
        key=lambda r: (
            i(r.get("tradeable_scenarios_5")),
            i(r.get("positive_scenarios_5")),
            f(r.get("gp_ret")),
            f(r.get("funding1bps_ret")),
        ),
        reverse=True,
    )
    selected = [r for r in ranked if i(r.get("tradeable_scenarios_5")) >= args.min_tradeable_scenarios][: args.top_n]
    if not selected:
        raise RuntimeError("No symbol passed min-tradeable-scenarios.")

    raw = {}
    items = []
    for r in selected:
        sym = r["symbol"]
        rets = [f(r.get("baseline_ret")), f(r.get("gp_ret")), f(r.get("funding1bps_ret")), f(r.get("highcost_ret")), f(r.get("ultra_ret"))]
        dds = [f(r.get("baseline_dd")), f(r.get("gp_dd")), f(r.get("funding1bps_dd")), f(r.get("highcost_dd")), f(r.get("ultra_dd"))]
        med_ret = median(rets)
        med_dd = max(0.5, median(dds))
        tradeable = i(r.get("tradeable_scenarios_5"))
        # Risk-adjusted raw score: favor stable profitability and lower drawdown.
        raw_score = max(0.05, (med_ret + 1.0) / med_dd * (tradeable / 5.0))
        raw[sym] = raw_score

        method = get_method_vote(sym, base, funding, high, ultra)
        params = median_fold_params(root / "short_ma_filters_1y_wf12", sym, method)
        items.append(
            {
                "symbol": sym,
                "method": method,
                "scenario_returns_pct": {
                    "baseline": round(rets[0], 2),
                    "gp": round(rets[1], 2),
                    "funding1bps": round(rets[2], 2),
                    "highcost": round(rets[3], 2),
                    "ultra": round(rets[4], 2),
                },
                "scenario_median_return_pct": round(med_ret, 2),
                "scenario_median_dd_pct": round(med_dd, 2),
                "tradeable_scenarios_5": tradeable,
                "frozen_params_from_wf_median": params,
            }
        )

    weights = normalize_with_cap(raw, args.max_symbol_weight)
    for it in items:
        sym = it["symbol"]
        it["target_weight"] = weights[sym]
        it["capital_usd"] = round(args.capital_usd * weights[sym], 2)

    out = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "model_scope": "short_only_after_bear_start",
        "data_scope": {
            "timeframe": "4H",
            "window": "recent_1y",
            "symbols_input": ["BTC-USDT", "ETH-USDT", "SOL-USDT", "BNB-USDT", "UNI-USDT", "STRK-USDT"],
            "bear_start_utc": "2025-10-01",
        },
        "portfolio": {
            "capital_usd": args.capital_usd,
            "max_gross_exposure": 1.0,
            "leverage": 1.0,
            "max_symbol_weight": args.max_symbol_weight,
            "selected_symbol_count": len(items),
            "selected_symbols": [x["symbol"] for x in items],
            "weights": weights,
        },
        "execution_and_risk": {
            "entry_order_type": "limit",
            "exit_order_type": "market",
            "maker_fee_bps": 5.0,
            "taker_fee_bps": 7.0,
            "market_slippage_bps": 4.0,
            "half_spread_bps": 2.0,
            "limit_offset_bps": 8.0,
            "limit_slippage_bps": 0.5,
            "limit_ttl_bars": 2,
            "limit_fill_model": "probabilistic",
            "queue_bps": 8.0,
            "limit_fallback_market": False,
            "stop_loss_pct": 30.0,
            "take_profit_pct": 10.0,
            "max_participation_rate": 0.001,
            "min_capacity_ratio": 1.0,
            "funding_bps_per_8h": 1.0,
            "daily_loss_limit_pct": 4.0,
            "portfolio_kill_switch_dd_pct": 15.0,
            "max_concurrent_positions": 2,
        },
        "symbols": items,
        "source_files": {
            "robustness_matrix": str(robust_csv),
            "base_summary": str(base_csv),
            "funding_summary": str(funding_csv),
            "highcost_summary": str(high_csv),
            "ultra_summary": str(ultra_csv),
        },
    }

    out_json = Path(args.out_json) if args.out_json else root / "short_portfolio_pack_1y.json"
    out_md = Path(args.out_md) if args.out_md else root / "short_portfolio_pack_1y.md"

    out_json.parent.mkdir(parents=True, exist_ok=True)
    with out_json.open("w") as fjson:
        json.dump(out, fjson, indent=2)

    lines = [
        "# Short Portfolio Pack (1Y / 4H / Bear-Start)",
        "",
        f"- Generated: `{out['generated_at_utc']}`",
        f"- Capital: `${args.capital_usd:,.2f}`",
        f"- Selected: `{', '.join(out['portfolio']['selected_symbols'])}`",
        "",
        "## Weights",
    ]
    for it in items:
        lines.append(
            f"- `{it['symbol']}`: weight `{it['target_weight']:.2%}` "
            f"(capital `${it['capital_usd']:,.2f}`), method `{it['method']}`"
        )
    lines += [
        "",
        "## Risk Controls",
        f"- stop loss `{out['execution_and_risk']['stop_loss_pct']}%`, take profit `{out['execution_and_risk']['take_profit_pct']}%`",
        f"- max participation `{out['execution_and_risk']['max_participation_rate']}`",
        f"- funding cost `{out['execution_and_risk']['funding_bps_per_8h']} bps/8h`",
        f"- daily loss limit `{out['execution_and_risk']['daily_loss_limit_pct']}%`",
        f"- kill switch dd `{out['execution_and_risk']['portfolio_kill_switch_dd_pct']}%`",
    ]
    with out_md.open("w") as fmd:
        fmd.write("\n".join(lines) + "\n")

    print(out_json)
    print(out_md)
    print("selected:", ", ".join(out["portfolio"]["selected_symbols"]))


if __name__ == "__main__":
    main()

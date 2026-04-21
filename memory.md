# memory.md (shared context for cross-agent collaboration)

## Project goal
- Build and iterate a crypto quant stack for **bear-market short strategy**.
- Keep research and execution realistic: walk-forward, capacity, slippage, fees, latency, funding.

## Collaboration setup
- This repo is mainly the **short-side** research/execution workspace.
- User runs another Claude Code session on Windows for **long-side** strategy work.
- We share outputs via GitHub and read this file as the latest handoff context.

## Current short-side baseline
- Direction: short-only after regime switch.
- Regime gate: `bear_start = 2025-10-01` (UTC).
- Timeframe/window: `4H`, recent `1 year`.
- Core risk config: `TP 10%`, `SL 30%`, `no leverage`.
- Universe (current): `BTC, ETH, SOL, BNB, UNI, STRK`.

## Key completed modules
- `src/run_short_regime_strict.py`
  - MA short + filters
  - walk-forward (grid + GA)
  - strict execution model
  - capacity constraints
  - optional funding carry (`--funding-bps-per-8h`)
- `src/run_short_regime_gp.py`
  - GP expression tuning with walk-forward
- `src/build_short_portfolio_pack.py`
  - builds portfolio/risk pack from robustness outputs
- `src/shadow_short_runner.py`
  - paper/shadow runtime (real market data, no live order placement)

## Latest summary outputs
- 5-scenario robustness matrix:
  - `results/short_1y_robustness_matrix_5scenarios.csv`
- Funding impact table:
  - `results/short_1y_funding_impact_1bps.csv`
- Portfolio pack (selected symbols/weights/risk):
  - `results/short_portfolio_pack_1y.json`
  - `results/short_portfolio_pack_1y.md`

## Current portfolio pack snapshot
- Selected symbols: `BTC-USDT`, `ETH-USDT`, `SOL-USDT`
- Weights (latest generated pack):
  - BTC `36.6359%`
  - ETH `50.0000%`
  - SOL `13.3641%`

## How to run quickly
- Rebuild pack:
  - `python3 src/build_short_portfolio_pack.py`
- Run short strict WF:
  - `python3 src/run_short_regime_strict.py --help`
- Run shadow/paper:
  - `python3 src/shadow_short_runner.py --help`

## Safety rules
- Do not commit secrets (`.env`, key files, token files).
- Keep this file sanitized and high-signal for cross-machine collaboration.

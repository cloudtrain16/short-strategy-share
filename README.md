# short-strategy-share

Short-side crypto strategy research workspace (1Y/4H default), including:

- MA short + filter layer backtesting
- Walk-forward validation
- GP tuning module
- Stress scenarios (cost/capacity/funding)
- Shadow runner (paper mode, no real orders)

## Structure

- `src/`: strategy, backtest, portfolio pack, and shadow runner scripts
- `results/`: generated backtest summaries and portfolio pack outputs

## Quick start

```bash
python3 src/run_short_regime_strict.py --help
python3 src/run_short_regime_gp.py --help
python3 src/build_short_portfolio_pack.py --help
python3 src/shadow_short_runner.py --help
```

## Safety

- This repo excludes `.env`, API key-like files, and `.pkl` artifacts via `.gitignore`.
- `shadow_short_runner.py` is paper-trading only and does not place exchange orders.

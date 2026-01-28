# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Initial project setup with Session-Driven Development
- Market neutralization diagnostic (A1) — regression + Brier score analysis on predictions.csv
- Comprehensive roadmap in ACTIONITEMS.md with Tracks A–E
- **Black-Litterman probability blending layer (A3)** — new module `src/models/black_litterman.py`
  - `BlackLittermanBlender` class with `BLConfig` dataclass
  - Log-odds space blending of model probabilities with devigged market prior
  - Per-prediction z-score confidence from MC distribution properties
  - Multiplicative devigging (equivalent to Shin's method for 2-outcome markets)
  - American-to-decimal odds conversion utility
  - 39 unit tests in `tests/test_black_litterman.py`
- `--bl-tau` CLI flag on `run_backtest.py` to enable BL blending (disabled by default)
- `posterior_prob` diagnostic field on `Bet` dataclass in `bet_simulator.py`
- BL diagnostic columns in predictions CSV: `model_over/under`, `market_over/under`, `confidence`, `posterior_over/under`

### Changed

- Updated ACTIONITEMS.md with corrected root cause diagnosis: model is catastrophically overconfident (Brier 0.2705), not market-correlated (R²=0.10)
- Promoted Black-Litterman blending (A3) to top priority based on diagnostic findings
- Reorganized priority matrix with A1 marked complete
- `backtest_harness.py`: Added `bl_blender` field and dual-path `_calculate_edges()` — BL path when blender is set, original path when None
- `run_backtest.py`: Added `--bl-tau` argument and BL blender construction
- `bet_simulator.py`: Added `posterior_prob` field to `Bet`, wired posterior storage in `evaluate_predictions()`
- Updated ARCHITECTURE.md with Stage E (Probability Blending), updated data flow diagram, backtesting CLI docs, and Known Issues section
- Marked A3 as implemented in ACTIONITEMS.md priority matrix

### Fixed

### Removed

### Changed
- Refactored project structure and moved files
- Updated test_backfill_league_priors.py

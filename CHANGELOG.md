# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Initial project setup with Session-Driven Development
- Market neutralization diagnostic (A1) — regression + Brier score analysis on predictions.csv
- Comprehensive roadmap in ACTIONITEMS.md with Tracks A–E

### Changed

- Updated ACTIONITEMS.md with corrected root cause diagnosis: model is catastrophically overconfident (Brier 0.2705), not market-correlated (R²=0.10)
- Promoted Black-Litterman blending (A3) to top priority based on diagnostic findings
- Reorganized priority matrix with A1 marked complete

### Fixed

### Removed

### Changed
- Refactored project structure and moved files
- Updated test_backfill_league_priors.py

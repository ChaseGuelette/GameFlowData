# Minutes Bimodality Analysis Tool

Standalone analysis tool to investigate whether starters' minutes follow a bimodal distribution in blowout games (full game ~32min vs pulled early ~24min) versus a simple unimodal shift.

## Why This Matters

The Monte Carlo predictor models minutes as a single distribution (quantile regression with inverse transform sampling). If real-world minutes are bimodal in blowout scenarios, the model produces a unimodal distribution centered between the two modes — overestimating minutes for pulled starters and underestimating minutes for starters who play through. This directly affects stat predictions since `total_stat = minutes x rate`.

The blowout factor in `monte_carlo.py` attempts to address this with a flat probability/reduction scalar, but a scalar can't capture bimodality. This analysis determines whether bimodality is real and whether the current minutes model already accounts for it via the `line_spread` feature.

## Usage

```bash
# Basic distribution analysis (no model needed)
python src/models/analyze_minutes_bimodality.py

# With model prediction comparison
python src/models/analyze_minutes_bimodality.py --model-dir src/models/artifacts/run_20260126_180535

# Save results to JSON
python src/models/analyze_minutes_bimodality.py --output bimodality_results.json

# Adjust starter threshold and season
python src/models/analyze_minutes_bimodality.py --min-starter-avg 24 --seasons 22024 22025
```

### Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `--seasons` | `22024` | Season IDs to load (space-separated) |
| `--model-dir` | None | Path to model artifacts for prediction comparison |
| `--min-starter-avg` | `20.0` | Minimum average minutes to classify a player as a starter |
| `--output` | None | Path to save JSON results |

## What It Analyzes

### Part 1: Distribution Shape by Spread Segment

Segments all starter game-logs by absolute spread:

| Segment | Spread Range | Purpose |
|---------|-------------|---------|
| close | \|spread\| <= 5 | Baseline distribution |
| moderate | 5 < \|spread\| <= 10 | Transition zone |
| blowout | \|spread\| > 10 | Primary test group |
| extreme_blowout | \|spread\| > 15 | Extreme cases |

For each segment, computes:
- **Descriptive stats**: mean, std, median, quantiles (Q10-Q90)
- **Bimodality Coefficient (BC)**: `(skew^2 + 1) / (kurtosis + 3)`. Values above 0.555 suggest bimodality. A normal distribution scores ~0.333.
- **Hartigan's dip test** (if `diptest` package installed): Statistical test where p < 0.05 rejects unimodality.
- **ASCII histogram**: Visual distribution shape in the terminal.
- **Cross-segment comparison**: Mean/std/quantile differences between close and blowout segments.

### Part 2: Model Prediction Comparison (optional, requires `--model-dir`)

Loads the minutes model and batch-predicts quantiles across all games, then checks calibration per spread segment:
- **Quantile calibration**: What fraction of actuals fall below each predicted quantile? Target: Q10=10%, Q25=25%, etc.
- **Median bias**: Does the model systematically over/under-predict minutes in blowouts?
- **Residual std**: Are prediction errors wider in blowouts?

## Interpreting Results

### Bimodality Coefficient

| BC Range | Interpretation |
|----------|---------------|
| < 0.40 | Strongly unimodal |
| 0.40 - 0.555 | Unimodal (possibly platykurtic) |
| > 0.555 | Evidence of bimodality |
| > 0.70 | Strong bimodality |

### Decision Matrix

| Finding | Action |
|---------|--------|
| BC blowout ~= BC close | Distribution just shifts down. Current model with `line_spread` feature is likely sufficient. No mixture needed. |
| BC blowout > 0.555, BC close < 0.555 | Bimodality emerges in blowouts. Implement inference-time mixture model for minutes. |
| Model Q50 well-calibrated across all segments | Model already captures blowout effects via `line_spread`. No changes needed. |
| Model Q50 over-predicts in blowouts, fine in close | Model doesn't adjust enough. Add conditional mixture or increase blowout factor. |
| Residual std much higher in blowouts | Even if centered correctly, blowouts have wider outcomes. Consider segment-specific variance inflation for minutes. |

## Relationship to Other Components

- **`monte_carlo.py` blowout factor**: The flat `probability/minutes_reduction` config that this analysis evaluates. If bimodality is confirmed, replace with a proper mixture.
- **`feature_store.py` MINUTES_FEATURES**: The `line_spread` feature that should already help the model. This analysis checks if it's sufficient.
- **`monte_carlo_tuning.md`**: The tuning guide for all Monte Carlo adjustments. Update with findings from this analysis.

## File

`src/models/analyze_minutes_bimodality.py`

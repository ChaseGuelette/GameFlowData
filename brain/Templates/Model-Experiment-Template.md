# Model Experiment Template

> Part of [[Templates]]

Use this template when running a model experiment, A/B test, or recalibration attempt.

---

```markdown
# Experiment: [Short Title]

**Date**: [YYYY-MM-DD]
**Session**: [Session number]
**Model Run ID**: [run_YYYYMMDD_HHMMSS]

## Hypothesis
What do we expect to happen and why?

## Setup
- **Training data**: [Seasons, date range]
- **Calibration data**: [Season, date range]
- **Changes from production**: [What's different]
- **Hyperparams**: [Locked from production / Tuned / Custom]

## Backtest Configuration
- **Date range**: [Start — End]
- **Edge threshold**: [per stat]
- **BL tau**: [value]
- **Kelly fraction**: [value]

## Results
| Metric | Production | Experiment | Delta |
|--------|-----------|------------|-------|
| Overall ROI | | | |
| Hit Rate | | | |
| PTS ROI | | | |
| REB ROI | | | |
| AST ROI | | | |
| ECE | | | |

## Conclusion
- [Promote / Reject / Further testing needed]
- [Key insight learned]

## Action Taken
- [ ] Promoted to production
- [ ] Backed up old model to `production_old_YYYYMMDD/`
- [ ] Updated MEMORY.md
- [ ] Updated [[Calibration-Guide]]
```

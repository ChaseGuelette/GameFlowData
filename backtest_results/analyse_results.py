# analyze_backtest.py
import pandas as pd
from pathlib import Path

# Load backtest results
results_dir = Path("bt_20260123_174611/")  # Update to your actual path

bets_df = pd.read_csv(results_dir / "bets.csv")
predictions_df = pd.read_csv(results_dir / "predictions.csv")

print("=" * 60)
print("OVER vs UNDER ANALYSIS")
print("=" * 60)

over_bets = bets_df[bets_df['side'] == 'over']
under_bets = bets_df[bets_df['side'] == 'under']

over_wins = (over_bets['outcome'] == 'win').sum()
over_losses = (over_bets['outcome'] == 'loss').sum()
under_wins = (under_bets['outcome'] == 'win').sum()
under_losses = (under_bets['outcome'] == 'loss').sum()

print(f"OVER bets:  {len(over_bets):,} | Wins: {over_wins} | Losses: {over_losses} | Hit rate: {over_wins/(over_wins+over_losses):.1%}")
print(f"UNDER bets: {len(under_bets):,} | Wins: {under_wins} | Losses: {under_losses} | Hit rate: {under_wins/(under_wins+under_losses):.1%}")

# ROI by side
over_profit = over_bets['profit'].sum()
over_staked = over_bets[over_bets['outcome'] != 'push']['stake'].sum()
under_profit = under_bets['profit'].sum()
under_staked = under_bets[under_bets['outcome'] != 'push']['stake'].sum()

print(f"\nOVER ROI:  {over_profit/over_staked:.2%} (${over_profit:,.2f} profit)")
print(f"UNDER ROI: {under_profit/under_staked:.2%} (${under_profit:,.2f} profit)")

print("\n" + "=" * 60)
print("PER-STAT CALIBRATION (Q50)")
print("=" * 60)

for stat in ['pts', 'reb', 'ast']:
    stat_preds = predictions_df[predictions_df['stat'] == stat]
    if 'actual' in stat_preds.columns and 'pred_q50' in stat_preds.columns:
        valid = stat_preds[stat_preds['actual'].notna()]
        q50_coverage = (valid['actual'] <= valid['pred_q50']).mean()
        print(f"{stat.upper()}: Q50 coverage = {q50_coverage:.3f} (target: 0.500, gap: {q50_coverage - 0.5:+.3f})")

print("\n" + "=" * 60)
print("PER-STAT OVER/UNDER BREAKDOWN")
print("=" * 60)

for stat in ['pts', 'reb', 'ast']:
    stat_bets = bets_df[bets_df['stat'] == stat]
    
    stat_over = stat_bets[stat_bets['side'] == 'over']
    stat_under = stat_bets[stat_bets['side'] == 'under']
    
    over_hr = (stat_over['outcome'] == 'win').mean() if len(stat_over) > 0 else 0
    under_hr = (stat_under['outcome'] == 'win').mean() if len(stat_under) > 0 else 0
    
    over_roi = stat_over['profit'].sum() / stat_over[stat_over['outcome'] != 'push']['stake'].sum() if len(stat_over) > 0 else 0
    under_roi = stat_under['profit'].sum() / stat_under[stat_under['outcome'] != 'push']['stake'].sum() if len(stat_under) > 0 else 0
    
    print(f"\n{stat.upper()}:")
    print(f"  OVER:  {len(stat_over):,} bets | Hit: {over_hr:.1%} | ROI: {over_roi:+.2%}")
    print(f"  UNDER: {len(stat_under):,} bets | Hit: {under_hr:.1%} | ROI: {under_roi:+.2%}")

print("\n" + "=" * 60)
print("EDGE BUCKET ANALYSIS")
print("=" * 60)

for low, high, label in [(0.05, 0.07, "5-7%"), (0.07, 0.10, "7-10%"), (0.10, 0.15, "10-15%"), (0.15, 1.0, "15%+")]:
    bucket = bets_df[(bets_df['edge'] >= low) & (bets_df['edge'] < high)]
    if len(bucket) == 0:
        continue
    wins = (bucket['outcome'] == 'win').sum()
    losses = (bucket['outcome'] == 'loss').sum()
    profit = bucket['profit'].sum()
    staked = bucket[bucket['outcome'] != 'push']['stake'].sum()
    
    print(f"Edge {label}: {len(bucket):,} bets | Hit: {wins/(wins+losses):.1%} | ROI: {profit/staked:+.2%}")
from pathlib import Path
import pandas as pd

sweeps = {
    'phase2_raw': 'backtest_results/mlb_sweep_pitcher_k_phase2_raw_under_20260413_20260510',
    'phase2_bl': 'backtest_results/mlb_sweep_pitcher_k_phase2_bl_under_focused_20260413_20260510',
    'phase3a_tuned_under': 'backtest_results/mlb_sweep_20260513_161315',
    'phase3a_tuned_both': 'backtest_results/mlb_sweep_20260513_161322',
    'phase3a_raw': 'backtest_results/mlb_sweep_pitcher_k_phase3a_raw_under_20260413_20260510',
    'phase3a_bl': 'backtest_results/mlb_sweep_pitcher_k_phase3a_bl_under_20260413_20260510',
}

show_cols = [
    'config_id', 'tau', 'z_max', 'max_weight', 'edge_threshold', 'total_bets',
    'wins', 'losses', 'hit_rate', 'roi', 'total_profit', 'sharpe_ratio', 'max_drawdown'
]

for name, d in sweeps.items():
    p = Path(d)
    print('\n==', name, d, 'exists', p.exists())
    if not (p / 'sweep_summary.csv').exists():
        continue
    df = pd.read_csv(p / 'sweep_summary.csv')
    print('cols', list(df.columns))
    print('shape', df.shape)
    available = [c for c in show_cols if c in df.columns]
    print(df.sort_values('roi', ascending=False).head(8)[available].to_string(index=False))
    m = df[df.total_bets >= 100] if 'total_bets' in df.columns else df.iloc[0:0]
    if len(m):
        print('best>=100')
        print(m.sort_values('roi', ascending=False).head(5)[available].to_string(index=False))
        row = m.sort_values('roi', ascending=False).iloc[0]
    else:
        row = df.sort_values('roi', ascending=False).iloc[0]
    dirs = []
    if 'config_id' in df.columns:
        cid = int(row['config_id'])
        dirs = [x for x in p.iterdir() if x.is_dir() and x.name.startswith(f'config_{cid}_')]
    if not dirs:
        # Legacy sweep folders are only ordered config_N; show first few and choose the folder whose metrics match roughly.
        all_dirs = [x for x in p.iterdir() if x.is_dir() and x.name.startswith('config_')]
        print('legacy config dirs sample', [x.name for x in sorted(all_dirs)[:8]])
        target_bets = int(row.get('total_bets', -999))
        target_roi = float(row.get('roi', 999))
        import json
        candidates = []
        for x in all_dirs:
            mf = x / 'metrics.json'
            if not mf.exists():
                continue
            try:
                met = json.loads(mf.read_text())
                bets = int(met.get('total_bets', met.get('bets', -1)))
                roi = float(met.get('roi', 999))
                candidates.append((abs(bets-target_bets)+abs(roi-target_roi), x, bets, roi))
            except Exception:
                pass
        if candidates:
            candidates.sort(key=lambda z: z[0])
            dirs = [candidates[0][1]]
            print('matched legacy candidate', dirs[0].name, 'bets', candidates[0][2], 'roi', candidates[0][3])
    if dirs:
        cd = dirs[0]
        print('sample config dir', cd.name)
        for fn in ['bets.csv', 'predictions.csv', 'metrics.json']:
            f = cd / fn
            print(fn, 'exists', f.exists())
            if f.exists() and fn.endswith('.csv'):
                sample = pd.read_csv(f, nrows=3)
                print(' ', fn, 'columns', list(sample.columns))
                print(sample.head(2).to_string(index=False))
            elif f.exists():
                print(' ', f.read_text()[:500])

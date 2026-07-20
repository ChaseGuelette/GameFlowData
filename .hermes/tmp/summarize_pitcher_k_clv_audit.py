#!/usr/bin/env python3
import json
from pathlib import Path

lines = Path('.hermes/tmp/audit_pitcher_k_clv_july2026.out').read_text(encoding='utf-8').splitlines()
data = [json.loads(lines[0]), json.loads(lines[1])]
for target in data:
    q = target['queries']
    print(f"\n== {target['target']} ==")
    for name in ('overview', 'recent_post_commence'):
        print(name, json.dumps(q[name], separators=(',', ':')))
    daily = q['recent_by_game_date']
    print('recent_total_rows', sum(r['rows'] for r in daily))
    print('recent_dates', len(daily), daily[0]['game_date'] if daily else None, daily[-1]['game_date'] if daily else None)
    print('daily')
    pairs = q['recent_pairing_by_game_date']
    pairmap = {r['game_date']: r for r in pairs} if isinstance(pairs, list) else {}
    for r in daily:
        p = pairmap.get(r['game_date'], {})
        print(r['game_date'], 'rows', r['rows'], 'games', r['api_games'], 'game_link', r['game_id_nonnull'], 'player_link', r['player_id_nonnull'], 'paired_pct', p.get('paired_pct'))
    print('july_reason_offsets', json.dumps(q['july_reason_offsets'], separators=(',', ':')))
    if isinstance(pairs, dict):
        print('pairing_error', pairs)

local, remote = data
ld = {r['game_date']: r['rows'] for r in local['queries']['recent_by_game_date']}
rd = {r['game_date']: r['rows'] for r in remote['queries']['recent_by_game_date']}
print('\n== LOCAL_REMOTE_DAILY_DIFF remote-local ==')
for d in sorted(set(ld) | set(rd)):
    print(d, rd.get(d, 0) - ld.get(d, 0), 'local', ld.get(d, 0), 'remote', rd.get(d, 0))

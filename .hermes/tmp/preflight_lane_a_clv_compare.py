#!/usr/bin/env python3
"""SELECT-only cross-target ID comparison and chunked requested-window aggregate."""
from __future__ import annotations
import json,os
from collections import Counter
from urllib.parse import urlparse,parse_qsl,urlencode,urlunparse
import psycopg2
from dotenv import load_dotenv
P={"market":"pitcher_strikeouts","start":"2026-07-01 00:00:00-04","end":"2026-07-20 00:00:00-04","lo":9223438,"hi":10951689}
IDS="""SELECT id,(commence_time AT TIME ZONE 'America/New_York')::date AS game_date FROM public.mlb_player_props_clv_snapshots WHERE market_key=%(market)s AND id BETWEEN %(lo)s AND %(hi)s AND commence_time >= %(start)s::timestamptz AND commence_time < %(end)s::timestamptz ORDER BY id;"""
CHUNK="""SELECT COUNT(*)::bigint,MIN(id)::bigint,MAX(id)::bigint,MIN(requested_snapshot_time),MAX(requested_snapshot_time) FROM public.mlb_player_props_clv_snapshots WHERE market_key=%(market)s AND id BETWEEN %(chunk_lo)s AND %(chunk_hi)s AND requested_snapshot_time >= %(start)s::timestamptz AND requested_snapshot_time < %(end)s::timestamptz;"""
def urlt(u):
 p=urlparse(u);q=dict(parse_qsl(p.query));q.setdefault('connect_timeout','10');return urlunparse((p.scheme,p.netloc,p.path,p.params,urlencode(q),p.fragment))
def conn(key,label):
 c=psycopg2.connect(urlt(os.environ[key]),application_name='gameflow:lane_a_select_compare_'+label);c.set_session(readonly=True,autocommit=False);return c
def getids(c):
 with c.cursor() as x:x.execute("SET LOCAL statement_timeout='90s'");x.execute("SET LOCAL TIME ZONE 'America/New_York'");x.execute(IDS,P);r=x.fetchall()
 c.rollback();return {i:str(d) for i,d in r}
def chunks(c):
 total=0;mi=ma=None;tmin=tmax=None;parts=[]
 for lo in range(P['lo'],P['hi']+1,250000):
  hi=min(lo+249999,P['hi']);p={**P,'chunk_lo':lo,'chunk_hi':hi}
  with c.cursor() as x:x.execute("SET LOCAL statement_timeout='45s'");x.execute("SET LOCAL TIME ZONE 'America/New_York'");x.execute(CHUNK,p);r=x.fetchone()
  c.rollback();parts.append({'lo':lo,'hi':hi,'rows':r[0]});total+=r[0]
  if r[1] is not None:mi=r[1] if mi is None else min(mi,r[1]);ma=r[2] if ma is None else max(ma,r[2]);tmin=r[3] if tmin is None else min(tmin,r[3]);tmax=r[4] if tmax is None else max(tmax,r[4])
 return {'rows':total,'first_id':mi,'last_id':ma,'first_requested':tmin.isoformat() if tmin else None,'last_requested':tmax.isoformat() if tmax else None,'chunks':parts}
def main():
 load_dotenv();lk='LOCAL_DATABASE_URL_AGENT' if os.getenv('LOCAL_DATABASE_URL_AGENT') else 'LOCAL_DATABASE_URL';lc=conn(lk,'local');rc=conn('DATABASE_URL','remote');L=getids(lc);R=getids(rc);missing=sorted(set(R)-set(L));extra=sorted(set(L)-set(R));by=Counter(R[i] for i in missing)
 print(json.dumps({'remote_target_ids':len(R),'local_target_ids':len(L),'remote_ids_absent_locally':len(missing),'absent_by_game_date':dict(sorted(by.items())),'first_absent_ids':missing[:5],'last_absent_ids':missing[-5:],'local_ids_absent_remotely':len(extra),'remote_requested_window_chunked':chunks(rc)},separators=(',',':')))
 lc.close();rc.close();print('SQL_USED_BEGIN');print("SET LOCAL statement_timeout='90s';\nSET LOCAL TIME ZONE 'America/New_York';\n-- exact ID sets on each target\n"+IDS+"\n-- remote requested-window aggregate, repeated over inclusive 250000-ID chunks\n"+CHUNK);print('-- binds '+json.dumps(P,sort_keys=True));print('SQL_USED_END')
if __name__=='__main__':main()

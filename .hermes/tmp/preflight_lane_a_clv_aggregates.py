#!/usr/bin/env python3
"""SELECT-only ID-windowed Lane A aggregate preflight."""
from __future__ import annotations
import json,os
from datetime import date,datetime
from decimal import Decimal
from urllib.parse import urlparse,parse_qsl,urlencode,urlunparse
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
P={"market":"pitcher_strikeouts","start":"2026-07-01 00:00:00-04","end":"2026-07-20 00:00:00-04","lo":9223438,"hi":10951689}
BASE="market_key=%(market)s AND id BETWEEN %(lo)s AND %(hi)s AND commence_time >= %(start)s::timestamptz AND commence_time < %(end)s::timestamptz"
Q={
"first_last_and_count":f"""SELECT COUNT(*)::bigint AS rows,MIN(id)::bigint AS first_id,MAX(id)::bigint AS last_id,MIN(commence_time) AS first_commence,MAX(commence_time) AS last_commence,MIN(requested_snapshot_time) AS first_requested,MAX(requested_snapshot_time) AS last_requested FROM public.mlb_player_props_clv_snapshots WHERE {BASE};""",
"by_date_reason_offset":f"""SELECT (commence_time AT TIME ZONE 'America/New_York')::date AS game_date,scrape_reason,target_offset_minutes,COUNT(*)::bigint AS rows FROM public.mlb_player_props_clv_snapshots WHERE {BASE} GROUP BY 1,2,3 ORDER BY 1,2,3 NULLS FIRST;""",
"linkage":f"""SELECT COUNT(*)::bigint AS rows,COUNT(*) FILTER (WHERE game_id IS NULL)::bigint AS game_id_null_rows,COUNT(*) FILTER (WHERE player_id IS NULL)::bigint AS player_id_null_rows,COUNT(DISTINCT api_player_name) FILTER (WHERE player_id IS NULL)::bigint AS distinct_unlinked_api_player_names FROM public.mlb_player_props_clv_snapshots WHERE {BASE};""",
"post_commence_guard":f"""SELECT COUNT(*) FILTER (WHERE requested_snapshot_time >= commence_time)::bigint AS requested_at_or_post,COUNT(*) FILTER (WHERE snapshot_time >= commence_time)::bigint AS snapshot_at_or_post,COUNT(*) FILTER (WHERE requested_snapshot_time >= commence_time OR snapshot_time >= commence_time)::bigint AS either_at_or_post FROM public.mlb_player_props_clv_snapshots WHERE {BASE} AND commence_time IS NOT NULL;""",
"requested_window_bounds":f"""SELECT COUNT(*)::bigint AS rows,MIN(id)::bigint AS first_id,MAX(id)::bigint AS last_id,MIN(requested_snapshot_time) AS first_requested,MAX(requested_snapshot_time) AS last_requested FROM public.mlb_player_props_clv_snapshots WHERE market_key=%(market)s AND id BETWEEN %(lo)s AND %(hi)s AND requested_snapshot_time >= %(start)s::timestamptz AND requested_snapshot_time < %(end)s::timestamptz;""",
}
def ser(v):
 if isinstance(v,(datetime,date)): return v.isoformat()
 if isinstance(v,Decimal): return str(v)
 raise TypeError(type(v).__name__)
def urlt(u):
 p=urlparse(u);q=dict(parse_qsl(p.query));q.setdefault('connect_timeout','10');return urlunparse((p.scheme,p.netloc,p.path,p.params,urlencode(q),p.fragment))
def run(label,key):
 u=os.getenv(key);out={"target":label,"env_key":key,"queries":{}}
 if not u: out['error']='missing '+key;print(json.dumps(out));return
 c=psycopg2.connect(urlt(u),application_name='gameflow:lane_a_id_window_preflight');c.set_session(readonly=True,autocommit=False)
 for n,s in Q.items():
  try:
   with c.cursor(cursor_factory=RealDictCursor) as x:
    x.execute("SET LOCAL statement_timeout='90s'");x.execute("SET LOCAL TIME ZONE 'America/New_York'");x.execute(s,P);out['queries'][n]=list(x.fetchall())
   c.rollback()
  except Exception as e:c.rollback();out['queries'][n]={"error":type(e).__name__+': '+str(e).splitlines()[0]}
 c.close();print(json.dumps(out,default=ser,separators=(',',':')))
def main():
 load_dotenv();run('LOCAL','LOCAL_DATABASE_URL_AGENT' if os.getenv('LOCAL_DATABASE_URL_AGENT') else 'LOCAL_DATABASE_URL');run('REMOTE','DATABASE_URL');print('SQL_USED_BEGIN');print("SET LOCAL statement_timeout='90s';\nSET LOCAL TIME ZONE 'America/New_York';");[print('-- '+n+'\n'+s) for n,s in Q.items()];print('-- binds '+json.dumps(P,sort_keys=True));print('SQL_USED_END')
if __name__=='__main__':main()

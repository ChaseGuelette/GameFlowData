#!/usr/bin/env python3
"""SELECT-only bounded preflight for Lane A MLB pitcher_strikeouts dense CLV."""
from __future__ import annotations
import json, os
from datetime import date, datetime
from decimal import Decimal
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

TABLE="public.mlb_player_props_clv_snapshots"
P={"market":"pitcher_strikeouts","start":"2026-07-01 00:00:00-04","end":"2026-07-20 00:00:00-04"}
Q={
"indexes":"""SELECT indexname,indexdef FROM pg_indexes WHERE schemaname='public' AND tablename='mlb_player_props_clv_snapshots' ORDER BY indexname;""",
"high_id_sample":"""SELECT id,commence_time,requested_snapshot_time,snapshot_time,scrape_reason,target_offset_minutes,game_id,player_id,api_player_name FROM public.mlb_player_props_clv_snapshots WHERE market_key=%(market)s ORDER BY id DESC LIMIT 12;""",
"commence_bounds":"""SELECT id,commence_time,requested_snapshot_time FROM public.mlb_player_props_clv_snapshots WHERE market_key=%(market)s AND commence_time >= %(start)s::timestamptz AND commence_time < %(end)s::timestamptz ORDER BY id ASC LIMIT 1;""",
"commence_last":"""SELECT id,commence_time,requested_snapshot_time FROM public.mlb_player_props_clv_snapshots WHERE market_key=%(market)s AND commence_time >= %(start)s::timestamptz AND commence_time < %(end)s::timestamptz ORDER BY id DESC LIMIT 1;""",
"requested_first":"""SELECT id,commence_time,requested_snapshot_time FROM public.mlb_player_props_clv_snapshots WHERE market_key=%(market)s AND requested_snapshot_time >= %(start)s::timestamptz AND requested_snapshot_time < %(end)s::timestamptz ORDER BY id ASC LIMIT 1;""",
"requested_last":"""SELECT id,commence_time,requested_snapshot_time FROM public.mlb_player_props_clv_snapshots WHERE market_key=%(market)s AND requested_snapshot_time >= %(start)s::timestamptz AND requested_snapshot_time < %(end)s::timestamptz ORDER BY id DESC LIMIT 1;""",
"sessions":"""SELECT pid,usename,application_name,state,wait_event_type,wait_event,backend_start,xact_start,query_start,left(regexp_replace(query,E'[\\n\\r\\t]+',' ','g'),180) AS query FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND query ILIKE '%%mlb_player_props_clv_snapshots%%' AND state <> 'idle' ORDER BY query_start;"""
}
def ser(v):
 if isinstance(v,(datetime,date)): return v.isoformat()
 if isinstance(v,Decimal): return str(v)
 raise TypeError(type(v).__name__)
def url_timeout(u):
 p=urlparse(u); q=dict(parse_qsl(p.query)); q.setdefault('connect_timeout','10'); return urlunparse((p.scheme,p.netloc,p.path,p.params,urlencode(q),p.fragment))
def run(label,key):
 u=os.getenv(key)
 if not u: print(json.dumps({"target":label,"error":"missing "+key})); return
 out={"target":label,"env_key":key,"queries":{}}
 try: c=psycopg2.connect(url_timeout(u),application_name='gameflow:lane_a_select_preflight')
 except Exception as e: print(json.dumps({"target":label,"error":type(e).__name__+': '+str(e).splitlines()[0]})); return
 c.set_session(readonly=True,autocommit=False)
 for n,s in Q.items():
  try:
   with c.cursor(cursor_factory=RealDictCursor) as cur:
    cur.execute("SET LOCAL statement_timeout='45s'"); cur.execute("SET LOCAL TIME ZONE 'America/New_York'"); cur.execute(s,P); out['queries'][n]=list(cur.fetchall())
   c.rollback()
  except Exception as e: c.rollback(); out['queries'][n]={"error":type(e).__name__+': '+str(e).splitlines()[0]}
 c.close(); print(json.dumps(out,default=ser,separators=(',',':')))
def main():
 load_dotenv(); lk='LOCAL_DATABASE_URL_AGENT' if os.getenv('LOCAL_DATABASE_URL_AGENT') else 'LOCAL_DATABASE_URL'; run('LOCAL',lk); run('REMOTE','DATABASE_URL')
 print('SQL_USED_BEGIN'); print("SET LOCAL statement_timeout='45s';\nSET LOCAL TIME ZONE 'America/New_York';")
 for n,s in Q.items(): print('-- '+n+'\n'+s)
 print('-- binds '+json.dumps(P,sort_keys=True)); print('SQL_USED_END')
if __name__=='__main__': main()

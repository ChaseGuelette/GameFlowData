import asyncio
from pathlib import Path

import yaml
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

cfg = yaml.safe_load((Path.home()/"AppData/Local/hermes/config.yaml").read_text())
s = cfg['mcp_servers']['gbrain']
url = s['url']
headers = s.get('headers') or {}

async def main():
    async with streamablehttp_client(url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            for name,args in [
                ('whoami', {}),
                ('sources_list', {}),
                ('get_page', {'slug':'operations/hard-facts'}),
                ('get_page', {'slug':'operations/hard-facts','source_id':'gameflow'}),
                ('list_pages', {'limit':3,'sort':'updated_desc'}),
                ('query', {'query':'operations hard facts','limit':2,'detail':'low','source_id':'__all__'}),
            ]:
                print(f"\n=== {name} {args} ===")
                try:
                    res = await session.call_tool(name, args)
                    for c in res.content:
                        print(getattr(c, 'text', c))
                except Exception as e:
                    print(type(e).__name__, str(e))
asyncio.run(main())

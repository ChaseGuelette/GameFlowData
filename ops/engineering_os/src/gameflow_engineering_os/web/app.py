from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from gameflow_engineering_os.config import EngineeringOSConfig, load_config
from gameflow_engineering_os.render import generate_brief
from gameflow_engineering_os.runner import collect_all
from gameflow_engineering_os.state import store_for_config
from gameflow_engineering_os.subprocesses import redact

ROOT = Path(__file__).parent
templates = Jinja2Templates(directory=str(ROOT / "templates"))


def create_app(config: EngineeringOSConfig | None = None) -> FastAPI:
    cfg = config or load_config()
    app = FastAPI(title="GameFlow Engineering OS")
    app.state.config = cfg
    store = store_for_config(cfg)
    app.mount("/static", StaticFiles(directory=str(ROOT / "static")), name="static")

    def format_time(value: datetime) -> str:
        return value.astimezone(ZoneInfo(cfg.timezone)).isoformat()

    def freshness_seconds(value: datetime) -> int:
        return max(0, int((datetime.now(UTC) - value).total_seconds()))

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        try:
            store.latest_results()
            return {"status": "ok"}
        except Exception:
            return Response(status_code=503)

    @app.get("/", response_class=HTMLResponse)
    def index(request: Request):
        try:
            results = store.latest_results()
            brief = store.latest_brief()
            events = store.events(limit=10)
            stale = False
            if results:
                newest = max(r.observed_at for r in results)
                stale = (datetime.now(UTC) - newest).total_seconds() > cfg.web.stale_after_minutes * 60
            return templates.TemplateResponse(
                request,
                "index.html",
                {
                    "brief": brief,
                    "results": results,
                    "events": events,
                    "stale": stale,
                    "empty": not results,
                    "redact": redact,
                    "format_time": format_time,
                    "freshness_seconds": freshness_seconds,
                },
            )
        except Exception:
            return templates.TemplateResponse(
                request,
                "index.html",
                {
                    "brief": None,
                    "results": [],
                    "events": [],
                    "stale": False,
                    "empty": True,
                    "db_error": True,
                    "redact": redact,
                    "format_time": format_time,
                    "freshness_seconds": freshness_seconds,
                },
                status_code=500,
            )

    @app.get("/briefs", response_class=HTMLResponse)
    def briefs(request: Request):
        return templates.TemplateResponse(request, "brief_history.html", {"briefs": store.brief_history()})

    @app.get("/health/{check_id}", response_class=HTMLResponse)
    def health_detail(request: Request, check_id: str):
        matches = [r for r in store.latest_results() if r.check_id == check_id]
        if not matches:
            raise HTTPException(status_code=404, detail="check not found")
        return templates.TemplateResponse(
            request,
            "health_detail.html",
            {
                "check": matches[0],
                "events": store.events(check_id=check_id),
                "redact": redact,
                "format_time": format_time,
                "freshness_seconds": freshness_seconds,
            },
        )

    @app.post("/refresh")
    def refresh(request: Request):
        if not cfg.web.manual_refresh_enabled or not cfg.web.csrf_token:
            raise HTTPException(status_code=404, detail="manual refresh disabled")
        origin = request.headers.get("origin") or ""
        host = request.headers.get("host") or ""
        if origin and host not in origin:
            raise HTTPException(status_code=403, detail="same-origin check failed")
        if request.headers.get("x-csrf-token") != cfg.web.csrf_token:
            raise HTTPException(status_code=403, detail="bad csrf token")
        results = collect_all(cfg)
        store.persist_results(results, cfg)
        store.save_brief(generate_brief(results, cfg), cfg.daily_brief.retain_days)
        return RedirectResponse("/", status_code=303)

    return app

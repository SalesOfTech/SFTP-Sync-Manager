"""Главный вход FastAPI и точка запуска приложения."""

from __future__ import annotations

import argparse
import logging
import threading
from pathlib import Path
from typing import Dict, Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
import uvicorn

from storage import Storage, default_storage_path
from sync_manager import SyncManager
from tray import TrayController
from updater import Updater

APP_VERSION = "1.0.0"
DEFAULT_UPDATE_FEED = "https://example.com/sftp-sync-manager/latest.json"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("sftp_sync_manager")

templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))
templates.env.globals["app_version"] = APP_VERSION


def create_app(storage: Storage, sync_manager: SyncManager, updater: Updater) -> FastAPI:
    app = FastAPI(title="SFTP Sync Manager")

    @app.get("/", response_class=HTMLResponse)
    async def index(request: Request):
        connections = storage.list_connections()
        update_info = updater.last_result
        settings = storage.get_settings()
        return templates.TemplateResponse(
            "index.html",
            {
                "request": request,
                "connections": connections,
                "update_info": update_info,
                "app_version": APP_VERSION,
                "settings": settings,
            },
        )

    @app.get("/connections/add", response_class=HTMLResponse)
    async def add_connection(request: Request):
        return templates.TemplateResponse(
            "edit_connection.html",
            {
                "request": request,
                "mode": "add",
                "connection": {
                    "name": "",
                    "host": "",
                    "port": 22,
                    "username": "",
                    "auth_type": "password",
                    "password": "",
                    "private_key_path": "",
                    "passphrase": "",
                    "remote_path": "",
                    "local_path": "",
                    "interval": 30,
                    "enabled": True,
                    "allow_delete": False,
                    "local_priority": False,
                },
            },
        )

    @app.get("/connections/{connection_id}/edit", response_class=HTMLResponse)
    async def edit_connection(connection_id: int, request: Request):
        connection = storage.get_connection(connection_id)
        if not connection:
            raise HTTPException(status_code=404, detail="Connection not found")
        return templates.TemplateResponse(
            "edit_connection.html",
            {"request": request, "mode": "edit", "connection": connection},
        )

    @app.post("/connections/save")
    async def save_connection(request: Request):
        form = await request.form()
        data = _connection_from_form(form)
        connection_id = form.get("id")
        if connection_id:
            storage.update_connection(int(connection_id), data)
            sync_manager.refresh_connection(int(connection_id))
        else:
            new_id = storage.create_connection(data)
            if data["enabled"]:
                sync_manager.start_connection(new_id)
        return RedirectResponse("/", status_code=303)

    @app.post("/connections/{connection_id}/delete")
    async def delete_connection(connection_id: int):
        sync_manager.remove_connection(connection_id)
        return RedirectResponse("/", status_code=303)

    @app.post("/connections/{connection_id}/start")
    async def start_connection(connection_id: int):
        sync_manager.start_connection(connection_id)
        return RedirectResponse("/", status_code=303)

    @app.post("/connections/{connection_id}/stop")
    async def stop_connection(connection_id: int):
        sync_manager.stop_connection(connection_id)
        return RedirectResponse("/", status_code=303)

    @app.post("/connections/{connection_id}/sync")
    async def sync_now(connection_id: int):
        sync_manager.sync_now(connection_id)
        return RedirectResponse("/", status_code=303)

    @app.get("/connections/{connection_id}/logs", response_class=HTMLResponse)
    async def connection_logs(connection_id: int, request: Request):
        connection = storage.get_connection(connection_id)
        if not connection:
            raise HTTPException(status_code=404, detail="Connection not found")
        logs = storage.get_logs(connection_id)
        return templates.TemplateResponse(
            "logs.html",
            {"request": request, "connection": connection, "logs": logs},
        )

    @app.get("/settings", response_class=HTMLResponse)
    async def settings_page(request: Request):
        return templates.TemplateResponse(
            "settings.html",
            {"request": request, "settings": storage.get_settings()},
        )

    @app.post("/settings/save")
    async def save_settings(request: Request):
        form = await request.form()
        values = {
            "web_port": int(form.get("web_port", 8000)),
            "autostart": form.get("autostart") == "on",
            "log_path": form.get("log_path") or "logs",
            "language": form.get("language") or "ru",
            "update_feed": form.get("update_feed") or DEFAULT_UPDATE_FEED,
        }
        storage.update_settings(values)
        return RedirectResponse("/settings", status_code=303)

    @app.post("/updates/check")
    async def check_updates():
        updater.check_for_updates()
        return RedirectResponse("/", status_code=303)

    @app.post("/updates/apply")
    async def apply_update():
        info = updater.last_result
        if not info or not info.get("available") or not info.get("latest"):
            info = updater.check_for_updates()
            if not info.get("available"):
                return RedirectResponse("/", status_code=303)
        updater.download_and_apply(info["latest"])
        return RedirectResponse("/", status_code=303)

    @app.on_event("startup")
    async def startup_event():
        settings = storage.get_settings()
        if settings.get("autostart"):
            sync_manager.start_all()
        port = settings.get("web_port", 8000)
        _launch_tray(sync_manager, updater, port)

    @app.on_event("shutdown")
    async def shutdown_event():
        sync_manager.stop_all()

    return app


def _connection_from_form(form: Dict[str, str]) -> Dict[str, object]:
    return {
        "name": form.get("name", ""),
        "host": form.get("host", ""),
        "port": int(form.get("port") or 22),
        "username": form.get("username", ""),
        "auth_type": form.get("auth_type", "password"),
        "password": form.get("password"),
        "private_key_path": form.get("private_key_path"),
        "passphrase": form.get("passphrase"),
        "remote_path": form.get("remote_path", ""),
        "local_path": form.get("local_path", ""),
        "interval": int(form.get("interval") or 30),
        "enabled": form.get("enabled") == "on",
        "allow_delete": form.get("allow_delete") == "on",
        "local_priority": form.get("local_priority") == "on",
    }


_tray_controller: Optional[TrayController] = None


def _launch_tray(sync_manager: SyncManager, updater: Updater, port: int) -> None:
    global _tray_controller
    if _tray_controller:
        return
    try:
        _tray_controller = TrayController(sync_manager, updater, port, lambda: None)
        _tray_controller.start()
    except Exception as exc:
        logger.warning("Tray is not available: %s", exc)


def main() -> None:
    parser = argparse.ArgumentParser(description="SFTP Sync Manager")
    parser.add_argument("--db", type=Path, default=default_storage_path(), help="Path to SQLite database")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()

    storage = Storage(args.db)
    sync_manager = SyncManager(storage)
    updater = Updater(APP_VERSION, storage, DEFAULT_UPDATE_FEED)
    app = create_app(storage, sync_manager, updater)

    uvicorn.run(app, host=args.host, port=args.port, log_level="info")


if __name__ == "__main__":
    main()

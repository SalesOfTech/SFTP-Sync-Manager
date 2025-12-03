"""Контроллер системного трея на базе pystray."""

from __future__ import annotations

import os
import threading
import webbrowser
from typing import Callable, Optional

from PIL import Image, ImageDraw
import pystray


class TrayController:
    """Создаёт и управляет иконкой, пробрасывая действия в веб-интерфейс."""

    def __init__(self, sync_manager, updater, web_port: int, update_callback: Callable[[], None]):
        self.sync_manager = sync_manager
        self.updater = updater
        self.web_port = web_port
        self.update_callback = update_callback
        self.icon: Optional[pystray.Icon] = None
        self.thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if self.thread and self.thread.is_alive():
            return
        self.thread = threading.Thread(target=self._run_icon, daemon=True)
        self.thread.start()

    def stop(self) -> None:
        if self.icon:
            self.icon.stop()
            self.icon = None

    def _run_icon(self) -> None:
        menu = pystray.Menu(
            pystray.MenuItem("Open Web UI", lambda: self._open_url("/")),
            pystray.MenuItem("Start all sync", lambda: self.sync_manager.start_all()),
            pystray.MenuItem("Stop all sync", lambda: self.sync_manager.stop_all()),
            pystray.MenuItem("Check for updates", self._check_updates),
            pystray.MenuItem("Settings", lambda: self._open_url("/settings")),
            pystray.MenuItem("Exit", self._handle_exit),
        )
        image = self._build_icon()
        self.icon = pystray.Icon("sftpsyncmanager", image, "SFTP Sync Manager", menu)
        self.icon.run()

    def _build_icon(self) -> Image.Image:
        image = Image.new("RGB", (64, 64), (32, 43, 86))
        draw = ImageDraw.Draw(image)
        draw.rectangle((8, 32, 56, 56), fill=(255, 255, 255))
        draw.polygon([(16, 32), (32, 12), (48, 32)], fill=(87, 149, 255))
        draw.rectangle((24, 40, 40, 48), fill=(32, 43, 86))
        return image

    def _open_url(self, path: str) -> None:
        webbrowser.open(f"http://127.0.0.1:{self.web_port}{path}")

    def _check_updates(self) -> None:
        result = self.updater.check_for_updates()
        if result.get("available"):
            self.update_callback()

    def _handle_exit(self) -> None:
        self.sync_manager.stop_all()
        self.stop()
        os._exit(0)

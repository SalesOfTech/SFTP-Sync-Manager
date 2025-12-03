"""Update helper that downloads new builds and restarts the app."""

from __future__ import annotations

import hashlib
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Dict, Optional

import httpx


class Updater:
    # RU_PLACEHOLDER_UPDATER_DESCRIPTION

    def __init__(self, current_version: str, storage, default_feed: str):
        self.current_version = current_version
        self.storage = storage
        self.default_feed = default_feed
        self._last_result: Optional[Dict[str, object]] = None

    @property
    def last_result(self) -> Optional[Dict[str, object]]:
        return self._last_result

    def check_for_updates(self) -> Dict[str, object]:
        feed = self.storage.get_settings().get("update_feed") or self.default_feed
        result = {"available": False, "latest": None, "error": None}
        if not feed:
            result["error"] = "Update feed URL is not configured"
            self._last_result = result
            return result
        try:
            response = httpx.get(feed, timeout=10.0)
            response.raise_for_status()
            payload = response.json()
            latest_version = payload.get("version")
            if latest_version and self._compare_versions(latest_version, self.current_version) > 0:
                result["available"] = True
                result["latest"] = payload
            else:
                result["latest"] = payload
        except Exception as exc:
            result["error"] = str(exc)
        self._last_result = result
        return result

    def download_and_apply(self, metadata: Dict[str, object]) -> bool:
        url = metadata.get("url")
        if not url:
            raise ValueError("Update metadata must provide download URL")
        checksum = metadata.get("checksum")
        downloaded = self._download_file(url)
        try:
            if checksum:
                self._verify_checksum(downloaded, checksum)
            self._replace_current_binary(downloaded)
            self._restart()
            return True
        finally:
            if downloaded.exists():
                downloaded.unlink(missing_ok=True)

    # ----- Internals -----
    def _compare_versions(self, version_a: str, version_b: str) -> int:
        def normalize(v: str):
            return [int(part) for part in v.split(".")]

        a_parts = normalize(version_a)
        b_parts = normalize(version_b)
        max_len = max(len(a_parts), len(b_parts))
        a_parts.extend([0] * (max_len - len(a_parts)))
        b_parts.extend([0] * (max_len - len(b_parts)))
        for a, b in zip(a_parts, b_parts):
            if a > b:
                return 1
            if a < b:
                return -1
        return 0

    def _download_file(self, url: str) -> Path:
        tmp_dir = Path(tempfile.mkdtemp(prefix="sftpsync-update-"))
        target = tmp_dir / "update.bin"
        with httpx.stream("GET", url, timeout=None) as response:
            response.raise_for_status()
            with target.open("wb") as fh:
                for chunk in response.iter_bytes():
                    fh.write(chunk)
        return target

    def _verify_checksum(self, file_path: Path, checksum: str) -> None:
        algo, expected = checksum.split(":", 1) if ":" in checksum else ("sha256", checksum)
        h = hashlib.new(algo)
        with file_path.open("rb") as fh:
            for chunk in iter(lambda: fh.read(1024 * 1024), b""):
                h.update(chunk)
        actual = h.hexdigest()
        if actual.lower() != expected.lower():
            raise ValueError("Checksum mismatch for downloaded update")

    def _replace_current_binary(self, new_binary: Path) -> None:
        current = Path(sys.argv[0]).resolve()
        if not current.exists():
            destination = Path(sys.argv[0]).resolve()
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(new_binary, destination)
            return
        backup = current.with_suffix(current.suffix + ".bak")
        shutil.copy2(current, backup)
        os.replace(new_binary, current)

    def _restart(self) -> None:
        current = Path(sys.argv[0]).resolve()
        args = [str(current)] + sys.argv[1:]
        subprocess.Popen(args, close_fds=False)
        os._exit(0)

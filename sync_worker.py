"""Background worker thread that performs bidirectional sync for one connection."""

from __future__ import annotations

import os
import threading
import time
from pathlib import Path, PurePosixPath
from typing import Dict, Optional, Set, Tuple

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from ignore_parser import IgnoreRules
from sftp_client import SFTPClient
from storage import Storage


class _LocalEventHandler(FileSystemEventHandler):
    """Обработчик watchdog, который помечает затронутые файлы как dirty."""

    def __init__(self, worker: "SyncWorker"):
        self.worker = worker

    def on_any_event(self, event):
        if event.is_directory:
            return
        self.worker.mark_dirty(event.src_path)


class SyncWorker(threading.Thread):
    """Отдельный поток для двусторонней синхронизации одного подключения."""

    def __init__(
        self,
        connection: Dict[str, object],
        storage: Storage,
        status_callback,
    ):
        super().__init__(daemon=True)
        self.connection = connection
        self.storage = storage
        self.status_callback = status_callback
        self.stop_event = threading.Event()
        self.wake_event = threading.Event()
        self.dirty_paths: Set[str] = set()
        self.observer: Optional[Observer] = None

    @property
    def local_root(self) -> Path:
        return Path(self.connection["local_path"]).expanduser()

    @property
    def remote_root(self) -> str:
        raw = str(self.connection["remote_path"]).strip() or "/"
        return str(PurePosixPath(raw))

    def mark_dirty(self, absolute_path: str) -> None:
        try:
            rel = Path(absolute_path).resolve().relative_to(self.local_root.resolve())
        except Exception:
            return
        relative_str = rel.as_posix()
        if relative_str:
            self.dirty_paths.add(relative_str)
            self.wake_event.set()

    def trigger_sync(self) -> None:
        self.wake_event.set()

    def stop(self) -> None:
        self.stop_event.set()
        self.wake_event.set()

    def run(self) -> None:
        self.local_root.mkdir(parents=True, exist_ok=True)
        self._start_observer()
        try:
            while not self.stop_event.is_set():
                self._sync_cycle()
                wait_time = max(5, int(self.connection.get("interval", 30)))
                self.wake_event.wait(wait_time)
                self.wake_event.clear()
        finally:
            self._stop_observer()
            self.status_callback(self.connection["id"], "Stopped", None)

    def update_connection(self, data: Dict[str, object]) -> None:
        self.connection = data

    # ----- Observer helpers -----
    def _start_observer(self) -> None:
        handler = _LocalEventHandler(self)
        self.observer = Observer()
        self.observer.schedule(handler, str(self.local_root), recursive=True)
        self.observer.start()

    def _stop_observer(self) -> None:
        if self.observer:
            self.observer.stop()
            self.observer.join(timeout=5)
            self.observer = None

    # ----- Sync logic -----
    def _sync_cycle(self) -> None:
        if self.stop_event.is_set():
            return
        conn_id = self.connection["id"]
        ignore = IgnoreRules(self.local_root)
        self.status_callback(conn_id, "Syncing", None)

        local_snapshot = self._scan_local(ignore)
        last_state = self.storage.load_sync_state(conn_id)
        dirty = self._consume_dirty()

        try:
            with SFTPClient(
                host=self.connection["host"],
                port=int(self.connection["port"]),
                username=self.connection["username"],
                auth_type=self.connection["auth_type"],
                password=self.connection.get("password"),
                private_key_path=self.connection.get("private_key_path"),
                passphrase=self.connection.get("passphrase"),
            ) as remote:
                remote_snapshot = {
                    path: meta
                    for path, meta in remote.list_recursive(self.remote_root).items()
                    if not ignore.should_ignore(path)
                }
                actions = self._plan_actions(
                    local_snapshot, remote_snapshot, last_state, dirty
                )
                if actions:
                    self.status_callback(conn_id, "Syncing", None)
                for action, path in actions:
                    self._execute_action(remote, action, path, local_snapshot, remote_snapshot)
        except Exception as exc:
            self.status_callback(conn_id, "Error", str(exc))
            self.storage.add_log(conn_id, "error", "", str(exc))
            time.sleep(5)
            return

        snapshot = self._build_snapshot(local_snapshot, remote_snapshot)
        self.storage.save_sync_state(conn_id, snapshot)
        self.status_callback(conn_id, "Running", None)

    def _scan_local(self, ignore: IgnoreRules) -> Dict[str, Dict[str, float]]:
        files: Dict[str, Dict[str, float]] = {}
        for root, dirs, filenames in os.walk(self.local_root, followlinks=False):
            root_path = Path(root)
            rel_dir = root_path.relative_to(self.local_root).as_posix() if root_path != self.local_root else ""
            dirs[:] = [
                d
                for d in dirs
                if not ignore.should_ignore(
                    str(Path(rel_dir, d).as_posix()).lstrip("./")
                )
            ]
            for name in filenames:
                rel_path = Path(rel_dir, name).as_posix().lstrip("./")
                if ignore.should_ignore(rel_path):
                    continue
                full_path = root_path / name
                try:
                    stat = full_path.stat()
                except FileNotFoundError:
                    continue
                files[rel_path] = {"mtime": stat.st_mtime, "size": stat.st_size}
        return files

    def _consume_dirty(self) -> Set[str]:
        dirty = set(self.dirty_paths)
        self.dirty_paths.clear()
        return dirty

    def _plan_actions(
        self,
        local_files: Dict[str, Dict[str, float]],
        remote_files: Dict[str, Dict[str, float]],
        last_state: Dict[str, Dict[str, float]],
        dirty: Set[str],
    ) -> Set[Tuple[str, str]]:
        all_paths = set(local_files.keys()) | set(remote_files.keys())
        ordered = list(dirty) + sorted(all_paths - dirty)
        actions = []
        allow_delete = bool(self.connection.get("allow_delete"))
        local_priority = bool(self.connection.get("local_priority"))

        for path in ordered:
            local_meta = local_files.get(path)
            remote_meta = remote_files.get(path)
            prev = last_state.get(path)

            if local_meta and remote_meta:
                if self._files_differ(local_meta, remote_meta):
                    if local_priority or local_meta["mtime"] >= remote_meta["mtime"]:
                        actions.append(("upload", path))
                    else:
                        actions.append(("download", path))
            elif local_meta and not remote_meta:
                if prev and prev.get("local_exists") and prev.get("remote_exists"):
                    if allow_delete:
                        actions.append(("delete_local", path))
                    else:
                        actions.append(("upload", path))
                else:
                    actions.append(("upload", path))
            elif remote_meta and not local_meta:
                if prev and prev.get("local_exists") and prev.get("remote_exists"):
                    if allow_delete:
                        actions.append(("delete_remote", path))
                    else:
                        actions.append(("download", path))
                else:
                    actions.append(("download", path))
        return actions

    def _files_differ(self, local_meta: Dict[str, float], remote_meta: Dict[str, float]) -> bool:
        size_diff = abs(local_meta["size"] - remote_meta["size"])
        time_diff = abs(local_meta["mtime"] - remote_meta["mtime"])
        return size_diff > 0 or time_diff > 1.0

    def _execute_action(
        self,
        remote: SFTPClient,
        action: str,
        path: str,
        local_files: Dict[str, Dict[str, float]],
        remote_files: Dict[str, Dict[str, float]],
    ) -> None:
        conn_id = self.connection["id"]
        local_path = self.local_root / path
        remote_path = f"{self.remote_root.rstrip('/')}/{path}" if path else self.remote_root

        try:
            if action == "upload":
                remote.upload_file(local_path, remote_path)
                stat = local_path.stat()
                remote_files[path] = {"mtime": stat.st_mtime, "size": stat.st_size}
                self.storage.add_log(conn_id, "upload", path, "Uploaded to remote host")
            elif action == "download":
                remote.download_file(remote_path, local_path)
                stat = local_path.stat()
                local_files[path] = {"mtime": stat.st_mtime, "size": stat.st_size}
                self.storage.add_log(conn_id, "download", path, "Downloaded from remote host")
            elif action == "delete_local":
                if local_path.exists():
                    local_path.unlink()
                local_files.pop(path, None)
                self.storage.add_log(conn_id, "delete_local", path, "Removed local file after remote delete")
            elif action == "delete_remote":
                remote.delete_path(remote_path)
                remote_files.pop(path, None)
                self.storage.add_log(conn_id, "delete_remote", path, "Removed remote file after local delete")
        except Exception as exc:
            self.storage.add_log(conn_id, "error", path, str(exc))

    def _build_snapshot(
        self,
        local_files: Dict[str, Dict[str, float]],
        remote_files: Dict[str, Dict[str, float]],
    ) -> Dict[str, Dict[str, float]]:
        snapshot: Dict[str, Dict[str, float]] = {}
        for path in set(local_files.keys()) | set(remote_files.keys()):
            snapshot[path] = {
                "local_exists": path in local_files,
                "local_mtime": local_files.get(path, {}).get("mtime"),
                "remote_exists": path in remote_files,
                "remote_mtime": remote_files.get(path, {}).get("mtime"),
            }
        return snapshot

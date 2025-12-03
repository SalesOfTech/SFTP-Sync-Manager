"""Coordinator for sync workers."""

from __future__ import annotations

import threading
from typing import Dict, Optional

from storage import Storage
from sync_worker import SyncWorker


class SyncManager:
    # RU_PLACEHOLDER_MANAGER_DESCRIPTION

    def __init__(self, storage: Storage):
        self.storage = storage
        self.workers: Dict[int, SyncWorker] = {}
        self.lock = threading.Lock()

    def start_connection(self, connection_id: int) -> bool:
        with self.lock:
            if connection_id in self.workers:
                worker = self.workers[connection_id]
                if not worker.is_alive():
                    worker = self._spawn_worker(connection_id)
                    if not worker:
                        return False
                    self.workers[connection_id] = worker
                worker.trigger_sync()
                return True
            worker = self._spawn_worker(connection_id)
            if not worker:
                return False
            self.workers[connection_id] = worker
            return True

    def stop_connection(self, connection_id: int) -> None:
        with self.lock:
            worker = self.workers.pop(connection_id, None)
        if worker:
            worker.stop()
            worker.join(timeout=10)
            self.storage.update_status(connection_id, "Stopped", None)

    def start_all(self) -> None:
        connections = self.storage.list_connections()
        for conn in connections:
            if conn.get("enabled"):
                self.start_connection(conn["id"])

    def stop_all(self) -> None:
        for conn_id in list(self.workers.keys()):
            self.stop_connection(conn_id)

    def sync_now(self, connection_id: int) -> None:
        worker = self.workers.get(connection_id)
        if worker:
            worker.trigger_sync()
        else:
            self.start_connection(connection_id)

    def refresh_connection(self, connection_id: int) -> None:
        self.stop_connection(connection_id)
        conn = self.storage.get_connection(connection_id)
        if conn and conn.get("enabled"):
            self.start_connection(connection_id)

    def remove_connection(self, connection_id: int) -> None:
        self.stop_connection(connection_id)
        self.storage.delete_connection(connection_id)

    # ----- Internal helpers -----
    def _spawn_worker(self, connection_id: int) -> Optional[SyncWorker]:
        connection = self.storage.get_connection(connection_id)
        if not connection:
            return None
        worker = SyncWorker(connection, self.storage, self._handle_status)
        worker.start()
        return worker

    def _handle_status(self, connection_id: int, status: str, error: Optional[str]) -> None:
        self.storage.update_status(connection_id, status, error)
        if status == "Stopped":
            with self.lock:
                worker = self.workers.get(connection_id)
                if worker and not worker.is_alive():
                    self.workers.pop(connection_id, None)

"""SQLite-хранилище для конфигурации, логов и состояния синхронизации."""

from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


class Storage:
    """Потокобезопасный слой над sqlite3 для CRUD операций и состояния файлов."""

    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._init_db()

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS connections (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    host TEXT NOT NULL,
                    port INTEGER NOT NULL DEFAULT 22,
                    username TEXT NOT NULL,
                    auth_type TEXT NOT NULL DEFAULT 'password',
                    password TEXT,
                    private_key_path TEXT,
                    passphrase TEXT,
                    remote_path TEXT NOT NULL,
                    local_path TEXT NOT NULL,
                    interval INTEGER NOT NULL DEFAULT 30,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    allow_delete INTEGER NOT NULL DEFAULT 0,
                    local_priority INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'Stopped',
                    last_error TEXT
                );
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    connection_id INTEGER,
                    timestamp TEXT NOT NULL,
                    type TEXT NOT NULL,
                    path TEXT,
                    message TEXT,
                    FOREIGN KEY(connection_id) REFERENCES connections(id) ON DELETE CASCADE
                );
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS sync_state (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    connection_id INTEGER NOT NULL,
                    path TEXT NOT NULL,
                    local_exists INTEGER NOT NULL,
                    local_mtime REAL,
                    remote_exists INTEGER NOT NULL,
                    remote_mtime REAL,
                    hash TEXT,
                    UNIQUE(connection_id, path),
                    FOREIGN KEY(connection_id) REFERENCES connections(id) ON DELETE CASCADE
                );
                """
            )

    @contextmanager
    def _connect(self):
        with self._lock:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.commit()
            conn.close()

    def list_connections(self) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            cur = conn.execute("SELECT * FROM connections ORDER BY id DESC")
            return [dict(row) for row in cur.fetchall()]

    def get_connection(self, connection_id: int) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT * FROM connections WHERE id = ?", (connection_id,)
            )
            row = cur.fetchone()
            return dict(row) if row else None

    def create_connection(self, data: Dict[str, Any]) -> int:
        with self._connect() as conn:
            columns = ", ".join(data.keys())
            placeholders = ", ".join(["?"] * len(data))
            cur = conn.execute(
                f"INSERT INTO connections ({columns}) VALUES ({placeholders})",
                tuple(data.values()),
            )
            return cur.lastrowid

    def update_connection(self, connection_id: int, data: Dict[str, Any]) -> None:
        assignments = ", ".join([f"{k}=?" for k in data.keys()])
        values = list(data.values())
        values.append(connection_id)
        with self._connect() as conn:
            conn.execute(
                f"UPDATE connections SET {assignments} WHERE id = ?", tuple(values)
            )

    def delete_connection(self, connection_id: int) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM connections WHERE id = ?", (connection_id,))

    def update_status(
        self, connection_id: int, status: str, last_error: Optional[str] = None
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE connections SET status = ?, last_error = ? WHERE id = ?",
                (status, last_error, connection_id),
            )

    def add_log(
        self, connection_id: int, log_type: str, path: str, message: str
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO logs (connection_id, timestamp, type, path, message)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    connection_id,
                    datetime.utcnow().isoformat(),
                    log_type,
                    path,
                    message,
                ),
            )

    def get_logs(self, connection_id: int, limit: int = 500) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT * FROM logs
                WHERE connection_id = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (connection_id, limit),
            )
            return [dict(row) for row in cur.fetchall()]

    # Значения по умолчанию, если пользователь ещё ничего не сохранял.
    _DEFAULT_SETTINGS = {
        "web_port": 8000,
        "autostart": True,
        "log_path": "logs",
        "language": "ru",
        "update_feed": "",
    }

    def get_settings(self) -> Dict[str, Any]:
        with self._connect() as conn:
            cur = conn.execute("SELECT key, value FROM settings")
            stored = {row["key"]: json.loads(row["value"]) for row in cur.fetchall()}
        merged = dict(self._DEFAULT_SETTINGS)
        merged.update(stored)
        return merged

    def update_settings(self, values: Dict[str, Any]) -> None:
        with self._connect() as conn:
            for key, value in values.items():
                conn.execute(
                    """
                    INSERT INTO settings(key, value)
                    VALUES(?, ?)
                    ON CONFLICT(key) DO UPDATE SET value=excluded.value
                    """,
                    (key, json.dumps(value)),
                )

    def load_sync_state(self, connection_id: int) -> Dict[str, Dict[str, Any]]:
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT path, local_exists, local_mtime, remote_exists, remote_mtime, hash "
                "FROM sync_state WHERE connection_id = ?",
                (connection_id,),
            )
            result: Dict[str, Dict[str, Any]] = {}
            for row in cur.fetchall():
                result[row["path"]] = {
                    "local_exists": bool(row["local_exists"]),
                    "local_mtime": row["local_mtime"],
                    "remote_exists": bool(row["remote_exists"]),
                    "remote_mtime": row["remote_mtime"],
                    "hash": row["hash"],
                }
            return result

    def save_sync_state(
        self, connection_id: int, entries: Dict[str, Dict[str, Any]]
    ) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM sync_state WHERE connection_id = ?", (connection_id,))
            for path, meta in entries.items():
                conn.execute(
                    """
                    INSERT INTO sync_state(
                        connection_id, path, local_exists, local_mtime,
                        remote_exists, remote_mtime, hash
                    ) VALUES(?,?,?,?,?,?,?)
                    """,
                    (
                        connection_id,
                        path,
                        int(meta.get("local_exists", False)),
                        meta.get("local_mtime"),
                        int(meta.get("remote_exists", False)),
                        meta.get("remote_mtime"),
                        meta.get("hash"),
                    ),
                )


def default_storage_path() -> Path:
    base = Path.home() / ".sftp_sync_manager"
    base.mkdir(parents=True, exist_ok=True)
    return base / "app.db"

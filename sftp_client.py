"""Тонкая обёртка над Paramiko для SFTP-синхронизации."""
from __future__ import annotations

import stat
from pathlib import Path, PurePosixPath
from typing import Dict, Optional

import paramiko


class SFTPClient:
    """Инкапсулирует подключение и базовые операции с удалённой ФС."""

    def __init__(
        self,
        *,
        host: str,
        port: int,
        username: str,
        auth_type: str,
        password: Optional[str] = None,
        private_key_path: Optional[str] = None,
        passphrase: Optional[str] = None,
    ):
        self.host = host
        self.port = port
        self.username = username
        self.auth_type = auth_type
        self.password = password
        self.private_key_path = private_key_path
        self.passphrase = passphrase
        self.transport: Optional[paramiko.Transport] = None
        self.sftp: Optional[paramiko.SFTPClient] = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    def connect(self) -> None:
        if self.sftp:
            return
        self.transport = paramiko.Transport((self.host, self.port))
        if self.auth_type == "key":
            key = paramiko.RSAKey.from_private_key_file(
                self.private_key_path, password=self.passphrase or None
            )
            self.transport.connect(username=self.username, pkey=key)
        else:
            self.transport.connect(username=self.username, password=self.password)
        self.sftp = paramiko.SFTPClient.from_transport(self.transport)

    def close(self) -> None:
        if self.sftp:
            self.sftp.close()
            self.sftp = None
        if self.transport:
            self.transport.close()
            self.transport = None

    # ---------- Remote FS helpers ----------
    def list_recursive(self, remote_path: str) -> Dict[str, Dict[str, int]]:
        if not self.sftp:
            raise RuntimeError("SFTP session is not connected")
        files: Dict[str, Dict[str, int]] = {}
        base = self._normalize_posix(remote_path)
        self._ensure_base(base)

        def _walk(path: str, rel: PurePosixPath) -> None:
            for entry in self.sftp.listdir_attr(path):
                if path == "/":
                    entry_path = f"/{entry.filename}"
                else:
                    entry_path = f"{path.rstrip('/')}/{entry.filename}"
                rel_path = rel / entry.filename
                if stat.S_ISDIR(entry.st_mode):
                    _walk(entry_path, rel_path)
                else:
                    files[str(rel_path)] = {
                        "mtime": int(entry.st_mtime),
                        "size": int(entry.st_size),
                    }

        _walk(base, PurePosixPath(""))
        return files

    def ensure_remote_dir(self, remote_path: str) -> None:
        if not self.sftp:
            raise RuntimeError("SFTP session is not connected")
        parent = PurePosixPath(remote_path).parent
        self._ensure_path(str(parent))

    def _ensure_base(self, remote_path: str) -> None:
        self._ensure_path(remote_path)

    def _ensure_path(self, remote_path: str) -> None:
        if not self.sftp:
            raise RuntimeError("SFTP session is not connected")
        normalized = self._normalize_posix(remote_path)
        if normalized == "/":
            return
        absolute = normalized.startswith("/")
        segments = [seg for seg in normalized.strip("/").split("/") if seg]
        current = "/" if absolute else ""
        for seg in segments:
            current = f"{current.rstrip('/')}/{seg}" if current else seg
            try:
                self.sftp.listdir(current)
            except IOError:
                self.sftp.mkdir(current)

    def _normalize_posix(self, remote_path: str) -> str:
        text = str(PurePosixPath(remote_path))
        return text if text else "/"

    def download_file(self, remote_path: str, local_path: Path) -> None:
        if not self.sftp:
            raise RuntimeError("SFTP session is not connected")
        local_path.parent.mkdir(parents=True, exist_ok=True)
        self.sftp.get(remote_path, str(local_path))

    def upload_file(self, local_path: Path, remote_path: str) -> None:
        if not self.sftp:
            raise RuntimeError("SFTP session is not connected")
        self.ensure_remote_dir(remote_path)
        self.sftp.put(str(local_path), remote_path)

    def delete_path(self, remote_path: str) -> None:
        if not self.sftp:
            raise RuntimeError("SFTP session is not connected")
        try:
            attrs = self.sftp.stat(remote_path)
        except IOError:
            return
        if stat.S_ISDIR(attrs.st_mode):
            for entry in self.sftp.listdir(remote_path):
                child = f"{remote_path.rstrip('/')}/{entry}" if remote_path != "/" else f"/{entry}"
                self.delete_path(child)
            self.sftp.rmdir(remote_path)
        else:
            self.sftp.remove(remote_path)

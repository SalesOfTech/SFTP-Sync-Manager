""".sftpsyncignore-парсер с поддержкой отрицания и относительных путей."""
from __future__ import annotations

import fnmatch
from pathlib import Path
from typing import List


class IgnoreRules:
    """Загружает шаблоны и проверяет относительные пути."""

    def __init__(self, root: Path):
        self.root = Path(root)
        self.patterns: List[str] = self._load_patterns()

    def _load_patterns(self) -> List[str]:
        ignore_file = self.root / ".sftpsyncignore"
        if not ignore_file.exists():
            return []
        patterns: List[str] = []
        for raw in ignore_file.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            patterns.append(line)
        return patterns

    def should_ignore(self, relative_path: str) -> bool:
        rel = relative_path.replace("\\", "/").lstrip("./")
        if rel.endswith("/") and len(rel) > 1:
            rel = rel.rstrip("/")

        matched = False
        for pattern in self.patterns:
            negate = pattern.startswith("!")
            rule = pattern[1:] if negate else pattern
            if rule.startswith("/"):
                rule = rule.lstrip("/")
            candidate = rel
            if rule.endswith("/"):
                rule = rule.rstrip("/")
                if not candidate.startswith(rule):
                    continue
                match = True
            else:
                match = fnmatch.fnmatch(candidate, rule) or fnmatch.fnmatch(
                    candidate.split("/")[-1], rule
                )
            if match:
                matched = not negate
        return matched

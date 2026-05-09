"""Fail fast on files or contents that should not be committed.

This script is intentionally conservative. It checks for:
- local-only files that should stay out of version control
- obvious secrets committed into text files
- absolute local filesystem paths

Run from the project root:
    python script/check_repo_hygiene.py
"""

from __future__ import annotations

import re
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]

SKIP_DIRS = {
    ".git",
    ".venv",
    ".pytest_cache",
    "__pycache__",
    "build",
    "dist",
    "performance_hub.egg-info",
}

SKIP_SCAN_PREFIXES = (
    "docs/",
    "tests/",
    "skills/",
    "storage/",
)

BLOCKED_GLOBS = (
    "*.pem",
    "*.key",
    "credentials.json",
)

REQUIRED_GITIGNORE_PATTERNS = (
    ".env",
    ".env.*",
    "skills/",
    "catapult_review_*.json",
    "*.log",
    "logs/",
)

REQUIRED_DOCKERIGNORE_PATTERNS = (
    ".env",
    ".env.*",
    "skills",
    "catapult_review_*.json",
)

SECRET_ASSIGNMENT_RE = re.compile(
    r"^\s*([A-Z0-9_]*(?:PASSWORD|SECRET|TOKEN|API_KEY|FERNET|SQL_ALCHEMY_CONN)[A-Z0-9_]*)=(.+?)\s*$"
)
JWT_RE = re.compile(r"\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9._-]+\.[A-Za-z0-9._-]+\b")
PRIVATE_KEY_RE = re.compile(r"-----BEGIN (?:RSA |OPENSSH |EC |DSA )?PRIVATE KEY-----")
DSN_RE = re.compile(r"://[^/\s:@]+:[^/\s@]+@")
LOCAL_PATH_RE = re.compile(r"([A-Za-z]:\\Users\\|/Users/|/home/)")


def _iter_files(root: Path) -> list[Path]:
    files: list[Path] = []
    for path in root.rglob("*"):
        relative_parts = path.relative_to(root).parts
        if any(part in SKIP_DIRS for part in relative_parts):
            continue
        if path.is_file():
            files.append(path)
    return files


def _is_placeholder(value: str) -> bool:
    lowered = value.strip().strip('"').strip("'").lower()
    return lowered in {
        "",
        "change-me",
        "your-postgres-host",
        "your-database",
        "your-user",
        "admin",
    } or lowered.startswith("your-") or lowered.startswith("<")


def _scan_file(path: Path) -> list[str]:
    relative = path.relative_to(PROJECT_ROOT).as_posix()
    if relative.endswith(".example"):
        return []
    if any(relative.startswith(prefix) for prefix in SKIP_SCAN_PREFIXES):
        return []
    if relative == ".env" or relative.startswith("skills/") or relative.startswith(".claude/"):
        return []
    if path.match("catapult_review_*.json"):
        return []

    try:
        text = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return []

    findings: list[str] = []
    for index, line in enumerate(text.splitlines(), start=1):
        secret_match = SECRET_ASSIGNMENT_RE.match(line)
        if secret_match and not _is_placeholder(secret_match.group(2)):
            findings.append(f"{relative}:{index} contains a populated secret-like assignment")
        if PRIVATE_KEY_RE.search(line):
            findings.append(f"{relative}:{index} contains a private key block")
        if DSN_RE.search(line) and "example" not in line.lower() and "${" not in line:
            findings.append(f"{relative}:{index} contains credentials inside a connection string")
        if JWT_RE.search(line) and "your_" not in line.lower() and "your-" not in line.lower():
            findings.append(f"{relative}:{index} contains a JWT-like token")
        if LOCAL_PATH_RE.search(line) and "LOCAL_PATH_RE" not in line:
            findings.append(f"{relative}:{index} contains an absolute local path")
    return findings


def main() -> int:
    findings: list[str] = []

    gitignore_text = (PROJECT_ROOT / ".gitignore").read_text(encoding="utf-8")
    dockerignore_text = (PROJECT_ROOT / ".dockerignore").read_text(encoding="utf-8")

    for pattern in REQUIRED_GITIGNORE_PATTERNS:
        if pattern not in gitignore_text:
            findings.append(f".gitignore is missing required pattern: {pattern}")

    for pattern in REQUIRED_DOCKERIGNORE_PATTERNS:
        if pattern not in dockerignore_text:
            findings.append(f".dockerignore is missing required pattern: {pattern}")

    for pattern in BLOCKED_GLOBS:
        for path in PROJECT_ROOT.glob(pattern):
            name = path.name
            if name == ".env.example":
                continue
            findings.append(f"{name} matches blocked pattern {pattern}")

    for path in _iter_files(PROJECT_ROOT):
        findings.extend(_scan_file(path))

    deduped = sorted(set(findings))
    if deduped:
        print("Repository hygiene check failed:")
        for finding in deduped:
            print(f" - {finding}")
        print("\nFix the items above before committing this folder.")
        return 1

    print("Repository hygiene check passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

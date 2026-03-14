from __future__ import annotations

from pathlib import Path


_VENV_CANDIDATES = (".venv_nwp", ".venv_gfs")


def repo_venv_dir(repo_root: Path) -> Path:
    base = Path(repo_root)
    for name in _VENV_CANDIDATES:
        candidate = base / name
        if candidate.exists():
            return candidate
    return base / _VENV_CANDIDATES[0]


def repo_venv_python(repo_root: Path) -> Path:
    return repo_venv_dir(repo_root) / "bin" / "python"

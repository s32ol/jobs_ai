from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class WorkspacePaths:
    project_root: Path
    data_dir: Path
    raw_dir: Path
    processed_dir: Path
    exports_dir: Path
    sessions_dir: Path
    logs_dir: Path
    database_path: Path


def discover_project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def build_workspace_paths(database_path: Path, project_root: Path | None = None) -> WorkspacePaths:
    root = discover_project_root() if project_root is None else project_root.resolve()
    data_dir = root / "data"
    raw_dir = data_dir / "raw"
    processed_dir = data_dir / "processed"
    exports_dir = data_dir / "exports"
    sessions_dir = root / "sessions"
    logs_dir = root / "logs"
    resolved_database_path = database_path if database_path.is_absolute() else root / database_path
    return WorkspacePaths(
        project_root=root,
        data_dir=data_dir,
        raw_dir=raw_dir,
        processed_dir=processed_dir,
        exports_dir=exports_dir,
        sessions_dir=sessions_dir,
        logs_dir=logs_dir,
        database_path=resolved_database_path,
    )


def ensure_workspace(paths: WorkspacePaths) -> list[Path]:
    created_paths: list[Path] = []
    required_dirs = list(
        dict.fromkeys(
            [
                paths.data_dir,
                paths.raw_dir,
                paths.processed_dir,
                paths.exports_dir,
                paths.sessions_dir,
                paths.logs_dir,
                paths.database_path.parent,
            ]
        )
    )
    for directory in required_dirs:
        if directory.exists():
            continue
        directory.mkdir(parents=True, exist_ok=True)
        created_paths.append(directory)
    return created_paths


def missing_workspace_paths(paths: WorkspacePaths) -> list[Path]:
    required_dirs = (
        paths.data_dir,
        paths.raw_dir,
        paths.processed_dir,
        paths.exports_dir,
        paths.sessions_dir,
        paths.logs_dir,
        paths.database_path.parent,
    )
    return [directory for directory in required_dirs if not directory.exists()]

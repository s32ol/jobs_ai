from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timezone
from pathlib import Path
import re

from ..collect.fetch import Fetcher, fetch_text
from ..workspace import WorkspacePaths
from .harness import run_source_seeding
from .infer import parse_company_inputs
from .models import SourceSeedRun
from .writers import write_source_seed_artifacts

_LABEL_RE = re.compile(r"[^A-Za-z0-9._-]+")


def run_seed_sources_command(
    paths: WorkspacePaths,
    *,
    companies: Sequence[str],
    from_file: Path | None,
    out_dir: Path | None,
    label: str | None,
    timeout_seconds: float,
    report_only: bool,
    created_at: datetime | None = None,
    fetcher: Fetcher | None = None,
) -> SourceSeedRun:
    raw_values = _load_company_values(companies, from_file)
    company_inputs = parse_company_inputs(raw_values)
    if not company_inputs:
        raise ValueError("at least one company entry is required via arguments or --from-file")

    created_at_dt = _normalize_created_at(created_at)
    normalized_label = _normalize_label(label)
    run_id = _build_run_id(normalized_label, created_at_dt)
    output_dir = _resolve_output_dir(paths, out_dir=out_dir, run_id=run_id)
    run = run_source_seeding(
        company_inputs,
        timeout_seconds=timeout_seconds,
        label=normalized_label,
        report_only=report_only,
        created_at=created_at_dt,
        fetcher=fetch_text if fetcher is None else fetcher,
    )
    finalized_at = created_at_dt if created_at is not None else _current_utc_datetime()
    return write_source_seed_artifacts(
        output_dir,
        run,
        run_id=run_id,
        finished_at=_format_created_at(finalized_at),
    )


def _load_company_values(companies: Sequence[str], from_file: Path | None) -> tuple[str, ...]:
    values = [value for value in companies]
    if from_file is not None:
        values.extend(from_file.read_text(encoding="utf-8").splitlines())
    return tuple(values)


def _resolve_output_dir(
    paths: WorkspacePaths,
    *,
    out_dir: Path | None,
    run_id: str,
) -> Path:
    if out_dir is not None:
        if out_dir.is_absolute():
            return out_dir
        return (paths.project_root / out_dir).resolve()
    return paths.processed_dir / run_id


def _normalize_label(label: str | None) -> str | None:
    if label is None:
        return None
    normalized_label = _LABEL_RE.sub("-", label.strip()).strip("-.")
    if not normalized_label:
        raise ValueError("label must contain at least one letter or number")
    return normalized_label


def _normalize_created_at(created_at: datetime | None) -> datetime:
    if created_at is None:
        return _current_utc_datetime()
    if created_at.tzinfo is None:
        return created_at.replace(tzinfo=timezone.utc)
    return created_at.astimezone(timezone.utc)


def _current_utc_datetime() -> datetime:
    return datetime.now(timezone.utc)


def _build_run_id(label: str | None, created_at: datetime) -> str:
    stamp = created_at.strftime("%Y%m%dT%H%M%S%fZ")
    if label is None:
        return f"seed-sources-{stamp}"
    return f"seed-sources-{label}-{stamp}"


def _format_created_at(created_at: datetime) -> str:
    return created_at.isoformat(timespec="seconds").replace("+00:00", "Z")

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal, Protocol
import csv
import json
import re

from ..portal_support import build_portal_support, extract_portal_board_root_url
from ..workspace import WorkspacePaths
from .adapters import DEFAULT_ADAPTERS
from .fetch import Fetcher, fetch_text
from .models import CensusSourceResult, OutcomeEvidence, SourceInput

_SUPPORTED_PORTAL_TYPES = ("greenhouse", "lever", "ashby")
_LABEL_RE = re.compile(r"[^A-Za-z0-9._-]+")


class BoardCensusAdapter(Protocol):
    adapter_key: str

    def census(
        self,
        source: SourceInput,
        *,
        timeout_seconds: float,
        fetcher: Fetcher,
    ) -> CensusSourceResult:
        """Estimate the live posting count for one supported board root."""


@dataclass(frozen=True, slots=True)
class BoardCensusBoardResult:
    status: Literal["counted", "failed"]
    board_root_url: str | None
    input_urls: tuple[str, ...]
    portal_type: str | None
    portal_label: str
    available_job_count: int | None
    reason_code: str
    reason: str
    evidence: OutcomeEvidence | None = None


@dataclass(frozen=True, slots=True)
class BoardCensusPortalTotal:
    portal_type: str
    portal_label: str
    board_count: int
    job_count: int


@dataclass(frozen=True, slots=True)
class BoardCensusArtifactPaths:
    output_dir: Path
    json_path: Path
    csv_path: Path


@dataclass(frozen=True, slots=True)
class BoardCensusRun:
    run_id: str
    created_at: str
    finished_at: str
    label: str | None
    input_path: Path
    timeout_seconds: float
    input_count: int
    unique_board_count: int
    duplicate_input_count: int
    counted_board_count: int
    failed_count: int
    portal_totals: tuple[BoardCensusPortalTotal, ...]
    grand_total: int
    counted_boards: tuple[BoardCensusBoardResult, ...]
    failed_boards: tuple[BoardCensusBoardResult, ...]
    artifact_paths: BoardCensusArtifactPaths


@dataclass(frozen=True, slots=True)
class _NormalizedBoardInput:
    board_root_url: str
    input_urls: tuple[str, ...]
    portal_type: str
    portal_label: str


def run_board_census_command(
    paths: WorkspacePaths,
    *,
    from_file: Path,
    out_dir: Path | None,
    label: str | None,
    timeout_seconds: float,
    created_at: datetime | None = None,
    adapter_registry: Mapping[str, BoardCensusAdapter] | None = None,
    fetcher: Fetcher | None = None,
) -> BoardCensusRun:
    source_values = _load_sources_from_file(from_file)
    if not source_values:
        raise ValueError("at least one board URL is required via --from-file")

    created_at_dt = _normalize_created_at(created_at)
    normalized_label = _normalize_label(label)
    run_id = _build_run_id(normalized_label, created_at_dt)
    output_dir = _resolve_output_dir(paths, out_dir=out_dir, run_id=run_id)
    prepared_inputs, failed_inputs, duplicate_input_count = _prepare_board_inputs(source_values)
    registry = DEFAULT_ADAPTERS if adapter_registry is None else adapter_registry
    active_fetcher = fetch_text if fetcher is None else fetcher

    collected_results: list[BoardCensusBoardResult] = []
    for index, prepared_input in enumerate(prepared_inputs, start=1):
        collected_results.append(
            _run_board_census_for_input(
                index=index,
                prepared_input=prepared_input,
                timeout_seconds=timeout_seconds,
                adapter_registry=registry,
                fetcher=active_fetcher,
            )
        )

    counted_boards = _sort_counted_boards(
        tuple(result for result in collected_results if result.status == "counted")
    )
    failed_boards = _sort_failed_boards(
        tuple([*failed_inputs, *(result for result in collected_results if result.status == "failed")])
    )
    portal_totals = _build_portal_totals(counted_boards)
    grand_total = sum(total.job_count for total in portal_totals)
    configured_board_count = len(counted_boards) + len(failed_boards)
    finished_at_dt = created_at_dt if created_at is not None else _current_utc_datetime()
    artifact_paths = _write_board_census_artifacts(
        output_dir=output_dir,
        run_id=run_id,
        created_at=_format_created_at(created_at_dt),
        finished_at=_format_created_at(finished_at_dt),
        label=normalized_label,
        input_path=from_file,
        timeout_seconds=timeout_seconds,
        input_count=len(source_values),
        unique_board_count=configured_board_count,
        duplicate_input_count=duplicate_input_count,
        counted_boards=counted_boards,
        failed_boards=failed_boards,
        portal_totals=portal_totals,
        grand_total=grand_total,
    )
    return BoardCensusRun(
        run_id=run_id,
        created_at=_format_created_at(created_at_dt),
        finished_at=_format_created_at(finished_at_dt),
        label=normalized_label,
        input_path=from_file,
        timeout_seconds=timeout_seconds,
        input_count=len(source_values),
        unique_board_count=configured_board_count,
        duplicate_input_count=duplicate_input_count,
        counted_board_count=len(counted_boards),
        failed_count=len(failed_boards),
        portal_totals=portal_totals,
        grand_total=grand_total,
        counted_boards=counted_boards,
        failed_boards=failed_boards,
        artifact_paths=artifact_paths,
    )


def _run_board_census_for_input(
    *,
    index: int,
    prepared_input: _NormalizedBoardInput,
    timeout_seconds: float,
    adapter_registry: Mapping[str, BoardCensusAdapter],
    fetcher: Fetcher,
) -> BoardCensusBoardResult:
    adapter = adapter_registry.get(prepared_input.portal_type)
    if adapter is None:
        return BoardCensusBoardResult(
            status="failed",
            board_root_url=prepared_input.board_root_url,
            input_urls=prepared_input.input_urls,
            portal_type=prepared_input.portal_type,
            portal_label=prepared_input.portal_label,
            available_job_count=None,
            reason_code="unsupported_portal",
            reason=f"{prepared_input.portal_label} board census is not available in this build.",
        )

    source = SourceInput(
        index=index,
        source_url=prepared_input.input_urls[0],
        normalized_url=prepared_input.board_root_url,
        portal_type=prepared_input.portal_type,
        portal_support=build_portal_support(
            prepared_input.board_root_url,
            portal_type=prepared_input.portal_type,
        ),
    )
    try:
        result = adapter.census(
            source,
            timeout_seconds=timeout_seconds,
            fetcher=fetcher,
        )
    except Exception as exc:
        reason = f"{adapter.adapter_key} adapter failed unexpectedly: {exc.__class__.__name__}: {exc}"
        return BoardCensusBoardResult(
            status="failed",
            board_root_url=prepared_input.board_root_url,
            input_urls=prepared_input.input_urls,
            portal_type=prepared_input.portal_type,
            portal_label=prepared_input.portal_label,
            available_job_count=None,
            reason_code="adapter_failed",
            reason=reason,
            evidence=OutcomeEvidence(error=reason),
        )

    return BoardCensusBoardResult(
        status=result.outcome,
        board_root_url=prepared_input.board_root_url,
        input_urls=prepared_input.input_urls,
        portal_type=prepared_input.portal_type,
        portal_label=prepared_input.portal_label,
        available_job_count=result.available_job_count,
        reason_code=result.reason_code,
        reason=result.reason,
        evidence=result.evidence,
    )


def _prepare_board_inputs(
    source_values: Sequence[str],
) -> tuple[tuple[_NormalizedBoardInput, ...], tuple[BoardCensusBoardResult, ...], int]:
    grouped_inputs: dict[str, _NormalizedBoardInput] = {}
    failed_inputs: dict[str, BoardCensusBoardResult] = {}
    duplicate_input_count = 0

    for raw_value in source_values:
        source_url = raw_value.strip()
        portal_support = build_portal_support(source_url)
        portal_type = portal_support.portal_type if portal_support is not None else None
        portal_label = portal_support.portal_label if portal_support is not None else "Unknown"

        if portal_type not in _SUPPORTED_PORTAL_TYPES:
            dedupe_key = (
                portal_support.normalized_apply_url
                if portal_support is not None
                else source_url
            )
            failed_result = BoardCensusBoardResult(
                status="failed",
                board_root_url=None,
                input_urls=(source_url,),
                portal_type=portal_type,
                portal_label=portal_label,
                available_job_count=None,
                reason_code="unsupported_source",
                reason="board-census supports Greenhouse, Lever, and Ashby board URLs only.",
            )
            if dedupe_key in failed_inputs:
                duplicate_input_count += 1
                failed_inputs[dedupe_key] = _merge_board_result_inputs(
                    failed_inputs[dedupe_key],
                    source_url,
                )
            else:
                failed_inputs[dedupe_key] = failed_result
            continue

        board_root_url = extract_portal_board_root_url(source_url, portal_type=portal_type)
        if board_root_url is None:
            dedupe_key = (
                portal_support.normalized_apply_url
                if portal_support is not None
                else source_url
            )
            failed_result = BoardCensusBoardResult(
                status="failed",
                board_root_url=None,
                input_urls=(source_url,),
                portal_type=portal_type,
                portal_label=portal_label,
                available_job_count=None,
                reason_code="unsupported_board_root",
                reason=f"unable to normalize the input into a {portal_label} board root.",
            )
            if dedupe_key in failed_inputs:
                duplicate_input_count += 1
                failed_inputs[dedupe_key] = _merge_board_result_inputs(
                    failed_inputs[dedupe_key],
                    source_url,
                )
            else:
                failed_inputs[dedupe_key] = failed_result
            continue

        existing = grouped_inputs.get(board_root_url)
        if existing is not None:
            duplicate_input_count += 1
            grouped_inputs[board_root_url] = _NormalizedBoardInput(
                board_root_url=existing.board_root_url,
                input_urls=(*existing.input_urls, source_url),
                portal_type=existing.portal_type,
                portal_label=existing.portal_label,
            )
            continue

        grouped_inputs[board_root_url] = _NormalizedBoardInput(
            board_root_url=board_root_url,
            input_urls=(source_url,),
            portal_type=portal_type,
            portal_label=portal_label,
        )

    return tuple(grouped_inputs.values()), tuple(failed_inputs.values()), duplicate_input_count


def _build_portal_totals(
    counted_boards: Sequence[BoardCensusBoardResult],
) -> tuple[BoardCensusPortalTotal, ...]:
    results: list[BoardCensusPortalTotal] = []
    for portal_type in _SUPPORTED_PORTAL_TYPES:
        portal_results = [
            result
            for result in counted_boards
            if result.portal_type == portal_type
        ]
        portal_label = portal_results[0].portal_label if portal_results else _portal_label_for(portal_type)
        results.append(
            BoardCensusPortalTotal(
                portal_type=portal_type,
                portal_label=portal_label,
                board_count=len(portal_results),
                job_count=sum(result.available_job_count or 0 for result in portal_results),
            )
        )
    return tuple(results)


def _sort_counted_boards(
    counted_boards: Sequence[BoardCensusBoardResult],
) -> tuple[BoardCensusBoardResult, ...]:
    return tuple(
        sorted(
            counted_boards,
            key=lambda result: (
                _portal_sort_key(result.portal_type),
                -(result.available_job_count or 0),
                result.board_root_url or result.input_urls[0],
            ),
        )
    )


def _sort_failed_boards(
    failed_boards: Sequence[BoardCensusBoardResult],
) -> tuple[BoardCensusBoardResult, ...]:
    return tuple(
        sorted(
            failed_boards,
            key=lambda result: (
                _portal_sort_key(result.portal_type),
                result.board_root_url or result.input_urls[0],
            ),
        )
    )


def _merge_board_result_inputs(
    result: BoardCensusBoardResult,
    source_url: str,
) -> BoardCensusBoardResult:
    if source_url in result.input_urls:
        return result
    return BoardCensusBoardResult(
        status=result.status,
        board_root_url=result.board_root_url,
        input_urls=(*result.input_urls, source_url),
        portal_type=result.portal_type,
        portal_label=result.portal_label,
        available_job_count=result.available_job_count,
        reason_code=result.reason_code,
        reason=result.reason,
        evidence=result.evidence,
    )


def _write_board_census_artifacts(
    *,
    output_dir: Path,
    run_id: str,
    created_at: str,
    finished_at: str,
    label: str | None,
    input_path: Path,
    timeout_seconds: float,
    input_count: int,
    unique_board_count: int,
    duplicate_input_count: int,
    counted_boards: Sequence[BoardCensusBoardResult],
    failed_boards: Sequence[BoardCensusBoardResult],
    portal_totals: Sequence[BoardCensusPortalTotal],
    grand_total: int,
) -> BoardCensusArtifactPaths:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "board_census.json"
    csv_path = output_dir / "board_census.csv"

    _write_json(
        json_path,
        {
            "run_id": run_id,
            "created_at": created_at,
            "finished_at": finished_at,
            "label": label,
            "input_path": str(input_path),
            "timeout_seconds": timeout_seconds,
            "totals": {
                "input_urls": input_count,
                "unique_boards": unique_board_count,
                "duplicates_collapsed": duplicate_input_count,
                "counted_boards": len(counted_boards),
                "failed_boards": len(failed_boards),
                "greenhouse_jobs": _portal_total_job_count(portal_totals, "greenhouse"),
                "lever_jobs": _portal_total_job_count(portal_totals, "lever"),
                "ashby_jobs": _portal_total_job_count(portal_totals, "ashby"),
                "grand_total": grand_total,
            },
            "portal_totals": [
                {
                    "portal_type": total.portal_type,
                    "portal_label": total.portal_label,
                    "board_count": total.board_count,
                    "job_count": total.job_count,
                }
                for total in portal_totals
            ],
            "board_counts": [
                _board_result_payload(result)
                for result in counted_boards
            ],
            "failed_boards": [
                _board_result_payload(result)
                for result in failed_boards
            ],
            "artifacts": {
                "output_dir": str(output_dir),
                "json_path": str(json_path),
                "csv_path": str(csv_path),
            },
        },
    )
    _write_csv(
        csv_path,
        tuple((*counted_boards, *failed_boards)),
    )
    return BoardCensusArtifactPaths(
        output_dir=output_dir,
        json_path=json_path,
        csv_path=csv_path,
    )


def _portal_total_job_count(
    portal_totals: Sequence[BoardCensusPortalTotal],
    portal_type: str,
) -> int:
    match = next((total for total in portal_totals if total.portal_type == portal_type), None)
    return 0 if match is None else match.job_count


def _portal_label_for(portal_type: str | None) -> str:
    if portal_type == "greenhouse":
        return "Greenhouse"
    if portal_type == "lever":
        return "Lever"
    if portal_type == "ashby":
        return "Ashby"
    if portal_type == "workday":
        return "Workday"
    return "Unknown"


def _portal_sort_key(portal_type: str | None) -> int:
    if portal_type == "greenhouse":
        return 0
    if portal_type == "lever":
        return 1
    if portal_type == "ashby":
        return 2
    if portal_type == "workday":
        return 3
    return 4


def _board_result_payload(result: BoardCensusBoardResult) -> dict[str, object]:
    return {
        "status": result.status,
        "board_root_url": result.board_root_url,
        "input_urls": list(result.input_urls),
        "portal_type": result.portal_type,
        "portal_label": result.portal_label,
        "available_job_count": result.available_job_count,
        "reason_code": result.reason_code,
        "reason": result.reason,
        "evidence": _evidence_payload(result.evidence),
    }


def _evidence_payload(evidence: OutcomeEvidence | None) -> dict[str, object] | None:
    if evidence is None:
        return None
    return {
        "final_url": evidence.final_url,
        "status_code": evidence.status_code,
        "content_type": evidence.content_type,
        "page_title": evidence.page_title,
        "detected_patterns": list(evidence.detected_patterns),
        "error": evidence.error,
    }


def _write_json(output_path: Path, payload: object) -> None:
    temp_path = output_path.with_name(f"{output_path.name}.tmp")
    with temp_path.open("w", encoding="utf-8") as output_file:
        json.dump(payload, output_file, indent=2, ensure_ascii=True)
        output_file.write("\n")
    temp_path.replace(output_path)


def _write_csv(output_path: Path, results: Sequence[BoardCensusBoardResult]) -> None:
    temp_path = output_path.with_name(f"{output_path.name}.tmp")
    with temp_path.open("w", encoding="utf-8", newline="") as output_file:
        writer = csv.DictWriter(
            output_file,
            fieldnames=(
                "status",
                "portal_type",
                "portal_label",
                "board_root_url",
                "available_job_count",
                "reason_code",
                "reason",
                "input_urls",
                "final_url",
            ),
        )
        writer.writeheader()
        for result in results:
            writer.writerow(
                {
                    "status": result.status,
                    "portal_type": result.portal_type or "",
                    "portal_label": result.portal_label,
                    "board_root_url": result.board_root_url or "",
                    "available_job_count": (
                        ""
                        if result.available_job_count is None
                        else result.available_job_count
                    ),
                    "reason_code": result.reason_code,
                    "reason": result.reason,
                    "input_urls": "; ".join(result.input_urls),
                    "final_url": (
                        ""
                        if result.evidence is None or result.evidence.final_url is None
                        else result.evidence.final_url
                    ),
                }
            )
    temp_path.replace(output_path)


def _load_sources_from_file(input_path: Path) -> tuple[str, ...]:
    lines = input_path.read_text(encoding="utf-8").splitlines()
    values: list[str] = []
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        values.append(stripped)
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
        return f"board-census-{stamp}"
    return f"board-census-{label}-{stamp}"


def _format_created_at(created_at: datetime) -> str:
    return created_at.isoformat(timespec="seconds").replace("+00:00", "Z")

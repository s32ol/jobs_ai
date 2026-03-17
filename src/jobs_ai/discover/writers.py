from __future__ import annotations

from pathlib import Path
import json

from ..collect.models import OutcomeEvidence
from .models import (
    DiscoverArtifactPaths,
    DiscoverCandidate,
    DiscoverCandidateResult,
    DiscoverCollectSummary,
    DiscoverImportSummary,
    DiscoverRun,
    DiscoverRunReport,
    SearchExecutionResult,
    SearchHit,
)


def write_discover_artifacts(
    output_dir: Path,
    run: DiscoverRun,
    *,
    run_id: str | None = None,
    finished_at: str | None = None,
    collect_summary: DiscoverCollectSummary | None = None,
    import_summary: DiscoverImportSummary | None = None,
) -> DiscoverRun:
    output_dir.mkdir(parents=True, exist_ok=True)

    confirmed_sources_path = None if run.report.report_only else output_dir / "confirmed_sources.txt"
    manual_review_sources_path = None if run.report.report_only else output_dir / "manual_review_sources.json"
    discover_report_path = output_dir / "discover_report.json"

    if confirmed_sources_path is not None:
        _write_text_lines(confirmed_sources_path, run.confirmed_sources)
    else:
        _remove_if_exists(output_dir / "confirmed_sources.txt")

    if manual_review_sources_path is not None:
        manual_review_items = [
            _manual_review_item_payload(result)
            for result in run.report.candidate_results
            if result.outcome == "manual_review"
        ]
        _write_json(
            manual_review_sources_path,
            {
                "run_id": run_id or run.report.run_id or output_dir.name,
                "created_at": run.report.created_at,
                "finished_at": finished_at or run.report.finished_at or run.report.created_at,
                "label": run.report.label,
                "query": run.report.query,
                "item_count": len(manual_review_items),
                "items": manual_review_items,
            },
        )
    else:
        _remove_if_exists(output_dir / "manual_review_sources.json")

    artifact_paths = DiscoverArtifactPaths(
        output_dir=output_dir,
        confirmed_sources_path=confirmed_sources_path,
        manual_review_sources_path=manual_review_sources_path,
        discover_report_path=discover_report_path,
        search_artifact_dir=_search_artifact_dir(run.report.search_results),
    )
    finalized_run = run.with_finalization(
        artifact_paths=artifact_paths,
        run_id=run_id or run.report.run_id or output_dir.name,
        finished_at=finished_at or run.report.finished_at or run.report.created_at,
        collect_summary=collect_summary,
        import_summary=import_summary,
    )
    _write_json(discover_report_path, _run_report_payload(finalized_run.report))
    return finalized_run


def _run_report_payload(report: DiscoverRunReport) -> dict[str, object]:
    return {
        "run_id": report.run_id,
        "status": "failed" if report.has_fatal_search_failure else "success",
        "search_failure": report.has_fatal_search_failure,
        "created_at": report.created_at,
        "finished_at": report.finished_at,
        "label": report.label,
        "query": report.query,
        "limit": report.limit,
        "timeout_seconds": report.timeout_seconds,
        "report_only": report.report_only,
        "collect_requested": report.collect_requested,
        "import_requested": report.import_requested,
        "inputs": {
            "query": report.query,
            "limit": report.limit,
            "timeout_seconds": report.timeout_seconds,
            "report_only": report.report_only,
            "collect": report.collect_requested,
            "import": report.import_requested,
        },
        "totals": {
            "search_queries": len(report.search_results),
            "raw_hits": report.raw_hit_count,
            "candidate_sources": report.candidate_source_count,
            "verified_candidates": report.verified_candidate_count,
            "confirmed_sources": report.confirmed_count,
            "manual_review_needed": report.manual_review_count,
            "skipped": report.skipped_count,
        },
        "raw_hit_count": report.raw_hit_count,
        "candidate_source_count": report.candidate_source_count,
        "verified_candidate_count": report.verified_candidate_count,
        "confirmed_count": report.confirmed_count,
        "manual_review_count": report.manual_review_count,
        "skipped_count": report.skipped_count,
        "artifacts": _artifact_paths_payload(report.artifact_paths),
        "search_results": [_search_result_payload(result) for result in report.search_results],
        "candidates": [_candidate_result_payload(result) for result in report.candidate_results],
        "collect": _collect_summary_payload(report.collect_summary),
        "import": _import_summary_payload(report.import_summary),
    }


def _artifact_paths_payload(
    artifact_paths: DiscoverArtifactPaths | None,
) -> dict[str, str | None] | None:
    if artifact_paths is None:
        return None
    return {
        "output_dir": str(artifact_paths.output_dir),
        "confirmed_sources_path": (
            str(artifact_paths.confirmed_sources_path)
            if artifact_paths.confirmed_sources_path is not None
            else None
        ),
        "manual_review_sources_path": (
            str(artifact_paths.manual_review_sources_path)
            if artifact_paths.manual_review_sources_path is not None
            else None
        ),
        "discover_report_path": str(artifact_paths.discover_report_path),
        "search_artifact_dir": (
            str(artifact_paths.search_artifact_dir)
            if artifact_paths.search_artifact_dir is not None
            else None
        ),
    }


def _search_result_payload(result: SearchExecutionResult) -> dict[str, object]:
    return {
        "portal_type": result.plan.portal_type,
        "site_filter": result.plan.site_filter,
        "search_text": result.plan.search_text,
        "search_url": result.plan.search_url,
        "status": result.status,
        "hit_count": result.hit_count,
        "attempt_count": result.attempt_count,
        "error": result.error,
        "evidence": _evidence_payload(result.evidence),
        "raw_artifact_paths": [str(path) for path in result.raw_artifact_paths],
        "attempts": [
            {
                "attempt_number": attempt.attempt_number,
                "status": attempt.status,
                "hit_count": attempt.hit_count,
                "error": attempt.error,
                "evidence": _evidence_payload(attempt.evidence),
                "raw_artifact_path": (
                    str(attempt.raw_artifact_path)
                    if attempt.raw_artifact_path is not None
                    else None
                ),
            }
            for attempt in result.attempts
        ],
    }


def _candidate_result_payload(result: DiscoverCandidateResult) -> dict[str, object]:
    primary_hit = _primary_supporting_hit(result.candidate)
    return {
        "portal_type": result.candidate.portal_type,
        "source_url": result.candidate.source_url,
        "normalized_url": result.candidate.normalized_url,
        "original_url": (
            primary_hit.target_url
            if primary_hit is not None
            else result.candidate.normalized_url or result.candidate.source_url
        ),
        "source_query": primary_hit.search_text if primary_hit is not None else None,
        "source_search_url": primary_hit.search_url if primary_hit is not None else None,
        "outcome": result.outcome,
        "reason_code": result.reason_code,
        "reason": result.reason,
        "suggested_next_action": result.suggested_next_action,
        "confirmed_source": result.confirmed_source,
        "collected_lead_count": result.collected_lead_count,
        "evidence": _evidence_payload(result.evidence),
        "supporting_results": [_search_hit_payload(hit) for hit in result.candidate.supporting_results],
    }


def _manual_review_item_payload(result: DiscoverCandidateResult) -> dict[str, object]:
    return _candidate_result_payload(result)


def _search_hit_payload(hit: SearchHit) -> dict[str, object]:
    return {
        "search_text": hit.search_text,
        "search_url": hit.search_url,
        "target_url": hit.target_url,
        "title": hit.title,
    }


def _primary_supporting_hit(candidate: DiscoverCandidate) -> SearchHit | None:
    if not candidate.supporting_results:
        return None
    return candidate.supporting_results[0]


def _collect_summary_payload(summary: DiscoverCollectSummary | None) -> dict[str, object] | None:
    if summary is None:
        return None
    return {
        "requested": summary.requested,
        "executed": summary.executed,
        "status": summary.status,
        "output_dir": str(summary.output_dir) if summary.output_dir is not None else None,
        "run_report_path": str(summary.run_report_path) if summary.run_report_path is not None else None,
        "leads_path": str(summary.leads_path) if summary.leads_path is not None else None,
        "manual_review_path": (
            str(summary.manual_review_path)
            if summary.manual_review_path is not None
            else None
        ),
        "collected_count": summary.collected_count,
        "manual_review_count": summary.manual_review_count,
        "skipped_count": summary.skipped_count,
    }


def _import_summary_payload(summary: DiscoverImportSummary | None) -> dict[str, object] | None:
    if summary is None:
        return None
    return {
        "requested": summary.requested,
        "executed": summary.executed,
        "status": summary.status,
        "input_path": str(summary.input_path) if summary.input_path is not None else None,
        "inserted_count": summary.inserted_count,
        "skipped_count": summary.skipped_count,
        "skipped": list(summary.skipped),
        "errors": list(summary.errors),
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


def _write_text_lines(output_path: Path, values: tuple[str, ...]) -> None:
    temp_path = output_path.with_name(f"{output_path.name}.tmp")
    with temp_path.open("w", encoding="utf-8") as output_file:
        if values:
            output_file.write("\n".join(values))
            output_file.write("\n")
        else:
            output_file.write("")
    temp_path.replace(output_path)


def _remove_if_exists(output_path: Path) -> None:
    if output_path.exists():
        output_path.unlink()


def _search_artifact_dir(
    search_results: tuple[SearchExecutionResult, ...],
) -> Path | None:
    for result in search_results:
        if not result.raw_artifact_paths:
            continue
        return result.raw_artifact_paths[0].parent
    return None

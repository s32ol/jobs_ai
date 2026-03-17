from __future__ import annotations

from pathlib import Path
import json

from ..collect.models import OutcomeEvidence
from .models import CandidateResult, CompanySeedInput, CompanySeedResult, SourceSeedArtifactPaths, SourceSeedRun, SourceSeedRunReport


def write_source_seed_artifacts(
    output_dir: Path,
    run: SourceSeedRun,
    *,
    run_id: str | None = None,
    finished_at: str | None = None,
) -> SourceSeedRun:
    output_dir.mkdir(parents=True, exist_ok=True)

    confirmed_sources_path = None if run.report.report_only else output_dir / "confirmed_sources.txt"
    manual_review_sources_path = None if run.report.report_only else output_dir / "manual_review_sources.json"
    seed_report_path = output_dir / "seed_report.json"

    if confirmed_sources_path is not None:
        _write_text_lines(confirmed_sources_path, run.confirmed_sources)
    else:
        _remove_if_exists(output_dir / "confirmed_sources.txt")
    if manual_review_sources_path is not None:
        manual_review_items = [
            _manual_review_item_payload(result)
            for result in run.report.company_results
            if result.outcome == "manual_review"
        ]
        _write_json(
            manual_review_sources_path,
            {
                "run_id": run_id or run.report.run_id or output_dir.name,
                "created_at": run.report.created_at,
                "finished_at": finished_at or run.report.finished_at or run.report.created_at,
                "label": run.report.label,
                "item_count": len(manual_review_items),
                "items": manual_review_items,
            },
        )
    else:
        _remove_if_exists(output_dir / "manual_review_sources.json")

    artifact_paths = SourceSeedArtifactPaths(
        output_dir=output_dir,
        confirmed_sources_path=confirmed_sources_path,
        manual_review_sources_path=manual_review_sources_path,
        seed_report_path=seed_report_path,
    )
    finalized_run = run.with_finalization(
        artifact_paths=artifact_paths,
        run_id=run_id or run.report.run_id or output_dir.name,
        finished_at=finished_at or run.report.finished_at or run.report.created_at,
    )
    _write_json(seed_report_path, _run_report_payload(finalized_run.report))
    return finalized_run


def _run_report_payload(report: SourceSeedRunReport) -> dict[str, object]:
    return {
        "run_id": report.run_id,
        "created_at": report.created_at,
        "finished_at": report.finished_at,
        "label": report.label,
        "timeout_seconds": report.timeout_seconds,
        "report_only": report.report_only,
        "inputs": {
            "label": report.label,
            "timeout_seconds": report.timeout_seconds,
            "report_only": report.report_only,
            "companies": [_company_input_payload(company_input) for company_input in report.input_companies],
        },
        "totals": {
            "input_companies": report.input_company_count,
            "confirmed": report.confirmed_count,
            "manual_review_needed": report.manual_review_count,
            "skipped": report.skipped_count,
            "confirmed_sources": report.confirmed_source_count,
        },
        "input_company_count": report.input_company_count,
        "confirmed_count": report.confirmed_count,
        "manual_review_count": report.manual_review_count,
        "skipped_count": report.skipped_count,
        "confirmed_source_count": report.confirmed_source_count,
        "artifacts": _artifact_paths_payload(report.artifact_paths),
        "companies": [_company_result_payload(result) for result in report.company_results],
    }


def _artifact_paths_payload(
    artifact_paths: SourceSeedArtifactPaths | None,
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
        "seed_report_path": str(artifact_paths.seed_report_path),
    }


def _manual_review_item_payload(result: CompanySeedResult) -> dict[str, object]:
    return {
        "company": result.company_input.company,
        "domain": result.company_input.domain,
        "notes": result.company_input.notes,
        "career_page_url": result.company_input.career_page_url,
        "reason_code": result.reason_code,
        "reason": result.reason,
        "suggested_next_action": result.suggested_next_action,
        "evidence": _evidence_payload(result.evidence),
        "manual_review_sources": [
            _manual_review_source_payload(source)
            for source in result.manual_review_sources
        ],
        "attempted_candidates": [
            _candidate_result_payload(candidate_result)
            for candidate_result in result.attempted_candidates
        ],
    }


def _company_result_payload(result: CompanySeedResult) -> dict[str, object]:
    return {
        "index": result.company_input.index,
        "company": result.company_input.company,
        "domain": result.company_input.domain,
        "notes": result.company_input.notes,
        "career_page_url": result.company_input.career_page_url,
        "raw_value": result.company_input.raw_value,
        "outcome": result.outcome,
        "reason_code": result.reason_code,
        "reason": result.reason,
        "suggested_next_action": result.suggested_next_action,
        "confirmed_sources": list(result.confirmed_sources),
        "evidence": _evidence_payload(result.evidence),
        "manual_review_sources": [
            _manual_review_source_payload(source)
            for source in result.manual_review_sources
        ],
        "attempted_candidates": [
            _candidate_result_payload(candidate_result)
            for candidate_result in result.attempted_candidates
        ],
    }


def _company_input_payload(company_input: CompanySeedInput) -> dict[str, object]:
    return {
        "index": company_input.index,
        "company": company_input.company,
        "domain": company_input.domain,
        "notes": company_input.notes,
        "career_page_url": company_input.career_page_url,
        "raw_value": company_input.raw_value,
    }


def _candidate_result_payload(result: CandidateResult) -> dict[str, object]:
    return {
        "portal_type": result.candidate.portal_type,
        "slug": result.candidate.slug,
        "url": result.candidate.url,
        "slug_source": result.candidate.slug_source,
        "confidence": result.candidate.confidence,
        "outcome": result.outcome,
        "reason_code": result.reason_code,
        "reason": result.reason,
        "suggested_next_action": result.suggested_next_action,
        "detected_company": result.detected_company,
        "confirmed_url": result.confirmed_url,
        "evidence": _evidence_payload(result.evidence),
    }


def _manual_review_source_payload(source) -> dict[str, object]:
    return {
        "source_url": source.source_url,
        "portal_type": source.portal_type,
        "reason_code": source.reason_code,
        "reason": source.reason,
        "suggested_next_action": source.suggested_next_action,
        "detected_company": source.detected_company,
        "evidence": _evidence_payload(source.evidence),
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


def _write_text_lines(output_path: Path, values) -> None:
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

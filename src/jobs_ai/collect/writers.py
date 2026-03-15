from __future__ import annotations

from pathlib import Path
import json

from .models import (
    CollectArtifactPaths,
    CollectRun,
    CollectRunReport,
    CollectedLead,
    ManualReviewItem,
    OutcomeEvidence,
    SourceResult,
)


def write_collect_artifacts(
    output_dir: Path,
    run: CollectRun,
    *,
    run_id: str | None = None,
    finished_at: str | None = None,
) -> CollectRun:
    output_dir.mkdir(parents=True, exist_ok=True)

    leads_path = None if run.report.report_only else output_dir / "leads.import.json"
    manual_review_path = None if run.report.report_only else output_dir / "manual_review.json"
    run_report_path = output_dir / "run_report.json"

    if leads_path is not None:
        _write_json(leads_path, [_lead_payload(lead) for lead in run.collected_leads])
    else:
        _remove_if_exists(output_dir / "leads.import.json")
    if manual_review_path is not None:
        _write_json(
            manual_review_path,
            {
                "run_id": run_id or run.report.run_id or output_dir.name,
                "created_at": run.report.created_at,
                "finished_at": finished_at or run.report.finished_at or run.report.created_at,
                "label": run.report.label,
                "item_count": len(run.manual_review_items),
                "items": [_manual_review_payload(item) for item in run.manual_review_items],
            },
        )
    else:
        _remove_if_exists(output_dir / "manual_review.json")

    artifact_paths = CollectArtifactPaths(
        output_dir=output_dir,
        leads_path=leads_path,
        manual_review_path=manual_review_path,
        run_report_path=run_report_path,
    )
    finalized_run = run.with_finalization(
        artifact_paths=artifact_paths,
        run_id=run_id or run.report.run_id or output_dir.name,
        finished_at=finished_at or run.report.finished_at or run.report.created_at,
    )
    _write_json(run_report_path, _run_report_payload(finalized_run.report))
    return finalized_run


def _run_report_payload(report: CollectRunReport) -> dict[str, object]:
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
            "sources": list(report.input_sources),
        },
        "totals": {
            "input_sources": report.input_source_count,
            "collected_automatically": report.collected_count,
            "manual_review_needed": report.manual_review_count,
            "skipped": report.skipped_count,
        },
        "input_sources": list(report.input_sources),
        "input_source_count": report.input_source_count,
        "collected_count": report.collected_count,
        "manual_review_count": report.manual_review_count,
        "skipped_count": report.skipped_count,
        "artifacts": _artifact_paths_payload(report.artifact_paths),
        "sources": [_source_result_payload(result) for result in report.source_results],
    }


def _artifact_paths_payload(artifact_paths: CollectArtifactPaths | None) -> dict[str, str | None] | None:
    if artifact_paths is None:
        return None
    return {
        "output_dir": str(artifact_paths.output_dir),
        "leads_path": str(artifact_paths.leads_path) if artifact_paths.leads_path is not None else None,
        "manual_review_path": (
            str(artifact_paths.manual_review_path)
            if artifact_paths.manual_review_path is not None
            else None
        ),
        "run_report_path": str(artifact_paths.run_report_path),
    }


def _source_result_payload(result: SourceResult) -> dict[str, object]:
    return {
        "index": result.source.index,
        "source_url": result.source.source_url,
        "normalized_url": result.source.normalized_url,
        "portal_type": result.source.portal_type,
        "adapter_key": result.adapter_key,
        "outcome": result.outcome,
        "reason_code": result.reason_code,
        "reason": result.reason,
        "suggested_next_action": result.suggested_next_action,
        "evidence": _evidence_payload(result.evidence),
        "lead_count": len(result.collected_leads),
        "collected_leads": [_lead_payload(lead) for lead in result.collected_leads],
        "manual_review_item": (
            _manual_review_payload(result.manual_review_item)
            if result.manual_review_item is not None
            else None
        ),
    }


def _lead_payload(lead: CollectedLead) -> dict[str, str | None]:
    return lead.to_import_record()


def _manual_review_payload(item: ManualReviewItem) -> dict[str, object]:
    return {
        "source_url": item.source_url,
        "normalized_url": item.normalized_url,
        "portal_type": item.portal_type,
        "adapter_key": item.adapter_key,
        "reason_code": item.reason_code,
        "reason": item.reason,
        "suggested_next_action": item.suggested_next_action,
        "company_apply_url": item.company_apply_url,
        "hints": list(item.hints),
        "evidence": _evidence_payload(item.evidence),
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


def _remove_if_exists(output_path: Path) -> None:
    if output_path.exists():
        output_path.unlink()

# Code Excerpt: Discovery, Collection, and Import

Exact excerpts from the current repo for top-level workflow orchestration, ATS discovery, source collection, registry-first collect/import, and JSON import normalization.

## Top-level run workflow orchestration
Source: `src/jobs_ai/run_workflow.py` lines 80-279

```python
def run_operator_workflow(
    paths: WorkspacePaths,
    *,
    query: str,
    discover_limit: int = DEFAULT_RUN_DISCOVER_LIMIT,
    collect_limit: int | None = None,
    session_limit: int,
    out_dir: Path | None,
    label: str | None,
    timeout_seconds: float = 10.0,
    open_urls: bool = False,
    executor_mode: str | None = None,
    capture_search_artifacts: bool = False,
    use_registry: bool = False,
    created_at: datetime | None = None,
    fetcher: Fetcher | None = None,
) -> RunWorkflowResult:
    effective_fetcher = fetch_text if fetcher is None else fetcher
    effective_created_at = _normalize_created_at(created_at)
    if use_registry:
        registry_collect_result = collect_registry_sources(
            paths,
            source_ids=(),
            limit=collect_limit,
            out_dir=out_dir,
            label=label,
            timeout_seconds=timeout_seconds,
            verify_if_needed=True,
            force_verify=False,
            import_results=True,
            source_query=query,
            created_at=effective_created_at,
            fetcher=effective_fetcher,
        )
        collect_run = registry_collect_result.collect_run
        collect_artifacts = collect_run.report.artifact_paths
        assert collect_artifacts is not None
        workflow_dir = collect_artifacts.output_dir
        batch_id = (
            registry_collect_result.import_result.batch_id
            if registry_collect_result.import_result is not None
            else (collect_run.report.run_id or workflow_dir.name)
        )
        session_batch_id = batch_id
        session_selection_scope: SessionSelectionScope | None = None
        if registry_collect_result.import_result is not None:
            import_result = registry_collect_result.import_result
            if import_result.inserted_count == 0:
                session_batch_id = None
                session_selection_scope = SessionSelectionScope(
                    batch_id=None,
                    source_query=query,
                    import_source=None,
                    selection_mode="registry_refresh_empty_reused_existing",
                    refresh_batch_id=import_result.batch_id,
                )
            else:
                session_selection_scope = SessionSelectionScope(
                    batch_id=import_result.batch_id,
                    source_query=query,
                    import_source=import_result.import_source,
                    selection_mode="registry_new_imports",
                    refresh_batch_id=import_result.batch_id,
                )
        initialize_schema(paths.database_path)
        session_result = start_session(
            paths.database_path,
            project_root=paths.project_root,
            default_exports_dir=paths.exports_dir,
            limit=session_limit,
            out_dir=workflow_dir,
            label=label,
            open_urls=open_urls,
            executor_mode=executor_mode,
            created_at=effective_created_at,
            ingest_batch_id=session_batch_id,
            source_query=query,
            job_query=query,
            selection_scope=session_selection_scope,
        )
        return RunWorkflowResult(
            query=query,
            output_dir=workflow_dir,
            intake_mode="registry",
            discover_run=None,
            registry_collect_result=registry_collect_result,
            collected_sources=collect_run.report.input_sources,
            collect_run=collect_run,
            import_result=registry_collect_result.import_result,
            session_result=session_result,
            discover_limit=discover_limit,
            collect_limit=collect_limit,
            session_limit=session_limit,
            label=label,
            open_requested=open_urls,
            executor_mode=session_result.executor_mode,
        )

    discover_run = run_discover_command(
        paths,
        query=query,
        limit=discover_limit,
        out_dir=out_dir,
        label=label,
        timeout_seconds=timeout_seconds,
        report_only=False,
        collect=False,
        import_results=False,
        capture_search_artifacts=capture_search_artifacts,
        created_at=effective_created_at,
        fetcher=effective_fetcher,
    )
    artifact_paths = discover_run.report.artifact_paths
    assert artifact_paths is not None
    if discover_run.report.has_fatal_search_failure:
        raise DiscoverSearchWorkflowError(discover_run)
    workflow_dir = artifact_paths.output_dir
    workflow_batch_id = discover_run.report.run_id or workflow_dir.name

    collected_sources = _select_collection_sources(
        discover_run.confirmed_sources,
        collect_limit=collect_limit,
    )
    collect_run: CollectRun | None = None
    import_result: JobImportResult | None = None

    if collected_sources:
        collect_run = run_collect_command(
            paths,
            sources=collected_sources,
            from_file=None,
            out_dir=workflow_dir / "collect",
            label=label,
            timeout_seconds=timeout_seconds,
            report_only=False,
            created_at=effective_created_at,
            fetcher=effective_fetcher,
        )
        collect_artifacts = collect_run.report.artifact_paths
        assert collect_artifacts is not None
        leads_path = collect_artifacts.leads_path
        if (
            leads_path is not None
            and leads_path.exists()
            and collect_run.report.collected_count > 0
        ):
            initialize_schema(paths.database_path)
            import_result = import_jobs_from_file(
                paths.database_path,
                leads_path,
                batch_id=workflow_batch_id,
                source_query=discover_run.report.query,
                import_source=str(leads_path),
                created_at=effective_created_at,
            )

    initialize_schema(paths.database_path)
    session_result = start_session(
        paths.database_path,
        project_root=paths.project_root,
        default_exports_dir=paths.exports_dir,
        limit=session_limit,
        out_dir=workflow_dir,
        label=label,
        open_urls=open_urls,
        executor_mode=executor_mode,
        created_at=effective_created_at,
        ingest_batch_id=workflow_batch_id,
        source_query=discover_run.report.query,
    )

    return RunWorkflowResult(
        query=query,
        output_dir=workflow_dir,
        intake_mode="discover",
        discover_run=discover_run,
        registry_collect_result=None,
        collected_sources=collected_sources,
        collect_run=collect_run,
        import_result=import_result,
        session_result=session_result,
        discover_limit=discover_limit,
        collect_limit=collect_limit,
        session_limit=session_limit,
        label=label,
        open_requested=open_urls,
        executor_mode=session_result.executor_mode,
    )


def _select_collection_sources(
    confirmed_sources: tuple[str, ...],
    *,
    collect_limit: int | None,
) -> tuple[str, ...]:
    if collect_limit is None:
        return confirmed_sources
    return confirmed_sources[:collect_limit]
```

## Discover command orchestration and follow-on collect/import
Source: `src/jobs_ai/discover/cli.py` lines 19-208

```python
def run_discover_command(
    paths: WorkspacePaths,
    *,
    query: str,
    limit: int,
    out_dir: Path | None,
    label: str | None,
    timeout_seconds: float,
    report_only: bool,
    collect: bool,
    import_results: bool,
    capture_search_artifacts: bool = False,
    created_at: datetime | None = None,
    fetcher: Fetcher | None = None,
) -> DiscoverRun:
    normalized_query = _normalize_query(query)
    created_at_dt = _normalize_created_at(created_at)
    normalized_label = _normalize_label(label)
    run_id = _build_run_id(normalized_label, created_at_dt)
    output_dir = _resolve_output_dir(paths, out_dir=out_dir, run_id=run_id)
    effective_collect = collect or import_results
    search_artifact_dir = (
        output_dir / "search_artifacts"
        if capture_search_artifacts and not report_only
        else None
    )

    run = run_discovery(
        normalized_query,
        limit=limit,
        timeout_seconds=timeout_seconds,
        label=normalized_label,
        report_only=report_only,
        collect_requested=effective_collect,
        import_requested=import_results,
        created_at=created_at_dt,
        fetcher=fetch_text if fetcher is None else fetcher,
        search_artifact_dir=search_artifact_dir,
    )

    collect_summary: DiscoverCollectSummary | None = None
    import_summary: DiscoverImportSummary | None = None
    if effective_collect:
        collect_summary, import_summary = _run_follow_on_steps(
            paths,
            run=run,
            run_id=run_id,
            output_dir=output_dir,
            label=normalized_label,
            timeout_seconds=timeout_seconds,
            created_at=created_at_dt,
            fetcher=fetch_text if fetcher is None else fetcher,
            import_results=import_results,
        )
    else:
        collect_summary = DiscoverCollectSummary(
            requested=False,
            executed=False,
            status="not_requested",
        )
        import_summary = DiscoverImportSummary(
            requested=False,
            executed=False,
            status="not_requested",
        )

    finalized_at = created_at_dt if created_at is not None else _current_utc_datetime()
    return write_discover_artifacts(
        output_dir,
        run,
        run_id=run_id,
        finished_at=_format_created_at(finalized_at),
        collect_summary=collect_summary,
        import_summary=import_summary,
    )


def _run_follow_on_steps(
    paths: WorkspacePaths,
    *,
    run: DiscoverRun,
    run_id: str,
    output_dir: Path,
    label: str | None,
    timeout_seconds: float,
    created_at: datetime,
    fetcher: Fetcher,
    import_results: bool,
) -> tuple[DiscoverCollectSummary, DiscoverImportSummary]:
    if not run.confirmed_sources:
        skip_status = (
            "skipped_search_failure"
            if run.report.has_fatal_search_failure
            else "skipped_no_confirmed_sources"
        )
        return (
            DiscoverCollectSummary(
                requested=True,
                executed=False,
                status=skip_status,
            ),
            DiscoverImportSummary(
                requested=import_results,
                executed=False,
                status=skip_status if import_results else "not_requested",
            ),
        )

    collect_run = run_collect_command(
        paths,
        sources=run.confirmed_sources,
        from_file=None,
        out_dir=output_dir / "collect",
        label=label,
        timeout_seconds=timeout_seconds,
        report_only=False,
        created_at=created_at,
        fetcher=fetcher,
    )
    collect_artifacts = collect_run.report.artifact_paths
    assert collect_artifacts is not None
    collect_summary = DiscoverCollectSummary(
        requested=True,
        executed=True,
        status="success",
        output_dir=collect_artifacts.output_dir,
        run_report_path=collect_artifacts.run_report_path,
        leads_path=collect_artifacts.leads_path,
        manual_review_path=collect_artifacts.manual_review_path,
        collected_count=collect_run.report.collected_count,
        manual_review_count=collect_run.report.manual_review_count,
        skipped_count=collect_run.report.skipped_count,
    )

    if not import_results:
        return (
            collect_summary,
            DiscoverImportSummary(
                requested=False,
                executed=False,
                status="not_requested",
            ),
        )

    leads_path = collect_artifacts.leads_path
    if (
        leads_path is None
        or not leads_path.exists()
        or collect_run.report.collected_count == 0
    ):
        return (
            collect_summary,
            DiscoverImportSummary(
                requested=True,
                executed=False,
                status=(
                    "skipped_no_collected_leads"
                    if collect_run.report.collected_count == 0
                    else "skipped_no_leads_artifact"
                ),
            ),
        )

    initialize_schema(paths.database_path)
    result = import_jobs_from_file(
        paths.database_path,
        leads_path,
        batch_id=run_id,
        source_query=run.report.query,
        import_source=str(leads_path),
        created_at=created_at,
    )
    return (
        collect_summary,
        DiscoverImportSummary(
            requested=True,
            executed=True,
            status="success" if not result.errors else "completed_with_errors",
            input_path=leads_path,
            batch_id=result.batch_id,
            source_query=result.source_query,
            inserted_count=result.inserted_count,
            skipped_count=result.skipped_count,
            duplicate_count=result.duplicate_count,
            skipped=result.skipped,
            errors=result.errors,
        ),
    )
```

## Collector harness and source normalization
Source: `src/jobs_ai/collect/harness.py` lines 14-169

```python
def run_collection(
    source_values: Sequence[str],
    *,
    timeout_seconds: float,
    label: str | None = None,
    report_only: bool = False,
    created_at: datetime | None = None,
    adapter_registry: Mapping[str, object] | None = None,
    generic_adapter: object | None = None,
    fetcher: Fetcher = fetch_text,
) -> CollectRun:
    created_at_dt = _normalize_created_at(created_at)
    source_results: list[SourceResult] = []
    collected_leads = []
    manual_review_items: list[ManualReviewItem] = []
    seen_urls: set[str] = set()

    for index, raw_value in enumerate(source_values, start=1):
        source, validation_problem = _prepare_source_input(index, raw_value)
        if validation_problem is not None:
            reason_code, reason = validation_problem
            source_results.append(
                build_skipped_result(
                    source,
                    adapter_key="harness",
                    reason_code=reason_code,
                    reason=reason,
                )
            )
            continue

        assert source.normalized_url is not None
        if source.normalized_url in seen_urls:
            source_results.append(
                build_skipped_result(
                    source,
                    adapter_key="harness",
                    reason_code="duplicate_normalized_source",
                    reason=f"duplicate source skipped after normalization: {source.normalized_url}",
                )
            )
            continue
        seen_urls.add(source.normalized_url)

        adapter = select_adapter(
            source,
            registry=DEFAULT_ADAPTERS if adapter_registry is None else adapter_registry,
            generic_adapter=GENERIC_ADAPTER if generic_adapter is None else generic_adapter,
        )
        result = _collect_source_result(
            adapter,
            source,
            timeout_seconds=timeout_seconds,
            fetcher=fetcher,
        )
        source_results.append(result)
        collected_leads.extend(result.collected_leads)
        if result.manual_review_item is not None:
            manual_review_items.append(result.manual_review_item)

    report = CollectRunReport(
        created_at=_format_created_at(created_at_dt),
        finished_at=None,
        run_id=None,
        label=label,
        timeout_seconds=timeout_seconds,
        report_only=report_only,
        input_sources=tuple(source_values),
        input_source_count=len(source_values),
        collected_count=len(collected_leads),
        manual_review_count=len(manual_review_items),
        skipped_count=sum(1 for result in source_results if result.outcome == "skipped"),
        source_results=tuple(source_results),
    )
    return CollectRun(
        report=report,
        collected_leads=tuple(collected_leads),
        manual_review_items=tuple(manual_review_items),
    )


def _prepare_source_input(index: int, raw_value: str) -> tuple[SourceInput, tuple[str, str] | None]:
    source_url = raw_value.strip()
    if not source_url:
        source = SourceInput(
            index=index,
            source_url=raw_value,
            normalized_url=None,
            portal_type=None,
            portal_support=None,
        )
        return source, ("blank_source_url", "blank source URL")

    parsed_url = urlparse(source_url)
    if parsed_url.scheme.lower() not in {"http", "https"}:
        source = SourceInput(
            index=index,
            source_url=source_url,
            normalized_url=None,
            portal_type=None,
            portal_support=None,
        )
        return source, ("unsupported_url_scheme", f"unsupported URL scheme: {parsed_url.scheme or '<missing>'}")
    if not parsed_url.netloc:
        source = SourceInput(
            index=index,
            source_url=source_url,
            normalized_url=None,
            portal_type=None,
            portal_support=None,
        )
        return source, ("missing_network_host", "source URL is missing a network host")

    portal_support = build_portal_support(source_url)
    normalized_url = (
        portal_support.company_apply_url
        if portal_support is not None and portal_support.company_apply_url is not None
        else portal_support.normalized_apply_url
        if portal_support is not None
        else parsed_url._replace(fragment="").geturl()
    )
    source = SourceInput(
        index=index,
        source_url=source_url,
        normalized_url=normalized_url,
        portal_type=portal_support.portal_type if portal_support is not None else None,
        portal_support=portal_support,
    )
    return source, None


def _collect_source_result(
    adapter: object,
    source: SourceInput,
    *,
    timeout_seconds: float,
    fetcher: Fetcher,
) -> SourceResult:
    adapter_key = getattr(adapter, "adapter_key", "unknown")
    try:
        return adapter.collect(
            source,
            timeout_seconds=timeout_seconds,
            fetcher=fetcher,
        )
    except Exception as exc:
        reason = f"{adapter_key} adapter failed unexpectedly: {exc.__class__.__name__}: {exc}"
        return build_skipped_result(
            source,
            adapter_key=adapter_key,
            reason_code="adapter_failed",
            reason=reason,
            evidence=OutcomeEvidence(error=reason),
        )
```

## Registry-first collect/import workflow
Source: `src/jobs_ai/sources/workflow.py` lines 20-147

```python
def collect_registry_sources(
    paths: WorkspacePaths,
    *,
    source_ids: Sequence[int] = (),
    limit: int | None = None,
    out_dir: Path | None = None,
    label: str | None = None,
    timeout_seconds: float,
    verify_if_needed: bool,
    force_verify: bool,
    import_results: bool,
    source_query: str | None = None,
    created_at: datetime | None = None,
    fetcher: Fetcher | None = None,
) -> SourceRegistryCollectResult:
    initialize_schema(paths.database_path)
    selected_entries = _select_registry_entries(
        paths,
        source_ids=source_ids,
    )
    verification_results = []

    if force_verify or verify_if_needed:
        verification_targets = [
            entry
            for entry in selected_entries
            if force_verify or entry.status != "active" or entry.last_verified_at is None
        ]
        verification_results = [
            verify_registry_source(
                paths.database_path,
                source_id=entry.source_id,
                timeout_seconds=timeout_seconds,
                created_at=created_at,
                fetcher=fetcher,
            )
            for entry in verification_targets
        ]

    active_entries = _active_collection_entries(
        paths,
        source_ids=source_ids,
    )
    if limit is not None:
        active_entries = active_entries[:limit]
    if not active_entries:
        raise ValueError(
            "no active registry sources are ready to collect; add or verify sources first"
        )

    collect_run = run_collect_command(
        paths,
        sources=tuple(entry.source_url for entry in active_entries),
        from_file=None,
        out_dir=out_dir,
        label=label,
        timeout_seconds=timeout_seconds,
        report_only=False,
        created_at=created_at,
        fetcher=fetch_text if fetcher is None else fetcher,
    )

    import_result = None
    if import_results:
        artifact_paths = collect_run.report.artifact_paths
        assert artifact_paths is not None
        leads_path = artifact_paths.leads_path
        if (
            leads_path is not None
            and leads_path.exists()
            and collect_run.report.collected_count > 0
        ):
            import_result = import_jobs_from_file(
                paths.database_path,
                leads_path,
                batch_id=collect_run.report.run_id,
                source_query=source_query,
                import_source=str(leads_path),
                created_at=created_at,
            )

    return SourceRegistryCollectResult(
        selected_entries=active_entries,
        verification_results=tuple(verification_results),
        collect_run=collect_run,
        import_result=import_result,
        import_requested=import_results,
    )


def _select_registry_entries(
    paths: WorkspacePaths,
    *,
    source_ids: Sequence[int],
):
    if source_ids:
        entries = list_registry_sources(paths.database_path, source_ids=source_ids)
        found_ids = {entry.source_id for entry in entries}
        missing_ids = [source_id for source_id in source_ids if source_id not in found_ids]
        if missing_ids:
            missing_text = ", ".join(str(source_id) for source_id in missing_ids)
            raise ValueError(f"registry source ids were not found: {missing_text}")
        return entries

    entries = list_registry_sources(paths.database_path, statuses=("active",))
    if not entries:
        raise ValueError(
            "the source registry does not contain any active sources yet; add sources or sync from seed-sources/discover first"
        )
    return entries


def _active_collection_entries(
    paths: WorkspacePaths,
    *,
    source_ids: Sequence[int],
):
    if source_ids:
        entries = [
            get_registry_source(paths.database_path, source_id=source_id)
            for source_id in source_ids
        ]
        return tuple(
            entry
            for entry in entries
            if entry is not None and entry.status == "active"
        )
    return list_registry_sources(paths.database_path, statuses=("active",))
```

## JSON import entrypoint and record normalization
Source: `src/jobs_ai/jobs/importer.py` lines 25-161

```python
@dataclass(frozen=True, slots=True)
class JobImportResult:
    inserted_count: int
    skipped_count: int
    batch_id: str | None = None
    source_query: str | None = None
    import_source: str | None = None
    duplicate_count: int = 0
    error_count: int = 0
    skipped: tuple[str, ...] = ()
    errors: tuple[str, ...] = ()


def import_jobs_from_file(
    database_path: Path,
    input_path: Path,
    *,
    batch_id: str | None = None,
    source_query: str | None = None,
    import_source: str | None = None,
    created_at: datetime | None = None,
) -> JobImportResult:
    records = load_job_records(input_path)
    inserted_count = 0
    duplicate_count = 0
    skipped: list[str] = []
    errors: list[str] = []
    normalized_batch_id = _resolve_batch_id(batch_id, created_at=created_at)
    normalized_source_query = normalize_optional_metadata(source_query)
    normalized_import_source = (
        normalize_optional_metadata(import_source)
        if import_source is not None
        else str(input_path)
    )

    with closing(connect_database(database_path)) as connection:
        for record_number, record in enumerate(records, start=1):
            job_record, error = normalize_import_record(record)
            if error is not None:
                errors.append(f"record {record_number}: {error}")
                continue

            duplicate_match = find_duplicate_job_match(connection, job_record)
            if duplicate_match is not None:
                duplicate_count += 1
                skipped.append(
                    f"record {record_number}: duplicate skipped via "
                    f"{describe_duplicate_match(duplicate_match)} "
                    f"(existing job id {duplicate_match.job_id})"
                )
                continue

            insert_job(
                connection,
                {
                    **job_record,
                    "ingest_batch_id": normalized_batch_id,
                    "source_query": normalized_source_query,
                    "import_source": normalized_import_source,
                },
            )
            inserted_count += 1
        connection.commit()

    return JobImportResult(
        inserted_count=inserted_count,
        skipped_count=duplicate_count + len(errors),
        batch_id=normalized_batch_id,
        source_query=normalized_source_query,
        import_source=normalized_import_source,
        duplicate_count=duplicate_count,
        error_count=len(errors),
        skipped=tuple(skipped),
        errors=tuple(errors),
    )


def load_job_records(input_path: Path) -> list[object]:
    if input_path.suffix.lower() not in SUPPORTED_IMPORT_SUFFIXES:
        supported = ", ".join(sorted(SUPPORTED_IMPORT_SUFFIXES))
        raise ValueError(f"unsupported file type '{input_path.suffix or '<none>'}'; supported types: {supported}")

    try:
        payload = json.loads(input_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"invalid JSON at line {exc.lineno}, column {exc.colno}: {exc.msg}"
        ) from exc

    if isinstance(payload, list):
        if not payload:
            raise ValueError("input file does not contain any job records")
        return payload
    if isinstance(payload, dict):
        return [payload]

    raise ValueError("JSON input must be an object or an array of objects")


def normalize_import_record(record: object) -> tuple[dict[str, str | None], str | None]:
    if not isinstance(record, dict):
        return {}, "expected a JSON object"

    normalized_record = normalize_job_import_fields(
        record,
        REQUIRED_IMPORT_FIELDS + OPTIONAL_IMPORT_FIELDS,
    )
    missing_fields = [field for field in REQUIRED_IMPORT_FIELDS if normalized_record[field] is None]
    if missing_fields:
        return {}, f"missing required fields: {', '.join(missing_fields)}"

    normalized_record["raw_json"] = json.dumps(record, ensure_ascii=True)
    return normalized_record, None


def describe_duplicate_match(match) -> str:
    return f"{match.rule}: {match.matched_value}"


def _resolve_batch_id(
    batch_id: str | None,
    *,
    created_at: datetime | None,
) -> str:
    normalized_batch_id = normalize_batch_id(batch_id)
    if normalized_batch_id is not None:
        return normalized_batch_id
    created_at_dt = _normalize_created_at(created_at)
    return f"import-{created_at_dt.strftime('%Y%m%dT%H%M%S%fZ')}"


def _normalize_created_at(created_at: datetime | None) -> datetime:
    if created_at is None:
        return datetime.now(timezone.utc)
    if created_at.tzinfo is None:
        return created_at.replace(tzinfo=timezone.utc)
    return created_at.astimezone(timezone.utc)
```

## Import field normalization helpers
Source: `src/jobs_ai/jobs/normalization.py` lines 1-43

```python
from __future__ import annotations

from collections.abc import Iterable, Mapping
import re

_COLLAPSE_WHITESPACE_FIELDS = frozenset(
    {
        "source",
        "company",
        "title",
        "location",
        "portal_type",
        "salary_text",
    }
)
_REPEATED_WHITESPACE_RE = re.compile(r"\s+")


def normalize_job_import_fields(
    record: Mapping[str, object],
    fields: Iterable[str],
) -> dict[str, str | None]:
    return {
        field: normalize_job_import_value(field, record.get(field))
        for field in fields
    }


def normalize_job_import_value(field: str, value: object) -> str | None:
    if not isinstance(value, str):
        return None

    text = value.strip()
    if not text:
        return None

    if field in _COLLAPSE_WHITESPACE_FIELDS:
        text = _REPEATED_WHITESPACE_RE.sub(" ", text)

    if field == "portal_type":
        text = text.lower()

    return text
```

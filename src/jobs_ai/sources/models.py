from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from ..collect.models import CollectRun, SourceResult
from ..jobs.importer import JobImportResult
from ..source_seed.models import SourceSeedRun

SourceRegistryStatus = Literal["active", "inactive", "manual_review"]
RegistrySeedOutcome = Literal["confirmed", "manual_review", "failed"]


@dataclass(frozen=True, slots=True)
class SourceRegistryEntry:
    source_id: int
    source_url: str
    normalized_url: str
    portal_type: str | None
    company: str | None
    label: str | None
    status: SourceRegistryStatus
    first_seen_at: str
    last_verified_at: str | None
    notes: str | None
    provenance: str | None
    verification_reason_code: str | None
    verification_reason: str | None
    created_at: str
    updated_at: str


@dataclass(frozen=True, slots=True)
class SourceRegistryMutationResult:
    action: str
    entry: SourceRegistryEntry
    source_result: SourceResult | None = None


@dataclass(frozen=True, slots=True)
class SourceRegistryImportResult:
    results: tuple[SourceRegistryMutationResult, ...]
    errors: tuple[str, ...]

    @property
    def created_count(self) -> int:
        return sum(1 for result in self.results if result.action == "created")

    @property
    def updated_count(self) -> int:
        return sum(1 for result in self.results if result.action == "updated")

    @property
    def unchanged_count(self) -> int:
        return sum(1 for result in self.results if result.action == "unchanged")

    @property
    def error_count(self) -> int:
        return len(self.errors)


@dataclass(frozen=True, slots=True)
class SourceRegistryVerificationResult:
    action: str
    before: SourceRegistryEntry
    after: SourceRegistryEntry
    source_result: SourceResult


@dataclass(frozen=True, slots=True)
class SourceRegistryCollectResult:
    selected_entries: tuple[SourceRegistryEntry, ...]
    verification_results: tuple[SourceRegistryVerificationResult, ...]
    collect_run: CollectRun
    import_result: JobImportResult | None
    import_requested: bool

    @property
    def selected_source_count(self) -> int:
        return len(self.selected_entries)

    @property
    def verified_source_count(self) -> int:
        return len(self.verification_results)


@dataclass(frozen=True, slots=True)
class SourceRegistryATSDiscoveryItemResult:
    slug: str
    portal_type: str
    source_url: str
    company: str | None
    status: SourceRegistryStatus
    reason_code: str
    reason: str
    lead_count: int
    mutation: SourceRegistryMutationResult | None = None

    @property
    def action(self) -> str | None:
        if self.mutation is None:
            return None
        return self.mutation.action


@dataclass(frozen=True, slots=True)
class SourceRegistryATSProviderCount:
    provider: str
    active: int
    manual_review: int
    ignored: int


@dataclass(frozen=True, slots=True)
class SourceRegistryATSDiscoveryResult:
    limit: int
    candidate_slug_count: int
    tested_slug_count: int
    item_results: tuple[SourceRegistryATSDiscoveryItemResult, ...]
    provider_counts: tuple[SourceRegistryATSProviderCount, ...]
    providers: tuple[str, ...]
    output_path: Path | None
    max_concurrency: int
    max_requests_per_second: float

    @property
    def active_item_results(self) -> tuple[SourceRegistryATSDiscoveryItemResult, ...]:
        return tuple(item for item in self.item_results if item.status == "active")

    @property
    def manual_review_item_results(self) -> tuple[SourceRegistryATSDiscoveryItemResult, ...]:
        return tuple(item for item in self.item_results if item.status == "manual_review")

    @property
    def greenhouse_count(self) -> int:
        return self._provider_active_count("greenhouse")

    @property
    def lever_count(self) -> int:
        return self._provider_active_count("lever")

    @property
    def ashby_count(self) -> int:
        return self._provider_active_count("ashby")

    @property
    def active_count(self) -> int:
        return sum(item.active for item in self.provider_counts)

    @property
    def manual_review_count(self) -> int:
        return sum(item.manual_review for item in self.provider_counts)

    @property
    def ignored_count(self) -> int:
        return sum(item.ignored for item in self.provider_counts)

    @property
    def created_count(self) -> int:
        return sum(
            1
            for item in self.item_results
            if item.action == "created"
        )

    @property
    def updated_count(self) -> int:
        return sum(
            1
            for item in self.item_results
            if item.action == "updated"
        )

    @property
    def unchanged_count(self) -> int:
        return sum(
            1
            for item in self.item_results
            if item.action == "unchanged"
        )

    @property
    def discovered_source_urls(self) -> tuple[str, ...]:
        return tuple(
            dict.fromkeys(item.source_url for item in self.active_item_results)
        )

    def provider_count(self, provider: str) -> SourceRegistryATSProviderCount:
        normalized_provider = provider.strip().lower()
        for item in self.provider_counts:
            if item.provider == normalized_provider:
                return item
        return SourceRegistryATSProviderCount(
            provider=normalized_provider,
            active=0,
            manual_review=0,
            ignored=0,
        )

    def _provider_active_count(self, provider: str) -> int:
        return self.provider_count(provider).active


@dataclass(frozen=True, slots=True)
class SourceRegistrySeedItemResult:
    raw_input: str
    outcome: RegistrySeedOutcome
    reason_code: str
    reason: str
    source_url: str | None = None
    portal_type: str | None = None
    mutation: SourceRegistryMutationResult | None = None


@dataclass(frozen=True, slots=True)
class SourceRegistrySeedBulkResult:
    seed_run: SourceSeedRun
    starter_lists: tuple[str, ...]
    item_results: tuple[SourceRegistrySeedItemResult, ...]

    @property
    def input_company_count(self) -> int:
        return self.seed_run.report.input_company_count

    @property
    def confirmed_count(self) -> int:
        return len(
            {
                item.mutation.entry.source_id
                for item in self.item_results
                if item.outcome == "confirmed" and item.mutation is not None
            }
        )

    @property
    def manual_review_count(self) -> int:
        return len(
            {
                item.mutation.entry.source_id
                for item in self.item_results
                if item.outcome == "manual_review" and item.mutation is not None
            }
        )

    @property
    def failed_count(self) -> int:
        return sum(1 for item in self.item_results if item.outcome == "failed")

    @property
    def mutation_results(self) -> tuple[SourceRegistryMutationResult, ...]:
        return tuple(
            item.mutation
            for item in self.item_results
            if item.mutation is not None
        )

    @property
    def created_count(self) -> int:
        return sum(1 for result in self.mutation_results if result.action == "created")

    @property
    def updated_count(self) -> int:
        return sum(1 for result in self.mutation_results if result.action == "updated")

    @property
    def unchanged_count(self) -> int:
        return sum(1 for result in self.mutation_results if result.action == "unchanged")


@dataclass(frozen=True, slots=True)
class SourceRegistrySiteDetectInputResult:
    raw_input: str
    resolved_start_url: str | None
    fetched_page_count: int
    outcome: RegistrySeedOutcome
    reason_code: str
    reason: str
    detected_source_urls: tuple[str, ...] = ()
    manual_review_source_urls: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class SourceRegistryDetectSitesResult:
    starter_lists: tuple[str, ...]
    input_results: tuple[SourceRegistrySiteDetectInputResult, ...]
    item_results: tuple[SourceRegistrySeedItemResult, ...]

    @property
    def input_count(self) -> int:
        return len(self.input_results)

    @property
    def confirmed_count(self) -> int:
        return len(
            {
                item.mutation.entry.source_id
                for item in self.item_results
                if item.outcome == "confirmed" and item.mutation is not None
            }
        )

    @property
    def manual_review_count(self) -> int:
        return len(
            {
                item.mutation.entry.source_id
                for item in self.item_results
                if item.outcome == "manual_review" and item.mutation is not None
            }
        )

    @property
    def failed_count(self) -> int:
        return sum(1 for item in self.input_results if item.outcome == "failed")

    @property
    def mutation_results(self) -> tuple[SourceRegistryMutationResult, ...]:
        return tuple(
            item.mutation
            for item in self.item_results
            if item.mutation is not None
        )

    @property
    def created_count(self) -> int:
        return sum(1 for result in self.mutation_results if result.action == "created")

    @property
    def updated_count(self) -> int:
        return sum(1 for result in self.mutation_results if result.action == "updated")

    @property
    def unchanged_count(self) -> int:
        return sum(1 for result in self.mutation_results if result.action == "unchanged")


@dataclass(frozen=True, slots=True)
class CompanyHarvestArtifactPaths:
    output_dir: Path
    harvested_domains_path: Path
    harvest_report_path: Path


@dataclass(frozen=True, slots=True)
class CompanyHarvestPageResult:
    source_name: str
    directory_url: str
    candidate_url_count: int
    harvested_domains: tuple[str, ...]
    error: str | None = None

    @property
    def harvested_domain_count(self) -> int:
        return len(self.harvested_domains)


@dataclass(frozen=True, slots=True)
class SourceRegistryHarvestCompaniesResult:
    source_names: tuple[str, ...]
    page_results: tuple[CompanyHarvestPageResult, ...]
    harvested_domains: tuple[str, ...]
    detect_sites_result: SourceRegistryDetectSitesResult
    timeout_seconds: float
    max_requests_per_second: float
    created_at: str
    finished_at: str
    run_id: str
    artifact_paths: CompanyHarvestArtifactPaths

    @property
    def directory_page_count(self) -> int:
        return len(self.page_results)

    @property
    def failed_page_count(self) -> int:
        return sum(1 for page_result in self.page_results if page_result.error is not None)

    @property
    def harvested_domain_count(self) -> int:
        return len(self.harvested_domains)


@dataclass(frozen=True, slots=True)
class SourceRegistryExpandResult:
    seed_result: SourceRegistrySeedBulkResult | None
    detect_sites_result: SourceRegistryDetectSitesResult | None
    discover_ats_result: SourceRegistryATSDiscoveryResult | None
    structured_clues_enabled: bool

    @property
    def mutation_results(self) -> tuple[SourceRegistryMutationResult, ...]:
        results: list[SourceRegistryMutationResult] = []
        if self.seed_result is not None:
            results.extend(self.seed_result.mutation_results)
        if self.detect_sites_result is not None:
            results.extend(self.detect_sites_result.mutation_results)
        if self.discover_ats_result is not None:
            results.extend(
                item.mutation
                for item in self.discover_ats_result.item_results
                if item.mutation is not None
            )
        return tuple(results)

    @property
    def created_count(self) -> int:
        return sum(1 for result in self.mutation_results if result.action == "created")

    @property
    def updated_count(self) -> int:
        return sum(1 for result in self.mutation_results if result.action == "updated")

    @property
    def unchanged_count(self) -> int:
        return sum(1 for result in self.mutation_results if result.action == "unchanged")

    @property
    def active_source_count(self) -> int:
        return len(
            {
                result.entry.source_id
                for result in self.mutation_results
                if result.entry.status == "active"
            }
        )

    @property
    def manual_review_source_count(self) -> int:
        return len(
            {
                result.entry.source_id
                for result in self.mutation_results
                if result.entry.status == "manual_review"
            }
        )

    @property
    def failed_input_count(self) -> int:
        failed_count = 0
        if self.seed_result is not None:
            failed_count += self.seed_result.failed_count
        if self.detect_sites_result is not None:
            failed_count += self.detect_sites_result.failed_count
        return failed_count

    @property
    def ignored_probe_count(self) -> int:
        if self.discover_ats_result is None:
            return 0
        return self.discover_ats_result.ignored_count

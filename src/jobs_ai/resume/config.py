from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ResumeVariant:
    key: str
    label: str
    summary: str


@dataclass(frozen=True, slots=True)
class ProfileSnippet:
    key: str
    label: str
    text: str


RESUME_VARIANTS: dict[str, ResumeVariant] = {
    "general-data": ResumeVariant(
        key="general-data",
        label="General Data Resume",
        summary="Broad data-platform baseline for mixed technical roles.",
    ),
    "data-engineering": ResumeVariant(
        key="data-engineering",
        label="Data Engineering Resume",
        summary="Python, warehouse, and pipeline-heavy resume focus.",
    ),
    "analytics-engineering": ResumeVariant(
        key="analytics-engineering",
        label="Analytics Engineering Resume",
        summary="SQL, semantic layer, and BI-oriented resume focus.",
    ),
    "telemetry-observability": ResumeVariant(
        key="telemetry-observability",
        label="Telemetry / Observability Resume",
        summary="Signals, instrumentation, and platform reliability resume focus.",
    ),
}


PROFILE_SNIPPETS: dict[str, ProfileSnippet] = {
    "general-data-platform": ProfileSnippet(
        key="general-data-platform",
        label="General Data Platform",
        text="Hands-on data platform work spanning ingestion, transformation, and production support.",
    ),
    "pipeline-delivery": ProfileSnippet(
        key="pipeline-delivery",
        label="Pipeline Delivery",
        text="Python-first pipeline delivery across SQL warehouses, BigQuery/GCP, and production data systems.",
    ),
    "analytics-modeling": ProfileSnippet(
        key="analytics-modeling",
        label="Analytics Modeling",
        text="Analytics engineering work centered on SQL modeling, semantic layers, and Looker-facing delivery.",
    ),
    "observability-signals": ProfileSnippet(
        key="observability-signals",
        label="Observability Signals",
        text="Telemetry and observability work across logs, metrics, traces, and instrumentation pipelines.",
    ),
}


def get_resume_variant(key: str) -> ResumeVariant:
    return RESUME_VARIANTS[key]


def get_profile_snippet(key: str) -> ProfileSnippet:
    return PROFILE_SNIPPETS[key]

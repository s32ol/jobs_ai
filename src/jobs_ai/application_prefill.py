from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from .applicant_profile import (
    ApplicantProfile,
    LoadedApplicantProfile,
    load_applicant_profile,
    resolve_applicant_resume_variant,
)
from .application_assist import (
    ApplicationAssistEntry,
    build_application_assist,
    select_application_assist_entry,
)
from .launch_plan import build_launch_plan
from .portal_support import PortalSupport, build_portal_support
from .prefill_browser import PrefillBrowserBackend
from .prefill_portals import (
    PortalFieldSpec,
    PortalPrefillAdapter,
    field_display_name,
    field_lookup_keys,
    find_unique_field,
    normalized_canned_answers,
    option_value_for_answer,
    select_portal_prefill_adapter,
)
from .session_manifest import ManifestSelection, load_session_manifest


@dataclass(frozen=True, slots=True)
class PrefillAction:
    field_key: str
    field_label: str
    selector: str
    action_type: str
    value: str


@dataclass(frozen=True, slots=True)
class PrefillSkippedField:
    field_key: str
    field_label: str
    reason: str


@dataclass(frozen=True, slots=True)
class ApplicationPrefillResult:
    manifest_path: Path
    applicant_profile_path: Path
    launch_order: int
    company: str | None
    title: str | None
    original_apply_url: str
    opened_url: str
    page_title: str | None
    portal_type: str | None
    portal_label: str
    support_level: str
    browser_backend: str
    recommended_resume_variant: ManifestSelection
    recommended_profile_snippet: ManifestSelection
    resolved_resume_path: Path | None
    filled_fields: tuple[PrefillAction, ...]
    skipped_fields: tuple[PrefillSkippedField, ...]
    unresolved_required_fields: tuple[str, ...]
    submit_controls: tuple[str, ...]
    stopped_before_submit: bool
    status: str
    notes: tuple[str, ...]


def run_application_prefill(
    manifest_path: Path,
    *,
    project_root: Path,
    applicant_profile_path: Path | None,
    launch_order: int | None,
    browser_backend: PrefillBrowserBackend,
    env: Mapping[str, str] | None = None,
) -> ApplicationPrefillResult:
    manifest = load_session_manifest(manifest_path)
    assist = build_application_assist(build_launch_plan(manifest))
    entry = select_application_assist_entry(assist, launch_order=launch_order)
    loaded_profile = load_applicant_profile(
        applicant_profile_path,
        project_root=project_root,
        env=env,
    )

    portal_support = build_portal_support(entry.apply_url, portal_type=entry.portal_type)
    opened_url = (
        portal_support.company_apply_url
        if portal_support is not None and portal_support.company_apply_url is not None
        else portal_support.normalized_apply_url
        if portal_support is not None
        else entry.apply_url
    )
    portal_type = (
        portal_support.portal_type
        if portal_support is not None
        else entry.portal_type
    )
    portal_adapter = select_portal_prefill_adapter(portal_type)

    browser_backend.open_url(opened_url)
    initial_snapshot = browser_backend.snapshot()
    filled_fields: list[PrefillAction] = []
    skipped_fields: list[PrefillSkippedField] = []
    notes: list[str] = []

    resolved_resume = resolve_applicant_resume_variant(
        loaded_profile,
        resume_variant_key=entry.recommended_resume_variant.key,
        project_root=project_root,
        env=env,
    )

    if portal_adapter is None:
        notes.append("No supported portal prefill adapter matched this application page.")
    elif portal_adapter.support_level != "supported":
        notes.append(
            f"{portal_adapter.portal_label} stays manual-review only in Phase 2; no fields were auto-filled."
        )
    else:
        fill_result = _fill_supported_portal_fields(
            entry,
            loaded_profile=loaded_profile,
            portal_adapter=portal_adapter,
            browser_backend=browser_backend,
            resolved_resume_path=resolved_resume.resolved_path,
            snapshot=initial_snapshot,
        )
        filled_fields.extend(fill_result.filled_fields)
        skipped_fields.extend(fill_result.skipped_fields)
        notes.extend(fill_result.notes)

    if resolved_resume.fallback_reason is not None:
        skipped_fields.append(
            PrefillSkippedField(
                field_key="resume",
                field_label="Resume",
                reason=resolved_resume.fallback_reason,
            )
        )

    final_snapshot = browser_backend.snapshot()
    unresolved_required_fields = tuple(
        field_display_name(field)
        for field in final_snapshot.fields
        if field.required and field.visible and not (field.current_value or "").strip()
    )

    support_level = portal_adapter.support_level if portal_adapter is not None else "unsupported"
    portal_label = portal_adapter.portal_label if portal_adapter is not None else "Unsupported / Unknown"

    status = _result_status(
        support_level=support_level,
        filled_count=len(filled_fields),
        unresolved_required_fields=unresolved_required_fields,
    )
    if unresolved_required_fields:
        notes.append(
            f"{len(unresolved_required_fields)} required field(s) remain unresolved for manual review."
        )

    return ApplicationPrefillResult(
        manifest_path=manifest_path,
        applicant_profile_path=loaded_profile.profile_path,
        launch_order=entry.launch_order,
        company=entry.company,
        title=entry.title,
        original_apply_url=entry.apply_url,
        opened_url=final_snapshot.url or opened_url,
        page_title=final_snapshot.title,
        portal_type=portal_type,
        portal_label=portal_label,
        support_level=support_level,
        browser_backend=browser_backend.backend_name,
        recommended_resume_variant=entry.recommended_resume_variant,
        recommended_profile_snippet=entry.recommended_profile_snippet,
        resolved_resume_path=resolved_resume.resolved_path,
        filled_fields=tuple(filled_fields),
        skipped_fields=tuple(skipped_fields),
        unresolved_required_fields=unresolved_required_fields,
        submit_controls=final_snapshot.submit_controls,
        stopped_before_submit=True,
        status=status,
        notes=tuple(notes),
    )


@dataclass(frozen=True, slots=True)
class _FillResult:
    filled_fields: tuple[PrefillAction, ...]
    skipped_fields: tuple[PrefillSkippedField, ...]
    notes: tuple[str, ...]


def _fill_supported_portal_fields(
    entry: ApplicationAssistEntry,
    *,
    loaded_profile: LoadedApplicantProfile,
    portal_adapter: PortalPrefillAdapter,
    browser_backend: PrefillBrowserBackend,
    resolved_resume_path: Path | None,
    snapshot,
) -> _FillResult:
    used_selectors: set[str] = set()
    filled_fields: list[PrefillAction] = []
    skipped_fields: list[PrefillSkippedField] = []
    notes: list[str] = []
    canned_answers = normalized_canned_answers(loaded_profile.profile.canned_answers)

    for spec in portal_adapter.safe_fields:
        if spec.field_key == "resume":
            result = _handle_resume_upload(
                spec,
                browser_backend=browser_backend,
                resolved_resume_path=resolved_resume_path,
                snapshot=snapshot,
                used_selectors=used_selectors,
            )
        else:
            result = _handle_profile_field(
                spec,
                profile=loaded_profile.profile,
                browser_backend=browser_backend,
                snapshot=snapshot,
                used_selectors=used_selectors,
            )
        if isinstance(result, PrefillAction):
            filled_fields.append(result)
        elif isinstance(result, PrefillSkippedField):
            skipped_fields.append(result)

    short_text_value = _select_short_text(
        loaded_profile.profile,
        recommended_profile_snippet=entry.recommended_profile_snippet,
    )
    if short_text_value is not None:
        field, reason = find_unique_field(
            snapshot,
            aliases=portal_adapter.short_text_aliases,
            control_types=("textarea", "text"),
            used_selectors=used_selectors,
        )
        if field is None:
            skipped_fields.append(
                PrefillSkippedField(
                    field_key="short_text",
                    field_label="Short text / cover letter",
                    reason=reason or "field not found",
                )
            )
        else:
            browser_backend.fill_text(field.selector, short_text_value)
            used_selectors.add(field.selector)
            filled_fields.append(
                PrefillAction(
                    field_key="short_text",
                    field_label=field_display_name(field),
                    selector=field.selector,
                    action_type="fill_text",
                    value=short_text_value,
                )
            )

    for field in snapshot.fields:
        if field.selector in used_selectors:
            continue
        matched_answer = _matched_canned_answer(field, canned_answers)
        if matched_answer is None:
            continue
        result = _fill_field_with_answer(
            field_key="canned_answer",
            field_label=field_display_name(field),
            answer=matched_answer,
            field=field,
            browser_backend=browser_backend,
        )
        if isinstance(result, PrefillAction):
            used_selectors.add(field.selector)
            filled_fields.append(result)
        elif isinstance(result, PrefillSkippedField):
            skipped_fields.append(result)

    if portal_adapter.portal_type == "ashby":
        notes.append("Ashby support is limited to single-page visible fields in Phase 2.")

    return _FillResult(
        filled_fields=tuple(filled_fields),
        skipped_fields=tuple(skipped_fields),
        notes=tuple(notes),
    )


def _handle_profile_field(
    spec: PortalFieldSpec,
    *,
    profile: ApplicantProfile,
    browser_backend: PrefillBrowserBackend,
    snapshot,
    used_selectors: set[str],
) -> PrefillAction | PrefillSkippedField | None:
    value = _profile_value_for_field(profile, spec.field_key)
    if value is None:
        return None
    field, reason = find_unique_field(
        snapshot,
        aliases=spec.aliases,
        control_types=spec.control_types,
        used_selectors=used_selectors,
    )
    if field is None:
        return PrefillSkippedField(
            field_key=spec.field_key,
            field_label=_field_label_from_spec(spec),
            reason=reason or "field not found",
        )
    result = _fill_field_with_answer(
        field_key=spec.field_key,
        field_label=field_display_name(field),
        answer=value,
        field=field,
        browser_backend=browser_backend,
    )
    if isinstance(result, PrefillAction):
        used_selectors.add(field.selector)
    return result


def _handle_resume_upload(
    spec: PortalFieldSpec,
    *,
    browser_backend: PrefillBrowserBackend,
    resolved_resume_path: Path | None,
    snapshot,
    used_selectors: set[str],
) -> PrefillAction | PrefillSkippedField | None:
    if resolved_resume_path is None:
        return None
    field, reason = find_unique_field(
        snapshot,
        aliases=spec.aliases,
        control_types=spec.control_types,
        include_hidden=True,
        used_selectors=used_selectors,
    )
    if field is None:
        return PrefillSkippedField(
            field_key="resume",
            field_label="Resume",
            reason=reason or "file input not found",
        )
    browser_backend.upload_file(field.selector, resolved_resume_path)
    used_selectors.add(field.selector)
    return PrefillAction(
        field_key="resume",
        field_label=field_display_name(field),
        selector=field.selector,
        action_type="upload_file",
        value=str(resolved_resume_path),
    )


def _fill_field_with_answer(
    *,
    field_key: str,
    field_label: str,
    answer: str | bool,
    field,
    browser_backend: PrefillBrowserBackend,
) -> PrefillAction | PrefillSkippedField:
    if field.control_type == "select":
        option_value = _option_value_for_field_answer(field_key, field, answer)
        if option_value is None:
            return PrefillSkippedField(
                field_key=field_key,
                field_label=field_label,
                reason="no matching select option for configured answer",
            )
        browser_backend.select_option(field.selector, option_value)
        return PrefillAction(
            field_key=field_key,
            field_label=field_label,
            selector=field.selector,
            action_type="select_option",
            value=option_value,
        )

    text_value = _text_value_for_answer(answer)
    if text_value is None:
        return PrefillSkippedField(
            field_key=field_key,
            field_label=field_label,
            reason="configured answer was blank",
        )
    browser_backend.fill_text(field.selector, text_value)
    return PrefillAction(
        field_key=field_key,
        field_label=field_label,
        selector=field.selector,
        action_type="fill_text",
        value=text_value,
    )


def _option_value_for_field_answer(
    field_key: str,
    field,
    answer: str | bool,
) -> str | None:
    for candidate in _answer_candidates(field_key, answer):
        option_value = option_value_for_answer(field, candidate)
        if option_value is not None:
            return option_value
    return None


def _answer_candidates(field_key: str, answer: str | bool) -> tuple[str, ...]:
    if isinstance(answer, bool):
        if field_key == "authorized_to_work_in_us":
            return (
                "Yes" if answer else "No",
                "Authorized" if answer else "Not authorized",
                "I am authorized to work in the United States"
                if answer
                else "I am not authorized to work in the United States",
            )
        if field_key == "requires_sponsorship":
            return (
                "Yes" if answer else "No",
                "Requires sponsorship" if answer else "Does not require sponsorship",
                "Will require sponsorship"
                if answer
                else "Will not require sponsorship",
            )
        return ("Yes",) if answer else ("No",)
    return (answer,)


def _text_value_for_answer(answer: str | bool) -> str | None:
    if isinstance(answer, bool):
        return "Yes" if answer else "No"
    text = answer.strip()
    return text or None


def _profile_value_for_field(profile: ApplicantProfile, field_key: str) -> str | bool | None:
    values: dict[str, str | bool | None] = {
        "full_name": profile.full_name,
        "first_name": profile.resolved_first_name,
        "last_name": profile.resolved_last_name,
        "email": profile.email,
        "phone": profile.phone,
        "location": profile.location,
        "linkedin_url": profile.linkedin_url,
        "github_url": profile.github_url,
        "portfolio_url": profile.portfolio_url,
        "work_authorization": profile.work_authorization,
        "authorized_to_work_in_us": profile.authorized_to_work_in_us,
        "requires_sponsorship": profile.requires_sponsorship,
    }
    return values.get(field_key)


def _select_short_text(
    profile: ApplicantProfile,
    *,
    recommended_profile_snippet: ManifestSelection,
) -> str | None:
    if profile.short_text is not None and profile.short_text.strip():
        return profile.short_text.strip()
    if (
        profile.use_recommended_profile_snippet
        and recommended_profile_snippet.text is not None
        and recommended_profile_snippet.text.strip()
    ):
        return recommended_profile_snippet.text.strip()
    return None


def _matched_canned_answer(field, answers: Mapping[str, str]) -> str | None:
    for lookup_key in field_lookup_keys(field):
        answer = answers.get(lookup_key)
        if answer is not None:
            return answer
    return None


def _result_status(
    *,
    support_level: str,
    filled_count: int,
    unresolved_required_fields: tuple[str, ...],
) -> str:
    if support_level != "supported":
        return "manual_handoff"
    if unresolved_required_fields:
        return "partial"
    if filled_count == 0:
        return "manual_handoff"
    return "success"


def _field_label_from_spec(spec: PortalFieldSpec) -> str:
    if spec.aliases:
        return spec.aliases[0].title()
    return spec.field_key

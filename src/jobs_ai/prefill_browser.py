from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
import sys
from typing import Protocol, runtime_checkable

from .autofill.profile_config import (
    BROWSER_PROFILE_DIRECTORY_ENV_VAR,
    LocalPlaywrightProfileConfig,
    resolve_local_playwright_profile_config,
)

SUPPORTED_PREFILL_BROWSER_BACKENDS = ("playwright",)


@dataclass(frozen=True, slots=True)
class BrowserFieldOption:
    label: str
    value: str


@dataclass(frozen=True, slots=True)
class BrowserFieldSnapshot:
    selector: str
    control_type: str
    label: str | None
    name: str | None
    placeholder: str | None
    required: bool
    visible: bool
    current_value: str | None = None
    options: tuple[BrowserFieldOption, ...] = ()


@dataclass(frozen=True, slots=True)
class BrowserPageSnapshot:
    url: str
    title: str | None
    fields: tuple[BrowserFieldSnapshot, ...]
    submit_controls: tuple[str, ...]


@runtime_checkable
class PrefillBrowserBackend(Protocol):
    backend_name: str

    def open_url(self, url: str) -> None:
        """Open one application URL."""

    def snapshot(self) -> BrowserPageSnapshot:
        """Return the current form state."""

    def fill_text(self, selector: str, value: str) -> None:
        """Fill a text-like field."""

    def select_option(self, selector: str, value: str) -> None:
        """Select an option by value or label."""

    def upload_file(self, selector: str, file_path: Path) -> None:
        """Attach a file to a file input."""

    def close(self) -> None:
        """Release browser resources."""


class FixturePrefillBrowserBackend:
    backend_name = "fixture"

    def __init__(self, pages_by_url: Mapping[str, BrowserPageSnapshot]) -> None:
        self._pages_by_url = {
            url: page
            for url, page in pages_by_url.items()
        }
        self._current_page: BrowserPageSnapshot | None = None

    def open_url(self, url: str) -> None:
        if url not in self._pages_by_url:
            raise ValueError(f"fixture browser has no page for {url}")
        self._current_page = self._pages_by_url[url]

    def snapshot(self) -> BrowserPageSnapshot:
        if self._current_page is None:
            raise ValueError("fixture browser has no open page")
        return self._current_page

    def fill_text(self, selector: str, value: str) -> None:
        self._mutate_field(selector, current_value=value)

    def select_option(self, selector: str, value: str) -> None:
        page = self.snapshot()
        field = _field_by_selector(page.fields, selector)
        normalized_value = value.strip()
        option_match = next(
            (
                option
                for option in field.options
                if option.value == normalized_value or option.label == normalized_value
            ),
            None,
        )
        if option_match is None:
            raise ValueError(f"fixture option {value!r} was not found for {selector}")
        self._mutate_field(selector, current_value=option_match.value)

    def upload_file(self, selector: str, file_path: Path) -> None:
        self._mutate_field(selector, current_value=file_path.name)

    def close(self) -> None:
        return None

    def _mutate_field(self, selector: str, *, current_value: str) -> None:
        page = self.snapshot()
        updated_fields = []
        for field in page.fields:
            if field.selector == selector:
                updated_fields.append(replace(field, current_value=current_value))
            else:
                updated_fields.append(field)
        self._current_page = replace(page, fields=tuple(updated_fields))


class PlaywrightPrefillBrowserBackend:
    backend_name = "playwright"

    def __init__(
        self,
        *,
        headless: bool = False,
        profile_config: LocalPlaywrightProfileConfig | None = None,
        sync_playwright_factory=None,
    ) -> None:
        if sync_playwright_factory is None:
            try:
                from playwright.sync_api import sync_playwright
            except ImportError as exc:
                raise ValueError(
                    "Playwright is not installed. Install it in the project environment to use "
                    "browser-fill mode."
                ) from exc
            sync_playwright_factory = sync_playwright

        self._sync_playwright = sync_playwright_factory
        self._playwright_context = None
        self._browser = None
        self._browser_context = None
        self._page = None
        self._headless = headless
        self._profile_config = profile_config

    def open_url(self, url: str) -> None:
        if self._page is None:
            try:
                self._playwright_context = self._sync_playwright().start()
                if self._profile_config is None:
                    self._browser = self._playwright_context.chromium.launch(headless=self._headless)
                    self._page = self._browser.new_page()
                else:
                    self._browser_context = self._launch_persistent_context_with_fallback()
                    pages = tuple(self._browser_context.pages)
                    self._page = pages[0] if pages else self._browser_context.new_page()
            except Exception as exc:
                self.close()
                if self._profile_config is not None:
                    if self._profile_config.profile_directory is None:
                        profile_hint = (
                            "Close other Chrome windows using that user data dir, or set "
                            f"{BROWSER_PROFILE_DIRECTORY_ENV_VAR} to an existing profile inside "
                            f"{self._profile_config.user_data_dir}. "
                        )
                    else:
                        profile_hint = (
                            "Close other Chrome windows using "
                            f"{self._profile_config.profile_directory!r}, or point "
                            f"{BROWSER_PROFILE_DIRECTORY_ENV_VAR} at a separate profile. "
                        )
                    raise ValueError(
                        "Playwright could not launch the configured local Chrome profile for "
                        f"application-assist --prefill. {profile_hint}"
                        f"Underlying error: {exc}"
                    ) from exc
                raise ValueError(f"Playwright could not launch the browser backend: {exc}") from exc
        assert self._page is not None
        self._page.goto(url, wait_until="domcontentloaded")
        self._settle_page_after_navigation()

    def _settle_page_after_navigation(self) -> None:
        if self._page is None:
            return
        try:
            self._page.wait_for_load_state("networkidle", timeout=1000)
        except Exception:
            pass
        # Modern hosted application pages often hydrate after domcontentloaded.
        # Give the client app one more beat so controlled inputs are stable
        # before snapshotting and filling.
        self._page.wait_for_timeout(250)

    def _launch_persistent_context_with_fallback(self):
        assert self._playwright_context is not None
        assert self._profile_config is not None
        launch_error: Exception | None = None
        for profile_config in _persistent_launch_attempts(self._profile_config):
            try:
                launch_kwargs = self._persistent_launch_kwargs(profile_config)
                return self._playwright_context.chromium.launch_persistent_context(
                    str(_persistent_launch_user_data_dir(profile_config)),
                    **launch_kwargs,
                )
            except Exception as exc:
                launch_error = exc
        assert launch_error is not None
        raise launch_error

    def _persistent_launch_kwargs(
        self,
        profile_config: LocalPlaywrightProfileConfig,
    ) -> dict[str, object]:
        kwargs: dict[str, object] = {
            "headless": self._headless,
        }
        if profile_config.channel is not None:
            kwargs["channel"] = profile_config.channel
        if profile_config.launch_args:
            kwargs["args"] = list(profile_config.launch_args)
        if _persistent_uses_native_window(profile_config):
            kwargs["no_viewport"] = True
        return kwargs

    def snapshot(self) -> BrowserPageSnapshot:
        if self._page is None:
            raise ValueError("no browser page is open")
        result = self._page.evaluate(_PLAYWRIGHT_SNAPSHOT_SCRIPT)
        fields = tuple(
            BrowserFieldSnapshot(
                selector=str(field["selector"]),
                control_type=str(field["control_type"]),
                label=_optional_string(field.get("label")),
                name=_optional_string(field.get("name")),
                placeholder=_optional_string(field.get("placeholder")),
                required=bool(field.get("required")),
                visible=bool(field.get("visible")),
                current_value=_optional_string(field.get("current_value")),
                options=tuple(
                    BrowserFieldOption(
                        label=str(option.get("label") or ""),
                        value=str(option.get("value") or ""),
                    )
                    for option in field.get("options", [])
                    if isinstance(option, dict)
                ),
            )
            for field in result.get("fields", [])
            if isinstance(field, dict)
        )
        submit_controls = tuple(
            label
            for label in (
                _optional_string(value)
                for value in result.get("submit_controls", [])
            )
            if label is not None
        )
        return BrowserPageSnapshot(
            url=str(result.get("url") or self._page.url),
            title=_optional_string(result.get("title")) or self._page.title(),
            fields=fields,
            submit_controls=submit_controls,
        )

    def fill_text(self, selector: str, value: str) -> None:
        if self._page is None:
            raise ValueError("no browser page is open")
        self._page.locator(selector).fill(value)

    def select_option(self, selector: str, value: str) -> None:
        if self._page is None:
            raise ValueError("no browser page is open")
        locator = self._page.locator(selector)
        try:
            locator.select_option(label=value)
        except Exception:
            locator.select_option(value=value)

    def upload_file(self, selector: str, file_path: Path) -> None:
        if self._page is None:
            raise ValueError("no browser page is open")
        self._page.locator(selector).set_input_files(str(file_path))

    def close(self) -> None:
        if self._browser_context is not None:
            self._browser_context.close()
            self._browser_context = None
        if self._browser is not None:
            self._browser.close()
            self._browser = None
        if self._playwright_context is not None:
            self._playwright_context.stop()
            self._playwright_context = None
        self._page = None


def create_prefill_browser_backend(
    backend_name: str,
    *,
    env: Mapping[str, str] | None = None,
) -> PrefillBrowserBackend:
    if backend_name == "playwright":
        return PlaywrightPrefillBrowserBackend(
            profile_config=resolve_local_playwright_profile_config(env),
        )
    supported = ", ".join(SUPPORTED_PREFILL_BROWSER_BACKENDS)
    raise ValueError(f"unsupported prefill browser backend: {backend_name}; expected one of: {supported}")


def _field_by_selector(
    fields: Sequence[BrowserFieldSnapshot],
    selector: str,
) -> BrowserFieldSnapshot:
    for field in fields:
        if field.selector == selector:
            return field
    raise ValueError(f"field selector was not found in snapshot: {selector}")


def _optional_string(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    return text or None


def _persistent_uses_native_window(
    profile_config: LocalPlaywrightProfileConfig,
) -> bool:
    # Playwright's Chromium startup only calls Browser.getWindowForTarget when
    # it is managing the default viewport. For the local macOS dedicated
    # user-data-dir flow, use the native Chrome window instead so this
    # review-first browser path stays closer to a normal visible local launch.
    if (
        sys.platform == "darwin"
        and profile_config.profile_directory is None
    ):
        return True
    return False


def _persistent_launch_user_data_dir(
    profile_config: LocalPlaywrightProfileConfig,
) -> Path:
    user_data_dir = profile_config.user_data_dir
    if not _persistent_uses_dedicated_chromium_profile_dir(profile_config):
        return user_data_dir
    profile_name = user_data_dir.name
    lowered_name = profile_name.lower()
    if "chromium" in lowered_name:
        return user_data_dir
    if "chrome" in lowered_name:
        start = lowered_name.index("chrome")
        chromium_name = f"{profile_name[:start]}chromium{profile_name[start + len('chrome'):]}"
    else:
        chromium_name = f"{profile_name}-chromium"
    return user_data_dir.with_name(chromium_name)


def _persistent_launch_attempts(
    profile_config: LocalPlaywrightProfileConfig,
) -> tuple[LocalPlaywrightProfileConfig, ...]:
    fallback = _fallback_profile_config(profile_config)
    if fallback is None:
        return (profile_config,)
    return (profile_config, fallback)


def _fallback_profile_config(
    profile_config: LocalPlaywrightProfileConfig,
) -> LocalPlaywrightProfileConfig | None:
    if (
        sys.platform == "darwin"
        and profile_config.profile_directory is None
        and profile_config.channel == "chrome"
    ):
        return LocalPlaywrightProfileConfig(
            channel=None,
            user_data_dir=profile_config.user_data_dir,
            profile_directory=profile_config.profile_directory,
        )
    return None


def _persistent_uses_dedicated_chromium_profile_dir(
    profile_config: LocalPlaywrightProfileConfig,
) -> bool:
    return (
        sys.platform == "darwin"
        and profile_config.profile_directory is None
        and profile_config.channel is None
    )


_PLAYWRIGHT_SNAPSHOT_SCRIPT = """
() => {
  const ensureSelector = (element, index) => {
    const existing = element.getAttribute("data-jobs-ai-selector");
    if (existing) {
      return `[data-jobs-ai-selector="${existing}"]`;
    }
    const value = `jobs-ai-${index}`;
    element.setAttribute("data-jobs-ai-selector", value);
    return `[data-jobs-ai-selector="${value}"]`;
  };

  const text = (value) => {
    if (typeof value !== "string") {
      return null;
    }
    const normalized = value.replace(/\\s+/g, " ").trim();
    return normalized || null;
  };

  const textFromIds = (value) => {
    const ids = text(value);
    if (!ids) {
      return null;
    }
    for (const id of ids.split(/\\s+/).filter(Boolean)) {
      const labelledElement = document.getElementById(id);
      if (!labelledElement) {
        continue;
      }
      const labelledText = text(labelledElement.innerText || labelledElement.textContent || "");
      if (labelledText) {
        return labelledText;
      }
    }
    return null;
  };

  const labelFor = (element) => {
    if ((element.getAttribute("type") || "").toLowerCase() === "file") {
      const uploadGroup = element.closest('[role="group"][aria-labelledby]');
      const uploadLabel = uploadGroup ? textFromIds(uploadGroup.getAttribute("aria-labelledby")) : null;
      if (uploadLabel) {
        return uploadLabel;
      }
    }
    const labels = element.labels ? Array.from(element.labels) : [];
    for (const label of labels) {
      const value = text(label.innerText || label.textContent || "");
      if (value) {
        return value;
      }
    }
    const ariaLabel = text(element.getAttribute("aria-label") || "");
    if (ariaLabel) {
      return ariaLabel;
    }
    const labelledBy = textFromIds(element.getAttribute("aria-labelledby") || "");
    if (labelledBy) {
      return labelledBy;
    }
    const ancestorLabelledBy = element.parentElement?.closest("[aria-labelledby]");
    if (ancestorLabelledBy) {
      const value = textFromIds(ancestorLabelledBy.getAttribute("aria-labelledby") || "");
      if (value) {
        return value;
      }
    }
    const fieldset = element.closest("fieldset");
    if (fieldset) {
      const legend = fieldset.querySelector("legend");
      const legendText = legend ? text(legend.innerText || legend.textContent || "") : null;
      if (legendText) {
        return legendText;
      }
    }
    return null;
  };

  const controlType = (element) => {
    const tag = element.tagName.toLowerCase();
    if (tag === "textarea" || tag === "select") {
      return tag;
    }
    if (tag !== "input") {
      return tag;
    }
    return (element.getAttribute("type") || "text").toLowerCase();
  };

  const isVisible = (element) => {
    const style = window.getComputedStyle(element);
    if (
      style.display === "none"
      || style.visibility === "hidden"
      || style.opacity === "0"
      || element.hidden
      || element.getAttribute("aria-hidden") === "true"
    ) {
      return false;
    }
    const rects = element.getClientRects();
    if (!rects.length) {
      return false;
    }
    return Array.from(rects).some((rect) => rect.width > 0 || rect.height > 0);
  };

  const fields = Array.from(document.querySelectorAll("input, textarea, select")).map((element, index) => {
    const tag = element.tagName.toLowerCase();
    const type = controlType(element);
    const options = tag === "select"
      ? Array.from(element.options || []).map((option) => ({
          label: text(option.label || option.textContent || "") || "",
          value: text(option.value || "") || "",
        }))
      : [];
    return {
      selector: ensureSelector(element, index + 1),
      control_type: type,
      label: labelFor(element),
      name: text(element.getAttribute("name") || element.getAttribute("id") || ""),
      placeholder: text(element.getAttribute("placeholder") || ""),
      required: Boolean(element.required || element.getAttribute("aria-required") === "true"),
      visible: isVisible(element) || type === "file",
      current_value: type === "file" ? text(element.value || "") : text(element.value || ""),
      options,
    };
  });

  const submitControls = Array.from(
    document.querySelectorAll('button[type="submit"], input[type="submit"]')
  ).map((element) => text(element.innerText || element.value || element.textContent || "")).filter(Boolean);

  return {
    url: window.location.href,
    title: document.title,
    fields,
    submit_controls: submitControls,
  };
}
"""

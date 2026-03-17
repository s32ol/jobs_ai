from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Protocol, runtime_checkable

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

    def __init__(self, *, headless: bool = False) -> None:
        try:
            from playwright.sync_api import sync_playwright
        except ImportError as exc:
            raise ValueError(
                "Playwright is not installed. Install it in the project environment to use "
                "browser-fill mode."
            ) from exc

        self._sync_playwright = sync_playwright
        self._playwright_context = None
        self._browser = None
        self._page = None
        self._headless = headless

    def open_url(self, url: str) -> None:
        if self._page is None:
            self._playwright_context = self._sync_playwright().start()
            self._browser = self._playwright_context.chromium.launch(headless=self._headless)
            self._page = self._browser.new_page()
        assert self._page is not None
        self._page.goto(url, wait_until="domcontentloaded")

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
        if self._browser is not None:
            self._browser.close()
            self._browser = None
        if self._playwright_context is not None:
            self._playwright_context.stop()
            self._playwright_context = None
        self._page = None


def create_prefill_browser_backend(
    backend_name: str,
) -> PrefillBrowserBackend:
    if backend_name == "playwright":
        return PlaywrightPrefillBrowserBackend()
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

  const labelFor = (element) => {
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
    const labelledBy = text(element.getAttribute("aria-labelledby") || "");
    if (labelledBy) {
      const ids = labelledBy.split(/\\s+/).filter(Boolean);
      for (const id of ids) {
        const labelledElement = document.getElementById(id);
        if (!labelledElement) {
          continue;
        }
        const value = text(labelledElement.innerText || labelledElement.textContent || "");
        if (value) {
          return value;
        }
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
    return style.display !== "none" && style.visibility !== "hidden";
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
      name: text(element.getAttribute("name") || ""),
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

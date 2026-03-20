from __future__ import annotations

from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

SRC_PATH = Path(__file__).resolve().parents[1] / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from jobs_ai.autofill.profile_config import LocalPlaywrightProfileConfig
from jobs_ai.prefill_browser import PlaywrightPrefillBrowserBackend


class PlaywrightPrefillBrowserBackendTest(unittest.TestCase):
    def test_open_url_uses_persistent_context_when_profile_config_is_present(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            user_data_dir = Path(tmp_dir) / "Chrome"
            profile_path = user_data_dir / "Profile 2"
            profile_path.mkdir(parents=True, exist_ok=True)
            chromium = _FakeChromium()
            backend = PlaywrightPrefillBrowserBackend(
                profile_config=LocalPlaywrightProfileConfig(
                    channel="chrome",
                    user_data_dir=user_data_dir,
                    profile_directory="Profile 2",
                ),
                sync_playwright_factory=lambda: _FakeSyncPlaywright(chromium),
            )

            backend.open_url("https://example.com/jobs/123")

        self.assertEqual(chromium.launch_calls, [])
        self.assertEqual(
            chromium.persistent_launch_calls,
            [
                {
                    "user_data_dir": str(user_data_dir),
                    "headless": False,
                    "channel": "chrome",
                    "args": ["--profile-directory=Profile 2"],
                    "ignore_default_args": None,
                    "no_viewport": None,
                }
            ],
        )
        self.assertEqual(chromium.last_page.url, "https://example.com/jobs/123")

    def test_open_url_uses_ephemeral_browser_when_profile_config_is_absent(self) -> None:
        chromium = _FakeChromium()
        backend = PlaywrightPrefillBrowserBackend(
            sync_playwright_factory=lambda: _FakeSyncPlaywright(chromium),
        )

        backend.open_url("https://example.com/jobs/456")

        self.assertEqual(
            chromium.launch_calls,
            [{"headless": False}],
        )
        self.assertEqual(chromium.persistent_launch_calls, [])
        self.assertEqual(chromium.last_page.url, "https://example.com/jobs/456")

    def test_open_url_uses_persistent_context_without_profile_arg_when_profile_directory_is_not_forced(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            user_data_dir = Path(tmp_dir) / "Chrome"
            chromium_user_data_dir = Path(tmp_dir) / "chromium"
            user_data_dir.mkdir(parents=True, exist_ok=True)
            chromium = _FakeChromium()
            backend = PlaywrightPrefillBrowserBackend(
                profile_config=LocalPlaywrightProfileConfig(
                    channel=None,
                    user_data_dir=user_data_dir,
                    profile_directory=None,
                ),
                sync_playwright_factory=lambda: _FakeSyncPlaywright(chromium),
            )

            with patch("jobs_ai.prefill_browser.sys.platform", "darwin"):
                backend.open_url("https://example.com/jobs/789")

        self.assertEqual(chromium.launch_calls, [])
        self.assertEqual(
            chromium.persistent_launch_calls,
            [
                {
                    "user_data_dir": str(chromium_user_data_dir),
                    "headless": False,
                    "channel": None,
                    "args": None,
                    "ignore_default_args": None,
                    "no_viewport": True,
                }
            ],
        )
        self.assertEqual(chromium.last_page.url, "https://example.com/jobs/789")

    def test_open_url_falls_back_to_playwright_chromium_when_local_chrome_launch_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            user_data_dir = Path(tmp_dir) / "Chrome"
            chromium_user_data_dir = Path(tmp_dir) / "chromium"
            user_data_dir.mkdir(parents=True, exist_ok=True)
            chromium = _FakeChromium(fail_persistent_channels={"chrome"})
            backend = PlaywrightPrefillBrowserBackend(
                profile_config=LocalPlaywrightProfileConfig(
                    channel="chrome",
                    user_data_dir=user_data_dir,
                    profile_directory=None,
                ),
                sync_playwright_factory=lambda: _FakeSyncPlaywright(chromium),
            )

            with patch("jobs_ai.prefill_browser.sys.platform", "darwin"):
                backend.open_url("https://example.com/jobs/999")

        self.assertEqual(
            chromium.persistent_launch_calls,
            [
                {
                    "user_data_dir": str(user_data_dir),
                    "headless": False,
                    "channel": "chrome",
                    "args": None,
                    "ignore_default_args": None,
                    "no_viewport": True,
                },
                {
                    "user_data_dir": str(chromium_user_data_dir),
                    "headless": False,
                    "channel": None,
                    "args": None,
                    "ignore_default_args": None,
                    "no_viewport": True,
                },
            ],
        )
        self.assertEqual(chromium.last_page.url, "https://example.com/jobs/999")

    def test_open_url_waits_for_page_settle_after_navigation(self) -> None:
        chromium = _FakeChromium()
        backend = PlaywrightPrefillBrowserBackend(
            sync_playwright_factory=lambda: _FakeSyncPlaywright(chromium),
        )

        backend.open_url("https://example.com/jobs/settle")

        self.assertEqual(
            chromium.last_page.load_state_calls,
            [("networkidle", 1000)],
        )
        self.assertEqual(chromium.last_page.wait_timeout_calls, [250])


class _FakeSyncPlaywright:
    def __init__(self, chromium: "_FakeChromium") -> None:
        self._driver = _FakePlaywrightDriver(chromium)

    def start(self) -> "_FakePlaywrightDriver":
        return self._driver


class _FakePlaywrightDriver:
    def __init__(self, chromium: "_FakeChromium") -> None:
        self.chromium = chromium

    def stop(self) -> None:
        return None


class _FakeChromium:
    def __init__(self, *, fail_persistent_channels: set[str] | None = None) -> None:
        self.launch_calls: list[dict[str, object]] = []
        self.persistent_launch_calls: list[dict[str, object]] = []
        self.last_page = _FakePage()
        self._fail_persistent_channels = fail_persistent_channels or set()

    def launch(self, *, headless: bool) -> "_FakeBrowser":
        self.launch_calls.append({"headless": headless})
        return _FakeBrowser(self.last_page)

    def launch_persistent_context(
        self,
        user_data_dir: str,
        *,
        headless: bool,
        channel: str | None = None,
        args: list[str] | None = None,
        ignore_default_args: list[str] | None = None,
        no_viewport: bool | None = None,
    ) -> "_FakeBrowserContext":
        self.persistent_launch_calls.append(
            {
                "user_data_dir": user_data_dir,
                "headless": headless,
                "channel": channel,
                "args": args,
                "ignore_default_args": ignore_default_args,
                "no_viewport": no_viewport,
            }
        )
        if channel in self._fail_persistent_channels:
            raise RuntimeError(f"launch failed for channel={channel}")
        return _FakeBrowserContext(self.last_page)


class _FakeBrowser:
    def __init__(self, page: "_FakePage") -> None:
        self._page = page

    def new_page(self) -> "_FakePage":
        return self._page

    def close(self) -> None:
        return None


class _FakeBrowserContext:
    def __init__(self, page: "_FakePage") -> None:
        self.pages = [page]

    def new_page(self) -> "_FakePage":
        return self.pages[0]

    def close(self) -> None:
        return None


class _FakePage:
    def __init__(self) -> None:
        self.url = ""
        self.load_state_calls: list[tuple[str, int | None]] = []
        self.wait_timeout_calls: list[int] = []

    def goto(self, url: str, *, wait_until: str) -> None:
        del wait_until
        self.url = url

    def wait_for_load_state(self, state: str, *, timeout: int | None = None) -> None:
        self.load_state_calls.append((state, timeout))

    def wait_for_timeout(self, timeout_ms: int) -> None:
        self.wait_timeout_calls.append(timeout_ms)


if __name__ == "__main__":
    unittest.main()

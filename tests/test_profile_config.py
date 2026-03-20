from __future__ import annotations

import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

SRC_PATH = Path(__file__).resolve().parents[1] / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from jobs_ai.autofill.profile_config import (
    BROWSER_CHANNEL_ENV_VAR,
    BROWSER_PROFILE_DIRECTORY_ENV_VAR,
    BROWSER_USER_DATA_DIR_ENV_VAR,
    resolve_local_playwright_profile_config,
)


class LocalPlaywrightProfileConfigTest(unittest.TestCase):
    def test_resolve_local_playwright_profile_config_uses_macos_defaults(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            home_path = Path(tmp_dir)
            profile_path = home_path / "Library" / "Application Support" / "Google" / "Chrome" / "Profile 2"
            profile_path.mkdir(parents=True, exist_ok=True)

            with patch("jobs_ai.autofill.profile_config.sys.platform", "darwin"):
                with patch("jobs_ai.autofill.profile_config.Path.home", return_value=home_path):
                    config = resolve_local_playwright_profile_config({})

        assert config is not None
        self.assertEqual(config.channel, "chrome")
        self.assertEqual(
            config.user_data_dir,
            (home_path / "Library" / "Application Support" / "Google" / "Chrome").resolve(),
        )
        self.assertEqual(config.profile_directory, "Profile 2")
        self.assertEqual(config.launch_args, ("--profile-directory=Profile 2",))

    def test_resolve_local_playwright_profile_config_expands_env_override_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            home_path = Path(tmp_dir)
            user_data_dir = home_path / "custom-chrome-data"
            profile_path = user_data_dir / "Profile 9"
            profile_path.mkdir(parents=True, exist_ok=True)

            with patch("jobs_ai.autofill.profile_config.sys.platform", "linux"):
                with patch.dict(os.environ, {"HOME": str(home_path)}, clear=False):
                    config = resolve_local_playwright_profile_config(
                        {
                            BROWSER_CHANNEL_ENV_VAR: "chrome",
                            BROWSER_USER_DATA_DIR_ENV_VAR: "~/custom-chrome-data",
                            BROWSER_PROFILE_DIRECTORY_ENV_VAR: "Profile 9",
                        }
                    )

        assert config is not None
        self.assertEqual(config.user_data_dir, user_data_dir.resolve())
        self.assertEqual(config.profile_directory, "Profile 9")

    def test_resolve_local_playwright_profile_config_uses_explicit_user_data_dir_without_forcing_profile(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            user_data_dir = Path(tmp_dir) / "playwright-chrome-profile"
            user_data_dir.mkdir(parents=True, exist_ok=True)
            (user_data_dir / "Default").mkdir(parents=True, exist_ok=True)

            with patch("jobs_ai.autofill.profile_config.sys.platform", "linux"):
                config = resolve_local_playwright_profile_config(
                    {
                        BROWSER_USER_DATA_DIR_ENV_VAR: str(user_data_dir),
                    }
                )

        assert config is not None
        self.assertEqual(config.user_data_dir, user_data_dir.resolve())
        self.assertIsNone(config.channel)
        self.assertIsNone(config.profile_directory)
        self.assertEqual(config.profile_path, user_data_dir.resolve())
        self.assertEqual(config.launch_args, ())

    def test_resolve_local_playwright_profile_config_keeps_explicit_channel_for_dedicated_user_data_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            user_data_dir = Path(tmp_dir) / "playwright-chrome-profile"
            user_data_dir.mkdir(parents=True, exist_ok=True)
            (user_data_dir / "Default").mkdir(parents=True, exist_ok=True)

            with patch("jobs_ai.autofill.profile_config.sys.platform", "darwin"):
                config = resolve_local_playwright_profile_config(
                    {
                        BROWSER_CHANNEL_ENV_VAR: "chrome",
                        BROWSER_USER_DATA_DIR_ENV_VAR: str(user_data_dir),
                    }
                )

        assert config is not None
        self.assertEqual(config.channel, "chrome")
        self.assertIsNone(config.profile_directory)

    def test_resolve_local_playwright_profile_config_returns_none_off_macos_without_overrides(self) -> None:
        with patch("jobs_ai.autofill.profile_config.sys.platform", "linux"):
            config = resolve_local_playwright_profile_config({})

        self.assertIsNone(config)

    def test_resolve_local_playwright_profile_config_fails_clearly_when_profile_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            home_path = Path(tmp_dir)
            chrome_root = home_path / "Library" / "Application Support" / "Google" / "Chrome"
            chrome_root.mkdir(parents=True, exist_ok=True)

            with patch("jobs_ai.autofill.profile_config.sys.platform", "darwin"):
                with patch("jobs_ai.autofill.profile_config.Path.home", return_value=home_path):
                    with self.assertRaises(ValueError) as context:
                        resolve_local_playwright_profile_config({})

        message = str(context.exception)
        self.assertIn("Profile 2", message)
        self.assertIn(BROWSER_PROFILE_DIRECTORY_ENV_VAR, message)
        self.assertIn(BROWSER_USER_DATA_DIR_ENV_VAR, message)


if __name__ == "__main__":
    unittest.main()

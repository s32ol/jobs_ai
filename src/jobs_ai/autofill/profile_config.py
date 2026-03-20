from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import os
from pathlib import Path
import sys

BROWSER_CHANNEL_ENV_VAR = "JOBS_AI_BROWSER_CHANNEL"
BROWSER_USER_DATA_DIR_ENV_VAR = "JOBS_AI_BROWSER_USER_DATA_DIR"
BROWSER_PROFILE_DIRECTORY_ENV_VAR = "JOBS_AI_BROWSER_PROFILE_DIRECTORY"

DEFAULT_BROWSER_CHANNEL = "chrome"
DEFAULT_BROWSER_PROFILE_DIRECTORY = "Profile 2"

_MACOS_CHROME_USER_DATA_DIR_PARTS = ("Library", "Application Support", "Google", "Chrome")
_BROWSER_PROFILE_ENV_VARS = (
    BROWSER_CHANNEL_ENV_VAR,
    BROWSER_USER_DATA_DIR_ENV_VAR,
    BROWSER_PROFILE_DIRECTORY_ENV_VAR,
)


@dataclass(frozen=True, slots=True)
class LocalPlaywrightProfileConfig:
    channel: str | None
    user_data_dir: Path
    profile_directory: str | None

    @property
    def profile_path(self) -> Path:
        if self.profile_directory is None:
            return self.user_data_dir
        return self.user_data_dir / self.profile_directory

    @property
    def launch_args(self) -> tuple[str, ...]:
        if self.profile_directory is None:
            return ()
        return (f"--profile-directory={self.profile_directory}",)


def resolve_local_playwright_profile_config(
    env: Mapping[str, str] | None = None,
) -> LocalPlaywrightProfileConfig | None:
    """Resolve the local Chrome profile for browser-assisted autofill only.

    On macOS, local autofill defaults to the operator's dedicated Chrome profile.
    On other platforms, the persistent-profile path stays opt-in via env vars so
    remote/server workflows can keep using the existing ephemeral Playwright browser.
    The user data root and profile directory remain separate to leave room for a
    future dedicated automation-only profile.
    """

    source = os.environ if env is None else env
    explicit_override = any(env_var in source for env_var in _BROWSER_PROFILE_ENV_VARS)
    if not explicit_override and sys.platform != "darwin":
        return None

    user_data_dir = _resolve_user_data_dir(source)
    profile_directory = _resolve_profile_directory(source)
    channel = _resolve_channel(source, profile_directory=profile_directory)

    profile_path = user_data_dir if profile_directory is None else user_data_dir / profile_directory
    if profile_directory is not None and not profile_path.is_dir():
        raise ValueError(
            "Local Playwright browser profile was not found: "
            f"{profile_path}. Open Chrome with that profile once, or set "
            f"{BROWSER_USER_DATA_DIR_ENV_VAR} and {BROWSER_PROFILE_DIRECTORY_ENV_VAR} "
            "to an existing Chrome user data dir/profile before running "
            "application-assist --prefill."
        )

    return LocalPlaywrightProfileConfig(
        channel=channel,
        user_data_dir=user_data_dir,
        profile_directory=profile_directory,
    )


def _default_macos_chrome_user_data_dir() -> Path:
    return Path.home().joinpath(*_MACOS_CHROME_USER_DATA_DIR_PARTS)


def _resolve_user_data_dir(source: Mapping[str, str]) -> Path:
    configured_value = source.get(BROWSER_USER_DATA_DIR_ENV_VAR)
    if configured_value is None:
        candidate = _default_macos_chrome_user_data_dir()
    else:
        candidate = _resolve_path_setting(
            configured_value,
            env_var=BROWSER_USER_DATA_DIR_ENV_VAR,
        )

    resolved_candidate = candidate.resolve()
    if not resolved_candidate.is_dir():
        raise ValueError(
            "Local Playwright browser user data dir was not found: "
            f"{resolved_candidate}. Install Chrome, create the profile locally, or set "
            f"{BROWSER_USER_DATA_DIR_ENV_VAR} to a valid Chrome user data directory "
            "before running application-assist --prefill."
        )
    return resolved_candidate


def _resolve_profile_directory(source: Mapping[str, str]) -> str | None:
    configured_value = source.get(BROWSER_PROFILE_DIRECTORY_ENV_VAR)
    if configured_value is None:
        if BROWSER_USER_DATA_DIR_ENV_VAR in source:
            return None
        profile_directory = DEFAULT_BROWSER_PROFILE_DIRECTORY
    else:
        profile_directory = _normalize_string_setting(
            configured_value,
            env_var=BROWSER_PROFILE_DIRECTORY_ENV_VAR,
        )

    normalized_path = Path(profile_directory)
    if normalized_path.name != profile_directory or len(normalized_path.parts) != 1:
        raise ValueError(
            f"{BROWSER_PROFILE_DIRECTORY_ENV_VAR} must be a Chrome profile directory name such as "
            f"{DEFAULT_BROWSER_PROFILE_DIRECTORY!r}, not a filesystem path."
        )
    return profile_directory


def _resolve_channel(
    source: Mapping[str, str],
    *,
    profile_directory: str | None,
) -> str | None:
    configured_value = source.get(BROWSER_CHANNEL_ENV_VAR)
    if configured_value is not None:
        return _normalize_string_setting(
            configured_value,
            env_var=BROWSER_CHANNEL_ENV_VAR,
        )
    if profile_directory is None and BROWSER_USER_DATA_DIR_ENV_VAR in source:
        return None
    return DEFAULT_BROWSER_CHANNEL


def _resolve_string_setting(
    source: Mapping[str, str],
    *,
    env_var: str,
    default: str,
) -> str:
    configured_value = source.get(env_var)
    if configured_value is None:
        return default
    return _normalize_string_setting(configured_value, env_var=env_var)


def _normalize_string_setting(value: str, *, env_var: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{env_var} must be a non-empty string when set.")
    return normalized


def _resolve_path_setting(value: str, *, env_var: str) -> Path:
    normalized = _normalize_string_setting(value, env_var=env_var)
    candidate = Path(normalized).expanduser()
    if not candidate.is_absolute():
        candidate = (Path.cwd() / candidate).resolve()
    return candidate

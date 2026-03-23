from __future__ import annotations

from pathlib import Path
import sys
import tempfile
import unittest

from typer.testing import CliRunner

SRC_PATH = Path(__file__).resolve().parents[1] / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from jobs_ai.cli import app
from jobs_ai.config import load_settings
from jobs_ai.workspace import build_workspace_paths, ensure_workspace, missing_workspace_paths

RUNNER = CliRunner()


class SmokeTest(unittest.TestCase):
    def test_cli_root_status_returns_success(self) -> None:
        result = RUNNER.invoke(app, [])
        self.assertEqual(result.exit_code, 0)
        self.assertIn("jobs_ai control tower", result.stdout)
        self.assertIn("current focus: milestone 11 operational polish", result.stdout)
        self.assertIn('jobs-ai run "python backend engineer remote"', result.stdout)

    def test_cli_help_includes_typical_sprint_flow(self) -> None:
        result = RUNNER.invoke(app, ["--help"])

        self.assertEqual(result.exit_code, 0)
        self.assertIn("Preferred daily flow:", result.stdout)
        self.assertIn("run", result.stdout)
        self.assertIn("session start", result.stdout)
        self.assertIn("launch-preview", result.stdout)

    def test_load_settings_defaults(self) -> None:
        settings = load_settings({})
        self.assertEqual(settings.environment, "dev")
        self.assertEqual(settings.profile, "default")
        self.assertEqual(settings.database_backend, "sqlite")
        self.assertEqual(settings.database_backend_source, "default")
        self.assertTrue(settings.database_fallback_triggered)
        self.assertEqual(settings.database_path, Path("data/jobs_ai.db"))

    def test_workspace_helpers_create_expected_directories(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            paths = build_workspace_paths(Path("state/jobs_ai.db"), project_root=Path(tmp_dir))
            created_paths = ensure_workspace(paths)

            self.assertTrue(created_paths)
            self.assertEqual(missing_workspace_paths(paths), [])
            self.assertTrue(paths.raw_dir.exists())
            self.assertTrue(paths.processed_dir.exists())
            self.assertTrue(paths.exports_dir.exists())
            self.assertTrue(paths.database_path.parent.exists())


if __name__ == "__main__":
    unittest.main()

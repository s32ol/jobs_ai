from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = "chatgpt_sources_core_v2"
CORE_SOURCE_FILES = [
    Path("README.md"),
    Path("src/jobs_ai/cli.py"),
    Path("src/jobs_ai/main.py"),
    Path("src/jobs_ai/db.py"),
    Path("src/jobs_ai/jobs/scoring.py"),
    Path("src/jobs_ai/jobs/queue.py"),
    Path("src/jobs_ai/jobs/fast_apply.py"),
    Path("src/jobs_ai/session_manifest.py"),
    Path("src/jobs_ai/session_start.py"),
    Path("src/jobs_ai/session_mark.py"),
    Path("src/jobs_ai/session_open.py"),
    Path("src/jobs_ai/launch_preview.py"),
    Path("src/jobs_ai/launch_executor.py"),
    Path("src/jobs_ai/resume/recommendations.py"),
    Path("src/jobs_ai/resume/config.py"),
    Path("src/jobs_ai/discover/cli.py"),
    Path("src/jobs_ai/discover/harness.py"),
    Path("src/jobs_ai/jobs/importer.py"),
    Path("src/jobs_ai/collect/adapters/greenhouse.py"),
    Path("src/jobs_ai/collect/adapters/lever.py"),
    Path("src/jobs_ai/collect/adapters/ashby.py"),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rebuild the ChatGPT source bundle from the current repo files."
    )
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT_DIR,
        help="Output directory name to create in the repo root.",
    )
    return parser.parse_args()


def resolve_output_dir(output_arg: str) -> Path:
    output_path = Path(output_arg)
    if output_path.is_absolute() or len(output_path.parts) != 1:
        raise ValueError("output must be a single directory name in the repo root")
    if output_path.name in {"", ".", ".."}:
        raise ValueError("output must be a valid directory name")
    return PROJECT_ROOT / output_path.name


def rebuild_bundle(output_dir: Path) -> tuple[list[Path], list[Path]]:
    shutil.rmtree(output_dir, ignore_errors=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    copied_files: list[Path] = []
    missing_files: list[Path] = []

    for relative_path in CORE_SOURCE_FILES:
        source_path = PROJECT_ROOT / relative_path
        if not source_path.is_file():
            missing_files.append(relative_path)
            continue

        destination_path = output_dir / relative_path
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_path, destination_path)
        copied_files.append(relative_path)

    return copied_files, missing_files


def print_summary(output_dir: Path, copied_files: list[Path], missing_files: list[Path]) -> None:
    print(f"output folder: {output_dir}")
    print("files copied:")
    if copied_files:
        for relative_path in copied_files:
            print(f"  - {relative_path.as_posix()}")
    else:
        print("  - none")

    print("missing files:")
    if missing_files:
        for relative_path in missing_files:
            print(f"  - {relative_path.as_posix()}")
    else:
        print("  - none")


def main() -> int:
    try:
        args = parse_args()
        output_dir = resolve_output_dir(args.output)
        copied_files, missing_files = rebuild_bundle(output_dir)
    except (OSError, ValueError) as exc:
        print(f"failed to build bundle: {exc}", file=sys.stderr)
        return 1

    print_summary(output_dir, copied_files, missing_files)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

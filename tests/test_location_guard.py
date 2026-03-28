from __future__ import annotations

from pathlib import Path
import sys
import unittest

SRC_PATH = Path(__file__).resolve().parents[1] / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from jobs_ai.jobs.location_guard import classify_job_location


class LocationGuardTest(unittest.TestCase):
    def test_classify_job_location_recognizes_us_non_us_and_ambiguous_examples(self) -> None:
        self.assertTrue(classify_job_location("USA Remote").is_us_allowed)
        self.assertTrue(
            classify_job_location("Sacramento, California, United States").is_us_allowed
        )
        self.assertTrue(classify_job_location("Toronto, Canada").is_non_us)
        self.assertTrue(classify_job_location("India").is_non_us)
        self.assertTrue(classify_job_location("Remote").is_ambiguous)
        self.assertTrue(classify_job_location(None).is_ambiguous)


if __name__ == "__main__":
    unittest.main()

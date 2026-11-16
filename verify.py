"""Run original business regressions, then replay a submitted design report."""
import json
import sys
import unittest
from pathlib import Path
import checks
import review
from observe import load, replay

if __name__ == "__main__":
    result = unittest.TextTestRunner(verbosity=2).run(
        unittest.defaultTestLoader.loadTestsFromModule(checks))
    if not result.wasSuccessful():
        raise SystemExit(1)
    count = review.check(Path(sys.argv[1]).read_text(encoding="utf-8"))
    print(json.dumps({"replayed_recordings": count,
                      "baseline_results": [
                          {"case": c["name"], "final": replay(c)["final"]}
                          for c in load("examples/cases.json")],
                      "protocol_correctness": "requires semantic review"},
                     ensure_ascii=False, sort_keys=True))

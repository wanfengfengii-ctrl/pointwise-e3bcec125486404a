"""Recompute report observations; this does not certify a proposed protocol."""
import json
import re
import sys
from pathlib import Path
from observe import load, replay


def check(text):
    blocks = re.findall(r"```json evidence\s*\n(.*?)\n```", text, re.S)
    if len(blocks) != 1:
        raise ValueError("exactly one json evidence block required")
    entries = json.loads(blocks[0])
    cases = {c["name"]: c for c in load("examples/cases.json")}
    if len(entries) != len(cases) or {e["case"] for e in entries} != set(cases):
        raise ValueError("recording coverage mismatch")
    for entry in entries:
        actual = replay(cases[entry["case"]])
        if entry["observed"] != actual:
            raise ValueError("observed baseline differs: " + entry["case"])
        if entry["predicted_final"] != actual["reference"]:
            raise ValueError("predicted multiset differs: " + entry["case"])
    locations = re.findall(r"\b(source|query|broadcast|sink)\.py:(\d+)\b", text)
    if {name for name, _ in locations} != {"source", "query", "broadcast", "sink"}:
        raise ValueError("source locations missing")
    for name, line in locations:
        if not 1 <= int(line) <= len(Path(name + ".py").read_text().splitlines()):
            raise ValueError("invalid source line")
    return len(entries)


if __name__ == "__main__":
    count = check(Path(sys.argv[1]).read_text(encoding="utf-8"))
    print(json.dumps({"replayed_recordings": count,
                      "protocol_correctness": "requires semantic review"}))

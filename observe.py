"""Replay finite recordings; request is an observation marker, not a switch."""
import json
import sys
from query import Query
from sink import canonical, reference


def replay(case):
    query = Query(case["events"])
    trace = []
    for number, action in enumerate(case["actions"]):
        op, *args = action
        if op == "read":
            for _ in range(args[0]):
                if not query.read_one():
                    raise ValueError("recording reads past source end")
        elif op in {"offer", "accept", "ack"}:
            getattr(query, op)(args[0])
        elif op == "finish":
            query.finish()
        elif op != "request":
            raise ValueError("unknown recording action")
        trace.append({"step": number, "action": action,
                      "state": query.snapshot(),
                      "accepted": query.sink.observation()})
    return {"case": case["name"], "trace": trace,
            "final": canonical(query.sink.observation()),
            "reference": reference(case["events"])}


def load(path):
    with open(path, encoding="utf-8") as stream:
        return json.load(stream)


def main():
    path = sys.argv[1] if len(sys.argv) > 1 else "examples/cases.json"
    print(json.dumps([replay(c) for c in load(path)],
                     ensure_ascii=False, sort_keys=True, indent=2))


if __name__ == "__main__":
    main()

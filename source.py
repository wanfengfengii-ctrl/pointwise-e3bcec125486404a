"""Deterministic interleaving of two finite, single-scan batch sources."""
class Source:
    def __init__(self, events):
        self.events = events
        self.opened = False
        self.position = 0

    def __iter__(self):
        if self.opened:
            raise RuntimeError("source cannot be reopened")
        self.opened = True
        counts = {"L": 0, "R": 0}
        seen = {"L": set(), "R": set()}
        ended = set()
        for seq, event in enumerate(self.events):
            self.position = seq + 1
            if len(event) == 2 and event[0] == "END":
                side = event[1]
                if side not in counts or side in ended:
                    raise ValueError("invalid end")
                ended.add(side)
                yield {"side": "END", "of": side, "seq": seq}
                continue
            if len(event) != 3:
                raise ValueError("invalid row")
            side, rid, key = event
            if side not in counts or side in ended:
                raise ValueError("row after end or invalid side")
            if not isinstance(rid, str) or not rid or rid in seen[side]:
                raise ValueError("invalid or repeated row ID")
            if type(key) is not int:
                raise ValueError("key must be an integer")
            seen[side].add(rid)
            row = {"side": side, "id": rid, "key": key,
                   "index": counts[side], "seq": seq}
            counts[side] += 1
            yield row
        if ended != {"L", "R"}:
            raise ValueError("both end markers required")

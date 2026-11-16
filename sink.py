"""Small-fixture stand-in for an external irreversible receiver."""
class Sink:
    def __init__(self):
        self._accepted = []

    def accept(self, pair):
        # No deduplication, revocation, or query-side readback.
        self._accepted.append(list(pair))

    def observation(self):
        # Laboratory-only tap; execution must never consult it.
        return [row[:] for row in self._accepted]


def canonical(rows):
    return sorted(rows, key=lambda p: (p[0], p[1] is None, p[1] or ""))


def reference(events):
    # Independent, intentionally quadratic oracle for finite fixtures.
    left = [e for e in events if e[0] == "L"]
    right = [e for e in events if e[0] == "R"]
    result = []
    for _, lid, key in left:
        matches = [rid for _, rid, rkey in right if rkey == key]
        result.extend([[lid, rid] for rid in matches] or [[lid, None]])
    return canonical(result)

"""Fixed symmetric broadcast join. No adaptive handoff protocol."""
class Worker:
    def __init__(self, wid):
        self.wid = wid
        self.rows = {"L": [], "R": []}
        self.by_key = {"L": {}, "R": {}}
        self.tasks = []
        self.head = 0
        self.pending = None
        self.matched = set()
        self.ends = set()
        self.null_pos = 0

    def ingest(self, row):
        side = row["side"]
        other = "R" if side == "L" else "L"
        bucket = self.by_key[other].get(row["key"], [])
        # A task owns pairs introduced by this arrival, with a fixed upper bound.
        self.tasks.append({"side": side, "row": row,
                           "stop": len(bucket), "pos": 0})
        self.rows[side].append(row)
        self.by_key[side].setdefault(row["key"], []).append(row)

    def end(self, side):
        self.ends.add(side)

    def offer(self):
        if self.pending is not None:
            return self.pending[:]
        while self.head < len(self.tasks):
            task = self.tasks[self.head]
            if task["pos"] == task["stop"]:
                self.head += 1
                continue
            side, row = task["side"], task["row"]
            other = "R" if side == "L" else "L"
            peer = self.by_key[other][row["key"]][task["pos"]]
            self.pending = ([row["id"], peer["id"]] if side == "L"
                            else [peer["id"], row["id"]])
            return self.pending[:]
        if self.ends == {"L", "R"}:
            left = self.rows["L"]
            while self.null_pos < len(left):
                lid = left[self.null_pos]["id"]
                if lid in self.matched:
                    self.null_pos += 1
                    continue
                self.pending = [lid, None]
                return self.pending[:]
        return None

    def ack(self):
        if self.pending is None:
            raise RuntimeError("ack without offer")
        lid, rid = self.pending
        if rid is None:
            self.null_pos += 1
        else:
            self.matched.add(lid)
            self.tasks[self.head]["pos"] += 1
        self.pending = None

    @property
    def done(self):
        return (self.ends == {"L", "R"} and self.pending is None
                and self.head == len(self.tasks)
                and self.null_pos == len(self.rows["L"]))

    def snapshot(self):
        return {"worker": self.wid, "head": self.head,
                "tasks": [{"seq": t["row"]["seq"], "side": t["side"],
                           "stop": t["stop"], "pos": t["pos"]}
                          for t in self.tasks],
                "left": [r["id"] for r in self.rows["L"]],
                "right": [r["id"] for r in self.rows["R"]],
                "matched": sorted(self.matched),
                "ends": sorted(self.ends), "null_pos": self.null_pos,
                "pending": self.pending}

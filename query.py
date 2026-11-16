"""Scheduler: source distribution and delivery lifecycle, fixed plan only."""
from source import Source
from broadcast import Worker
from sink import Sink


class Query:
    def __init__(self, events):
        self.source = Source(events)
        self.iterator = iter(self.source)
        self.workers = [Worker(0), Worker(1)]
        self.sink = Sink()
        self.flights = [None, None]
        self.exhausted = False
        self.counts = {"L": 0, "R": 0}

    def read_one(self):
        if self.exhausted:
            return False
        try:
            row = next(self.iterator)
        except StopIteration:
            self.exhausted = True
            return False
        if row["side"] == "END":
            for worker in self.workers:
                worker.end(row["of"])
        else:
            side = row["side"]
            self.counts[side] += 1
            targets = (self.workers if side == "R"
                       else [self.workers[row["index"] % 2]])
            for worker in targets:
                worker.ingest(row)
        return True

    def offer(self, wid):
        if self.flights[wid] is not None:
            raise RuntimeError("delivery already in flight")
        pair = self.workers[wid].offer()
        if pair is not None:
            self.flights[wid] = {"pair": pair, "accepted": False}
        return pair

    def accept(self, wid):
        flight = self.flights[wid]
        if flight is None or flight["accepted"]:
            raise RuntimeError("invalid accept")
        self.sink.accept(flight["pair"])
        flight["accepted"] = True

    def ack(self, wid):
        flight = self.flights[wid]
        if flight is None or not flight["accepted"]:
            raise RuntimeError("invalid ack")
        self.workers[wid].ack()
        self.flights[wid] = None

    def tick(self, wid):
        flight = self.flights[wid]
        if flight is None:
            self.offer(wid)
        elif flight["accepted"]:
            self.ack(wid)
        else:
            self.accept(wid)

    def finish(self):
        while self.read_one():
            pass
        while not all(w.done for w in self.workers):
            for wid in range(2):
                self.tick(wid)

    def snapshot(self):
        return {"source_position": self.source.position,
                "counts": dict(self.counts), "exhausted": self.exhausted,
                "flights": [None if f is None else
                            {"pair": f["pair"][:], "accepted": f["accepted"]}
                            for f in self.flights],
                "workers": [w.snapshot() for w in self.workers]}

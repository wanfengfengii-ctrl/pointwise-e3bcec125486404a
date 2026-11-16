import itertools
import unittest
from query import Query
from source import Source
from observe import load, replay
from sink import canonical, reference


class BaselineChecks(unittest.TestCase):
    def test_recordings(self):
        for case in load("examples/cases.json"):
            with self.subTest(case=case["name"]):
                observation = replay(case)
                self.assertEqual(observation["final"], observation["reference"])

    def test_all_small_keys_and_arrivals(self):
        # Both empty sides, repeated keys, and all L/R interleavings up to 2+2.
        for nl, nr in itertools.product(range(3), repeat=2):
            for keys in itertools.product(range(2), repeat=nl + nr):
                for lslots in itertools.combinations(range(nl + nr), nl):
                    events, li, ri = [], 0, 0
                    for pos in range(nl + nr):
                        if pos in lslots:
                            events.append(["L", "l" + str(li), keys[li]])
                            li += 1
                        else:
                            events.append(["R", "r" + str(ri), keys[nl + ri]])
                            ri += 1
                    events += [["END", "L"], ["END", "R"]]
                    query = Query(events)
                    while query.read_one():
                        for wid in range(2):
                            query.tick(wid)
                    query.finish()
                    self.assertEqual(canonical(query.sink.observation()),
                                     reference(events))

    def test_receiver_acceptance_precedes_ack(self):
        query = Query([["L", "a", 0], ["R", "b", 0],
                       ["END", "L"], ["END", "R"]])
        query.read_one()
        query.read_one()
        query.offer(0)
        query.accept(0)
        self.assertEqual(query.sink.observation(), [["a", "b"]])
        self.assertEqual(query.workers[0].tasks[-1]["pos"], 0)
        query.finish()
        self.assertEqual(query.sink.observation(), [["a", "b"]])

    def test_source_cannot_reopen(self):
        source = Source([["END", "L"], ["END", "R"]])
        list(source)
        with self.assertRaises(RuntimeError):
            list(source)


if __name__ == "__main__":
    unittest.main()

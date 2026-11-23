"""kvhead / ihead stop after n items (xdol#65)."""

import itertools

from xdol.from_dol import ihead, kvhead


def test_ihead_of_an_infinite_iterable_returns():
    assert ihead(itertools.count(), 2) == [0, 1]


def test_kvhead_reads_only_n_keys():
    seen = []

    class Store(dict):
        def __iter__(self):
            for k in super().__iter__():
                seen.append(k)
                yield k

    store = Store({str(i): i for i in range(100)})
    assert kvhead(store, 2) == [("0", 0), ("1", 1)]
    assert len(seen) == 2

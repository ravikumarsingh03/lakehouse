"""Executable tests for the CDC merge. Run: ``python3 test_cdc_merge.py``."""
from __future__ import annotations

import random
from dataclasses import dataclass, field

from cdc_merge import (DELETE, INSERT, SNAPSHOT, UPDATE, BootstrapState,
                       CdcContractError, ChangeEvent, Phase, bootstrap,
                       merge_batch)


def ev(key, pos, op, event_id=None, **row):
    identity = event_id or f"{key}:{pos!r}:{op}:{sorted(row.items())!r}"
    return ChangeEvent(key=(key,), position=pos, op=op, row=row or None,
                       event_id=identity)


def visible(state):
    return {key: dict(value.row) for key, value in state.items() if not value.deleted}


def test_out_of_order_input_converges_to_source_state():
    """Arrival order must not choose the winner."""
    truth = [ev(1, 10, INSERT, v="a"), ev(1, 20, UPDATE, v="b"),
             ev(1, 30, UPDATE, v="c"), ev(2, 15, INSERT, v="x")]
    for seed in range(5):
        shuffled = truth[:]
        random.Random(seed).shuffle(shuffled)
        state = {}
        for event in shuffled:
            merge_batch(state, [event])
        assert visible(state) == {(1,): {"v": "c"}, (2,): {"v": "x"}}


def test_same_commit_uses_event_order_not_first_arrival():
    """A transaction can change a key twice at one LSN/GTID position."""
    state = merge_batch({}, [ev(1, (50, 1), UPDATE, v="first"),
                             ev(1, (50, 2), UPDATE, v="last")])
    assert visible(state) == {(1,): {"v": "last"}}
    assert state[(1,)].position == (50, 2)


def test_conflicting_equal_source_position_is_rejected():
    """Silently keeping the first equal-position event would be nondeterministic."""
    try:
        merge_batch({}, [ev(1, (50, 1), UPDATE, "event-a", v="a"),
                         ev(1, (50, 1), UPDATE, "event-b", v="b")])
        raise AssertionError("ambiguous source order must fail the batch")
    except CdcContractError as error:
        assert "ambiguous source order" in str(error)


def test_conflicting_equal_source_position_is_rejected_across_batches():
    """Batch boundaries cannot change source-corruption handling."""
    state = merge_batch({}, [ev(1, (50, 1), UPDATE, "event-a", v="a")])
    try:
        merge_batch(state, [ev(1, (50, 1), UPDATE, "event-b", v="b")])
        raise AssertionError("equal positions with different event ids must fail")
    except CdcContractError as error:
        assert "across batches" in str(error)
    assert visible(state) == {(1,): {"v": "a"}}


def test_null_primary_key_is_rejected():
    try:
        ChangeEvent(key=(None,), position=10, op=INSERT, row={"v": "a"}, event_id="bad")
        raise AssertionError("primary keys must not be nullable")
    except CdcContractError as error:
        assert "non-null" in str(error)


def test_delete_beats_stale_late_update():
    state = {}
    merge_batch(state, [ev(1, 10, INSERT, v="a")])
    merge_batch(state, [ev(1, 30, DELETE)])
    merge_batch(state, [ev(1, 20, UPDATE, v="stale")])
    assert visible(state) == {}
    assert state[(1,)].deleted and state[(1,)].position == (30, 0)


def test_replaying_the_same_range_is_a_noop():
    batch = [ev(1, 10, INSERT, v="a"), ev(1, 20, UPDATE, v="b"),
             ev(2, 5, INSERT, v="x"), ev(2, 8, DELETE)]
    state = merge_batch({}, batch)
    before = {key: (row.position, row.row, row.deleted) for key, row in state.items()}
    merge_batch(state, batch)
    merge_batch(state, batch[:2])
    after = {key: (row.position, row.row, row.deleted) for key, row in state.items()}
    assert before == after


def test_unsupported_table_wide_event_is_not_misread_as_row_cdc():
    try:
        ev(1, 10, "t", v="ignored")
        raise AssertionError("truncate requires an explicit table-level recovery flow")
    except CdcContractError as error:
        assert "unsupported Debezium operation" in str(error)


def test_snapshot_before_persisted_watermark_is_rejected():
    """A stale snapshot would otherwise leave a gap before the captured stream."""
    class StaleSnapshot(FakeSource):
        def consistent_snapshot(self):
            return 99, [ev(1, 99, SNAPSHOT, "stale-snapshot", v="old")]

    try:
        bootstrap("t", StaleSnapshot(), FakeStore(), {})
        raise AssertionError("snapshot must be at or after the persisted watermark")
    except CdcContractError as error:
        assert "precedes watermark" in str(error)


@dataclass
class FakeStore:
    saved: BootstrapState = field(default_factory=BootstrapState)

    def load(self, table):
        return self.saved

    def save(self, table, state):
        self.saved = state


class FakeSource:
    def __init__(self, retained=None):
        self.retained = retained or []
        self.capture_starts = []

    def current_log_position(self):
        return 100

    def ensure_capture_from(self, table, watermark):
        self.capture_starts.append((table, watermark))

    def consistent_snapshot(self):
        rows = [ev(1, 105, SNAPSHOT, "snapshot-1", v="b"),
                ev(2, 105, SNAPSHOT, "snapshot-2", v="x")]
        return 105, rows

    def captured_after(self, table, watermark):
        assert watermark == (100, 0)
        return list(self.retained)


def test_cutover_neither_loses_nor_double_applies():
    source = FakeSource([ev(1, 103, UPDATE, v="b-again"),
                         ev(2, 107, UPDATE, v="y")])
    store = FakeStore()
    state = bootstrap("t", source, store, {})
    assert visible(state) == {(1,): {"v": "b"}, (2,): {"v": "y"}}
    assert state[(1,)].position == (105, -1) and state[(2,)].position == (107, 0)
    assert store.saved.phase is Phase.COMPLETE
    assert source.capture_starts  # capture is owned by the source adapter, not caller input


def test_cutover_crash_and_rerun_converges():
    store = FakeStore()

    class CrashesOnFirstSnapshot(FakeSource):
        def __init__(self):
            super().__init__([ev(2, 107, UPDATE, v="y")])
            self.calls = 0

        def consistent_snapshot(self):
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("crash mid-bootstrap")
            return super().consistent_snapshot()

    source, state = CrashesOnFirstSnapshot(), {}
    try:
        bootstrap("t", source, store, state)
        raise AssertionError("the simulated crash must propagate")
    except RuntimeError:
        pass
    assert store.saved.phase is Phase.WATERMARKED
    assert visible(bootstrap("t", source, store, state)) == {
        (1,): {"v": "b"}, (2,): {"v": "y"}}
    assert store.saved.phase is Phase.COMPLETE


def test_snapshot_overlap_at_same_commit_is_not_a_permanent_conflict():
    """Synthetic snapshot rows sort below a real event at the same commit."""
    source = FakeSource([ev(1, (105, 0), UPDATE, "stream-at-s", v="stream")])
    state = bootstrap("t", source, FakeStore(), {})
    assert visible(state) == {(1,): {"v": "stream"}, (2,): {"v": "x"}}
    assert state[(1,)].position == (105, 0)


if __name__ == "__main__":
    for name, function in sorted(globals().items()):
        if name.startswith("test_") and callable(function):
            function()
            print(f"ok {name}")
    print("all cdc_merge tests passed")

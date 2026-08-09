"""CDC merge: Debezium change log -> curated current state, replay-safe.

INVARIANT: a curated key is replaced only by a strictly later source position
(commit_order = LSN/GTID, event_order = in-transaction sequence) — never by
Kafka offset or arrival time. Deletes stay tombstones for the source retention
window, so an old replay cannot resurrect a deleted key.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Iterable, Mapping, Optional, Protocol, TypeAlias

INSERT, UPDATE, DELETE, SNAPSHOT = "c", "u", "d", "r"
SUPPORTED_OPERATIONS = frozenset((INSERT, UPDATE, DELETE, SNAPSHOT))
Position: TypeAlias = tuple[int, int]


class CdcContractError(ValueError):
    """The source did not provide a deterministic, supported change event."""


def position_key(position: int | Position) -> Position:
    """``int`` (examples only) means ``(position, 0)``; adapters emit both parts."""
    if isinstance(position, int):
        return (position, 0)
    if (not isinstance(position, tuple) or len(position) != 2
            or not all(isinstance(part, int) for part in position)):
        raise CdcContractError("position must be (commit_order, event_order)")
    return position


@dataclass(frozen=True)
class ChangeEvent:
    key: tuple[Any, ...]
    position: int | Position
    op: str
    row: Optional[Mapping[str, Any]] = None
    event_id: str = ""  # immutable source event id; validation requires it

    def __post_init__(self) -> None:
        _, event_order = position_key(self.position)
        if not self.key or any(value is None for value in self.key):
            raise CdcContractError("primary-key values must be present and non-null")
        if not self.event_id:
            raise CdcContractError("source_event_id is required for replay deduplication")
        if self.op not in SUPPORTED_OPERATIONS:
            raise CdcContractError(
                f"unsupported Debezium operation {self.op!r}; "
                "handle truncate/schema events in a separate table-level flow")
        if self.op != DELETE and self.row is None:
            raise CdcContractError(f"{self.op!r} requires an after-image")
        if self.op != SNAPSHOT and event_order < 0:
            raise CdcContractError("stream event_order must be non-negative")

    @property
    def order(self) -> Position:
        return position_key(self.position)


@dataclass
class CuratedRow:
    position: Position
    event_id: str
    op: str
    row: Optional[Mapping[str, Any]]
    deleted: bool


CuratedState: TypeAlias = dict[tuple[Any, ...], CuratedRow]


def _same_delivery(left: ChangeEvent, right: ChangeEvent) -> bool:
    """An equal position is harmless only when it is the same event replayed."""
    return (left.event_id == right.event_id and left.op == right.op
            and left.row == right.row)


def reduce_latest(events: Iterable[ChangeEvent]) -> dict[tuple[Any, ...], ChangeEvent]:
    """One winner per key. Equal position + different event is never resolved by
    arrival order — that hides a broken source contract — so the batch fails."""
    winners: dict[tuple[Any, ...], ChangeEvent] = {}
    for event in events:
        current = winners.get(event.key)
        if current is None or event.order > current.order:
            winners[event.key] = event
        elif event.order == current.order and not _same_delivery(event, current):
            raise CdcContractError(
                f"ambiguous source order for key={event.key!r}, position={event.order!r}")
    return winners


def merge_batch(state: CuratedState, events: Iterable[ChangeEvent]) -> CuratedState:
    """Apply one batch idempotently; the strict position comparison IS the
    replay guard. Callers quarantine ``CdcContractError`` for operator triage."""
    for key, event in reduce_latest(events).items():
        current = state.get(key)
        if current is not None:
            if event.order < current.position:
                continue
            if event.order == current.position:
                if (event.event_id != current.event_id or event.op != current.op
                        or event.row != current.row):
                    raise CdcContractError(
                        f"ambiguous source order across batches for key={key!r}, "
                        f"position={event.order!r}")
                continue
        state[key] = CuratedRow(event.order, event.event_id, event.op, event.row,
                                deleted=event.op == DELETE)
    return state


# --- snapshot -> stream cutover ---------------------------------------------

class Phase(str, Enum):
    INIT = "init"
    WATERMARKED = "watermarked"      # W persisted; capture resumes from W
    SNAPSHOT_APPLIED = "snapshot_applied"
    COMPLETE = "complete"


@dataclass
class BootstrapState:
    phase: Phase = Phase.INIT
    watermark: Optional[Position] = None
    snapshot_pos: Optional[Position] = None


class StateStore(Protocol):
    """Durable transactional state, e.g. a Postgres row locked per table."""

    def load(self, table: str) -> BootstrapState: ...
    def save(self, table: str, state: BootstrapState) -> None: ...


class Source(Protocol):
    def current_log_position(self) -> int | Position:
        """Return the position at which the source retention is available."""

    def ensure_capture_from(self, table: str, watermark: Position) -> None:
        """Durably start (or resume) capture from W. It must be idempotent."""

    def consistent_snapshot(self) -> tuple[int | Position, Iterable[ChangeEvent]]:
        """Return one repeatable-read snapshot and its consistent point S."""

    def captured_after(self, table: str, watermark: Position) -> Iterable[ChangeEvent]:
        """Read the durable captured/raw stream from W, not a caller list."""


def _stamp_snapshot_rows(snapshot_pos: Position,
                         rows: Iterable[ChangeEvent]) -> Iterable[ChangeEvent]:
    """Stamp snapshot rows at ``(S, -1)`` — below every real event at commit S —
    so a captured event at ``(S, 0)`` wins cleanly instead of tripping the
    equal-position corruption check (stream event orders are non-negative)."""
    snapshot_order = (snapshot_pos[0], -1)
    for event in rows:
        if event.op != SNAPSHOT:
            raise CdcContractError("consistent_snapshot must yield SNAPSHOT events")
        yield ChangeEvent(event.key, snapshot_order, SNAPSHOT, event.row, event.event_id)


def bootstrap(table: str, source: Source, store: StateStore,
              state: CuratedState) -> CuratedState:
    """Durable W -> capture from W -> snapshot S (>= W) -> replay captured stream.

    Every transition is idempotent, so a crash at any step re-converges; the
    source must retain records from W until this state reaches ``COMPLETE``."""
    saved = store.load(table)
    if saved.phase is Phase.INIT:
        saved.watermark = position_key(source.current_log_position())
        saved.phase = Phase.WATERMARKED
        store.save(table, saved)

    assert saved.watermark is not None
    if saved.phase is Phase.WATERMARKED:
        source.ensure_capture_from(table, saved.watermark)
        snapshot_pos, snapshot_rows = source.consistent_snapshot()
        saved.snapshot_pos = position_key(snapshot_pos)
        if saved.snapshot_pos < saved.watermark:
            raise CdcContractError(
                f"snapshot point {saved.snapshot_pos!r} precedes watermark "
                f"{saved.watermark!r}")
        merge_batch(state, _stamp_snapshot_rows(saved.snapshot_pos, snapshot_rows))
        saved.phase = Phase.SNAPSHOT_APPLIED
        store.save(table, saved)

    if saved.phase is Phase.SNAPSHOT_APPLIED:
        source.ensure_capture_from(table, saved.watermark)
        merge_batch(state, source.captured_after(table, saved.watermark))
        saved.phase = Phase.COMPLETE
        store.save(table, saved)
    return state


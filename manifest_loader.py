"""Manifest-driven ingestion with delivery fencing and controlled reprocessing.

INVARIANT: each unique delivery's rows land in raw exactly once across
duplicates, re-sends, and crashes. Content identity = (source_id, table,
sha256); processing identity adds ``initial`` or a named, operator-approved
reprocess attempt (version changes never mutate an accepted delivery).
"""
from __future__ import annotations

import hashlib
import tempfile
import time
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Any, BinaryIO, Callable, Iterable, Mapping, Optional, Protocol


class Status(str, Enum):
    PENDING = "pending"
    LOADED = "loaded"
    QUARANTINED = "quarantined"


class Outcome(str, Enum):
    LOADED = "loaded"
    DUPLICATE = "duplicate"
    ALREADY_QUARANTINED = "already_quarantined"
    QUARANTINED_FILE = "quarantined_file"
    REPROCESS_REQUIRED = "reprocess_required"
    IN_PROGRESS = "in_progress"


class ClaimKind(str, Enum):
    ACQUIRED = "acquired"
    DUPLICATE = "duplicate"
    ALREADY_QUARANTINED = "already_quarantined"
    PATH_CONFLICT = "path_conflict"
    REPROCESS_REQUIRED = "reprocess_required"
    IN_PROGRESS = "in_progress"


class PayloadTooLarge(ValueError):
    pass


@dataclass
class FileRecord:
    file_id: str                 # stable scoped content identity, full SHA-256
    attempt_id: str              # initial or reprocess:<operator-approved-id>
    source_id: str
    table: str
    path: str
    delivery_key: str            # path by default; source sequence/date when names recur
    checksum: str
    size: int
    parser_version: str
    contract_version: str
    owner: str
    status: Status
    row_count: int = 0
    quarantined_rows: int = 0


@dataclass(frozen=True)
class Claim:
    kind: ClaimKind
    record: FileRecord
    lease_token: Optional[str] = None
    prior: Optional[FileRecord] = None


class ManifestStore(Protocol):
    """Transactional Postgres manifest, leases, and alert outbox.

    ``claim`` is one transaction over a delivery-key lock + content lock; a live
    lease returns IN_PROGRESS *before* any version check, and a version change
    on the initial attempt returns REPROCESS_REQUIRED without mutating it.
    ``mark_loaded`` is conditional on (attempt, lease token) and writes the
    row-quarantine outbox record in the same transaction."""

    def claim(self, record: FileRecord, now: float) -> Claim: ...
    def release_claim(self, file_id: str, lease_token: str) -> None: ...
    def mark_loaded(self, file_id: str, attempt_id: str, lease_token: str,
                    row_count: int, quarantined_rows: int) -> None: ...


class RawWriter(Protocol):
    def overwrite_file_partition(self, table: str, file_id: str, attempt_id: str,
                                 rows: Iterable[Mapping[str, Any]]) -> int:
        """Atomically replace this source_file_id partition, recording attempt id."""


class RowQuarantine(Protocol):
    def put_once(self, file_id: str, attempt_id: str, row_number: int,
                 row: Mapping[str, Any], reason: str) -> None:
        """Upsert by (file_id, attempt_id, row_number, contract_version)."""


RowContract = Callable[[Mapping[str, Any]], Optional[str]]
ParseFn = Callable[[BinaryIO], Iterable[Mapping[str, Any]]]
Payload = bytes | Iterable[bytes]
DEFAULT_MAX_BYTES = 5 * 1024 * 1024 * 1024


def checksum_of(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _spool_and_hash(payload: Payload, max_bytes: int) -> tuple[BinaryIO, str, int]:
    """Hash a finalized stream while bounding memory and temporary-disk usage."""
    stream = tempfile.SpooledTemporaryFile(max_size=8 * 1024 * 1024, mode="w+b")
    digest, size = hashlib.sha256(), 0
    chunks: Iterable[bytes] = (payload,) if isinstance(payload, bytes) else payload
    try:
        for chunk in chunks:
            if not isinstance(chunk, bytes):
                raise TypeError("payload chunks must be bytes")
            size += len(chunk)
            if size > max_bytes:
                raise PayloadTooLarge(f"delivery exceeds configured {max_bytes}-byte limit")
            digest.update(chunk)
            stream.write(chunk)
        stream.seek(0)
        return stream, digest.hexdigest(), size
    except Exception:
        stream.close()
        raise


def _file_id(source_id: str, table: str, checksum: str) -> str:
    scoped = f"{source_id}\0{table}\0{checksum}".encode("utf-8")
    return "f_" + hashlib.sha256(scoped).hexdigest()


def _attempt_id(reprocess_id: Optional[str]) -> str:
    if reprocess_id is None:
        return "initial"
    if not reprocess_id or len(reprocess_id) > 128:
        raise ValueError("reprocess_id must be a non-empty identifier up to 128 characters")
    return f"reprocess:{reprocess_id}"


def ingest(*, source_id: str, table: str, owner: str, path: str, payload: Payload,
           parse: ParseFn, contract: RowContract, parser_version: str,
           contract_version: str, store: ManifestStore, raw: RawWriter,
           quarantine: RowQuarantine, delivery_key: Optional[str] = None,
           reprocess_id: Optional[str] = None, max_bytes: int = DEFAULT_MAX_BYTES,
           clock: Callable[[], float] = time.time) -> Outcome:
    """Load a finalized delivery once, or run an operator-approved reprocess.

    Recurring filenames (``daily.csv``) must pass a stable ``delivery_key``
    (vendor sequence / business date); unset means strict immutable-path policy.
    A reprocess overwrites the same raw partition under an auditable attempt id
    — never triggered by an accidental retry."""
    if max_bytes <= 0:
        raise ValueError("max_bytes must be positive")
    stream, digest, size = _spool_and_hash(payload, max_bytes)
    record = FileRecord(
        file_id=_file_id(source_id, table, digest), attempt_id=_attempt_id(reprocess_id),
        source_id=source_id, table=table, path=path, delivery_key=delivery_key or path,
        checksum=digest, size=size, parser_version=parser_version,
        contract_version=contract_version, owner=owner, status=Status.PENDING,
    )
    try:
        claim = store.claim(record, clock())
        if claim.kind is ClaimKind.DUPLICATE:
            return Outcome.DUPLICATE
        if claim.kind is ClaimKind.ALREADY_QUARANTINED:
            return Outcome.ALREADY_QUARANTINED
        if claim.kind is ClaimKind.IN_PROGRESS:
            return Outcome.IN_PROGRESS
        if claim.kind is ClaimKind.PATH_CONFLICT:
            return Outcome.QUARANTINED_FILE
        if claim.kind is ClaimKind.REPROCESS_REQUIRED:
            return Outcome.REPROCESS_REQUIRED

        active, token = claim.record, claim.lease_token
        if token is None:
            raise RuntimeError("manifest store returned an acquired claim without a lease token")
        rejected = 0

        def good_rows() -> Iterable[Mapping[str, Any]]:
            nonlocal rejected
            for row_number, row in enumerate(parse(stream), start=1):
                reason = contract(row)
                if reason is None:
                    yield row
                else:
                    quarantine.put_once(active.file_id, active.attempt_id, row_number, row, reason)
                    rejected += 1

        try:
            loaded = raw.overwrite_file_partition(table, active.file_id, active.attempt_id,
                                                   good_rows())
            store.mark_loaded(active.file_id, active.attempt_id, token, loaded, rejected)
        except Exception:
            # Raw replacement and quarantine upserts are replay-safe; a later
            # lease holder repeats the same attempt instead of appending.
            store.release_claim(active.file_id, token)
            raise
        return Outcome.LOADED
    finally:
        stream.close()

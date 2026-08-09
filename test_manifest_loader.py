"""Executable manifest-ingestion tests. Run: ``python3 test_manifest_loader.py``."""
from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from typing import Optional

from manifest_loader import (Claim, ClaimKind, FileRecord, Outcome, PayloadTooLarge,
                             Status, checksum_of, ingest)


class FakeStore:
    """In-memory implementation of the transaction/lease contract in the module."""
    def __init__(self):
        self.attempts: dict[tuple[str, str, str, str], FileRecord] = {}
        self.deliveries: dict[tuple[str, str, str], str] = {}
        self.active: dict[str, tuple[str, str]] = {}  # file_id -> (lease, attempt_id)
        self.outbox: list[str] = []

    @staticmethod
    def _key(record):
        return record.source_id, record.table, record.checksum, record.attempt_id

    def _first_attempt(self, source_id, table, checksum):
        return self.attempts.get((source_id, table, checksum, "initial"))

    def _claim(self, record):
        token = uuid.uuid4().hex
        self.active[record.file_id] = (token, record.attempt_id)
        return Claim(ClaimKind.ACQUIRED, record, token)

    def claim(self, record, now) -> Claim:
        delivery = record.source_id, record.table, record.delivery_key
        delivered_checksum = self.deliveries.get(delivery)
        if delivered_checksum is not None and delivered_checksum != record.checksum:
            existing = self.attempts.get(self._key(record))
            if existing is not None and existing.status is Status.QUARANTINED:
                return Claim(ClaimKind.ALREADY_QUARANTINED, existing)
            record.status = Status.QUARANTINED
            self.attempts[self._key(record)] = record
            self.outbox.append(f"delivery conflict: {record.delivery_key}")
            return Claim(ClaimKind.PATH_CONFLICT, record,
                         self._first_attempt(record.source_id, record.table, delivered_checksum))
        self.deliveries.setdefault(delivery, record.checksum)

        # This check deliberately precedes version checking: a v2 retry must
        # never mutate or quarantine a v1 worker that still owns the file.
        active = self.active.get(record.file_id)
        if active is not None:
            existing = self.attempts[(record.source_id, record.table, record.checksum,
                                      active[1])]
            return Claim(ClaimKind.IN_PROGRESS, existing)

        existing = self.attempts.get(self._key(record))
        initial = self._first_attempt(record.source_id, record.table, record.checksum)
        if record.attempt_id == "initial" and initial is not None:
            if (initial.parser_version != record.parser_version
                    or initial.contract_version != record.contract_version):
                self.outbox.append(f"reprocess required: {record.path}")
                return Claim(ClaimKind.REPROCESS_REQUIRED, initial)
            if initial.status is Status.LOADED:
                return Claim(ClaimKind.DUPLICATE, initial)
            if initial.status is Status.QUARANTINED:
                return Claim(ClaimKind.ALREADY_QUARANTINED, initial)
            return self._claim(initial)

        if existing is not None:
            if existing.status is Status.LOADED:
                return Claim(ClaimKind.DUPLICATE, existing)
            if existing.status is Status.QUARANTINED:
                return Claim(ClaimKind.ALREADY_QUARANTINED, existing)
            return self._claim(existing)

        if record.attempt_id != "initial" and initial is None:
            return Claim(ClaimKind.REPROCESS_REQUIRED, record)
        self.attempts[self._key(record)] = record
        return self._claim(record)

    def release_claim(self, file_id, lease_token):
        if self.active.get(file_id, (None,))[0] == lease_token:
            del self.active[file_id]

    def mark_loaded(self, file_id, attempt_id, lease_token, row_count, quarantined_rows):
        if self.active.get(file_id) != (lease_token, attempt_id):
            raise RuntimeError("stale manifest lease cannot acknowledge a load")
        for record in self.attempts.values():
            if record.file_id == file_id and record.attempt_id == attempt_id:
                if record.status is not Status.PENDING:
                    raise RuntimeError("only a pending attempt can become loaded")
                record.status = Status.LOADED
                record.row_count, record.quarantined_rows = row_count, quarantined_rows
                del self.active[file_id]
                if quarantined_rows:
                    self.outbox.append(f"row quarantine: {record.path}:{attempt_id}")
                return
        raise AssertionError(f"unknown attempt {file_id}/{attempt_id}")

    def record(self, source_id, table, payload, attempt_id="initial") -> Optional[FileRecord]:
        return self.attempts.get((source_id, table, checksum_of(payload), attempt_id))


class FakeRaw:
    def __init__(self):
        self.partitions: dict[str, tuple[str, list]] = {}
        self.overwrites = 0

    def overwrite_file_partition(self, table, file_id, attempt_id, rows):
        self.overwrites += 1
        self.partitions[file_id] = (attempt_id, list(rows))
        return len(self.partitions[file_id][1])

    def all_rows(self):
        return [row for _, rows in self.partitions.values() for row in rows]


@dataclass
class FakeQuarantine:
    rows: dict[tuple[str, str, int], tuple] = field(default_factory=dict)

    def put_once(self, file_id, attempt_id, row_number, row, reason):
        self.rows.setdefault((file_id, attempt_id, row_number), (row, reason))


def parse_jsonl(stream):
    return [json.loads(line) for line in stream.read().decode().splitlines() if line]


def contract(row):
    return None if isinstance(row.get("amount_paise"), int) else "amount_paise must be integer paise"


def env(**overrides):
    values = dict(source_id="partner-a", table="raw.partner_files", owner="lending-ops",
                  parse=parse_jsonl, contract=contract, parser_version="jsonl-v1",
                  contract_version="money-v1", store=FakeStore(), raw=FakeRaw(),
                  quarantine=FakeQuarantine())
    values.update(overrides)
    return values


GOOD = b'{"id": 1, "amount_paise": 12050}\n{"id": 2, "amount_paise": 990}\n'
MIXED = GOOD + b'{"id": 3, "amount_paise": "1,200.50"}\n'


def test_same_file_delivered_twice_loads_once():
    e = env()
    assert ingest(path="drop/a.jsonl", payload=GOOD, **e) is Outcome.LOADED
    assert ingest(path="drop/a.jsonl", payload=GOOD, **e) is Outcome.DUPLICATE
    assert ingest(path="drop/a_retry.jsonl", payload=GOOD, **e) is Outcome.DUPLICATE
    assert len(e["raw"].all_rows()) == 2


def test_identity_is_scoped_to_source_and_target_table():
    e = env()
    assert ingest(path="drop/a.jsonl", payload=GOOD, **e) is Outcome.LOADED
    second = dict(e, source_id="partner-b")
    assert ingest(path="drop/a.jsonl", payload=GOOD, **second) is Outcome.LOADED
    assert len(e["raw"].all_rows()) == 4


def test_same_delivery_key_different_bytes_quarantines_then_reports_state():
    e = env()
    ingest(path="daily.csv", payload=GOOD, **e)
    changed = GOOD.replace(b"12050", b"99999")
    assert ingest(path="daily.csv", payload=changed, **e) is Outcome.QUARANTINED_FILE
    assert ingest(path="daily.csv", payload=changed, **e) is Outcome.ALREADY_QUARANTINED
    assert len(e["raw"].all_rows()) == 2


def test_recurring_filename_is_supported_with_a_stable_delivery_key():
    e = env()
    changed = GOOD.replace(b"12050", b"99999")
    assert ingest(path="daily.csv", delivery_key="2026-08-08", payload=GOOD, **e) is Outcome.LOADED
    assert ingest(path="daily.csv", delivery_key="2026-08-09", payload=changed, **e) is Outcome.LOADED
    assert len(e["raw"].all_rows()) == 4


def test_crash_after_raw_write_replay_replaces_rows_and_quarantine_once():
    e = env()
    original, calls = e["raw"].overwrite_file_partition, {"count": 0}

    def crash_after_write(table, file_id, attempt_id, rows):
        result = original(table, file_id, attempt_id, rows)
        calls["count"] += 1
        if calls["count"] == 1:
            raise OSError("killed before manifest acknowledgement")
        return result

    e["raw"].overwrite_file_partition = crash_after_write
    try:
        ingest(path="drop/a.jsonl", payload=MIXED, **e)
        raise AssertionError("the simulated crash must propagate")
    except OSError:
        pass
    assert e["store"].record("partner-a", "raw.partner_files", MIXED).status is Status.PENDING
    assert ingest(path="drop/a.jsonl", payload=MIXED, **e) is Outcome.LOADED
    assert len(e["raw"].all_rows()) == 2 and len(e["quarantine"].rows) == 1


def test_version_change_requires_an_explicit_reprocess_attempt():
    e = env()
    assert ingest(path="drop/a.jsonl", payload=GOOD, **e) is Outcome.LOADED
    changed = dict(e, contract_version="money-v2")
    assert ingest(path="drop/a.jsonl", payload=GOOD, **changed) is Outcome.REPROCESS_REQUIRED

    def v2_contract(row):
        return None if row["id"] == 1 else "reprocess policy rejects id 2"

    reprocess = dict(changed, contract=v2_contract)
    assert ingest(path="drop/a.jsonl", payload=GOOD, reprocess_id="2026-08-contract-v2",
                  **reprocess) is Outcome.LOADED
    record = e["store"].record("partner-a", "raw.partner_files", GOOD,
                                "reprocess:2026-08-contract-v2")
    assert record.status is Status.LOADED and record.quarantined_rows == 1
    assert len(e["raw"].all_rows()) == 1


def test_live_v1_attempt_wins_over_a_v2_retry_without_status_corruption():
    e = env()

    class RacingRaw(FakeRaw):
        def overwrite_file_partition(self, table, file_id, attempt_id, rows):
            self.concurrent = ingest(
                source_id="partner-a", table="raw.partner_files", owner="lending-ops",
                path="drop/a.jsonl", payload=GOOD, parse=parse_jsonl, contract=contract,
                parser_version="jsonl-v1", contract_version="money-v2", store=e["store"],
                raw=self, quarantine=e["quarantine"])
            return super().overwrite_file_partition(table, file_id, attempt_id, rows)

    e["raw"] = RacingRaw()
    assert ingest(path="drop/a.jsonl", payload=GOOD, **e) is Outcome.LOADED
    assert e["raw"].concurrent is Outcome.IN_PROGRESS
    assert e["store"].record("partner-a", "raw.partner_files", GOOD).status is Status.LOADED


def test_delivery_size_limit_is_enforced_before_a_manifest_claim():
    e = env()
    try:
        ingest(path="oversized.jsonl", payload=GOOD, max_bytes=4, **e)
        raise AssertionError("oversized input must be rejected")
    except PayloadTooLarge:
        pass
    assert not e["store"].attempts


if __name__ == "__main__":
    for name, function in sorted(globals().items()):
        if name.startswith("test_") and callable(function):
            function()
            print(f"ok {name}")
    print("all manifest_loader tests passed")

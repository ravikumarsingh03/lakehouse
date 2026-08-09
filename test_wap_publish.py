"""Executable WAP tests. Run: ``python3 test_wap_publish.py``."""
from __future__ import annotations

import time
from dataclasses import dataclass

from adapters import SparkIcebergCatalog
from wap_publish import (BranchNotFound, CheckResult, ConflictError,
                         ExpiredBranch, PublishBlocked, PublishContention,
                         Report, expire_stale_audit_branches,
                         forbid_float_money, publish)


@dataclass
class Branch:
    base: int
    head: int
    run_id: str | None = None


class FakeCatalog:
    def __init__(self):
        self.main, self.main_run_id = 100, None
        self.branches: dict[str, Branch] = {}
        self._next = 200

    def main_snapshot(self, table):
        return self.main

    def published_snapshot_for_run(self, table, run_id):
        return self.main if self.main_run_id == run_id else None

    def create_branch(self, table, branch, at_snapshot):
        self.branches[branch] = Branch(base=at_snapshot, head=at_snapshot)

    def drop_branch(self, table, branch):
        if branch not in self.branches:
            raise BranchNotFound(branch)
        del self.branches[branch]

    def commit_to_branch(self, branch, run_id):
        self._next += 1
        self.branches[branch].head = self._next
        self.branches[branch].run_id = run_id

    def fast_forward_main(self, table, branch, expected_main):
        item = self.branches[branch]
        if self.main != expected_main or item.base != self.main:
            raise ConflictError("main moved")
        self.main, self.main_run_id = item.head, item.run_id
        return self.main


@dataclass
class AuditRecord:
    lease_at: float
    state: str = "intent"
    claimed: bool = False
    reason: str = ""


class FakeAuditBranches:
    def __init__(self):
        self.records: dict[str, AuditRecord] = {}
        self.heartbeats = 0

    def register_intent(self, table, branch, run_id, base_snapshot, created_at):
        self.records[branch] = AuditRecord(created_at)

    def mark_created(self, table, branch):
        self.records[branch].state = "created"

    def heartbeat(self, table, branch, now):
        self.heartbeats += 1
        self.records[branch].lease_at = now

    def retain_for_debugging(self, table, branch, reason):
        self.records[branch].state, self.records[branch].reason = "retained", reason

    def mark_published(self, table, branch, snapshot_id):
        self.records[branch].state = "published"

    def record_cleanup_failure(self, table, branch, error):
        self.records[branch].state, self.records[branch].reason = "cleanup_failed", error

    def release(self, table, branch):
        self.records.pop(branch, None)

    def claim_expired(self, table, now, ttl_seconds):
        claimed = []
        for branch, record in self.records.items():
            if not record.claimed and now - record.lease_at > ttl_seconds:
                record.claimed = True
                claimed.append(ExpiredBranch(branch, record.state))
        return claimed

    def release_expiry_claim(self, table, branch):
        self.records[branch].claimed = False


def passing(_branch):
    return Report([CheckResult("ok", True)])


def failing(_branch):
    return Report([CheckResult("row_count_within_bounds", False, "0 rows")])


def test_reader_of_main_never_sees_unvalidated_data():
    cat, audit = FakeCatalog(), FakeAuditBranches()
    try:
        publish(cat, audit, "t", "run1", cat.commit_to_branch, failing,
                heartbeat_interval_seconds=0)
        raise AssertionError("publish must raise PublishBlocked")
    except PublishBlocked:
        pass
    assert cat.main == 100
    assert audit.records and all(item.state == "retained" for item in audit.records.values())


def test_float_money_column_blocks_publish():
    schema = {"loan_id": "bigint", "amount_paise": "double"}
    result = forbid_float_money(schema, ["amount_paise"])
    assert not result.ok and "amount_paise" in result.detail


def test_concurrent_writer_rebuilds_instead_of_overwriting():
    cat, audit, writes = FakeCatalog(), FakeAuditBranches(), []

    def write(branch, run_id):
        if not writes:
            cat.main = 150
        writes.append(branch)
        cat.commit_to_branch(branch, run_id)

    final = publish(cat, audit, "t", "run2", write, passing, heartbeat_interval_seconds=0)
    assert len(writes) == 2 and final == cat.main != 150
    assert not cat.branches and not audit.records


def test_same_run_id_is_rechecked_after_a_lost_cas():
    """A double-fired run must not rebuild on top of its own published output."""
    class OtherWorkerWins(FakeCatalog):
        def fast_forward_main(self, table, branch, expected_main):
            # Simulate another publisher for this same run landing after this
            # worker wrote its branch but before its CAS.
            self.main = 999
            self.main_run_id = self.branches[branch].run_id
            raise ConflictError("same run already published")

    cat, audit, writes = OtherWorkerWins(), FakeAuditBranches(), []

    def write(branch, run_id):
        writes.append(branch)
        cat.commit_to_branch(branch, run_id)

    assert publish(cat, audit, "t", "run2b", write, passing,
                   heartbeat_interval_seconds=0) == 999
    assert len(writes) == 1


def test_lost_fast_forward_ack_does_not_rewrite_the_run():
    """A retry discovers its run-id on main rather than blindly appending again."""
    class AckLostCatalog(FakeCatalog):
        def fast_forward_main(self, table, branch, expected_main):
            value = super().fast_forward_main(table, branch, expected_main)
            raise OSError("connection dropped after commit")

    cat, audit, writes = AckLostCatalog(), FakeAuditBranches(), []

    def write(branch, run_id):
        writes.append(branch)
        cat.commit_to_branch(branch, run_id)

    try:
        publish(cat, audit, "t", "run3", write, passing, heartbeat_interval_seconds=0)
        raise AssertionError("lost acknowledgement must surface as uncertain")
    except OSError:
        pass
    assert publish(cat, audit, "t", "run3", write, passing, heartbeat_interval_seconds=0) == cat.main
    assert len(writes) == 1


def test_heartbeat_stays_live_during_a_long_write():
    cat, audit = FakeCatalog(), FakeAuditBranches()

    def slow_write(branch, run_id):
        time.sleep(0.04)
        cat.commit_to_branch(branch, run_id)

    publish(cat, audit, "t", "run4", slow_write, passing,
            heartbeat_interval_seconds=0.005)
    assert audit.heartbeats >= 5


def test_janitor_converges_phantoms_and_continues_past_failures():
    class CleanupFailureCatalog(FakeCatalog):
        def drop_branch(self, table, branch):
            if branch == "broken":
                raise OSError("catalog outage")
            return super().drop_branch(table, branch)

    cat, audit = CleanupFailureCatalog(), FakeAuditBranches()
    # `phantom` represents process death after registry intent, before DDL.
    audit.register_intent("t", "phantom", "x", 100, 1.0)
    audit.register_intent("t", "broken", "x", 100, 1.0)
    audit.register_intent("t", "good", "x", 100, 1.0)
    cat.branches["broken"] = Branch(100, 100)
    cat.branches["good"] = Branch(100, 100)
    report = expire_stale_audit_branches(cat, audit, "t", now=100.0, ttl_seconds=1.0)
    assert report.already_absent == ["phantom"]
    assert report.dropped == ["good"]
    assert report.failures == [("broken", "catalog outage")]
    assert set(audit.records) == {"broken"} and not audit.records["broken"].claimed


def test_unknown_create_outcome_keeps_intent_for_the_janitor():
    class CreateUnknownCatalog(FakeCatalog):
        def create_branch(self, table, branch, at_snapshot):
            super().create_branch(table, branch, at_snapshot)
            raise TimeoutError("create response lost")

    cat, audit = CreateUnknownCatalog(), FakeAuditBranches()
    try:
        publish(cat, audit, "t", "run5", cat.commit_to_branch, passing,
                clock=lambda: 1.0, heartbeat_interval_seconds=0)
        raise AssertionError("unknown create outcome must propagate")
    except TimeoutError:
        pass
    assert len(cat.branches) == len(audit.records) == 1
    report = expire_stale_audit_branches(cat, audit, "t", 100.0, 1.0)
    assert len(report.dropped) == 1 and not cat.branches and not audit.records


def test_contention_exhaustion_cleans_losing_branches():
    class AlwaysLoses(FakeCatalog):
        def fast_forward_main(self, table, branch, expected_main):
            self.main += 1
            raise ConflictError("lost again")

    cat, audit = AlwaysLoses(), FakeAuditBranches()
    try:
        publish(cat, audit, "t", "run6", cat.commit_to_branch, passing,
                max_attempts=3, heartbeat_interval_seconds=0)
        raise AssertionError("must raise PublishContention")
    except PublishContention:
        pass
    assert not cat.branches and not audit.records


def test_spark_adapter_reads_named_updated_ref_not_column_zero():
    class Row:
        def __init__(self, values): self.values = values
        def __getitem__(self, key): return self.values[key]
        def asDict(self, recursive=False): return self.values

    class Query:
        def __init__(self, row): self.row = row
        def first(self): return self.row

    class Spark:
        def sql(self, statement):
            if statement.startswith("SELECT snapshot_id"):
                return Query(Row({"snapshot_id": 100}))
            return Query(Row({"branch_updated": "main", "previous_ref": 100,
                              "updated_ref": 201}))

    assert SparkIcebergCatalog(Spark(), "cat").fast_forward_main("db.t", "audit_x", 100) == 201


if __name__ == "__main__":
    for name, function in sorted(globals().items()):
        if name.startswith("test_") and callable(function):
            function()
            print(f"ok {name}")
    print("all wap_publish tests passed")

"""Write-Audit-Publish for curated Iceberg tables.

INVARIANT: ``main`` advances only through a validated, compare-and-swap
fast-forward — never last-writer-wins. Branch lifecycle (intent -> created ->
published/retained) lives in a durable store, not inferred from Iceberg refs,
so create, fast-forward, and cleanup stay safe under process death.
"""
from __future__ import annotations

import re
import time
import uuid
from dataclasses import dataclass, field
from threading import Event, Thread
from typing import Callable, Iterable, Mapping, Optional, Protocol, TypeVar


class ConflictError(RuntimeError):
    """``main`` moved or is no longer an ancestor of the audit branch."""


class BranchNotFound(RuntimeError):
    """Cleanup is already complete (or creation never reached Iceberg)."""


class PublishBlocked(RuntimeError):
    """Validation failed; main is unchanged and the branch is retained."""


class PublishContention(RuntimeError):
    """The publisher lost its compare-and-swap ``max_attempts`` times."""


class Catalog(Protocol):
    def main_snapshot(self, table: str) -> int: ...
    def published_snapshot_for_run(self, table: str, run_id: str) -> Optional[int]: ...
    def create_branch(self, table: str, branch: str, at_snapshot: int) -> None: ...
    def drop_branch(self, table: str, branch: str) -> None: ...
    def fast_forward_main(self, table: str, branch: str, expected_main: int) -> int: ...


@dataclass(frozen=True)
class ExpiredBranch:
    branch: str
    state: str


@dataclass
class JanitorReport:
    dropped: list[str] = field(default_factory=list)
    already_absent: list[str] = field(default_factory=list)
    failures: list[tuple[str, str]] = field(default_factory=list)


class AuditBranchStore(Protocol):
    """Transactional lease/lifecycle records (Postgres). ``register_intent``
    commits before branch DDL; ``claim_expired`` claims atomically; ``release``
    only after Iceberg confirms the branch is gone or known absent."""

    def register_intent(self, table: str, branch: str, run_id: str,
                        base_snapshot: int, created_at: float) -> None: ...
    def mark_created(self, table: str, branch: str) -> None: ...
    def heartbeat(self, table: str, branch: str, now: float) -> None: ...
    def retain_for_debugging(self, table: str, branch: str, reason: str) -> None: ...
    def mark_published(self, table: str, branch: str, snapshot_id: int) -> None: ...
    def record_cleanup_failure(self, table: str, branch: str, error: str) -> None: ...
    def release(self, table: str, branch: str) -> None: ...
    def claim_expired(self, table: str, now: float,
                      ttl_seconds: float) -> list[ExpiredBranch]: ...
    def release_expiry_claim(self, table: str, branch: str) -> None: ...


@dataclass
class CheckResult:
    name: str
    ok: bool
    detail: str = ""


@dataclass
class Report:
    results: list[CheckResult] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return all(result.ok for result in self.results)

    def failures(self) -> list[CheckResult]:
        return [result for result in self.results if not result.ok]


def forbid_float_money(schema: Mapping[str, str],
                       money_columns: Iterable[str]) -> CheckResult:
    bad = [column for column in money_columns
           if schema.get(column, "<missing>").lower() not in ("bigint", "long")]
    return CheckResult("money_is_integer_paise", not bad,
                       f"non-integer money columns: {bad}" if bad else "")


WriteFn = Callable[[str, str], None]
ValidateFn = Callable[[str], Report]
T = TypeVar("T")


def _new_branch_name(run_id: str, attempt: int) -> str:
    safe_run_id = re.sub(r"[^a-zA-Z0-9_]", "_", run_id).strip("_") or "run"
    return f"audit_{safe_run_id}_a{attempt}_{uuid.uuid4().hex[:12]}"


def _with_lease_heartbeat(action: Callable[[], T], heartbeat: Callable[[], None],
                          interval_seconds: float) -> T:
    """Keep the lease live during a long write/validation; TTL must comfortably
    exceed the interval. Process death stops the daemon -> janitor collects."""
    heartbeat()
    if interval_seconds <= 0:
        return action()
    stop = Event()
    failures: list[Exception] = []

    def renew() -> None:
        while not stop.wait(interval_seconds):
            try:
                heartbeat()
            except Exception as exc:  # action must not publish with a lost lease
                failures.append(exc)
                stop.set()
                return

    worker = Thread(target=renew, name="wap-lease-heartbeat", daemon=True)
    worker.start()
    try:
        result = action()
    finally:
        stop.set()
        worker.join(timeout=max(interval_seconds * 2, 1.0))
        # A timed-out worker can finish one already-started renewal. Its
        # callback is bound to this branch attempt by publish(), so it cannot
        # renew a later retry's lease.
    if failures:
        raise RuntimeError("audit branch lease heartbeat failed") from failures[0]
    return result


def _cleanup_branch(catalog: Catalog, branches: AuditBranchStore, table: str,
                    branch: str) -> bool:
    """Return True when cleanup converged; retain evidence on a real failure."""
    try:
        catalog.drop_branch(table, branch)
    except BranchNotFound:
        branches.release(table, branch)
        return True
    except Exception as exc:
        branches.record_cleanup_failure(table, branch, str(exc))
        return False
    branches.release(table, branch)
    return True


def publish(catalog: Catalog, branches: AuditBranchStore, table: str, run_id: str,
            write: WriteFn, validate: ValidateFn, max_attempts: int = 3,
            clock: Callable[[], float] = time.time,
            heartbeat_interval_seconds: float = 30.0) -> int:
    """Write, validate, and atomically publish one idempotent run.

    ``write(branch, run_id)`` must stamp the run id into its snapshot summary;
    every attempt first checks main's ancestry for that id, closing the crash
    window between a successful fast-forward and its acknowledgement."""
    for attempt in range(1, max_attempts + 1):
        # Check on *every* retry. Another worker using this run id may have
        # published while this worker was writing or losing its prior CAS.
        published = catalog.published_snapshot_for_run(table, run_id)
        if published is not None:
            return published
        base = catalog.main_snapshot(table)
        branch = _new_branch_name(run_id, attempt)
        branches.register_intent(table, branch, run_id, base, clock())
        try:
            catalog.create_branch(table, branch, at_snapshot=base)
            branches.mark_created(table, branch)
        except Exception as exc:
            # Creation may have succeeded despite a timeout. Preserve the
            # intent record so a later janitor can converge it safely.
            branches.retain_for_debugging(table, branch, f"create outcome unknown: {exc}")
            raise

        heartbeat = lambda branch=branch: branches.heartbeat(table, branch, clock())
        try:
            _with_lease_heartbeat(lambda: write(branch, run_id), heartbeat,
                                  heartbeat_interval_seconds)
            report = _with_lease_heartbeat(lambda: validate(branch), heartbeat,
                                           heartbeat_interval_seconds)
        except Exception as exc:
            branches.retain_for_debugging(table, branch, f"write or validation failed: {exc}")
            raise

        if not report.ok:
            branches.retain_for_debugging(
                table, branch,
                "; ".join(f"{result.name}: {result.detail}" for result in report.failures()))
            raise PublishBlocked(f"{table}: {[result.name for result in report.failures()]}")

        try:
            published_snapshot = catalog.fast_forward_main(table, branch, expected_main=base)
        except ConflictError:
            _cleanup_branch(catalog, branches, table, branch)
            continue
        except Exception as exc:
            branches.retain_for_debugging(table, branch, f"fast-forward outcome unknown: {exc}")
            raise

        # Record success before cleanup. If the process dies later, retries
        # return this run's current-main snapshot instead of writing it again.
        branches.mark_published(table, branch, published_snapshot)
        _cleanup_branch(catalog, branches, table, branch)
        return published_snapshot
    raise PublishContention(f"{table}: lost the CAS {max_attempts} times")


def expire_stale_audit_branches(catalog: Catalog, branches: AuditBranchStore,
                                table: str, now: float,
                                ttl_seconds: float) -> JanitorReport:
    """Best-effort, convergent cleanup; one bad branch never wedges a sweep."""
    report = JanitorReport()
    for record in branches.claim_expired(table, now, ttl_seconds):
        try:
            catalog.drop_branch(table, record.branch)
        except BranchNotFound:
            branches.release(table, record.branch)
            report.already_absent.append(record.branch)
        except Exception as exc:
            branches.record_cleanup_failure(table, record.branch, str(exc))
            branches.release_expiry_claim(table, record.branch)
            report.failures.append((record.branch, str(exc)))
        else:
            branches.release(table, record.branch)
            report.dropped.append(record.branch)
    return report


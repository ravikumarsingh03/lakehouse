# Deep-dive code — Lakehouse Data Platform take-home

Three correctness boundaries from the design doc (§8), implemented as
standard-library modules with typed ports; Debezium, Iceberg, Postgres,
landing storage, and alerting are declared contracts the production adapters
must satisfy. `adapters.py` is the appendix: the real Spark/Iceberg API
surface (guarded MERGE, branch DDL, `system.fast_forward`, snapshot-summary
run stamping), kept out of the core modules so the guarantee-bearing logic
stays compact.

| Module | Invariant | Non-negotiable production contract |
| --- | --- | --- |
| `cdc_merge.py` | A key reflects the latest valid source event across reorder, replay, and batch boundaries. | The source emits a total `(commit_order, event_order)`, immutable event id, and non-null primary key. Curated retains order, event id, and delete tombstones. |
| `wap_publish.py` | `main` cannot expose an unvalidated snapshot, and an uncertain publish is never blindly written twice. | Every write stamps `app.run_id`; every attempt queries current-main ancestry for it. A transactional branch registry tracks intent, created, published, retained, leases, and cleanup failures. |
| `manifest_loader.py` | A finalized source object has one initial processing attempt; changing parsing semantics is explicit and auditable. | Postgres atomically fences delivery keys and content leases, conditionally acknowledges attempts, and persists alert outbox records. Raw replaces one `source_file_id` partition. |

## Layout and size

```
cdc_merge.py / wap_publish.py / manifest_loader.py   core modules (the guarantees)
adapters.py                                           appendix: real Spark/Iceberg surface
test_*.py                                             30 executable tests (stdlib only)
reference-impl-854-lines.tar.gz                       pre-trim reference snapshot
```

The brief asks for roughly 200–400 lines. The core modules run over that
(~630 lines) and the overage is deliberate: the extra lines are crash windows,
lease fencing, and replay convergence — the places where the guarantees either
hold or don't. We chose depth on three problems over a thinner sketch of the
same three; the adapters are separated so the core reads as the submission and
the appendix as evidence the APIs are real.

## Run

```bash
python3 test_cdc_merge.py && python3 test_wap_publish.py && python3 test_manifest_loader.py
```

The 30 tests cover reordering, composite ordering, equal-position conflicts
within and across batches, null keys, cutover crash/retry, validation gates,
CAS contention, same-run duplicate firing, lost fast-forward acknowledgements,
background lease heartbeats, phantom and failed branch cleanup, named Iceberg
procedure output, source/table file scope, filename reuse, crash/replay,
version races, named reprocessing, and input-size limits. Each test docstring
states how it would fail if the design were wrong — the test plan, as code.

## Operational semantics

### CDC

- Deletes remain tombstones until no replayable source range can precede them.
- Equal positions are harmless only for the same event id and payload. A
  different event at an already applied position is source corruption, not a
  last-arrival-wins choice.
- Bootstrap persists W, starts/resumes capture from W, obtains a snapshot S
  with `S >= W`, stamps synthetic snapshot rows at `(S, -1)`, then replays
  durable captured changes. A real event at `(S, 0)` therefore wins without
  creating a false equal-position conflict. The adapter retains W through
  completion and routes invalid source records to a DLQ.

### WAP

- A publisher records branch intent before DDL. An unknown create outcome is
  retained for janitor convergence rather than assumed absent.
- Write and validation execute under a heartbeat. The configured branch TTL
  must be substantially longer than the heartbeat interval.
- `write(branch, run_id)` must stamp `app.run_id` in the Iceberg snapshot
  summary. Every attempt first searches current-main ancestors for that id,
  closing both the lost-acknowledgement window and the double-fired-run race.
- The janitor treats a missing branch as successful cleanup and continues past
  other per-branch failures. Cleanup failures remain visible in the registry.
- The Spark adapter reads procedure output by the named `updated_ref` field;
  pin and integration-test against the deployed Iceberg version.

### Manifest ingestion

- By default, `path` is the immutable delivery key. A source that legitimately
  reuses `daily.csv` must supply a stable unique `delivery_key` such as vendor
  sequence or business date.
- A delivery-key/content conflict is a delivery error, not a parsing error. A
  named reprocess cannot override it; the operator needs a new approved
  delivery key or a source-correction workflow. Identical bytes under a new
  delivery key are a duplicate load but still receive a receipt in the delivery
  ledger for completeness monitoring.
- Parser/contract changes on the initial attempt return `REPROCESS_REQUIRED`.
  A named `reprocess_id` creates a separate auditable attempt under the same
  source-file lease. The controlled attempt replaces the materialized raw file
  partition and records its attempt id; Iceberg snapshot history preserves the
  prior version — an explicit, audited exception to immutable first-landing
  semantics, not a silent rewrite (stated in design doc §3).
- Input is spooled with an explicit byte limit (5 GiB default). Production
  should stream finalized objects from landing storage with a source-specific
  limit below worker capacity.
- Row quarantine is keyed by file, attempt, and row number; alert records are
  written to the manifest outbox in the same transaction as state changes.

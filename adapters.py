"""Appendix: the real Spark/Iceberg API surface behind the core modules.

Kept separate so the guarantee-bearing logic in cdc_merge / wap_publish /
manifest_loader is distinct from deployment-specific API integration. Nothing
here is pseudocode; integration-test against the pinned Iceberg version
(design doc open question #2).
"""
from __future__ import annotations

import re
from typing import Optional

from cdc_merge import CdcContractError
from wap_publish import BranchNotFound, ConflictError


class SparkCdcMerge:
    """Spark port of ``reduce_latest`` and ``merge_batch``.

    Staged events contain ``source_commit_order``, ``source_event_order`` and
    ``source_event_id``. The adapter rejects an ambiguous position both within
    the staged batch and against the target before the MERGE; the target stores
    both order columns, ``_source_event_id``, and ``_deleted``.
    """

    AMBIGUOUS_POSITION_SQL = """
      SELECT {pk_list}, source_commit_order, normalized_event_order
      FROM (
        SELECT s.*, CASE WHEN op = 'r' THEN -1 ELSE source_event_order END
          AS normalized_event_order
        FROM {staged_batch} s
      )
      GROUP BY {pk_list}, source_commit_order, normalized_event_order
      HAVING COUNT(DISTINCT source_event_id) > 1
      LIMIT 1
    """

    TARGET_POSITION_CONFLICT_SQL = """
      SELECT 1
      FROM (
        SELECT s.*, CASE WHEN op = 'r' THEN -1 ELSE source_event_order END
          AS normalized_event_order
        FROM {staged_batch} s
      ) s
      JOIN {curated} t ON {pk_join}
        AND s.source_commit_order = t._source_commit_order
        AND s.normalized_event_order = t._source_event_order
      WHERE s.source_event_id <> t._source_event_id
      LIMIT 1
    """

    MERGE_SQL = """
    MERGE INTO {curated} t
    USING (
      SELECT * FROM (
        SELECT s.*, row_number() OVER (
          PARTITION BY {pk_list}
          ORDER BY source_commit_order DESC, normalized_event_order DESC
        ) AS rn
        FROM (
          SELECT s.*, CASE WHEN op = 'r' THEN -1 ELSE source_event_order END
            AS normalized_event_order
          FROM {staged_batch} s
        ) s
      ) WHERE rn = 1
    ) s
    ON {pk_join}
    WHEN MATCHED AND (
      s.source_commit_order > t._source_commit_order OR
      (s.source_commit_order = t._source_commit_order AND
       s.normalized_event_order > t._source_event_order)
    ) THEN UPDATE SET
      {assign_data_cols},
      t._source_commit_order = s.source_commit_order,
      t._source_event_order = s.normalized_event_order,
      t._source_event_id = s.source_event_id,
      t._deleted = (s.op = 'd')
    WHEN NOT MATCHED THEN INSERT ({data_cols}, _source_commit_order,
                                  _source_event_order, _source_event_id, _deleted)
      VALUES ({src_data_cols}, s.source_commit_order,
              s.normalized_event_order, s.source_event_id, s.op = 'd')
    """

    def __init__(self, spark, curated_table: str, pk: list[str], data_cols: list[str]):
        self.spark, self.table, self.pk, self.cols = spark, curated_table, pk, data_cols

    def run(self, staged_batch_view: str) -> None:
        values = dict(
            curated=self.table,
            staged_batch=staged_batch_view,
            pk_list=", ".join(self.pk),
            pk_join=" AND ".join(f"t.{column} = s.{column}" for column in self.pk),
            assign_data_cols=", ".join(f"t.{column} = s.{column}" for column in self.cols),
            data_cols=", ".join(self.cols),
            src_data_cols=", ".join(f"s.{column}" for column in self.cols),
        )
        if self.spark.sql(self.AMBIGUOUS_POSITION_SQL.format(**values)).take(1):
            raise CdcContractError("staged batch has conflicting source positions")
        if self.spark.sql(self.TARGET_POSITION_CONFLICT_SQL.format(**values)).take(1):
            raise CdcContractError("staged batch conflicts with an applied source position")
        self.spark.sql(self.MERGE_SQL.format(**values))


class SparkIcebergCatalog:
    """Iceberg/Spark adapter; integration-test this against the pinned version."""

    def __init__(self, spark, catalog_name: str):
        self.spark, self.catalog_name = spark, catalog_name

    @staticmethod
    def branch_identifier(table: str, branch: str) -> str:
        if not re.fullmatch(r"[A-Za-z0-9_]+", branch):
            raise ValueError(f"unsafe branch name: {branch!r}")
        return f"{table}.branch_{branch}"

    @staticmethod
    def _field(row, name: str):
        try:
            return row[name]
        except (KeyError, TypeError):
            values = row.asDict(recursive=False) if hasattr(row, "asDict") else {}
            if name not in values:
                raise RuntimeError(f"Iceberg procedure result lacks {name!r}: {values!r}")
            return values[name]

    @staticmethod
    def _sql_literal(value: str) -> str:
        return "'" + value.replace("'", "''") + "'"

    def write_to_branch(self, dataframe, table: str, branch: str, run_id: str) -> None:
        (dataframe.writeTo(self.branch_identifier(table, branch))
         .option("snapshot-property.app.run_id", run_id)
         .append())

    def main_snapshot(self, table: str) -> int:
        row = self.spark.sql(
            f"SELECT snapshot_id FROM {table}.refs WHERE name = 'main'"
        ).first()
        return int(self._field(row, "snapshot_id"))

    def published_snapshot_for_run(self, table: str, run_id: str) -> Optional[int]:
        """Find this run only when its commit is an ancestor of current main."""
        row = self.spark.sql(
            f"SELECT h.snapshot_id FROM {table}.history h "
            f"JOIN {table}.snapshots s ON h.snapshot_id = s.snapshot_id "
            f"WHERE h.is_current_ancestor "
            f"AND s.summary['app.run_id'] = {self._sql_literal(run_id)} "
            f"ORDER BY h.made_current_at DESC LIMIT 1"
        ).first()
        return None if row is None else int(self._field(row, "snapshot_id"))

    def create_branch(self, table: str, branch: str, at_snapshot: int) -> None:
        self.branch_identifier(table, branch)
        self.spark.sql(
            f"ALTER TABLE {table} CREATE BRANCH `{branch}` AS OF VERSION {at_snapshot}")

    @staticmethod
    def _is_branch_not_found(exc: Exception) -> bool:
        message = str(exc).lower()
        return "branch" in message and ("not found" in message or "does not exist" in message)

    def drop_branch(self, table: str, branch: str) -> None:
        self.branch_identifier(table, branch)
        try:
            self.spark.sql(f"ALTER TABLE {table} DROP BRANCH `{branch}`")
        except Exception as exc:
            if self._is_branch_not_found(exc):
                raise BranchNotFound(branch) from exc
            raise

    @staticmethod
    def _is_ref_conflict(exc: Exception) -> bool:
        message = str(exc).lower()
        return ("not an ancestor" in message or "cannot fast-forward" in message
                or ("reference" in message and "changed" in message))

    def fast_forward_main(self, table: str, branch: str, expected_main: int) -> int:
        if self.main_snapshot(table) != expected_main:
            raise ConflictError(f"{table}: main moved before fast-forward")
        try:
            result = self.spark.sql(
                f"CALL {self.catalog_name}.system.fast_forward("
                f"table => '{table}', branch => 'main', to => '{branch}')").first()
        except Exception as exc:
            if self._is_ref_conflict(exc):
                raise ConflictError(str(exc)) from exc
            raise
        return int(self._field(result, "updated_ref"))

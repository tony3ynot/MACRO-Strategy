"""Ingestor base class with audit-logged run lifecycle.

Subclass and implement `_execute(start, end) -> rows`. The base handles:
- Inserting an `ingestion_runs` row with status='running'
- Updating to 'success' or 'failed' on completion
- Returning IngestionResult with timing
"""
from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date, datetime, timezone

from sqlalchemy import text
from sqlalchemy.engine import Engine

from .db import make_sync_engine

logger = logging.getLogger(__name__)


@dataclass
class IngestionResult:
    source: str
    mode: str
    rows: int
    started_at: datetime
    ended_at: datetime

    @property
    def duration_seconds(self) -> float:
        return (self.ended_at - self.started_at).total_seconds()


class Ingestor(ABC):
    source: str

    def __init__(self, engine: Engine | None = None):
        self.engine = engine or make_sync_engine()

    @abstractmethod
    def _execute(self, start: date, end: date) -> int:
        """Subclass entry point. Return total rows upserted across all tables."""
        ...

    def run(self, start: date, end: date, mode: str = "backfill") -> IngestionResult:
        started_at = datetime.now(tz=timezone.utc)
        run_id = self._audit_start(mode, started_at, start, end)
        logger.info("ingestion start: source=%s mode=%s run_id=%s", self.source, mode, run_id)
        try:
            rows = self._execute(start, end)
            ended_at = datetime.now(tz=timezone.utc)
            self._audit_finish(run_id, "success", rows, ended_at, error=None)
            logger.info(
                "ingestion success: source=%s rows=%s duration=%.1fs",
                self.source, rows, (ended_at - started_at).total_seconds(),
            )
            return IngestionResult(self.source, mode, rows, started_at, ended_at)
        except Exception as exc:
            ended_at = datetime.now(tz=timezone.utc)
            self._audit_finish(run_id, "failed", 0, ended_at, error=str(exc))
            logger.exception("ingestion failed: source=%s", self.source)
            raise

    def _audit_start(
        self, mode: str, started_at: datetime, start: date, end: date
    ) -> int:
        metadata = json.dumps({"start": start.isoformat(), "end": end.isoformat()})
        with self.engine.begin() as conn:
            row = conn.execute(
                text("""
                    INSERT INTO ingestion_runs (source, mode, started_at, status, metadata)
                    VALUES (:source, :mode, :started_at, 'running', CAST(:metadata AS JSONB))
                    RETURNING id
                """),
                {
                    "source": self.source,
                    "mode": mode,
                    "started_at": started_at,
                    "metadata": metadata,
                },
            )
            return row.scalar_one()

    def _audit_finish(
        self,
        run_id: int,
        status: str,
        rows: int,
        ended_at: datetime,
        error: str | None,
    ) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                text("""
                    UPDATE ingestion_runs
                    SET status = :status,
                        rows_ingested = :rows,
                        ended_at = :ended_at,
                        error = :error
                    WHERE id = :id
                """),
                {
                    "id": run_id,
                    "status": status,
                    "rows": rows,
                    "ended_at": ended_at,
                    "error": error,
                },
            )

"""
Process-logging helpers for the ForecastAI pipeline.

Provides:
    ProcessLogger   -- records step start/end into zcube.process_log
    ListHandler     -- logging.Handler that buffers records for later retrieval
"""

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Union

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DB import (best-effort — the module still works without a database)
# ---------------------------------------------------------------------------
_files_dir = Path(__file__).resolve().parent.parent
import sys
if str(_files_dir) not in sys.path:
    sys.path.insert(0, str(_files_dir))

try:
    from db.db import get_conn
    _DB_OK = True
except Exception:
    _DB_OK = False
    logger.warning("db.db not available — ProcessLogger will only log locally")


# ═══════════════════════════════════════════════════════════════════════════
# ListHandler
# ═══════════════════════════════════════════════════════════════════════════

class ListHandler(logging.Handler):
    """
    A logging.Handler that buffers formatted log lines in memory.

    Attach to any logger; call ``get_tail(n)`` later to retrieve
    the most recent *n* lines (default 200).
    """

    def __init__(self, max_records: int = 5000):
        super().__init__()
        self._records: list[str] = []
        self._max = max_records

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            self._records.append(msg)
            # Keep bounded so long-running steps don't eat memory
            if len(self._records) > self._max:
                self._records = self._records[-self._max:]
        except Exception:
            self.handleError(record)

    def get_tail(self, n: int = 200) -> Optional[str]:
        """Return the last *n* formatted log lines joined by newlines."""
        if not self._records:
            return None
        return "\n".join(self._records[-n:])


# ═══════════════════════════════════════════════════════════════════════════
# ProcessLogger
# ═══════════════════════════════════════════════════════════════════════════

class ProcessLogger:
    """
    Thin wrapper that writes pipeline step progress into ``zcube.process_log``.

    Usage::

        pl = ProcessLogger(run_id="abc-123")
        log_id = pl.start_step("etl")
        ...
        pl.end_step(log_id, "success", rows=42000, log_tail="last few lines")
    """

    def __init__(self, config_path=None, run_id: str = ""):
        self.config_path = Path(config_path) if config_path else None
        self.run_id = run_id
        self._start_times: dict[int, datetime] = {}

    # ── helpers ──────────────────────────────────────────────────────────

    def _get_conn(self):
        """Best-effort database connection; returns None when unavailable."""
        if not _DB_OK:
            return None
        try:
            return get_conn()
        except Exception as exc:
            logger.warning("ProcessLogger: cannot connect to DB — %s", exc)
            return None

    # ── public API ───────────────────────────────────────────────────────

    def start_step(self, step_name: str) -> Optional[int]:
        """
        Record the start of a pipeline step.

        Returns the ``process_log.id`` (an int) or *None* if the DB is not
        reachable.  The returned id is passed to :meth:`end_step`.
        """
        now = datetime.now(timezone.utc)
        conn = self._get_conn()
        if conn is None:
            logger.info("[ProcessLogger] start_step(%s) — DB unavailable, skipping", step_name)
            return None

        log_id = None
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO process_log (run_id, step_name, status, started_at)
                    VALUES (%s, %s, 'running', %s)
                    RETURNING id
                    """,
                    (self.run_id, step_name, now),
                )
                row = cur.fetchone()
                log_id = row[0] if row else None
            conn.commit()
            if log_id is not None:
                self._start_times[log_id] = now
            logger.debug("ProcessLogger: step '%s' started (log_id=%s)", step_name, log_id)
        except Exception as exc:
            conn.rollback()
            logger.warning("ProcessLogger: failed to record start_step — %s", exc)
        finally:
            conn.close()

        return log_id

    def end_step(
        self,
        log_id: Optional[int],
        status: str,
        *,
        rows: Optional[int] = None,
        log_tail: Optional[str] = None,
        error: Optional[str] = None,
    ) -> None:
        """
        Mark a pipeline step as finished (``'success'`` or ``'error'``).

        Parameters
        ----------
        log_id : int | None
            Value returned by :meth:`start_step`.  If *None* (DB was
            unavailable) the method returns silently.
        status : str
            ``'success'`` or ``'error'``.
        rows : int, optional
            Number of rows processed / produced.
        log_tail : str, optional
            Last N lines of log output captured by :class:`ListHandler`.
        error : str, optional
            Error message (for ``status='error'``).
        """
        if log_id is None:
            logger.info("[ProcessLogger] end_step — no log_id, skipping DB write")
            return

        now = datetime.now(timezone.utc)
        started = self._start_times.pop(log_id, None)
        duration = (now - started).total_seconds() if started else None

        conn = self._get_conn()
        if conn is None:
            return

        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE process_log
                       SET status         = %s,
                           ended_at       = %s,
                           duration_s     = %s,
                           rows_processed = %s,
                           error_message  = %s,
                           log_tail       = %s
                     WHERE id = %s
                    """,
                    (status, now, duration, rows, error, log_tail, log_id),
                )
            conn.commit()
            logger.debug("ProcessLogger: step log_id=%s finished (%s)", log_id, status)
        except Exception as exc:
            conn.rollback()
            logger.warning("ProcessLogger: failed to record end_step — %s", exc)
        finally:
            conn.close()

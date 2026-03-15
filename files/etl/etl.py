"""
ETL Pipeline for Demand Forecasting System

Two-database architecture:
  * **Source DB** (Neon remote) — ``dp_plan.calc_dp_actual``
    Contains the raw demand actuals that are the single source of truth.
  * **Local DB** (localhost / zcube) — ``zcube.demand_actuals``
    Working copy populated by this ETL and consumed by every downstream
    pipeline step (characterization, forecasting, evaluation, API).

The extract step reads from the source DB; the load step writes to the
local DB.  If source_db is not configured the pipeline falls back to
extracting from the local DB (self-reload mode).
"""

import json
import psycopg2
import psycopg2.extras
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any
from pathlib import Path
import logging
from datetime import datetime

# Local DB helper
import sys
_files_dir = Path(__file__).resolve().parent.parent
if str(_files_dir) not in sys.path:
    sys.path.insert(0, str(_files_dir))

from db.db import get_conn, get_schema, init_schema


class ETLPipeline:
    """
    ETL pipeline that extracts demand data from a remote source database
    (dp_plan.calc_dp_actual on Neon), transforms it into a standardized
    time series format (unique_id, date, y), and loads it into the local
    working table (zcube.demand_actuals) for downstream consumers.
    """

    # Frequency mapping from config shorthand to pandas offset aliases
    FREQ_MAP = {
        "D": "D",
        "W": "W",
        "M": "MS",      # Month Start for cleaner date alignment
        "Q": "QS",
        "Y": "YS",
    }

    # Aggregation method mapping
    AGG_MAP = {
        "sum": "sum",
        "mean": "mean",
        "median": "median",
    }

    # ------------------------------------------------------------------
    # Per-account helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _is_sql_expr(value: str) -> bool:
        """Return True when *value* looks like a SQL expression rather than a
        plain column name.  Heuristic: contains a space, parenthesis, pipe, or
        arithmetic operator — any of which would be invalid in a bare identifier.
        """
        return any(c in str(value) for c in (' ', '(', ')', '|', '+', '*', '/'))

    @staticmethod
    def _apply_date_placeholders(query: str) -> str:
        """Convert ``{min_date}`` / ``{max_date}`` Python-format placeholders in
        *query* to psycopg2 ``%(min_date)s`` / ``%(max_date)s`` style so that
        date binding stays parameterised (no raw string injection).
        Unknown placeholders are left unchanged.
        """
        return query.replace("{min_date}", "%(min_date)s").replace("{max_date}", "%(max_date)s")

    def _run_hook_sql(self, conn, statements: List[str], label: str) -> None:
        """Execute *statements* on *conn*, auto-committing after each one.

        Parameters
        ----------
        conn       psycopg2 connection (must be open; caller owns close).
        statements List of SQL strings to execute in order.
        label      Short tag used in log messages (e.g. ``'pre_extract'``).
        """
        if not statements:
            return
        self.logger.info(f"Running {len(statements)} {label} hook(s)...")
        with conn.cursor() as cur:
            for i, stmt in enumerate(statements, 1):
                stmt = stmt.strip()
                if not stmt:
                    continue
                self.logger.debug("  [%s hook %d] %.120s", label, i, stmt)
                try:
                    cur.execute(stmt)
                    conn.commit()
                    self.logger.info(
                        "  [%s hook %d] OK — %d row(s) affected",
                        label, i, cur.rowcount if cur.rowcount >= 0 else 0,
                    )
                except Exception as exc:
                    conn.rollback()
                    self.logger.error(
                        "  [%s hook %d] FAILED: %s\n  SQL: %s", label, i, exc, stmt
                    )
                    raise

    def __init__(self, config_path=None):
        """
        Initialize the ETL pipeline.

        All configuration is read from the database (data_source and etl
        parameter sets).  The legacy *config_path* parameter is accepted
        but ignored.
        """
        self.logger = logging.getLogger(__name__)

        # Load config from DB (data_source + etl parameter types)
        from db.db import load_config_from_db
        self.config = load_config_from_db()

        # Extract relevant config sections
        data_src = self.config.get("data_source", {})
        self.etl_config = self.config.get("etl", {})
        self.query_config = self.etl_config.get("query", {})
        self.agg_config = self.etl_config.get("aggregation", {})

        # ── Source DB (remote) — for extraction ──
        self.source_db_config = data_src.get("source_db") or {}
        if self.source_db_config.get("host"):
            self.source_demand_table = self.source_db_config.get(
                "demand_table", "dp_plan.calc_dp_actual"
            )
            # Column mapping — allows the remote table to use different / computed names.
            # Values may be plain column names OR SQL expressions (anything containing a
            # space, parenthesis, or SQL operator is treated as an expression and used
            # verbatim in the SELECT; plain names are prefixed with the 'd.' table alias).
            src_cols = self.source_db_config.get("columns", {})
            self.src_col_item_id  = src_cols.get("item_id",  "item_id")
            self.src_col_site_id  = src_cols.get("site_id",  "site_id")
            self.src_col_date     = src_cols.get("date",     "date")
            self.src_col_qty      = src_cols.get("qty",      "qty")
            self.src_col_channel  = src_cols.get("channel",  "channel")
            # Optional SQL expression for the series key (unique_id).
            # When set, replaces the default item_id||'_'||site_id concatenation.
            self.src_col_unique_id: Optional[str] = src_cols.get("unique_id", None)
        else:
            self.source_demand_table = None
            self.src_col_unique_id   = None

        # ── Per-account ETL hooks & query overrides ───────────────────────
        # custom_extract_query: full SQL template that bypasses _build_source_extract_query().
        #   Supports {min_date} / {max_date} placeholders which are converted to psycopg2
        #   %(min_date)s / %(max_date)s style at runtime so date binding stays parameterised.
        #   Example in data_source config (JSONB):
        #     "custom_extract_query": "SELECT ... FROM acme.orders WHERE date >= {min_date}"
        self.custom_extract_query: Optional[str] = (
            data_src.get("custom_extract_query")
            or self.source_db_config.get("custom_extract_query")
            or None
        )
        # pre_extract_sql: list of SQL statements executed on the source connection
        #   *before* the extract query runs.  Use for session setup, materialized-view
        #   refreshes, or temporary staging tables.  Each statement is auto-committed.
        #   Example: ["SET search_path TO acme,public",
        #              "REFRESH MATERIALIZED VIEW acme.mv_demand_snapshot"]
        self.pre_extract_sql: List[str] = list(data_src.get("pre_extract_sql") or [])
        # post_load_sql: list of SQL statements executed on the *local* DB connection
        #   after demand_actuals has been loaded.  Use for derived-column updates,
        #   quality checks, or reporting-view rebuilds.
        #   Example: ["UPDATE zcube.demand_actuals SET channel='default' WHERE channel=''"]
        self.post_load_sql: List[str] = list(data_src.get("post_load_sql") or [])

        # ── Local DB schema name ──
        schema = get_schema()

        # ── Table names ──
        self.tables = {
            "item": f"{schema}.item",
            "site": f"{schema}.site",
            "demand_actual": f"{schema}.demand_actuals",
            "forecast_results": f"{schema}.forecast_results",
            "forecast_adjustments": f"{schema}.forecast_adjustments",
            "process_log": f"{schema}.process_log",
        }

        # Column name overrides from config (used during transform)
        self.date_column = self.query_config.get("date_column", "date")
        self.value_column = self.query_config.get("value_column", "qty")
        self.id_column = self.query_config.get("id_column", "item_id")

        # Data quality settings
        self.min_observations = self.query_config.get("min_observations", 12)
        self.min_date = self.query_config.get("min_date", None)
        self.max_date = self.query_config.get("max_date", None)

        # Aggregation settings
        self.frequency = self.agg_config.get("frequency", "M")
        self.agg_method = self.agg_config.get("method", "sum")

        # Output table
        self.output_table = self.tables.get("demand_actual", f"{schema}.demand_actuals")

        src_label = self.source_demand_table or "(local fallback)"
        self.logger.info("ETLPipeline initialized")
        self.logger.info(f"  Source : {src_label}")
        self.logger.info(f"  Output : {self.output_table}")
        if self.custom_extract_query:
            self.logger.info("  Custom extract query : YES (bypasses built-in SELECT)")
        if self.src_col_unique_id:
            self.logger.info(f"  unique_id expr       : {self.src_col_unique_id}")
        if self.pre_extract_sql:
            self.logger.info(f"  pre_extract hooks    : {len(self.pre_extract_sql)}")
        if self.post_load_sql:
            self.logger.info(f"  post_load hooks      : {len(self.post_load_sql)}")

    # ------------------------------------------------------------------
    # Schema initialisation
    # ------------------------------------------------------------------

    def init_db(self) -> None:
        """Ensure the local zcube schema and all tables exist."""
        self.logger.info("Initialising local database schema...")
        init_schema()
        self.logger.info("Database schema ready")

    # ------------------------------------------------------------------
    # Extraction
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # Source DB connection helper
    # ------------------------------------------------------------------

    def _get_source_conn(self):
        """
        Return a psycopg2 connection to the **source** database (remote Neon).
        Falls back to the local DB if source_db is not configured.
        """
        cfg = self.source_db_config
        if cfg:
            return psycopg2.connect(
                host=cfg["host"],
                port=cfg.get("port", 5432),
                database=cfg["database"],
                user=cfg["user"],
                password=cfg["password"],
                sslmode=cfg.get("sslmode", "require"),
            )
        # Fallback: use local DB
        return get_conn()

    # ------------------------------------------------------------------
    # Query builders
    # ------------------------------------------------------------------

    def _build_source_extract_query(self) -> str:
        """Build the SQL SELECT for the remote source table.

        Column-mapping values may be plain column names **or** SQL expressions.
        A value is treated as an expression when it contains a space, parenthesis,
        pipe ``||``, or arithmetic operator — it is then used verbatim in the
        SELECT without the ``d.`` table-alias prefix.  Plain names get ``d.``
        prepended automatically.

        Per-account customisation points (set via ``data_source`` config):

        ``columns.unique_id``
            SQL expression that produces the series key. When omitted the
            default ``CAST(item_id AS TEXT) || '_' || CAST(site_id AS TEXT)``
            concat is used.

        ``columns.qty``
            Plain name or expression for the demand quantity column.
            ``ABS()`` is applied automatically only for plain column references;
            expressions are used verbatim so the account can embed its own
            arithmetic (e.g. ``"qty_cases * 12"``).

        Any other column (item_id, site_id, date, channel) also accepts SQL
        expressions following the same rule.
        """
        tbl = self.source_demand_table

        def _e(col_value: str) -> str:
            """Prefix *col_value* with 'd.' unless it is already an expression."""
            return col_value if self._is_sql_expr(col_value) else f"d.{col_value}"

        col_item = self.src_col_item_id
        col_site = self.src_col_site_id
        col_date = self.src_col_date
        col_qty  = self.src_col_qty
        col_chan  = self.src_col_channel

        item_expr    = _e(col_item)
        site_expr    = _e(col_site)
        date_expr    = _e(col_date)
        channel_expr = _e(col_chan) if col_chan else "''"

        # Quantity: auto-wrap with ABS() for plain column refs; trust expressions.
        if self._is_sql_expr(col_qty):
            qty_expr = _e(col_qty)
        else:
            qty_expr = f"ABS({_e(col_qty)})"

        # unique_id: use custom expression or fall back to item||'_'||site concat.
        if self.src_col_unique_id:
            unique_id_expr = self.src_col_unique_id   # already a SQL expression
        else:
            unique_id_expr = (
                f"CAST({item_expr} AS TEXT) || '_' || CAST({site_expr} AS TEXT)"
            )

        where_clauses: List[str] = []
        if self.min_date:
            where_clauses.append(f"({date_expr}) >= %(min_date)s")
        if self.max_date:
            where_clauses.append(f"({date_expr}) <= %(max_date)s")
        where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

        query = f"""
            SELECT
                {unique_id_expr}  AS unique_id,
                {item_expr}       AS item_id,
                {site_expr}       AS site_id,
                {channel_expr}    AS channel,
                {date_expr}       AS date,
                {qty_expr}        AS y,
                ''                AS item_name,
                ''                AS site_name
            FROM {tbl} d
            {where_sql}
            ORDER BY 1, {date_expr}
        """
        return query

    def _build_local_extract_query(self) -> str:
        """
        Fallback query when source_db is not configured — reads from the
        local zcube.demand_actuals table (self-reload mode).
        """
        demand_table = self.tables.get("demand_actual", "zcube.demand_actuals")
        item_table   = self.tables.get("item",           "zcube.item")
        site_table   = self.tables.get("site",           "zcube.site")

        where_clauses: List[str] = []
        if self.min_date:
            where_clauses.append(f"d.{self.date_column} >= %(min_date)s")
        if self.max_date:
            where_clauses.append(f"d.{self.date_column} <= %(max_date)s")
        where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

        query = f"""
            SELECT
                COALESCE(d.unique_id,
                         CAST(d.item_id AS TEXT) || '_' || CAST(d.site_id AS TEXT)
                ) AS unique_id,
                d.item_id,
                d.site_id,
                d.channel,
                d.{self.date_column}  AS date,
                ABS(d.{self.value_column}) AS y,
                COALESCE(d.item_name, i.name, '') AS item_name,
                COALESCE(d.site_name, s.name, '') AS site_name
            FROM {demand_table} d
            LEFT JOIN {item_table} i ON d.item_id = i.id
            LEFT JOIN {site_table} s ON d.site_id = s.id
            {where_sql}
            ORDER BY unique_id, d.{self.date_column}
        """
        return query

    # ------------------------------------------------------------------
    # Extraction
    # ------------------------------------------------------------------

    def extract(self) -> pd.DataFrame:
        """
        Extract demand data.

        When ``source_db`` is configured, connects to the remote Neon
        database and reads from ``dp_plan.calc_dp_actual``.  Otherwise
        falls back to the local ``zcube.demand_actuals`` table.

        Returns:
            Raw DataFrame with columns:
            unique_id, item_id, site_id, channel, date, y, item_name, site_name
        """
        params: Dict[str, Any] = {}
        if self.min_date:
            params["min_date"] = self.min_date
        if self.max_date:
            params["max_date"] = self.max_date

        use_remote = bool(self.source_db_config and self.source_db_config.get("host"))
        if self.custom_extract_query:
            # Per-account custom SQL: convert {min_date}/{max_date} placeholders
            # to psycopg2 %(name)s style so parameterised binding is preserved.
            query = self._apply_date_placeholders(self.custom_extract_query)
            label = "custom_extract_query"
        elif use_remote:
            query = self._build_source_extract_query()
            label = f"remote ({self.source_demand_table})"
        else:
            query = self._build_local_extract_query()
            label = f"local ({self.output_table})"

        self.logger.info(f"Extracting from {label}...")
        self.logger.debug(f"Query:\n{query}")

        conn = self._get_source_conn()
        try:
            # ── Pre-extract hooks (e.g. session setup, mat-view refresh) ──
            if self.pre_extract_sql:
                self.logger.info("--- PRE-EXTRACT HOOKS ---")
                self._run_hook_sql(conn, self.pre_extract_sql, "pre_extract")

            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(query, params or None)
                rows = cur.fetchall()
                cols = [desc[0] for desc in cur.description]
            df = pd.DataFrame(rows, columns=cols)
        except Exception as e:
            self.logger.error(f"Extraction from {label} failed: {e}")
            raise
        finally:
            conn.close()

        self.logger.info(
            f"Extracted {len(df):,} rows, "
            f"{df['unique_id'].nunique() if not df.empty else 0:,} unique series"
        )
        return df

    # ------------------------------------------------------------------
    # Transformation
    # ------------------------------------------------------------------

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform raw extracted data into the standardized time series format.

        Preserves extra columns (item_id, site_id, channel, item_name, site_name)
        alongside the required (unique_id, date, y) so they can be written
        back to zcube.demand_actuals.

        Steps:
            1. Rename source columns to standard names.
            2. Parse and validate date column.
            3. Cast value column to numeric.
            4. Apply date range filters.
            5. Aggregate to the configured frequency.
            6. Filter series with too few observations.
            7. Sort by (unique_id, date).
        """
        self.logger.info("Starting transformation...")
        initial_rows = len(df)
        initial_series = df["unique_id"].nunique() if not df.empty else 0

        # --- 1. Standardize column names ---
        rename_map: Dict[str, str] = {}
        if self.date_column in df.columns and self.date_column != "date":
            rename_map[self.date_column] = "date"
        if self.value_column in df.columns and self.value_column != "y":
            rename_map[self.value_column] = "y"
        if rename_map:
            df = df.rename(columns=rename_map)

        # Verify required columns exist
        required = {"unique_id", "date", "y"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(
                f"Missing required columns after rename: {missing}. "
                f"Available columns: {list(df.columns)}"
            )

        # Extra dimension columns to carry through
        extra_cols = [c for c in ["item_id", "site_id", "channel", "item_name", "site_name"]
                      if c in df.columns]

        # --- 2. Parse dates ---
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        null_dates = df["date"].isna().sum()
        if null_dates > 0:
            self.logger.warning(f"Dropping {null_dates:,} rows with unparseable dates")
            df = df.dropna(subset=["date"])

        # --- 3. Cast value to numeric ---
        df["y"] = pd.to_numeric(df["y"], errors="coerce")
        null_values = df["y"].isna().sum()
        if null_values > 0:
            self.logger.info(
                f"Dropping {null_values:,} rows with null/non-numeric values"
            )
            df = df.dropna(subset=["y"])

        # --- 4. Apply date range filter ---
        if self.min_date:
            min_dt = pd.to_datetime(self.min_date)
            before = len(df)
            df = df[df["date"] >= min_dt]
            dropped = before - len(df)
            if dropped > 0:
                self.logger.info(f"Date filter (>= {self.min_date}): dropped {dropped:,} rows")

        if self.max_date:
            max_dt = pd.to_datetime(self.max_date)
            before = len(df)
            df = df[df["date"] <= max_dt]
            dropped = before - len(df)
            if dropped > 0:
                self.logger.info(f"Date filter (<= {self.max_date}): dropped {dropped:,} rows")

        # --- 5. Aggregate to target frequency ---
        pd_freq = self.FREQ_MAP.get(self.frequency, "MS")
        agg_func = self.AGG_MAP.get(self.agg_method, "sum")

        self.logger.info(
            f"Aggregating to frequency={self.frequency} ({pd_freq}), method={agg_func}"
        )

        # Build aggregation dict: y is aggregated; extras use first value
        agg_dict: Dict[str, Any] = {"y": (agg_func)}
        extra_agg = {col: "first" for col in extra_cols}

        df = (
            df
            .groupby(["unique_id", pd.Grouper(key="date", freq=pd_freq)])
            .agg(y=("y", agg_func), **{col: (col, "first") for col in extra_cols})
            .reset_index()
        )

        # --- 6. Filter by minimum observations ---
        obs_counts = df.groupby("unique_id").size()
        sufficient = obs_counts[obs_counts >= self.min_observations].index
        insufficient_count = len(obs_counts) - len(sufficient)

        if insufficient_count > 0:
            self.logger.info(
                f"Filtering series with < {self.min_observations} observations: "
                f"removing {insufficient_count:,} series"
            )
        df = df[df["unique_id"].isin(sufficient)].copy()

        # --- 7. Sort ---
        df = df.sort_values(["unique_id", "date"]).reset_index(drop=True)

        final_rows = len(df)
        final_series = df["unique_id"].nunique()
        self.logger.info(
            f"Transformation complete: "
            f"{initial_rows:,} -> {final_rows:,} rows, "
            f"{initial_series:,} -> {final_series:,} series"
        )

        return df

    # ------------------------------------------------------------------
    # Loading
    # ------------------------------------------------------------------

    def _load_to_db(self, df: pd.DataFrame) -> int:
        """
        Bulk-insert the transformed DataFrame into zcube.demand_actuals.
        Uses TRUNCATE + execute_values for a clean reload on each ETL run.

        Returns:
            Number of rows inserted.
        """
        demand_table = self.tables.get("demand_actual", "zcube.demand_actuals")

        # Columns we write — must match zcube.demand_actuals schema
        db_cols = ["item_id", "site_id", "channel", "date", "qty", "item_name", "site_name", "unique_id"]

        # Map from transformed df columns to DB columns
        col_map = {
            "item_id":   "item_id",
            "site_id":   "site_id",
            "channel":   "channel",
            "date":      "date",
            "y":         "qty",
            "item_name": "item_name",
            "site_name": "site_name",
            "unique_id": "unique_id",
        }

        # Build rows list
        rows = []
        for _, row in df.iterrows():
            item_id = int(row.get("item_id", 0) or 0)
            site_id = int(row.get("site_id", 0) or 0)
            # Skip orphan rows early (item_id/site_id = 0 means no FK match)
            if item_id == 0 or site_id == 0:
                continue
            rows.append((
                item_id,
                site_id,
                str(row.get("channel", "") or ""),
                row["date"].date() if hasattr(row["date"], "date") else row["date"],
                abs(float(row["y"])),   # enforce non-negative demand (ABS safety net)
                str(row.get("item_name", "") or ""),
                str(row.get("site_name", "") or ""),
                str(row["unique_id"]),
            ))

        if not rows:
            self.logger.warning("No rows to insert into demand_actuals")
            return 0

        schema = get_schema()
        item_table = f"{schema}.item"
        site_table = f"{schema}.site"

        conn = get_conn()
        try:
            with conn.cursor() as cur:
                # Truncate for a clean reload
                self.logger.info(f"Truncating {demand_table}...")
                cur.execute(f"TRUNCATE TABLE {demand_table}")

                # Bulk insert
                insert_sql = f"""
                    INSERT INTO {demand_table}
                        (item_id, site_id, channel, date, qty, item_name, site_name, unique_id)
                    VALUES %s
                """
                self.logger.info(f"Inserting {len(rows):,} rows into {demand_table}...")
                psycopg2.extras.execute_values(cur, insert_sql, rows, page_size=5000)

                # Back-fill item_name / site_name from the dimension tables.
                # Source-DB rows are extracted with empty name strings; this
                # UPDATE resolves them from the freshly loaded item/site tables.
                self.logger.info("Back-filling item_name / site_name from dimension tables...")
                cur.execute(f"""
                    UPDATE {demand_table} da
                    SET item_name = COALESCE(NULLIF(da.item_name, ''), i.name, ''),
                        site_name = COALESCE(NULLIF(da.site_name, ''), s.name, '')
                    FROM {item_table} i, {site_table} s
                    WHERE da.item_id = i.id
                      AND da.site_id = s.id
                      AND (da.item_name = '' OR da.site_name = '')
                """)
                filled = cur.rowcount
                self.logger.info(f"Back-filled names for {filled:,} row(s)")

                # Remove orphan rows that have no matching item or site in the
                # dimension tables (item_id = 0, NULL, or FK not found).
                # These rows have no business meaning and would pollute forecasts.
                self.logger.info("Removing orphan rows with no matching item or site...")
                cur.execute(f"""
                    DELETE FROM {demand_table}
                    WHERE item_id IS NULL
                       OR item_id = 0
                       OR site_id IS NULL
                       OR site_id = 0
                       OR item_id NOT IN (SELECT id FROM {item_table} WHERE id IS NOT NULL)
                       OR site_id NOT IN (SELECT id FROM {site_table} WHERE id IS NOT NULL)
                """)
                deleted = cur.rowcount
                self.logger.info(f"Removed {deleted:,} orphan row(s) with no matching item/site")

            conn.commit()
            self.logger.info(f"Inserted {len(rows):,} rows into {demand_table}")
            # After a successful load, record per-series data hashes for incremental
            # processing.  Non-fatal: a hash failure must not block the ETL result.
            try:
                self._upsert_series_hashes(df)
            except Exception as _hash_err:
                self.logger.warning(f"Series hash upsert failed (non-fatal): {_hash_err}")
            return len(rows)
        except Exception as e:
            conn.rollback()
            self.logger.error(f"DB load failed: {e}", exc_info=True)
            raise
        finally:
            conn.close()

    def _upsert_series_hashes(self, df: pd.DataFrame) -> None:
        """
        Compute an MD5 fingerprint per series from its sorted (date, qty) values
        and upsert into zcube.series_hashes.  Used by incremental processing to
        skip series that haven't changed since the last forecast run.
        """
        import hashlib
        schema = get_schema()
        table = f"{schema}.series_hashes"
        rows = []
        for uid, group in df.groupby('unique_id'):
            sorted_vals = group.sort_values('date')[['date', 'y']].values
            hash_input = '|'.join(f"{r[0]},{float(r[1]):.6f}" for r in sorted_vals)
            data_hash = hashlib.md5(hash_input.encode()).hexdigest()
            rows.append((str(uid), data_hash))
        if not rows:
            return
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                # Include forecast_hash column (NULL on fresh insert) to match the
                # 4-column table schema: (unique_id, data_hash, forecast_hash, hashed_at)
                psycopg2.extras.execute_values(
                    cur,
                    f"""INSERT INTO {table} (unique_id, data_hash, forecast_hash, hashed_at)
                        VALUES %s
                        ON CONFLICT (unique_id) DO UPDATE
                          SET data_hash  = EXCLUDED.data_hash,
                              hashed_at  = NOW()""",
                    [(uid, dh, None, None) for uid, dh in rows],
                )
            conn.commit()
            self.logger.info(f"Upserted {len(rows):,} series hashes into {table}")
        except Exception as e:
            conn.rollback()
            self.logger.warning(f"Could not upsert series hashes: {e}")
        finally:
            conn.close()

    # Known structural columns in the local item/site tables.
    # Any source columns beyond these are gathered into `attributes` JSONB.
    _DIM_KNOWN_COLS = {"id", "xuid", "name", "description", "attributes", "type_id"}

    def _extract_dimension_rows(self, src_conn, src_table: str, dim: str,
                                type_names: dict = None):
        """
        Read all rows from a source dimension table and return them as a
        list of tuples ready for INSERT into the local item/site table.

        Extra columns (beyond id, xuid, name, description, type_id) are
        always collected into the ``attributes`` JSONB dict.  If the source
        table already has an ``attributes`` JSONB column its content is used
        as the base and the extra columns are merged on top of it.

        If *type_names* is provided (a {type_id -> name} dict) the resolved
        ``type_name`` is added to the attributes automatically.

        Returns
        -------
        (rows, extra_cols) where rows is a list of
            (id, xuid, name, description, attributes_json, type_id)
        and extra_cols is the list of column names gathered into attributes.
        """
        if type_names is None:
            type_names = {}

        with src_conn.cursor(
            cursor_factory=psycopg2.extras.DictCursor
        ) as cur:
            # Discover what columns the source table actually has
            cur.execute(f"SELECT * FROM {src_table} LIMIT 0")
            src_columns = [desc[0] for desc in cur.description]

            has_native_attrs = "attributes" in src_columns

            # Extra columns = anything beyond the known structural ones
            extra_cols = [
                c for c in src_columns if c not in self._DIM_KNOWN_COLS
            ]

            if extra_cols:
                self.logger.info(
                    f"    Source {src_table}: gathering {len(extra_cols)} "
                    f"extra column(s) into attributes JSONB: {extra_cols}"
                )

            # Fetch all rows (SELECT *)
            cur.execute(f"SELECT * FROM {src_table}")
            raw = cur.fetchall()

            rows = []
            for r in raw:
                # Start from existing attributes JSONB (if present)
                base = {}
                if has_native_attrs and r.get("attributes"):
                    val = r["attributes"]
                    if isinstance(val, dict):
                        base = val
                    elif isinstance(val, str):
                        try:
                            base = json.loads(val)
                        except Exception:
                            base = {}

                # Merge extra columns on top
                for col in extra_cols:
                    val = r.get(col)
                    if val is not None:
                        if isinstance(val, (int, float, bool, str)):
                            base[col] = val
                        else:
                            base[col] = str(val)

                # Resolve type_id -> type_name
                tid = r.get("type_id")
                if tid is not None and tid in type_names:
                    base["type_name"] = type_names[tid]

                rows.append(
                    (
                        r["id"],
                        r.get("xuid"),
                        r.get("name"),
                        r.get("description"),
                        psycopg2.extras.Json(base) if base else None,
                        r.get("type_id"),
                    )
                )

            return rows, extra_cols

    # Columns in plan.site that are useful as site attributes for
    # segmentation (beyond the standard id/xuid/name/description/type_id).
    _PLAN_SITE_EXTRA = {
        "address", "city", "state", "postal_code",
        "latitude", "longitude", "country_id",
    }

    def _merge_plan_attributes(self, src_conn, dst_conn, local_schema: str):
        """
        Enrich local item/site attributes from ``plan.item`` / ``plan.site``.

        The ``plan`` schema tables often carry a richly-populated ``attributes``
        JSONB column (e.g. Product, Country_of_origin, level1–5, Quality …)
        plus extra structural columns (address, city, …).

        For every local item/site that has a matching ``xuid`` in the plan
        schema, the plan attributes are merged **under** the existing local
        attributes (i.e. local values take precedence in case of key clash).
        Empty-list values (``[]``) in the plan JSONB are skipped.
        """
        for dim, plan_table, extra_cols in [
            ("item", "plan.item", set()),
            ("site", "plan.site", self._PLAN_SITE_EXTRA),
        ]:
            try:
                # Check if plan table exists
                with src_conn.cursor() as chk:
                    chk.execute(
                        "SELECT 1 FROM information_schema.tables "
                        "WHERE table_schema = 'plan' AND table_name = %s",
                        (dim,),
                    )
                    if not chk.fetchone():
                        self.logger.info(
                            f"  {plan_table} does not exist — "
                            f"skipping attribute enrichment for {dim}"
                        )
                        continue

                # Discover available columns
                with src_conn.cursor() as cur:
                    cur.execute(f"SELECT * FROM {plan_table} LIMIT 0")
                    plan_cols = {desc[0] for desc in cur.description}

                usable_extra = sorted(extra_cols & plan_cols)

                # Build SELECT for plan table
                sel_parts = ["xuid", "attributes"]
                sel_parts.extend(usable_extra)
                sel_sql = ", ".join(sel_parts)

                with src_conn.cursor(
                    cursor_factory=psycopg2.extras.DictCursor
                ) as cur:
                    cur.execute(
                        f"SELECT {sel_sql} FROM {plan_table} "
                        f"WHERE xuid IS NOT NULL"
                    )
                    plan_rows = cur.fetchall()

                # Build xuid -> merged-attrs dict
                plan_attrs_by_xuid = {}
                for pr in plan_rows:
                    xuid = pr["xuid"]
                    merged = {}

                    # Plan JSONB attributes
                    raw_attrs = pr.get("attributes")
                    if raw_attrs:
                        if isinstance(raw_attrs, dict):
                            d = raw_attrs
                        elif isinstance(raw_attrs, str):
                            try:
                                d = json.loads(raw_attrs)
                            except Exception:
                                d = {}
                        else:
                            d = {}

                        for k, v in d.items():
                            # Skip empty lists / None
                            if v is None or v == [] or v == "":
                                continue
                            if isinstance(v, (int, float, bool, str)):
                                merged[k] = v
                            elif isinstance(v, list):
                                # list of strings -> join
                                merged[k] = ", ".join(str(x) for x in v)
                            else:
                                merged[k] = str(v)

                    # Extra structural columns
                    for col in usable_extra:
                        val = pr.get(col)
                        if val is not None:
                            if isinstance(val, (int, float, bool, str)):
                                merged[col] = val
                            else:
                                merged[col] = str(val)

                    if merged:
                        plan_attrs_by_xuid[xuid] = merged

                if not plan_attrs_by_xuid:
                    self.logger.info(
                        f"  {plan_table}: no enrichable attributes found"
                    )
                    continue

                self.logger.info(
                    f"  {plan_table}: enriching {len(plan_attrs_by_xuid):,} "
                    f"{dim}(s) with plan attributes"
                )

                # Read current local attributes and merge
                with dst_conn.cursor(
                    cursor_factory=psycopg2.extras.DictCursor
                ) as cur:
                    cur.execute(
                        f"SELECT id, xuid, attributes "
                        f"FROM {local_schema}.{dim}"
                    )
                    local_rows = cur.fetchall()

                updates = []
                for lr in local_rows:
                    xuid = lr.get("xuid")
                    if not xuid or xuid not in plan_attrs_by_xuid:
                        continue

                    plan_a = plan_attrs_by_xuid[xuid]
                    local_a = lr.get("attributes") or {}
                    if isinstance(local_a, str):
                        try:
                            local_a = json.loads(local_a)
                        except Exception:
                            local_a = {}

                    # Plan attrs go first, local overrides on top
                    combined = {**plan_a, **local_a}
                    if combined != local_a:
                        updates.append(
                            (psycopg2.extras.Json(combined), lr["id"])
                        )

                if updates:
                    with dst_conn.cursor() as cur:
                        psycopg2.extras.execute_batch(
                            cur,
                            f"UPDATE {local_schema}.{dim} "
                            f"SET attributes = %s WHERE id = %s",
                            updates,
                        )
                    dst_conn.commit()
                    self.logger.info(
                        f"    -> {len(updates):,} {dim}(s) enriched "
                        f"with plan attributes"
                    )
                else:
                    self.logger.info(
                        f"    -> no {dim} updates needed (already enriched)"
                    )

            except Exception as exc:
                self.logger.warning(
                    f"  Could not enrich {dim} from {plan_table}: {exc}"
                )

    def _load_dimension_tables(self) -> None:
        """
        Extract dimension data from the source DB and load into the local
        zcube schema.

        Extracted tables (source -> local):
            dp_plan.dp_item_type  ->  zcube.item_type
            dp_plan.dp_site_type  ->  zcube.site_type
            dp_plan.dp_item       ->  zcube.item
            dp_plan.dp_site       ->  zcube.site

        Uses INSERT … ON CONFLICT DO UPDATE (upsert) so the method is safe
        to call repeatedly.  Skips silently when source_db is not configured.
        """
        if not self.source_db_config:
            self.logger.info(
                "No source_db configured — skipping dimension table load"
            )
            return

        # Derive the source schema from the demand table name
        # e.g. "dp_plan.calc_dp_actual"  ->  "dp_plan"
        src_schema = (
            self.source_demand_table.split(".")[0]
            if self.source_demand_table and "." in self.source_demand_table
            else "dp_plan"
        )

        item_type_src = f"{src_schema}.dp_item_type"
        site_type_src = f"{src_schema}.dp_site_type"
        item_src      = f"{src_schema}.dp_item"
        site_src      = f"{src_schema}.dp_site"

        local_schema = get_schema()

        self.logger.info(
            f"Loading dimension tables from {src_schema} "
            f"-> {local_schema}..."
        )

        src_conn = self._get_source_conn()
        dst_conn = get_conn()

        try:
            # ── Item types ────────────────────────────────────────────────
            self.logger.info(
                f"  {item_type_src} -> {local_schema}.item_type"
            )
            with src_conn.cursor(
                cursor_factory=psycopg2.extras.DictCursor
            ) as cur:
                cur.execute(
                    f"SELECT id, xuid, name, description FROM {item_type_src}"
                )
                item_type_rows = cur.fetchall()

            with dst_conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    f"""
                    INSERT INTO {local_schema}.item_type
                        (id, xuid, name, description)
                    VALUES %s
                    ON CONFLICT (id) DO UPDATE SET
                        xuid        = EXCLUDED.xuid,
                        name        = EXCLUDED.name,
                        description = EXCLUDED.description
                    """,
                    [
                        (r["id"], r["xuid"], r["name"], r["description"])
                        for r in item_type_rows
                    ],
                )
            dst_conn.commit()
            self.logger.info(
                f"    -> {len(item_type_rows):,} item type(s) upserted"
            )

            # ── Site types ────────────────────────────────────────────────
            self.logger.info(
                f"  {site_type_src} -> {local_schema}.site_type"
            )
            with src_conn.cursor(
                cursor_factory=psycopg2.extras.DictCursor
            ) as cur:
                cur.execute(
                    f"SELECT id, xuid, name, description FROM {site_type_src}"
                )
                site_type_rows = cur.fetchall()

            with dst_conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    f"""
                    INSERT INTO {local_schema}.site_type
                        (id, xuid, name, description)
                    VALUES %s
                    ON CONFLICT (id) DO UPDATE SET
                        xuid        = EXCLUDED.xuid,
                        name        = EXCLUDED.name,
                        description = EXCLUDED.description
                    """,
                    [
                        (r["id"], r["xuid"], r["name"], r["description"])
                        for r in site_type_rows
                    ],
                )
            dst_conn.commit()
            self.logger.info(
                f"    -> {len(site_type_rows):,} site type(s) upserted"
            )

            # ── Build type-name lookups so we can enrich attributes ─────
            type_name_lookup = {}
            for type_tbl, prefix in [(item_type_src, "item"), (site_type_src, "site")]:
                try:
                    with src_conn.cursor(
                        cursor_factory=psycopg2.extras.DictCursor
                    ) as tcur:
                        tcur.execute(f"SELECT id, name FROM {type_tbl}")
                        type_name_lookup[prefix] = {
                            r["id"]: r["name"] for r in tcur.fetchall()
                        }
                except Exception:
                    type_name_lookup[prefix] = {}

            # ── Items ─────────────────────────────────────────────────────
            self.logger.info(f"  {item_src} -> {local_schema}.item")
            item_rows, item_extra = self._extract_dimension_rows(
                src_conn, item_src, "item",
                type_names=type_name_lookup.get("item", {}),
            )

            with dst_conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    f"""
                    INSERT INTO {local_schema}.item
                        (id, xuid, name, description, attributes, type_id)
                    VALUES %s
                    ON CONFLICT (id) DO UPDATE SET
                        xuid        = EXCLUDED.xuid,
                        name        = EXCLUDED.name,
                        description = EXCLUDED.description,
                        attributes  = EXCLUDED.attributes,
                        type_id     = EXCLUDED.type_id
                    """,
                    item_rows,
                )
            dst_conn.commit()
            self.logger.info(
                f"    -> {len(item_rows):,} item(s) upserted"
            )
            if item_extra:
                self.logger.info(
                    f"    -> extra columns gathered into attributes: {item_extra}"
                )

            # ── Sites ─────────────────────────────────────────────────────
            self.logger.info(f"  {site_src} -> {local_schema}.site")
            site_rows, site_extra = self._extract_dimension_rows(
                src_conn, site_src, "site",
                type_names=type_name_lookup.get("site", {}),
            )

            with dst_conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    f"""
                    INSERT INTO {local_schema}.site
                        (id, xuid, name, description, attributes, type_id)
                    VALUES %s
                    ON CONFLICT (id) DO UPDATE SET
                        xuid        = EXCLUDED.xuid,
                        name        = EXCLUDED.name,
                        description = EXCLUDED.description,
                        attributes  = EXCLUDED.attributes,
                        type_id     = EXCLUDED.type_id
                    """,
                    site_rows,
                )
            dst_conn.commit()
            self.logger.info(
                f"    -> {len(site_rows):,} site(s) upserted"
            )
            if site_extra:
                self.logger.info(
                    f"    -> extra columns gathered into attributes: {site_extra}"
                )

            # ── Enrich attributes from plan.item / plan.site ────────────
            # The plan schema (plan.item, plan.site) often contains a
            # richly populated attributes JSONB (e.g. Product, level1-5,
            # Country_of_origin, etc.) plus extra columns (address, city).
            # We merge those into the local attributes so they are
            # available for segmentation criteria.
            self._merge_plan_attributes(src_conn, dst_conn, local_schema)

            self.logger.info("Dimension tables loaded successfully")

        except Exception as e:
            dst_conn.rollback()
            self.logger.error(
                f"Dimension table load failed: {e}", exc_info=True
            )
            raise
        finally:
            src_conn.close()
            dst_conn.close()

    def load(self, df: pd.DataFrame) -> Path:
        """
        Write the transformed DataFrame to PostgreSQL (zcube.demand_actuals).

        After the bulk load completes any ``post_load_sql`` hooks configured in
        the account's ``data_source`` parameters are executed on the local DB
        connection.  This lets per-account logic update derived columns, rebuild
        reporting views, or run data-quality assertions after every ETL run.

        Args:
            df: Transformed DataFrame.

        Returns:
            The DataFrame (for downstream pipeline consumption).
        """
        rows_inserted = self._load_to_db(df)
        self.logger.info(
            f"Loaded {rows_inserted:,} rows into {self.output_table}"
        )

        # ── Post-load hooks (e.g. derived-column updates, view rebuilds) ──
        if self.post_load_sql:
            self.logger.info("--- POST-LOAD HOOKS ---")
            conn = get_conn()
            try:
                self._run_hook_sql(conn, self.post_load_sql, "post_load")
            finally:
                conn.close()

        return df

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def run(self) -> pd.DataFrame:
        """
        Execute the full ETL pipeline: Init DB -> Extract -> Transform -> Load.

        Returns:
            The final standardized DataFrame.
        """
        start_time = datetime.now()
        self.logger.info("=" * 80)
        self.logger.info("ETL PIPELINE START")
        self.logger.info(f"  Timestamp : {start_time.isoformat()}")
        src_cfg = self.source_db_config
        if src_cfg.get("host"):
            self.logger.info(
                f"  Source    : {src_cfg['host']}:"
                f"{src_cfg.get('port', 5432)}/{src_cfg.get('database', '?')}"
            )
        else:
            self.logger.info("  Source    : local fallback (no source_db configured)")
        self.logger.info(f"  Tables    : {list(self.tables.values())}")
        self.logger.info(f"  Frequency : {self.frequency}")
        self.logger.info(f"  Output    : {self.output_table}")
        self.logger.info("=" * 80)

        try:
            # 0. Ensure DB schema exists
            self.logger.info("\n--- INIT DB ---")
            self.init_db()

            # 0b. Load dimension tables (item_type, site_type, item, site)
            self.logger.info("\n--- DIMENSION TABLES ---")
            self._load_dimension_tables()

            # 1. Extract
            self.logger.info("\n--- EXTRACT ---")
            raw_df = self.extract()

            # 2. Transform
            self.logger.info("\n--- TRANSFORM ---")
            clean_df = self.transform(raw_df)

            # 3. Load
            self.logger.info("\n--- LOAD ---")
            self.load(clean_df)

            # Summary
            elapsed = (datetime.now() - start_time).total_seconds()
            self.logger.info("\n" + "=" * 80)
            self.logger.info("ETL PIPELINE COMPLETE")
            self.logger.info(f"  Duration        : {elapsed:.1f}s")
            self.logger.info(f"  Total rows      : {len(clean_df):,}")
            self.logger.info(f"  Total series    : {clean_df['unique_id'].nunique():,}")
            if not clean_df.empty:
                self.logger.info(
                    f"  Date range      : {clean_df['date'].min()} to {clean_df['date'].max()}"
                )
            self.logger.info(f"  Output table    : {self.output_table}")
            self.logger.info("=" * 80)

            return clean_df

        except Exception as e:
            self.logger.error(f"ETL pipeline failed: {e}", exc_info=True)
            raise


# --------------------------------------------------------------------------
# Factory — returns the correct ETL class for the current account config
# --------------------------------------------------------------------------

def create_etl_pipeline(config_path=None) -> "ETLPipeline":
    """
    Instantiate and return the appropriate ETL pipeline class for the current
    account's ``data_source`` configuration.

    When ``data_source.source_type == "excel"`` the ``ExcelETLPipeline``
    adapter (``etl/adapters/excel_etl.py``) is returned; otherwise the
    default ``ETLPipeline`` is used.

    This factory is the **recommended entry point** for all callers that
    previously instantiated ``ETLPipeline()`` directly.
    """
    # Quick-read of data_source to determine the source_type before __init__
    try:
        from db.db import load_config_from_db
        config = load_config_from_db()
        source_type = config.get("data_source", {}).get("source_type", "").lower()
    except Exception:
        source_type = ""

    if source_type == "excel":
        from etl.adapters.excel_etl import ExcelETLPipeline
        return ExcelETLPipeline(config_path=config_path)

    return ETLPipeline(config_path=config_path)


# --------------------------------------------------------------------------
# Convenience entry point
# --------------------------------------------------------------------------

def main():
    """Run the ETL pipeline from the command line."""
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    parser = argparse.ArgumentParser(description="ETL pipeline step")
    parser.add_argument(
        "--scenario-id", type=int, default=1,
        help="Forecast scenario ID (default=1, the base scenario)",
    )
    args = parser.parse_args()
    scenario_id = args.scenario_id

    # For non-base scenarios, ETL is a no-op: demand_actuals is already loaded.
    if scenario_id != 1:
        logging.getLogger(__name__).info(
            "Skipping ETL for non-base scenario (scenario_id=%d)", scenario_id
        )
        print(f"ETL skipped for scenario_id={scenario_id}")
        return

    pipeline = create_etl_pipeline()
    df = pipeline.run()
    print(f"\nETL complete. Output shape: {df.shape}")
    print(f"Series: {df['unique_id'].nunique()}")
    print(f"Sample:\n{df.head(10)}")


if __name__ == "__main__":
    main()

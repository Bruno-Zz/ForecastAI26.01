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
import yaml
from datetime import datetime

# Local DB helper
import sys
_files_dir = Path(__file__).resolve().parent.parent
if str(_files_dir) not in sys.path:
    sys.path.insert(0, str(_files_dir))

from db.db import get_conn, init_schema


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

    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the ETL pipeline.

        Args:
            config_path: Path to config.yaml. If None, resolves to
                         config/config.yaml relative to the files/ directory.
        """
        self.logger = logging.getLogger(__name__)

        # Resolve config path
        if config_path is None:
            files_dir = Path(__file__).resolve().parent.parent
            config_path = str(files_dir / "config" / "config.yaml")

        self.config_path = Path(config_path)
        if not self.config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")

        with open(self.config_path, "r") as f:
            self.config = yaml.safe_load(f)

        # Extract relevant config sections
        self.pg_config = self.config["data_source"]["postgres"]
        self.etl_config = self.config["etl"]
        self.query_config = self.etl_config["query"]
        self.agg_config = self.etl_config.get("aggregation", {})

        # ── Source DB (remote Neon) — for extraction ──
        self.source_db_config = self.config["data_source"].get("source_db")
        if self.source_db_config:
            self.source_demand_table = self.source_db_config.get(
                "demand_table", "dp_plan.calc_dp_actual"
            )
            # Column mapping — allows the remote table to have different names
            src_cols = self.source_db_config.get("columns", {})
            self.src_col_item_id = src_cols.get("item_id", "item_id")
            self.src_col_site_id = src_cols.get("site_id", "site_id")
            self.src_col_date    = src_cols.get("date", "date")
            self.src_col_qty     = src_cols.get("qty", "qty")
            self.src_col_channel = src_cols.get("channel", "channel")  # None = skip
        else:
            self.source_demand_table = None

        # ── Local DB tables (for load + downstream) ──
        self.tables = self.pg_config.get("tables", {
            "item": "zcube.item",
            "site": "zcube.site",
            "demand_actual": "zcube.demand_actuals",
            "forecast_results": "zcube.forecast_results",
            "forecast_adjustments": "zcube.forecast_adjustments",
            "process_log": "zcube.process_log",
        })

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

        # Output table (PostgreSQL — no parquet)
        self.output_table = self.tables.get("demand_actual", "zcube.demand_actuals")

        src_label = self.source_demand_table or "(local fallback)"
        self.logger.info("ETLPipeline initialized")
        self.logger.info(f"  Config : {self.config_path}")
        self.logger.info(f"  Source : {src_label}")
        self.logger.info(f"  Output : {self.output_table}")

    # ------------------------------------------------------------------
    # Schema initialisation
    # ------------------------------------------------------------------

    def init_db(self) -> None:
        """Ensure the local zcube schema and all tables exist."""
        self.logger.info("Initialising local database schema...")
        init_schema(str(self.config_path))
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
        return get_conn(str(self.config_path))

    # ------------------------------------------------------------------
    # Query builders
    # ------------------------------------------------------------------

    def _build_source_extract_query(self) -> str:
        """
        Build the SQL query for the *remote* source table
        (dp_plan.calc_dp_actual).  No joins — the source table is a flat
        fact table; we construct unique_id from item_id || '_' || site_id.
        """
        tbl = self.source_demand_table
        col_item = self.src_col_item_id
        col_site = self.src_col_site_id
        col_date = self.src_col_date
        col_qty  = self.src_col_qty
        col_chan  = self.src_col_channel

        where_clauses: List[str] = []
        if self.min_date:
            where_clauses.append(f"d.{col_date} >= %(min_date)s")
        if self.max_date:
            where_clauses.append(f"d.{col_date} <= %(max_date)s")
        where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

        channel_expr = f"d.{col_chan}" if col_chan else "''"

        query = f"""
            SELECT
                CAST(d.{col_item} AS TEXT) || '_' || CAST(d.{col_site} AS TEXT)
                    AS unique_id,
                d.{col_item}  AS item_id,
                d.{col_site}  AS site_id,
                {channel_expr} AS channel,
                d.{col_date}  AS date,
                d.{col_qty}   AS y,
                ''            AS item_name,
                ''            AS site_name
            FROM {tbl} d
            {where_sql}
            ORDER BY 1, d.{col_date}
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
                d.{self.value_column} AS y,
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

        use_remote = self.source_db_config is not None
        if use_remote:
            query = self._build_source_extract_query()
            label = f"remote ({self.source_demand_table})"
        else:
            query = self._build_local_extract_query()
            label = f"local ({self.output_table})"

        self.logger.info(f"Extracting from {label}...")
        self.logger.debug(f"Query:\n{query}")

        conn = self._get_source_conn()
        try:
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
            rows.append((
                int(row.get("item_id", 0) or 0),
                int(row.get("site_id", 0) or 0),
                str(row.get("channel", "") or ""),
                row["date"].date() if hasattr(row["date"], "date") else row["date"],
                float(row["y"]),
                str(row.get("item_name", "") or ""),
                str(row.get("site_name", "") or ""),
                str(row["unique_id"]),
            ))

        if not rows:
            self.logger.warning("No rows to insert into demand_actuals")
            return 0

        conn = get_conn(str(self.config_path))
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
            conn.commit()
            self.logger.info(f"Inserted {len(rows):,} rows into {demand_table}")
            return len(rows)
        except Exception as e:
            conn.rollback()
            self.logger.error(f"DB load failed: {e}", exc_info=True)
            raise
        finally:
            conn.close()

    def _load_dimension_tables(self) -> None:
        """
        Extract dimension data from the source DB and load into the local
        zcube schema.

        Extracted tables (source → local):
            dp_plan.dp_item_type  →  zcube.item_type
            dp_plan.dp_site_type  →  zcube.site_type
            dp_plan.dp_item       →  zcube.item
            dp_plan.dp_site       →  zcube.site

        Uses INSERT … ON CONFLICT DO UPDATE (upsert) so the method is safe
        to call repeatedly.  Skips silently when source_db is not configured.
        """
        if not self.source_db_config:
            self.logger.info(
                "No source_db configured — skipping dimension table load"
            )
            return

        # Derive the source schema from the demand table name
        # e.g. "dp_plan.calc_dp_actual"  →  "dp_plan"
        src_schema = (
            self.source_demand_table.split(".")[0]
            if self.source_demand_table and "." in self.source_demand_table
            else "dp_plan"
        )

        item_type_src = f"{src_schema}.dp_item_type"
        site_type_src = f"{src_schema}.dp_site_type"
        item_src      = f"{src_schema}.dp_item"
        site_src      = f"{src_schema}.dp_site"

        local_schema = self.pg_config.get("schema", "zcube")

        self.logger.info(
            f"Loading dimension tables from {src_schema} "
            f"→ {local_schema}..."
        )

        src_conn = self._get_source_conn()
        dst_conn = get_conn(str(self.config_path))

        try:
            # ── Item types ────────────────────────────────────────────────
            self.logger.info(
                f"  {item_type_src} → {local_schema}.item_type"
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
                f"    → {len(item_type_rows):,} item type(s) upserted"
            )

            # ── Site types ────────────────────────────────────────────────
            self.logger.info(
                f"  {site_type_src} → {local_schema}.site_type"
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
                f"    → {len(site_type_rows):,} site type(s) upserted"
            )

            # ── Items ─────────────────────────────────────────────────────
            self.logger.info(f"  {item_src} → {local_schema}.item")
            with src_conn.cursor(
                cursor_factory=psycopg2.extras.DictCursor
            ) as cur:
                cur.execute(
                    f"""
                    SELECT id, xuid, name, description, attributes, type_id
                    FROM {item_src}
                    """
                )
                item_rows = cur.fetchall()

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
                    [
                        (
                            r["id"],
                            r["xuid"],
                            r["name"],
                            r["description"],
                            # psycopg2 auto-deserialises JSONB → dict;
                            # re-serialise to str for the INSERT adapter.
                            psycopg2.extras.Json(r["attributes"])
                            if r["attributes"] is not None
                            else None,
                            r["type_id"],
                        )
                        for r in item_rows
                    ],
                )
            dst_conn.commit()
            self.logger.info(
                f"    → {len(item_rows):,} item(s) upserted"
            )

            # ── Sites ─────────────────────────────────────────────────────
            self.logger.info(f"  {site_src} → {local_schema}.site")
            with src_conn.cursor(
                cursor_factory=psycopg2.extras.DictCursor
            ) as cur:
                cur.execute(
                    f"""
                    SELECT id, xuid, name, description, attributes, type_id
                    FROM {site_src}
                    """
                )
                site_rows = cur.fetchall()

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
                    [
                        (
                            r["id"],
                            r["xuid"],
                            r["name"],
                            r["description"],
                            psycopg2.extras.Json(r["attributes"])
                            if r["attributes"] is not None
                            else None,
                            r["type_id"],
                        )
                        for r in site_rows
                    ],
                )
            dst_conn.commit()
            self.logger.info(
                f"    → {len(site_rows):,} site(s) upserted"
            )

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

        Args:
            df: Transformed DataFrame.

        Returns:
            The DataFrame (for downstream pipeline consumption).
        """
        rows_inserted = self._load_to_db(df)
        self.logger.info(
            f"Loaded {rows_inserted:,} rows into {self.output_table}"
        )
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
        self.logger.info(
            f"  Source    : {self.pg_config['host']}:"
            f"{self.pg_config['port']}/{self.pg_config['database']}"
        )
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
# Convenience entry point
# --------------------------------------------------------------------------

def main():
    """Run the ETL pipeline from the command line."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    pipeline = ETLPipeline()
    df = pipeline.run()
    print(f"\nETL complete. Output shape: {df.shape}")
    print(f"Series: {df['unique_id'].nunique()}")
    print(f"Sample:\n{df.head(10)}")


if __name__ == "__main__":
    main()

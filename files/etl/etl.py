"""
ETL Pipeline for Demand Forecasting System
Extracts data from local PostgreSQL (zcube schema) via psycopg2,
joins zcube.item, zcube.site, and zcube.demand_actuals tables,
transforms into standardized time series format, and loads to Parquet.
Also bulk-inserts into zcube.demand_actuals for persistence.
"""

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
    ETL pipeline that extracts demand data from local PostgreSQL via psycopg2,
    transforms it into a standardized time series format (unique_id, date, y),
    and saves it as a Parquet file.  Also writes the clean data back to
    zcube.demand_actuals for downstream consumers.
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

        # Table mappings from config
        self.tables = self.pg_config.get("tables", {
            "item": "zcube.item",
            "site": "zcube.site",
            "demand_actual": "zcube.demand_actuals",
            "forecast_results": "zcube.forecast_results",
            "forecast_adjustments": "zcube.forecast_adjustments",
            "process_log": "zcube.process_log",
        })

        # Column name overrides from config
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

        # Output path (relative to files/ directory)
        files_dir = Path(__file__).resolve().parent.parent
        raw_output = self.etl_config.get("output_path", "./data/time_series.parquet")
        self.output_path = files_dir / raw_output

        self.logger.info("ETLPipeline initialized (psycopg2 / local PostgreSQL)")
        self.logger.info(f"  Config: {self.config_path}")
        self.logger.info(f"  Tables: {self.tables}")
        self.logger.info(f"  Output: {self.output_path}")

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

    def _build_extract_query(self) -> str:
        """
        Build the SQL query that joins item, site, and demand_actuals.
        Returns a parameterised query with %s placeholders for date bounds.
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
                CAST(d.item_id AS TEXT) || '_' || CAST(d.site_id AS TEXT) AS unique_id,
                d.item_id,
                d.site_id,
                d.channel,
                d.{self.date_column}  AS date,
                d.{self.value_column} AS y,
                COALESCE(i.name, '') AS item_name,
                COALESCE(s.name, '') AS site_name
            FROM {demand_table} d
            LEFT JOIN {item_table} i ON d.item_id = i.id
            LEFT JOIN {site_table} s ON d.site_id = s.id
            {where_sql}
            ORDER BY unique_id, d.{self.date_column}
        """
        return query

    def extract(self) -> pd.DataFrame:
        """
        Extract data from local PostgreSQL via psycopg2.

        Returns:
            Raw DataFrame with columns:
            unique_id, item_id, site_id, channel, date, y, item_name, site_name
        """
        params: Dict[str, Any] = {}
        if self.min_date:
            params["min_date"] = self.min_date
        if self.max_date:
            params["max_date"] = self.max_date

        query = self._build_extract_query()
        self.logger.info("Executing extraction query against local PostgreSQL...")
        self.logger.debug(f"Query:\n{query}")

        conn = get_conn(str(self.config_path))
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(query, params or None)
                rows = cur.fetchall()
                cols = [desc[0] for desc in cur.description]
            df = pd.DataFrame(rows, columns=cols)
        except Exception as e:
            self.logger.error(f"Extraction failed: {e}")
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

    def load(self, df: pd.DataFrame) -> Path:
        """
        Save the transformed DataFrame to Parquet and write to local DB.

        Args:
            df: Transformed DataFrame.

        Returns:
            Path to the saved Parquet file.
        """
        # --- Parquet ---
        output = Path(self.output_path)
        output.parent.mkdir(parents=True, exist_ok=True)

        compression = self.config.get("output", {}).get("compression", "snappy")

        # Parquet gets the y column (not qty) — keep it clean
        parquet_df = df[["unique_id", "date", "y"]].copy()
        parquet_df.to_parquet(output, index=False, compression=compression)
        file_size_mb = output.stat().st_size / (1024 * 1024)
        self.logger.info(f"Saved Parquet: {output} ({file_size_mb:.2f} MB)")

        # --- Database ---
        try:
            rows_inserted = self._load_to_db(df)
        except Exception as e:
            self.logger.warning(f"DB load skipped due to error: {e}")

        return output

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
        self.logger.info(f"  Output    : {self.output_path}")
        self.logger.info("=" * 80)

        try:
            # 0. Ensure DB schema exists
            self.logger.info("\n--- INIT DB ---")
            self.init_db()

            # 1. Extract
            self.logger.info("\n--- EXTRACT ---")
            raw_df = self.extract()

            # 2. Transform
            self.logger.info("\n--- TRANSFORM ---")
            clean_df = self.transform(raw_df)

            # 3. Load
            self.logger.info("\n--- LOAD ---")
            output_path = self.load(clean_df)

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
            self.logger.info(f"  Output file     : {output_path}")
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

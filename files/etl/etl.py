"""
ETL Pipeline for Demand Forecasting System
Extracts data from PostgreSQL (Neon) via DuckDB's postgres_scanner extension,
joins plan.item, plan.site, and plan.demand_actual tables,
transforms into standardized time series format, and loads to Parquet.
"""

import duckdb
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any
from pathlib import Path
import logging
import yaml
from datetime import datetime


class ETLPipeline:
    """
    ETL pipeline that extracts demand data from PostgreSQL using DuckDB,
    transforms it into a standardized time series format (unique_id, date, y),
    and saves it as a Parquet file.
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
            # Default: files/config/config.yaml
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
            "item": "plan.item",
            "site": "plan.site",
            "demand_actual": "plan.demand_actual",
        })

        # Column name overrides from config
        self.date_column = self.query_config.get("date_column", "date")
        self.value_column = self.query_config.get("value_column", "demand")
        self.id_column = self.query_config.get("id_column", "sku_id")

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

        # DuckDB connection (in-memory)
        self.con = None

        self.logger.info("ETLPipeline initialized")
        self.logger.info(f"  Config: {self.config_path}")
        self.logger.info(f"  Tables: {self.tables}")
        self.logger.info(f"  Output: {self.output_path}")

    # ------------------------------------------------------------------
    # Connection helpers
    # ------------------------------------------------------------------

    def _build_dsn(self) -> str:
        """Build a PostgreSQL DSN string from config."""
        host = self.pg_config["host"]
        port = self.pg_config.get("port", 5432)
        database = self.pg_config["database"]
        user = self.pg_config["user"]
        password = self.pg_config["password"]
        sslmode = self.pg_config.get("sslmode", "require")
        return (
            f"host={host} port={port} dbname={database} "
            f"user={user} password={password} sslmode={sslmode}"
        )

    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        """
        Get or create a DuckDB connection with the postgres_scanner
        extension installed and a PostgreSQL source attached.
        """
        if self.con is not None:
            return self.con

        self.logger.info("Creating DuckDB connection and attaching PostgreSQL...")
        self.con = duckdb.connect(database=":memory:")

        # Install and load the postgres extension
        self.con.execute("INSTALL postgres;")
        self.con.execute("LOAD postgres;")

        # Attach the remote Neon PostgreSQL database
        dsn = self._build_dsn()
        self.con.execute(f"""
            ATTACH '{dsn}' AS pg_db (TYPE POSTGRES, READ_ONLY);
        """)
        self.logger.info("PostgreSQL database attached as 'pg_db'")

        return self.con

    def _close_connection(self):
        """Close the DuckDB connection."""
        if self.con is not None:
            try:
                self.con.execute("DETACH pg_db;")
            except Exception:
                pass
            self.con.close()
            self.con = None
            self.logger.info("DuckDB connection closed")

    # ------------------------------------------------------------------
    # Schema discovery
    # ------------------------------------------------------------------

    def discover_schema(self) -> Dict[str, List[str]]:
        """
        Connect to PostgreSQL and print the columns of each table in the
        plan schema.  Useful for understanding the data model before
        building the join query.

        Returns:
            Dictionary mapping table name to list of column names.
        """
        con = self._get_connection()
        schema_info: Dict[str, List[str]] = {}

        for label, full_table in self.tables.items():
            self.logger.info(f"Discovering schema for {full_table}...")
            try:
                cols_df = con.execute(f"""
                    SELECT column_name, data_type
                    FROM pg_db.information_schema.columns
                    WHERE table_schema || '.' || table_name = '{full_table}'
                    ORDER BY ordinal_position;
                """).fetchdf()

                if cols_df.empty:
                    # Fallback: try PRAGMA table_info via DuckDB
                    self.logger.info(f"  information_schema empty, trying PRAGMA for {full_table}...")
                    cols_df = con.execute(f"""
                        PRAGMA table_info('pg_db.{full_table}');
                    """).fetchdf()

                col_names = cols_df["column_name"].tolist() if "column_name" in cols_df.columns else []
                schema_info[label] = col_names

                self.logger.info(f"  {label} ({full_table}):")
                if not cols_df.empty:
                    for _, row in cols_df.iterrows():
                        col = row.get("column_name", row.get("name", "?"))
                        dtype = row.get("data_type", row.get("type", "?"))
                        self.logger.info(f"    - {col}  ({dtype})")
                else:
                    self.logger.warning(f"    No columns found for {full_table}")

            except Exception as e:
                self.logger.error(f"  Failed to discover {full_table}: {e}")
                schema_info[label] = []

        return schema_info

    # ------------------------------------------------------------------
    # Extraction
    # ------------------------------------------------------------------

    def _build_extract_query(self) -> str:
        """
        Build the SQL query that joins item, site, and demand_actual.

        The join logic:
        - demand_actual is the fact table containing date, value, and
          foreign keys to item and site.
        - item and site are dimension tables.
        - unique_id is constructed as <item_identifier>_<site_identifier>.

        Because exact column names may vary, the query uses the configured
        column names (date_column, value_column, id_column) and makes
        reasonable assumptions about foreign key columns.  The
        discover_schema() method can be run first to verify.
        """
        item_table = f"pg_db.{self.tables['item']}"
        site_table = f"pg_db.{self.tables['site']}"
        demand_table = f"pg_db.{self.tables['demand_actual']}"

        # Build WHERE clause for date filtering and original-rows-only filter
        where_clauses = ["d.original = 1"]   # only keep original (non-mapped) demand rows
        if self.min_date:
            where_clauses.append(f"d.{self.date_column} >= '{self.min_date}'")
        if self.max_date:
            where_clauses.append(f"d.{self.date_column} <= '{self.max_date}'")

        where_sql = ""
        if where_clauses:
            where_sql = "WHERE " + " AND ".join(where_clauses)

        # The query joins demand_actual (d) with item (i) and site (s).
        # We construct unique_id from item and site identifiers so every
        # combination is a distinct time series.
        # Only select the columns we need to avoid duplicate 'id' clashes.
        query = f"""
            SELECT
                CONCAT(CAST(d.item_id AS VARCHAR),
                       '_',
                       CAST(d.site_id AS VARCHAR))
                    AS unique_id,
                d.{self.date_column}  AS date,
                d.{self.value_column} AS y,
                i.name AS item_name,
                i.xuid AS item_xuid,
                s.name AS site_name,
                s.xuid AS site_xuid
            FROM {demand_table} d
            LEFT JOIN {item_table} i
                ON d.item_id = i.id
            LEFT JOIN {site_table} s
                ON d.site_id = s.id
            {where_sql}
            ORDER BY unique_id, d.{self.date_column};
        """
        return query

    def extract(self) -> pd.DataFrame:
        """
        Extract data from PostgreSQL via DuckDB.

        Returns:
            Raw DataFrame with at least (unique_id, date, y) plus any
            dimension columns from the item and site tables.
        """
        con = self._get_connection()

        query = self._build_extract_query()
        self.logger.info("Executing extraction query...")
        self.logger.debug(f"Query:\n{query}")

        try:
            df = con.execute(query).fetchdf()
        except Exception as e:
            self.logger.error(f"Extraction query failed: {e}")
            self.logger.info("Attempting fallback extraction with simplified query...")
            df = self._extract_fallback(con)

        self.logger.info(
            f"Extracted {len(df):,} rows, "
            f"{df['unique_id'].nunique():,} unique series"
        )
        return df

    def _extract_fallback(self, con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
        """
        Fallback extraction when the primary join query fails.
        Pulls each table individually and attempts a pandas-level merge.
        """
        demand_table = f"pg_db.{self.tables['demand_actual']}"
        item_table = f"pg_db.{self.tables['item']}"
        site_table = f"pg_db.{self.tables['site']}"

        self.logger.info("Fallback: reading tables individually...")

        demand_df = con.execute(f"SELECT * FROM {demand_table};").fetchdf()
        self.logger.info(f"  demand_actual: {len(demand_df):,} rows, columns: {list(demand_df.columns)}")
        # Keep only original (non-mapped) demand rows
        if "original" in demand_df.columns:
            before = len(demand_df)
            demand_df = demand_df[demand_df["original"] == 1].copy()
            self.logger.info(f"  Filtered to original=1: {before - len(demand_df):,} mapped rows removed, {len(demand_df):,} remain")

        item_df = con.execute(f"SELECT * FROM {item_table};").fetchdf()
        self.logger.info(f"  item: {len(item_df):,} rows, columns: {list(item_df.columns)}")

        site_df = con.execute(f"SELECT * FROM {site_table};").fetchdf()
        self.logger.info(f"  site: {len(site_df):,} rows, columns: {list(site_df.columns)}")

        # Find the join keys by looking for common columns
        demand_cols = set(demand_df.columns)

        # Item join key
        item_key_candidates = ["item_id", "item", "id"]
        item_join_demand = None
        item_join_item = None
        for key in item_key_candidates:
            if key in demand_cols and ("id" in item_df.columns):
                item_join_demand = key
                item_join_item = "id"
                break
        if item_join_demand is None:
            # Try matching column names directly
            common_item = demand_cols.intersection(set(item_df.columns))
            if common_item:
                item_join_demand = item_join_item = list(common_item)[0]

        # Site join key
        site_key_candidates = ["site_id", "site", "id"]
        site_join_demand = None
        site_join_site = None
        for key in site_key_candidates:
            if key in demand_cols and ("id" in site_df.columns):
                site_join_demand = key
                site_join_site = "id"
                break
        if site_join_demand is None:
            common_site = demand_cols.intersection(set(site_df.columns))
            if common_site:
                site_join_demand = site_join_site = list(common_site)[0]

        # Merge
        df = demand_df.copy()
        if item_join_demand and item_join_item:
            self.logger.info(f"  Merging item on demand.{item_join_demand} = item.{item_join_item}")
            df = df.merge(
                item_df, left_on=item_join_demand, right_on=item_join_item,
                how="left", suffixes=("", "_item")
            )
        if site_join_demand and site_join_site:
            self.logger.info(f"  Merging site on demand.{site_join_demand} = site.{site_join_site}")
            df = df.merge(
                site_df, left_on=site_join_demand, right_on=site_join_site,
                how="left", suffixes=("", "_site")
            )

        # Build unique_id
        item_id_col = item_join_demand or "item_id"
        site_id_col = site_join_demand or "site_id"
        if item_id_col in df.columns and site_id_col in df.columns:
            df["unique_id"] = (
                df[item_id_col].astype(str) + "_" + df[site_id_col].astype(str)
            )
        elif item_id_col in df.columns:
            df["unique_id"] = df[item_id_col].astype(str)
        else:
            df["unique_id"] = df.index.astype(str)

        # Rename value/date columns to standard names
        date_renames = {self.date_column: "date"} if self.date_column in df.columns and self.date_column != "date" else {}
        value_renames = {self.value_column: "y"} if self.value_column in df.columns and self.value_column != "y" else {}
        df.rename(columns={**date_renames, **value_renames}, inplace=True)

        return df

    # ------------------------------------------------------------------
    # Transformation
    # ------------------------------------------------------------------

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform raw extracted data into the standardized time series format.

        Steps:
            1. Ensure standard column names (unique_id, date, y).
            2. Parse and validate date column.
            3. Cast value column to numeric.
            4. Aggregate to the configured frequency.
            5. Filter series with too few observations.
            6. Sort by (unique_id, date).

        Args:
            df: Raw extracted DataFrame.

        Returns:
            Cleaned DataFrame with columns [unique_id, date, y].
        """
        self.logger.info("Starting transformation...")
        initial_rows = len(df)
        initial_series = df["unique_id"].nunique()

        # --- 1. Standardize column names ---
        rename_map = {}
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

        # Keep only the standardized columns
        df = df[["unique_id", "date", "y"]].copy()

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
            self.logger.info(f"Dropping {null_values:,} rows with null/non-numeric values (expected: some source records have no quantity)")
            df = df.dropna(subset=["y"])

        # --- 4. Apply date range filter (post-extraction safety net) ---
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

        self.logger.info(f"Aggregating to frequency={self.frequency} ({pd_freq}), method={agg_func}")

        df = (
            df
            .groupby(["unique_id", pd.Grouper(key="date", freq=pd_freq)])
            .agg(y=("y", agg_func))
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

    def load(self, df: pd.DataFrame) -> Path:
        """
        Save the transformed DataFrame to Parquet.

        Args:
            df: Transformed DataFrame with columns [unique_id, date, y].

        Returns:
            Path to the saved Parquet file.
        """
        output = Path(self.output_path)
        output.parent.mkdir(parents=True, exist_ok=True)

        compression = self.config.get("output", {}).get("compression", "snappy")

        df.to_parquet(output, index=False, compression=compression)
        file_size_mb = output.stat().st_size / (1024 * 1024)

        self.logger.info(f"Saved to {output} ({file_size_mb:.2f} MB, {compression} compression)")
        return output

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def run(self) -> pd.DataFrame:
        """
        Execute the full ETL pipeline: Extract -> Transform -> Load.

        Returns:
            The final standardized DataFrame.
        """
        start_time = datetime.now()
        self.logger.info("=" * 80)
        self.logger.info("ETL PIPELINE START")
        self.logger.info(f"  Timestamp : {start_time.isoformat()}")
        self.logger.info(f"  Source    : {self.pg_config['host']}:{self.pg_config['port']}/{self.pg_config['database']}")
        self.logger.info(f"  Tables    : {list(self.tables.values())}")
        self.logger.info(f"  Frequency : {self.frequency}")
        self.logger.info(f"  Output    : {self.output_path}")
        self.logger.info("=" * 80)

        try:
            # Extract
            self.logger.info("\n--- EXTRACT ---")
            raw_df = self.extract()

            # Transform
            self.logger.info("\n--- TRANSFORM ---")
            clean_df = self.transform(raw_df)

            # Load
            self.logger.info("\n--- LOAD ---")
            output_path = self.load(clean_df)

            # Summary
            elapsed = (datetime.now() - start_time).total_seconds()
            self.logger.info("\n" + "=" * 80)
            self.logger.info("ETL PIPELINE COMPLETE")
            self.logger.info(f"  Duration        : {elapsed:.1f}s")
            self.logger.info(f"  Total rows      : {len(clean_df):,}")
            self.logger.info(f"  Total series    : {clean_df['unique_id'].nunique():,}")
            self.logger.info(f"  Date range      : {clean_df['date'].min()} to {clean_df['date'].max()}")
            self.logger.info(f"  Output file     : {output_path}")
            self.logger.info("=" * 80)

            return clean_df

        except Exception as e:
            self.logger.error(f"ETL pipeline failed: {e}", exc_info=True)
            raise

        finally:
            self._close_connection()


# --------------------------------------------------------------------------
# Convenience entry point
# --------------------------------------------------------------------------

def main():
    """Run the ETL pipeline from the command line."""
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    pipeline = ETLPipeline()

    # Optionally discover the schema first
    import sys
    if "--discover" in sys.argv:
        schema = pipeline.discover_schema()
        print("\nDiscovered Schema:")
        for table, columns in schema.items():
            print(f"\n  {table}:")
            for col in columns:
                print(f"    - {col}")
        return

    # Run full ETL
    df = pipeline.run()
    print(f"\nETL complete. Output shape: {df.shape}")
    print(f"Series: {df['unique_id'].nunique()}")
    print(f"Sample:\n{df.head(10)}")


if __name__ == "__main__":
    main()

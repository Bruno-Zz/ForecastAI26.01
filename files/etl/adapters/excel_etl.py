"""
ExcelETLPipeline — per-account ETL adapter that reads from an Excel workbook.

Activated when the account's ``data_source`` parameter row contains:

    {
        "source_type": "excel",
        "excel_source": {
            "file_path":      "<absolute or relative path to .xlsx>",
            "demand_sheet":   "demand_actuals",   // default
            "items_sheet":    "items",            // default (null = skip)
            "sites_sheet":    "sites",            // default (null = skip)
            "columns": {
                "unique_id":  "unique_id",   // column name in the sheet
                "item_id":    "item_id",
                "site_id":    "site_id",
                "channel":    "channel",     // optional
                "date":       "date",
                "qty":        "qty"
            }
        }
    }

Dimension loading (items + sites) uses the items_sheet / sites_sheet if present
so that item/site FK constraints are satisfied in zcube.demand_actuals.

All transform and load logic is inherited from the base ETLPipeline — only
the extract() and _load_dimension_tables() methods are overridden.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import psycopg2
import psycopg2.extras

# Locate the parent package (files/) on sys.path
_files_dir = Path(__file__).resolve().parents[2]
if str(_files_dir) not in sys.path:
    sys.path.insert(0, str(_files_dir))

from etl.etl import ETLPipeline
from db.db import get_conn, get_schema


class ExcelETLPipeline(ETLPipeline):
    """
    ETL pipeline that reads demand data (and optionally dimensions) from an
    Excel workbook instead of a remote PostgreSQL source.

    Configuration is read from the account's ``data_source`` parameter row
    exactly like the base class; the extra ``excel_source`` sub-key provides
    the workbook path and sheet/column mapping.
    """

    def __init__(self, config_path=None):
        super().__init__(config_path=config_path)

        data_src = self.config.get("data_source", {})
        xl_cfg   = data_src.get("excel_source") or {}

        raw_path = xl_cfg.get("file_path", "")
        if not raw_path:
            raise ValueError(
                "ExcelETLPipeline: data_source.excel_source.file_path is required"
            )
        self.excel_path = Path(raw_path)
        if not self.excel_path.is_absolute():
            # Resolve relative to the files/ directory
            self.excel_path = _files_dir.parent / self.excel_path

        self.demand_sheet = xl_cfg.get("demand_sheet", "demand_actuals")
        self.items_sheet  = xl_cfg.get("items_sheet",  "items")
        self.sites_sheet  = xl_cfg.get("sites_sheet",  "sites")

        xl_cols = xl_cfg.get("columns") or {}
        self.xl_col_unique_id = xl_cols.get("unique_id", "unique_id")
        self.xl_col_item_id   = xl_cols.get("item_id",   "item_id")
        self.xl_col_site_id   = xl_cols.get("site_id",   "site_id")
        self.xl_col_channel   = xl_cols.get("channel",   "channel")
        self.xl_col_date      = xl_cols.get("date",      "date")
        self.xl_col_qty       = xl_cols.get("qty",       "qty")

        self.logger.info("ExcelETLPipeline initialised")
        self.logger.info(f"  Workbook : {self.excel_path}")
        self.logger.info(f"  Demand sheet : {self.demand_sheet}")
        if self.items_sheet:
            self.logger.info(f"  Items sheet  : {self.items_sheet}")
        if self.sites_sheet:
            self.logger.info(f"  Sites sheet  : {self.sites_sheet}")

    # ------------------------------------------------------------------
    # Override: extract() reads from Excel instead of PostgreSQL
    # ------------------------------------------------------------------

    def extract(self) -> pd.DataFrame:
        """
        Read the demand_actuals sheet from the Excel workbook and return a
        DataFrame with the standard columns expected by ``transform()``:
        unique_id, item_id, site_id, channel, date, y, item_name, site_name.
        """
        if not self.excel_path.exists():
            raise FileNotFoundError(
                f"ExcelETLPipeline: workbook not found: {self.excel_path}"
            )

        self.logger.info(f"Reading '{self.demand_sheet}' from {self.excel_path} …")
        df = pd.read_excel(
            self.excel_path,
            sheet_name=self.demand_sheet,
            dtype={
                self.xl_col_item_id:   "Int64",
                self.xl_col_site_id:   "Int64",
                self.xl_col_qty:       float,
            },
        )
        self.logger.info(f"  Read {len(df):,} rows from Excel")

        # Rename to canonical names expected by transform()
        rename = {}
        if self.xl_col_unique_id in df.columns and self.xl_col_unique_id != "unique_id":
            rename[self.xl_col_unique_id] = "unique_id"
        if self.xl_col_item_id in df.columns and self.xl_col_item_id != "item_id":
            rename[self.xl_col_item_id] = "item_id"
        if self.xl_col_site_id in df.columns and self.xl_col_site_id != "site_id":
            rename[self.xl_col_site_id] = "site_id"
        if self.xl_col_channel in df.columns and self.xl_col_channel != "channel":
            rename[self.xl_col_channel] = "channel"
        if self.xl_col_date in df.columns and self.xl_col_date != "date":
            rename[self.xl_col_date] = "date"
        if self.xl_col_qty in df.columns and self.xl_col_qty != "y":
            rename[self.xl_col_qty] = "y"
        if rename:
            df = df.rename(columns=rename)

        # If 'y' not present but 'qty' is (no rename needed), alias it
        if "y" not in df.columns and "qty" in df.columns:
            df = df.rename(columns={"qty": "y"})

        # Synthesise unique_id from item_id + site_id if not present
        if "unique_id" not in df.columns:
            df["unique_id"] = (
                df["item_id"].astype(str).str.strip()
                + "_"
                + df["site_id"].astype(str).str.strip()
            )

        # Ensure channel, item_name, site_name columns exist
        if "channel"   not in df.columns: df["channel"]   = ""
        if "item_name" not in df.columns: df["item_name"] = ""
        if "site_name" not in df.columns: df["site_name"] = ""

        self.logger.info(
            f"Extracted {len(df):,} rows, "
            f"{df['unique_id'].nunique() if not df.empty else 0:,} unique series"
        )
        return df

    # ------------------------------------------------------------------
    # Override: dimension loading reads from Excel instead of remote DB
    # ------------------------------------------------------------------

    def _load_dimension_tables(self) -> None:
        """
        Load item and site dimension data from the Excel workbook's
        ``items`` and ``sites`` sheets into the local zcube schema.

        Skips gracefully if the sheets are not configured or not present.
        """
        schema = get_schema()
        conn   = get_conn()
        try:
            self._load_items_from_excel(conn, schema)
            self._load_sites_from_excel(conn, schema)
        finally:
            conn.close()

    def _load_items_from_excel(self, conn, schema: str) -> None:
        if not self.items_sheet:
            self.logger.info("items_sheet not configured — skipping item dimension load")
            return

        self.logger.info(f"Loading items from Excel sheet '{self.items_sheet}' …")
        df = pd.read_excel(self.excel_path, sheet_name=self.items_sheet,
                           dtype={"item_id": "Int64"})

        # Map Excel columns → local item table columns
        # Required: item_id, item_name
        # Optional: description, attributes (gather extra cols into JSONB)
        id_col   = next((c for c in ["item_id", "id"] if c in df.columns), None)
        name_col = next((c for c in ["item_name", "name"] if c in df.columns), None)
        if id_col is None or name_col is None:
            self.logger.warning("Items sheet missing id/name columns — skipping")
            return

        rows = []
        for _, r in df.iterrows():
            item_id  = int(r[id_col]) if pd.notna(r[id_col]) else None
            if item_id is None:
                continue
            name     = str(r.get(name_col, "") or "")
            desc_col = next((c for c in ["description", "desc"] if c in df.columns), None)
            desc     = str(r.get(desc_col, "") or "") if desc_col else ""
            # Pack remaining columns into attributes JSONB
            skip = {id_col, name_col, desc_col, "item_code","mover_class"}
            attrs = {}
            for col in df.columns:
                if col in skip:
                    continue
                v = r.get(col)
                if pd.notna(v) if not isinstance(v, float) else not pd.isna(v):
                    attrs[col] = v if isinstance(v, (int, float, bool, str)) else str(v)
            xuid_col = next((c for c in ["item_code","xuid"] if c in df.columns), None)
            xuid = str(r.get(xuid_col, "") or "") if xuid_col else None
            rows.append((item_id, xuid or None, name, desc,
                         psycopg2.extras.Json(attrs) if attrs else None, None))

        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                f"""INSERT INTO {schema}.item (id, xuid, name, description, attributes, type_id)
                    VALUES %s
                    ON CONFLICT (id) DO UPDATE SET
                        xuid        = EXCLUDED.xuid,
                        name        = EXCLUDED.name,
                        description = EXCLUDED.description,
                        attributes  = EXCLUDED.attributes""",
                rows,
            )
        conn.commit()
        self.logger.info(f"  -> {len(rows):,} item(s) upserted into {schema}.item")

    def _load_sites_from_excel(self, conn, schema: str) -> None:
        if not self.sites_sheet:
            self.logger.info("sites_sheet not configured — skipping site dimension load")
            return

        self.logger.info(f"Loading sites from Excel sheet '{self.sites_sheet}' …")
        df = pd.read_excel(self.excel_path, sheet_name=self.sites_sheet,
                           dtype={"site_id": "Int64"})

        id_col   = next((c for c in ["site_id", "id"] if c in df.columns), None)
        name_col = next((c for c in ["site_name", "name"] if c in df.columns), None)
        if id_col is None or name_col is None:
            self.logger.warning("Sites sheet missing id/name columns — skipping")
            return

        rows = []
        for _, r in df.iterrows():
            site_id = int(r[id_col]) if pd.notna(r[id_col]) else None
            if site_id is None:
                continue
            name = str(r.get(name_col, "") or "")
            desc_col = next((c for c in ["description","desc"] if c in df.columns), None)
            desc = str(r.get(desc_col, "") or "") if desc_col else ""
            skip = {id_col, name_col, desc_col}
            attrs = {}
            for col in df.columns:
                if col in skip:
                    continue
                v = r.get(col)
                if pd.notna(v) if not isinstance(v, float) else not pd.isna(v):
                    attrs[col] = v if isinstance(v, (int, float, bool, str)) else str(v)
            xuid_col = next((c for c in ["site_code","xuid"] if c in df.columns), None)
            xuid = str(r.get(xuid_col, "") or "") if xuid_col else None
            rows.append((site_id, xuid or None, name, desc,
                         psycopg2.extras.Json(attrs) if attrs else None, None))

        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                f"""INSERT INTO {schema}.site (id, xuid, name, description, attributes, type_id)
                    VALUES %s
                    ON CONFLICT (id) DO UPDATE SET
                        xuid        = EXCLUDED.xuid,
                        name        = EXCLUDED.name,
                        description = EXCLUDED.description,
                        attributes  = EXCLUDED.attributes""",
                rows,
            )
        conn.commit()
        self.logger.info(f"  -> {len(rows):,} site(s) upserted into {schema}.site")

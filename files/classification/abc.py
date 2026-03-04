"""
ABC Classification Engine — configurable multi-metric classification.

Supports:
  - Metrics: hits (order count), demand (qty sum), value (qty * price)
  - Granularity: per item/site or per item (propagated to all sites)
  - Methods: cumulative_pct, rank_pct, rank_absolute
  - Segment-scoped classification (optional)
  - Multiple concurrent classification configurations
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class ABCClassifier:
    """Run configurable ABC classifications against demand data."""

    def __init__(self, config_path: Union[str, Path]):
        self.config_path = str(config_path)

    # ──────────────────────────────────────────────────────────────────
    # Public API
    # ──────────────────────────────────────────────────────────────────

    def run_classification(self, config_id: int) -> Dict[str, Any]:
        """
        Execute a single ABC classification.

        1. Load the configuration from ``abc_configuration``.
        2. Compute the metric over the lookback window.
        3. Rank and assign class labels.
        4. Persist results to ``abc_results``.

        Returns a summary dict: ``{ config_id, name, total, per_class: {label: count} }``.
        """
        from db.db import get_conn, get_schema

        schema = get_schema(self.config_path)
        conn = get_conn(self.config_path)
        try:
            cfg = self._load_config(conn, schema, config_id)
        finally:
            conn.close()

        if cfg is None:
            raise ValueError(f"ABC configuration {config_id} not found")

        logger.info(
            "Running ABC classification %r (metric=%s, method=%s, lookback=%d mo, granularity=%s)",
            cfg["name"], cfg["metric"], cfg["method"],
            cfg["lookback_months"], cfg["granularity"],
        )

        # 1. Compute metric values
        metric_df = self._compute_metric(cfg)
        if metric_df.empty:
            logger.warning("No data for ABC config %d (%s)", config_id, cfg["name"])
            return {"config_id": config_id, "name": cfg["name"], "total": 0, "per_class": {}}

        # 2. Rank and classify
        result_df = self._classify(metric_df, cfg)

        # 3. Propagate item-level results to all unique_ids (if granularity == 'item')
        if cfg["granularity"] == "item":
            result_df = self._propagate_to_sites(result_df, cfg)

        # 4. Persist
        self._save_results(config_id, result_df)

        # Summary
        per_class = result_df["class_label"].value_counts().to_dict()
        summary = {
            "config_id": config_id,
            "name": cfg["name"],
            "total": len(result_df),
            "per_class": per_class,
        }
        logger.info(
            "ABC %r complete: %d series — %s",
            cfg["name"], summary["total"],
            ", ".join(f"{k}={v}" for k, v in sorted(per_class.items())),
        )
        return summary

    def run_all_active(self) -> List[Dict[str, Any]]:
        """Run every active ABC configuration. Returns list of summaries."""
        from db.db import get_conn, get_schema

        schema = get_schema(self.config_path)
        conn = get_conn(self.config_path)
        try:
            configs = pd.read_sql(
                f"SELECT id, name FROM {schema}.abc_configuration WHERE is_active = TRUE ORDER BY id",
                conn,
            )
        finally:
            conn.close()

        if configs.empty:
            logger.info("No active ABC configurations to run")
            return []

        summaries = []
        for _, row in configs.iterrows():
            try:
                summary = self.run_classification(int(row["id"]))
                summaries.append(summary)
            except Exception as exc:
                logger.error("ABC config %d (%s) failed: %s", row["id"], row["name"], exc)
        return summaries

    def check_price_available(self) -> Dict[str, bool]:
        """
        Check whether price data is available for the 'value' metric.

        Returns ``{ "item_site": bool, "item_attributes": bool, "available": bool }``.
        """
        from db.db import get_conn, get_schema

        schema = get_schema(self.config_path)
        conn = get_conn(self.config_path)
        result = {"item_site": False, "item_attributes": False, "available": False}
        try:
            with conn.cursor() as cur:
                # Check item_site table
                cur.execute(
                    "SELECT EXISTS ("
                    "  SELECT 1 FROM information_schema.columns "
                    "  WHERE table_schema = %s AND table_name = 'item_site' "
                    "    AND column_name = 'cost_price'"
                    ")",
                    (schema,),
                )
                result["item_site"] = cur.fetchone()[0]

                # Check item.attributes for any row with a 'price' key
                if not result["item_site"]:
                    cur.execute(
                        "SELECT EXISTS ("
                        f"  SELECT 1 FROM {schema}.item "
                        "  WHERE attributes ? 'price' LIMIT 1"
                        ")"
                    )
                    result["item_attributes"] = cur.fetchone()[0]

                result["available"] = result["item_site"] or result["item_attributes"]
        except Exception as exc:
            logger.warning("Price availability check failed: %s", exc)
        finally:
            conn.close()
        return result

    # ──────────────────────────────────────────────────────────────────
    # Internal helpers
    # ──────────────────────────────────────────────────────────────────

    @staticmethod
    def _load_config(conn, schema: str, config_id: int) -> Optional[Dict[str, Any]]:
        df = pd.read_sql(
            f"SELECT * FROM {schema}.abc_configuration WHERE id = %s",
            conn,
            params=(config_id,),
        )
        if df.empty:
            return None
        row = df.iloc[0].to_dict()
        # Parse JSONB fields
        for col in ("class_labels", "thresholds"):
            if isinstance(row[col], str):
                row[col] = json.loads(row[col])
        return row

    def _compute_metric(self, cfg: Dict[str, Any]) -> pd.DataFrame:
        """
        Query demand_actuals and compute the chosen metric.

        Returns DataFrame with columns ``[unique_id, item_id, metric_value]``
        (or ``[item_id, metric_value]`` when granularity == 'item').
        """
        from db.db import get_conn, get_schema

        schema = get_schema(self.config_path)
        conn = get_conn(self.config_path)

        # Date window
        date_filter = ""
        if cfg["lookback_months"] and cfg["lookback_months"] > 0:
            date_filter = (
                f"AND da.date >= (SELECT MAX(date) FROM {schema}.demand_actuals) "
                f"  - INTERVAL '{cfg['lookback_months']} months'"
            )

        # Segment filter
        segment_join = ""
        segment_where = ""
        if cfg.get("segment_id"):
            segment_join = (
                f"JOIN {schema}.segment_membership sm "
                f"  ON sm.unique_id = da.unique_id "
                f"  AND sm.segment_id = {int(cfg['segment_id'])}"
            )

        # Group-by column
        if cfg["granularity"] == "item":
            group_col = "da.item_id"
            select_id = "da.item_id"
        else:
            group_col = "da.unique_id"
            select_id = "da.unique_id, da.item_id"

        # Metric expression
        metric = cfg["metric"]
        if metric == "hits":
            metric_expr = "COUNT(*)"
        elif metric == "demand":
            metric_expr = "SUM(COALESCE(da.corrected_qty, da.qty))"
        elif metric == "value":
            # Fallback chain: item_site.cost_price → item.attributes->>'price' → 1
            price_info = self.check_price_available()
            if price_info["item_site"]:
                price_join = (
                    f"LEFT JOIN {schema}.item_site isite "
                    f"  ON isite.item_id = da.item_id AND isite.site_id = da.site_id "
                )
                price_expr = "COALESCE(isite.cost_price, 1)"
            elif price_info["item_attributes"]:
                price_join = (
                    f"LEFT JOIN {schema}.item itm "
                    f"  ON itm.id = da.item_id "
                )
                price_expr = "COALESCE((itm.attributes->>'price')::numeric, 1)"
            else:
                logger.warning("No price data available — 'value' metric will use qty only")
                price_join = ""
                price_expr = "1"
            metric_expr = f"SUM(COALESCE(da.corrected_qty, da.qty) * {price_expr})"
        else:
            raise ValueError(f"Unknown metric: {metric}")

        # Build value-metric price join (only for 'value' metric)
        value_join = ""
        if metric == "value":
            value_join = price_join  # type: ignore[possibly-undefined]

        query = f"""
            SELECT {select_id}, {metric_expr} AS metric_value
            FROM {schema}.demand_actuals da
            {segment_join}
            {value_join}
            WHERE 1=1 {date_filter}
            GROUP BY {group_col}
            {"" if cfg["granularity"] == "item" else ", da.item_id"}
            HAVING {metric_expr} > 0
            ORDER BY metric_value DESC
        """

        try:
            df = pd.read_sql(query, conn)
        finally:
            conn.close()

        return df

    @staticmethod
    def _classify(metric_df: pd.DataFrame, cfg: Dict[str, Any]) -> pd.DataFrame:
        """
        Assign class labels based on method and thresholds.

        Returns the metric_df with added columns: ``rank``, ``cumulative_pct``,
        ``class_label``.
        """
        df = metric_df.sort_values("metric_value", ascending=False).reset_index(drop=True)
        n = len(df)
        labels = cfg["class_labels"]
        thresholds = cfg["thresholds"]
        method = cfg["method"]

        # Rank (1-based)
        df["rank"] = range(1, n + 1)

        # Cumulative percentage
        total = df["metric_value"].sum()
        if total > 0:
            df["cumulative_pct"] = (df["metric_value"].cumsum() / total * 100).round(2)
        else:
            df["cumulative_pct"] = 0.0

        # Assign last label as default, then override from top
        df["class_label"] = labels[-1]

        if method == "cumulative_pct":
            # thresholds = [80, 95] → A if cum_pct ≤ 80, B if ≤ 95, C otherwise
            for i in range(len(thresholds) - 1, -1, -1):
                df.loc[df["cumulative_pct"] <= thresholds[i], "class_label"] = labels[i]

        elif method == "rank_pct":
            # thresholds = [20, 50] → A if rank in top 20%, B if top 50%, C otherwise
            for i in range(len(thresholds) - 1, -1, -1):
                cutoff_rank = max(1, int(np.ceil(n * thresholds[i] / 100)))
                df.loc[df["rank"] <= cutoff_rank, "class_label"] = labels[i]

        elif method == "rank_absolute":
            # thresholds = [100, 500] → A if rank ≤ 100, B if rank ≤ 500, C otherwise
            for i in range(len(thresholds) - 1, -1, -1):
                df.loc[df["rank"] <= thresholds[i], "class_label"] = labels[i]

        else:
            raise ValueError(f"Unknown classification method: {method}")

        return df

    def _propagate_to_sites(self, result_df: pd.DataFrame, cfg: Dict[str, Any]) -> pd.DataFrame:
        """
        When granularity == 'item', result_df is grouped by item_id.
        Expand to all unique_ids that share each item_id.
        """
        from db.db import get_conn, get_schema

        schema = get_schema(self.config_path)
        conn = get_conn(self.config_path)
        try:
            uid_map = pd.read_sql(
                f"SELECT DISTINCT unique_id, item_id FROM {schema}.demand_actuals",
                conn,
            )
        finally:
            conn.close()

        # Merge: for each item_id in result_df, get all unique_ids
        item_results = result_df[["item_id", "class_label", "metric_value", "rank", "cumulative_pct"]].copy()
        expanded = uid_map.merge(item_results, on="item_id", how="inner")
        return expanded

    def _save_results(self, config_id: int, result_df: pd.DataFrame) -> None:
        """Delete old results and insert new ones."""
        from db.db import get_conn, get_schema
        from psycopg2.extras import execute_values

        schema = get_schema(self.config_path)
        conn = get_conn(self.config_path)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"DELETE FROM {schema}.abc_results WHERE config_id = %s",
                    (config_id,),
                )
                if not result_df.empty:
                    rows = [
                        (
                            config_id,
                            row["unique_id"],
                            row["class_label"],
                            float(row["metric_value"]) if pd.notna(row["metric_value"]) else None,
                            int(row["rank"]) if pd.notna(row.get("rank")) else None,
                            float(row["cumulative_pct"]) if pd.notna(row.get("cumulative_pct")) else None,
                        )
                        for _, row in result_df.iterrows()
                    ]
                    execute_values(
                        cur,
                        f"INSERT INTO {schema}.abc_results "
                        f"(config_id, unique_id, class_label, metric_value, rank, cumulative_pct) "
                        f"VALUES %s",
                        rows,
                        page_size=5000,
                    )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

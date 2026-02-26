"""
SegmentationEngine — ABC classification and criteria-based segment assignment.

Criteria JSON schema (stored as JSONB in segment.criteria):

  Group node:
    { "type": "group", "operator": "AND" | "OR", "children": [...] }

  Condition node:
    { "type": "condition",
      "field": "<category>.<path>",   e.g. "item.attributes.color"
      "op": "=" | "!=" | "<" | ">" | "<=" | ">=" |
             "contains" | "starts_with" | "is_null" | "is_not_null" |
             "is_true" | "is_false" | "in",
      "valueType": "literal" | "field",
      "value": <scalar or list for 'in'> }

  Empty / null criteria dict  →  matches all series.

Available field keys:
  item.name, item.xuid, item.description, item.type_id
  item.attributes.{key}          (dynamic JSONB keys)
  site.name, site.xuid, site.description, site.type_id
  site.attributes.{key}
  demand.n_observations, demand.mean, demand.std
  demand.zero_ratio, demand.adi, demand.cov
  demand.has_trend, demand.is_intermittent, demand.has_seasonality
  demand.complexity_level, demand.abc_class
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional, Union

import pandas as pd

logger = logging.getLogger(__name__)


class SegmentationEngine:
    """Evaluate segment criteria and assign series to segments."""

    def __init__(self, config_path: Union[str, Path]):
        self.config_path = str(config_path)
        self._series_df: Optional[pd.DataFrame] = None  # lazy cache

    # ─────────────────────────────────────────────────────────────────────
    # Public API
    # ─────────────────────────────────────────────────────────────────────

    def compute_abc_classification(self) -> pd.DataFrame:
        """
        Compute ABC classification from demand_actuals total volume.

        A = top 80 % of cumulative qty   (roughly 20 % of SKUs)
        B = 80–95 %                       (roughly 15 % of SKUs)
        C = 95–100 %                      (remaining ~65 % of SKUs)

        Updates time_series_characteristics.abc_class for every series
        that appears in demand_actuals.

        Returns
        -------
        pd.DataFrame with columns [unique_id, abc_class]
        """
        from db.db import get_conn, get_schema

        schema = get_schema(self.config_path)
        conn = get_conn(self.config_path)
        try:
            df = pd.read_sql(
                f"SELECT unique_id, SUM(COALESCE(corrected_qty, qty)) AS total_qty "
                f"FROM {schema}.demand_actuals "
                f"GROUP BY unique_id",
                conn,
            )
        finally:
            conn.close()

        if df.empty:
            logger.warning("compute_abc_classification: demand_actuals is empty")
            return pd.DataFrame(columns=["unique_id", "abc_class"])

        df = df.sort_values("total_qty", ascending=False).reset_index(drop=True)
        total = df["total_qty"].sum()
        if total == 0:
            df["abc_class"] = "C"
        else:
            df["cum_pct"] = df["total_qty"].cumsum() / total
            df["abc_class"] = "C"
            df.loc[df["cum_pct"] <= 0.95, "abc_class"] = "B"
            df.loc[df["cum_pct"] <= 0.80, "abc_class"] = "A"

        result = df[["unique_id", "abc_class"]].copy()

        # Persist to DB
        conn = get_conn(self.config_path)
        try:
            with conn.cursor() as cur:
                for _, row in result.iterrows():
                    cur.execute(
                        f"""
                        UPDATE {schema}.time_series_characteristics
                           SET abc_class = %s
                         WHERE unique_id = %s
                        """,
                        (row["abc_class"], row["unique_id"]),
                    )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        logger.info(
            "ABC classification: A=%d  B=%d  C=%d",
            (result["abc_class"] == "A").sum(),
            (result["abc_class"] == "B").sum(),
            (result["abc_class"] == "C").sum(),
        )
        return result

    def evaluate_criteria(self, criteria: dict) -> List[str]:
        """
        Evaluate criteria against all known series.

        Empty / null criteria → all unique_ids.

        Returns
        -------
        list[str]  unique_ids that match
        """
        if not criteria:
            df = self._get_series_df()
            return df["unique_id"].tolist()

        df = self._get_series_df()
        mask = self._apply_node(df, criteria)
        return df.loc[mask, "unique_id"].tolist()

    def assign_segment(self, segment_id: int, criteria: dict) -> int:
        """
        Delete existing membership rows for *segment_id* and re-populate
        them from the current criteria.

        Returns
        -------
        int  number of series assigned
        """
        from db.db import get_conn, get_schema

        matched_ids = self.evaluate_criteria(criteria)
        schema = get_schema(self.config_path)

        # Build lookup: unique_id → (item_id, site_id)
        df = self._get_series_df()
        uid_map = df.set_index("unique_id")[["item_id", "site_id"]].to_dict("index")

        rows = [
            (
                segment_id,
                uid,
                uid_map.get(uid, {}).get("item_id"),
                uid_map.get(uid, {}).get("site_id"),
            )
            for uid in matched_ids
        ]

        conn = get_conn(self.config_path)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"DELETE FROM {schema}.segment_membership WHERE segment_id = %s",
                    (segment_id,),
                )
                if rows:
                    import psycopg2.extras

                    psycopg2.extras.execute_values(
                        cur,
                        f"""
                        INSERT INTO {schema}.segment_membership
                            (segment_id, unique_id, item_id, site_id)
                        VALUES %s
                        ON CONFLICT (segment_id, unique_id) DO NOTHING
                        """,
                        rows,
                        page_size=2000,
                    )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        logger.info("Segment %d: assigned %d series", segment_id, len(rows))
        return len(rows)

    def run_all(self) -> Dict[str, int]:
        """
        Full segmentation run:
          1. Compute ABC classification (updates time_series_characteristics)
          2. Assign all series to the 'All' (is_default=TRUE) segment
          3. Re-evaluate every non-default segment against the refreshed data

        Returns
        -------
        dict  {segment_name: count_assigned}
        """
        logger.info("Segmentation: computing ABC classification …")
        self.compute_abc_classification()

        # Invalidate cache so abc_class is fresh for criteria evaluation
        self._series_df = None

        from db.db import get_conn, get_schema

        schema = get_schema(self.config_path)
        conn = get_conn(self.config_path)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT id, name, criteria, is_default FROM {schema}.segment ORDER BY id"
                )
                segments = cur.fetchall()
        finally:
            conn.close()

        results: Dict[str, int] = {}
        for seg_id, seg_name, criteria, is_default in segments:
            if is_default:
                # "All" segment: assign every series from demand_actuals directly
                count = self._assign_all_series(seg_id, schema)
            else:
                count = self.assign_segment(seg_id, criteria or {})
            results[seg_name] = count
            logger.info("  Segment '%s': %d series", seg_name, count)

        return results

    # ─────────────────────────────────────────────────────────────────────
    # Internal helpers
    # ─────────────────────────────────────────────────────────────────────

    def _assign_all_series(self, segment_id: int, schema: str) -> int:
        """Fast path for the 'All' segment — insert every unique_id directly."""
        from db.db import get_conn

        conn = get_conn(self.config_path)
        try:
            with conn.cursor() as cur:
                # Fetch all unique (unique_id, item_id, site_id) combos
                cur.execute(
                    f"""
                    SELECT DISTINCT unique_id,
                           CAST(item_id AS BIGINT),
                           CAST(site_id AS BIGINT)
                    FROM {schema}.demand_actuals
                    WHERE unique_id IS NOT NULL
                    """
                )
                rows_raw = cur.fetchall()

                rows = [(segment_id, r[0], r[1], r[2]) for r in rows_raw]

                cur.execute(
                    f"DELETE FROM {schema}.segment_membership WHERE segment_id = %s",
                    (segment_id,),
                )
                if rows:
                    import psycopg2.extras

                    psycopg2.extras.execute_values(
                        cur,
                        f"""
                        INSERT INTO {schema}.segment_membership
                            (segment_id, unique_id, item_id, site_id)
                        VALUES %s
                        ON CONFLICT (segment_id, unique_id) DO NOTHING
                        """,
                        rows,
                        page_size=2000,
                    )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        return len(rows)

    def _get_series_df(self) -> pd.DataFrame:
        """
        Return (and cache) a flat DataFrame with one row per unique_id.

        Columns:
          unique_id, item_id, site_id,
          item_name, item_xuid, item_description, item_type_id,
          site_name, site_xuid, site_description, site_type_id,
          item_attr_{key}, site_attr_{key},   ← expanded JSONB
          demand.* columns from time_series_characteristics (all numeric/bool/text)
        """
        if self._series_df is not None:
            return self._series_df

        from db.db import get_conn, get_schema

        schema = get_schema(self.config_path)
        conn = get_conn(self.config_path)
        try:
            # Base: one row per unique_id with item / site / demand info
            sql = f"""
                SELECT
                    da.unique_id,
                    da.item_id,
                    da.site_id,
                    i.name           AS item_name,
                    i.xuid           AS item_xuid,
                    i.description    AS item_description,
                    i.type_id        AS item_type_id,
                    i.attributes     AS item_attributes,
                    s.name           AS site_name,
                    s.xuid           AS site_xuid,
                    s.description    AS site_description,
                    s.type_id        AS site_type_id,
                    s.attributes     AS site_attributes,
                    tsc.n_observations,
                    tsc.mean,
                    tsc.std,
                    tsc.zero_ratio,
                    tsc.adi,
                    tsc.cov,
                    tsc.has_trend,
                    tsc.is_intermittent,
                    tsc.has_seasonality,
                    tsc.complexity_level,
                    tsc.abc_class
                FROM (
                    SELECT DISTINCT unique_id,
                           CAST(item_id AS BIGINT) AS item_id,
                           CAST(site_id AS BIGINT) AS site_id
                    FROM {schema}.demand_actuals
                    WHERE unique_id IS NOT NULL
                ) da
                LEFT JOIN {schema}.item i ON i.id = da.item_id
                LEFT JOIN {schema}.site s ON s.id = da.site_id
                LEFT JOIN {schema}.time_series_characteristics tsc
                       ON tsc.unique_id = da.unique_id
            """
            df = pd.read_sql(sql, conn)
        finally:
            conn.close()

        # Expand item.attributes JSONB → item_attr_{key} columns
        df = self._expand_attributes(df, "item_attributes", "item_attr_")
        df = self._expand_attributes(df, "site_attributes", "site_attr_")

        self._series_df = df
        return df

    @staticmethod
    def _expand_attributes(df: pd.DataFrame, col: str, prefix: str) -> pd.DataFrame:
        """
        Expand a JSONB-sourced column (dict/None values) into flat columns.
        Drops the original column.
        """
        if col not in df.columns:
            return df

        def _safe_dict(v):
            if isinstance(v, dict):
                return v
            if isinstance(v, str):
                import json

                try:
                    return json.loads(v)
                except Exception:
                    return {}
            return {}

        expanded = df[col].apply(_safe_dict).apply(pd.Series)
        if not expanded.empty and len(expanded.columns):
            expanded = expanded.rename(
                columns={c: f"{prefix}{c}" for c in expanded.columns}
            )
            df = pd.concat([df.drop(columns=[col]), expanded], axis=1)
        else:
            df = df.drop(columns=[col])
        return df

    # ─── Criteria evaluation ────────────────────────────────────────────

    def _apply_node(self, df: pd.DataFrame, node: dict) -> pd.Series:
        """Recursively evaluate a criteria tree node, returning a boolean mask."""
        node_type = node.get("type", "group")

        if node_type == "group":
            children = node.get("children", [])
            if not children:
                return pd.Series(True, index=df.index)
            operator = node.get("operator", "AND").upper()
            masks = [self._apply_node(df, child) for child in children]
            if operator == "AND":
                result = masks[0]
                for m in masks[1:]:
                    result = result & m
                return result
            else:  # OR
                result = masks[0]
                for m in masks[1:]:
                    result = result | m
                return result

        elif node_type == "condition":
            return self._apply_condition(df, node)

        # Unknown node type → match all
        return pd.Series(True, index=df.index)

    def _apply_condition(self, df: pd.DataFrame, cond: dict) -> pd.Series:
        """Evaluate a single condition against the dataframe."""
        field = cond.get("field", "")
        op = cond.get("op", "=")
        value = cond.get("value")
        value_type = cond.get("valueType", "literal")

        # Map field path → DataFrame column name
        col_name = self._field_to_column(field)
        false_mask = pd.Series(False, index=df.index)

        if col_name not in df.columns:
            logger.warning("Segment criteria: unknown field '%s' (column '%s')", field, col_name)
            return false_mask

        series = df[col_name]

        # Field-to-field comparison
        if value_type == "field":
            right_col = self._field_to_column(str(value))
            if right_col not in df.columns:
                logger.warning(
                    "Segment criteria: unknown right-hand field '%s'", value
                )
                return false_mask
            right = df[right_col]
        else:
            right = value  # scalar / list

        try:
            if op == "=":
                return series == right
            elif op == "!=":
                return series != right
            elif op == "<":
                return pd.to_numeric(series, errors="coerce") < float(right)
            elif op == ">":
                return pd.to_numeric(series, errors="coerce") > float(right)
            elif op == "<=":
                return pd.to_numeric(series, errors="coerce") <= float(right)
            elif op == ">=":
                return pd.to_numeric(series, errors="coerce") >= float(right)
            elif op == "contains":
                return series.astype(str).str.contains(str(right), case=False, na=False)
            elif op == "starts_with":
                return series.astype(str).str.startswith(str(right), na=False)
            elif op == "is_null":
                return series.isna()
            elif op == "is_not_null":
                return series.notna()
            elif op == "is_true":
                return series.fillna(False).astype(bool)
            elif op == "is_false":
                return ~series.fillna(False).astype(bool)
            elif op == "in":
                if not isinstance(right, list):
                    right = [right]
                return series.isin(right)
            else:
                logger.warning("Segment criteria: unsupported operator '%s'", op)
                return false_mask
        except Exception as exc:
            logger.warning(
                "Segment criteria error evaluating '%s %s %s': %s", field, op, right, exc
            )
            return false_mask

    @staticmethod
    def _field_to_column(field: str) -> str:
        """
        Map a criteria field key to the corresponding DataFrame column name.

        item.name                → item_name
        item.xuid                → item_xuid
        item.type_id             → item_type_id
        item.attributes.color    → item_attr_color
        site.name                → site_name
        site.attributes.country  → site_attr_country
        demand.n_observations    → n_observations
        demand.abc_class         → abc_class
        demand.has_trend         → has_trend
        """
        if field.startswith("item.attributes."):
            key = field[len("item.attributes."):]
            return f"item_attr_{key}"
        if field.startswith("site.attributes."):
            key = field[len("site.attributes."):]
            return f"site_attr_{key}"
        if field.startswith("item."):
            sub = field[len("item."):]
            return f"item_{sub}"
        if field.startswith("site."):
            sub = field[len("site."):]
            return f"site_{sub}"
        if field.startswith("demand."):
            return field[len("demand."):]
        return field

"""
ParameterResolver — Bulk-load parameter assignments and resolve per-SKU config.

Resolution hierarchy (per unique_id per business type):
  1. hyperparameter_overrides  (per-SKU, highest priority — partial key merge)
  2. parameters.parameters_set via series_parameter_assignment  (segment-based)
  3. Default parameter set (is_default=TRUE fallback)

Usage:
    resolver = ParameterResolver(config_path)
    groups = resolver.group_series_by_param_set(uid_list, 'forecasting')
    for param_id, uids in groups.items():
        override = resolver.build_group_config_override(param_id, 'forecasting')
        forecaster = StatisticalForecaster(config_path, config_override=override)
        ...
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Set, Union

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

BTYPE_COLUMNS = {
    "forecasting": "forecasting_parameter_id",
    "outlier_detection": "outlier_detection_parameter_id",
    "characterization": "characterization_parameter_id",
    "evaluation": "evaluation_parameter_id",
    "best_method": "best_method_parameter_id",
}

# Which top-level config.yaml section each business type maps to
BTYPE_CONFIG_SECTION = {
    "forecasting": "forecasting",
    "outlier_detection": "outlier_detection",
    "characterization": "characterization",
    "evaluation": "forecasting",       # evaluator reads from forecasting.backtesting
    "best_method": "best_method",
}

BUSINESS_TYPES = list(BTYPE_COLUMNS.keys())


class ParameterResolver:
    """
    Bulk-loads parameter assignments, parameter sets, and per-SKU overrides
    into memory for fast lookup during pipeline execution.
    """

    def __init__(self, config_path=None):
        self.config_path = str(config_path) if config_path else None
        # {unique_id: {btype: param_id}}
        self._assignment_map: Dict[str, Dict[str, Optional[int]]] = {}
        # {param_id: parameters_set dict}
        self._param_by_id: Dict[int, dict] = {}
        # {btype: default parameters_set dict}
        self._defaults: Dict[str, dict] = {}
        # {unique_id: {method_or_btype: overrides_dict}}
        self._overrides_by_uid: Dict[str, Dict[str, dict]] = {}

        self._bulk_load()

    # ─────────────────────────────────────────────────────────────────────
    # Bulk load
    # ─────────────────────────────────────────────────────────────────────

    def _bulk_load(self) -> None:
        """Load all three tables in a single DB connection."""
        from db.db import get_conn, get_schema

        schema = get_schema()
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                # 1. series_parameter_assignment
                cur.execute(
                    f"SELECT unique_id, forecasting_parameter_id, "
                    f"outlier_detection_parameter_id, characterization_parameter_id, "
                    f"evaluation_parameter_id, best_method_parameter_id "
                    f"FROM {schema}.series_parameter_assignment"
                )
                for row in cur.fetchall():
                    uid = row[0]
                    self._assignment_map[uid] = {
                        "forecasting": row[1],
                        "outlier_detection": row[2],
                        "characterization": row[3],
                        "evaluation": row[4],
                        "best_method": row[5],
                    }

                # 2. parameters (all versions)
                cur.execute(
                    f"SELECT id, parameter_type, parameters_set, is_default "
                    f"FROM {schema}.parameters"
                )
                for pid, ptype, pset, is_def in cur.fetchall():
                    ps = pset if isinstance(pset, dict) else json.loads(pset)
                    self._param_by_id[pid] = ps
                    if is_def:
                        self._defaults[ptype] = ps

                # 3. hyperparameter_overrides
                cur.execute(
                    f"SELECT unique_id, method, overrides "
                    f"FROM {schema}.hyperparameter_overrides"
                )
                for uid, method, ovr in cur.fetchall():
                    ovr_dict = ovr if isinstance(ovr, dict) else json.loads(ovr)
                    self._overrides_by_uid.setdefault(uid, {})[method] = ovr_dict

        except Exception as exc:
            logger.warning("ParameterResolver bulk load failed: %s", exc)
        finally:
            conn.close()

        logger.info(
            "ParameterResolver loaded: %d assignments, %d param sets, %d override series",
            len(self._assignment_map),
            len(self._param_by_id),
            len(self._overrides_by_uid),
        )

    # ─────────────────────────────────────────────────────────────────────
    # Static helpers
    # ─────────────────────────────────────────────────────────────────────

    @staticmethod
    def deep_merge(base: dict, override: dict) -> dict:
        """
        Recursively merge *override* into *base*. Override keys win.
        Returns a new dict (does not mutate either input).
        """
        merged = dict(base)
        for key, value in override.items():
            if (
                key in merged
                and isinstance(merged[key], dict)
                and isinstance(value, dict)
            ):
                merged[key] = ParameterResolver.deep_merge(merged[key], value)
            else:
                merged[key] = value
        return merged

    # ─────────────────────────────────────────────────────────────────────
    # Resolution
    # ─────────────────────────────────────────────────────────────────────

    def resolve(self, unique_id: str, business_type: str) -> dict:
        """
        Get the effective config section for a series + business type.

        Resolution hierarchy:
          1. hyperparameter_overrides (per-SKU)
          2. parameters.parameters_set (segment-based assignment)
          3. Default parameter set (fallback)
        """
        # Base: segment-assigned parameter set (or default)
        assignment = self._assignment_map.get(unique_id, {})
        param_id = assignment.get(business_type)

        if param_id is not None and param_id in self._param_by_id:
            base = dict(self._param_by_id[param_id])
        else:
            base = dict(self._defaults.get(business_type, {}))

        # Per-SKU override (partial merge)
        uid_overrides = self._overrides_by_uid.get(unique_id, {})
        sku_override = uid_overrides.get(business_type, {})
        if sku_override:
            base = self.deep_merge(base, sku_override)

        return base

    def get_param_id_for_series(
        self, unique_id: str, business_type: str
    ) -> Optional[int]:
        """Return the assigned parameter ID (or None for default)."""
        return self._assignment_map.get(unique_id, {}).get(business_type)

    # ─────────────────────────────────────────────────────────────────────
    # Grouping
    # ─────────────────────────────────────────────────────────────────────

    def group_series_by_param_set(
        self, unique_ids: List[str], business_type: str
    ) -> Dict[Optional[int], List[str]]:
        """
        Group series by their assigned parameter_set_id for batch processing.

        Returns {param_id: [uid, ...]} where param_id=None means "default".
        """
        groups: Dict[Optional[int], List[str]] = {}
        for uid in unique_ids:
            pid = self._assignment_map.get(uid, {}).get(business_type)
            groups.setdefault(pid, []).append(uid)
        return groups

    def get_series_with_overrides(
        self, unique_ids: List[str], business_type: str
    ) -> Set[str]:
        """Return the subset of unique_ids that have per-SKU overrides for this type."""
        return {
            uid
            for uid in unique_ids
            if business_type in self._overrides_by_uid.get(uid, {})
        }

    # ─────────────────────────────────────────────────────────────────────
    # Config override builders
    # ─────────────────────────────────────────────────────────────────────

    def build_group_config_override(
        self, param_id: Optional[int], business_type: str
    ) -> Optional[dict]:
        """
        Build a config override dict for a parameter group.

        Returns {config_section: parameters_set} suitable for passing to
        a component constructor as config_override, or None if using defaults.
        """
        if param_id is None:
            # Default parameter set — component loads defaults from DB
            return None

        params_set = self._param_by_id.get(param_id)
        if not params_set:
            return None

        section_key = BTYPE_CONFIG_SECTION.get(business_type, business_type)
        return {section_key: params_set}

    def build_config_override(
        self, unique_id: str, business_type: str
    ) -> Optional[dict]:
        """
        Build a per-series config override (includes group params + SKU overrides).

        Returns {config_section: merged_params} or None if using defaults.
        """
        resolved = self.resolve(unique_id, business_type)
        if not resolved:
            return None

        section_key = BTYPE_CONFIG_SECTION.get(business_type, business_type)
        return {section_key: resolved}

    @property
    def is_loaded(self) -> bool:
        """True if any assignments or parameters were loaded."""
        return bool(self._assignment_map) or bool(self._param_by_id)

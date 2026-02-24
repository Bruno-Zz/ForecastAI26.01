"""
Best Method Selection Module
Ranks forecasting methods per time series after backtesting and selects the winner
based on a weighted composite score of multiple accuracy metrics.
"""

import pandas as pd
import numpy as np
from typing import Dict, Optional
import logging
import yaml
from pathlib import Path


# ---------------------------------------------------------------------------
# Default metric weights used when no config file is provided / section missing
# ---------------------------------------------------------------------------
_DEFAULT_WEIGHTS = {
    "mae": 0.40,
    "rmse": 0.20,
    "bias": 0.15,
    "coverage_90": 0.15,
    "mase": 0.10,
}


class MethodSelector:
    """
    Selects the best forecasting method for every time series based on
    backtest metrics.  Methods are ranked using a weighted composite score
    that combines normalised accuracy and coverage measures.

    Lower composite_score == better.
    """

    def __init__(self, config_path: str = "config/config.yaml"):
        """
        Initialise the selector.

        Parameters
        ----------
        config_path : str
            Path to the YAML configuration file.  The weights are read from
            the ``best_method.weights`` section.  If the file or section is
            missing the hard-coded defaults are used instead.
        """
        self.logger = logging.getLogger(__name__)
        self.weights = self._load_weights(config_path)
        self.logger.info(
            "MethodSelector initialised with weights: %s", self.weights
        )

    # ------------------------------------------------------------------
    # Weight loading
    # ------------------------------------------------------------------
    def _load_weights(self, config_path: str) -> Dict[str, float]:
        """Load metric weights from config, falling back to defaults."""
        try:
            cfg_file = Path(config_path)
            if cfg_file.exists():
                with open(cfg_file, "r") as fh:
                    config = yaml.safe_load(fh) or {}
                weights = (
                    config.get("best_method", {}).get("weights", None)
                )
                if weights is not None:
                    self.logger.info(
                        "Loaded weights from config: %s", config_path
                    )
                    return {str(k): float(v) for k, v in weights.items()}
            self.logger.warning(
                "Config '%s' not found or missing best_method.weights – "
                "using default weights.",
                config_path,
            )
        except Exception as exc:
            self.logger.warning(
                "Failed to read config '%s': %s – using default weights.",
                config_path,
                exc,
            )
        return dict(_DEFAULT_WEIGHTS)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def select_best_methods(
        self, metrics_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Rank methods for every unique_id and return the best / runner-up.

        Parameters
        ----------
        metrics_df : pd.DataFrame
            Backtest metrics produced by :class:`ForecastEvaluator`.  Expected
            columns include at least ``unique_id``, ``method``,
            ``forecast_origin``, and the metric columns referenced by the
            configured weights (e.g. ``mae``, ``rmse``, ``bias``,
            ``coverage_90``, ``mase``).

        Returns
        -------
        pd.DataFrame
            One row per unique_id with columns:
            ``unique_id``, ``best_method``, ``best_score``,
            ``runner_up_method``, ``runner_up_score``, ``all_rankings``.
        """
        if metrics_df is None or metrics_df.empty:
            self.logger.warning("metrics_df is empty – returning empty result.")
            return self._empty_result()

        required_cols = {"unique_id", "method"}
        missing = required_cols - set(metrics_df.columns)
        if missing:
            raise ValueError(
                f"metrics_df is missing required columns: {missing}"
            )

        results = []
        for uid, group in metrics_df.groupby("unique_id"):
            row = self._rank_methods_for_series(uid, group)
            results.append(row)

        if not results:
            return self._empty_result()

        result_df = pd.DataFrame(results)

        # Persist to parquet
        self._save_output(result_df)

        return result_df

    def get_best_method(
        self, unique_id: str, metrics_df: pd.DataFrame
    ) -> str:
        """
        Convenience helper – return the best method name for *one* series.

        Parameters
        ----------
        unique_id : str
            Series identifier.
        metrics_df : pd.DataFrame
            Full backtest metrics (may contain many series).

        Returns
        -------
        str
            Name of the winning method, or ``""`` when no valid result
            can be determined.
        """
        if metrics_df is None or metrics_df.empty:
            self.logger.warning(
                "metrics_df is empty – cannot determine best method "
                "for '%s'.",
                unique_id,
            )
            return ""

        series_metrics = metrics_df.loc[
            metrics_df["unique_id"] == unique_id
        ]
        if series_metrics.empty:
            self.logger.warning(
                "No metrics found for unique_id='%s'.", unique_id
            )
            return ""

        row = self._rank_methods_for_series(unique_id, series_metrics)
        return row.get("best_method", "")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _rank_methods_for_series(
        self, unique_id: str, group: pd.DataFrame
    ) -> Dict:
        """
        Compute composite scores and rank methods for a single series.

        Steps
        -----
        1. Average metrics across all forecast origins (windows).
        2. Min-max normalise each metric to [0, 1] within the series.
        3. Use |bias| so that both positive and negative bias are penalised.
        4. Transform coverage_90 so that deviation from 0.9 is penalised.
        5. Compute weighted composite score.
        6. Rank methods (lower is better).
        """
        metric_cols = [m for m in self.weights if m in group.columns]

        if not metric_cols:
            self.logger.warning(
                "No usable metric columns for '%s'. "
                "Returning first method as default.",
                unique_id,
            )
            return self._default_result(unique_id, group)

        # --- Step 1: average across forecast origins -----------------------
        avg = (
            group.groupby("method")[metric_cols]
            .mean()
            .copy()
        )

        # Handle the case where every metric is NaN for all methods
        if avg[metric_cols].isna().all().all():
            self.logger.warning(
                "All metrics are NaN for '%s'. "
                "Returning first method as default.",
                unique_id,
            )
            return self._default_result(unique_id, group)

        # --- Step 2 & 3: transform before normalisation --------------------
        # Bias: use absolute value
        if "bias" in avg.columns:
            avg["bias"] = avg["bias"].abs()

        # Coverage_90: penalise deviation from the nominal 0.9 level
        if "coverage_90" in avg.columns:
            avg["coverage_90"] = (avg["coverage_90"] - 0.9).abs()

        # --- Step 4: min-max normalisation to [0, 1] -----------------------
        normed = avg.copy()
        for col in metric_cols:
            col_series = avg[col].dropna()
            if col_series.empty:
                normed[col] = np.nan
                continue
            col_min = col_series.min()
            col_max = col_series.max()
            if col_max - col_min > 0:
                normed[col] = (avg[col] - col_min) / (col_max - col_min)
            else:
                # All methods share the same value – no discriminating power
                normed[col] = 0.0

        # --- Step 5: weighted composite score ------------------------------
        composite = pd.Series(0.0, index=normed.index)
        total_weight = 0.0

        for col in metric_cols:
            w = self.weights[col]
            valid_mask = normed[col].notna()
            composite[valid_mask] += w * normed.loc[valid_mask, col]
            # Only accumulate weight for metrics that actually exist for a
            # method so that the score is comparable across methods with
            # different metric availability.
            total_weight_per_method = valid_mask.astype(float) * w
            # We normalise later per method.
            composite += 0  # no-op; keep for clarity

        # Re-normalise: divide each method's score by the sum of weights of
        # the metrics that were *not* NaN for that method.
        weight_sums = pd.Series(0.0, index=normed.index)
        for col in metric_cols:
            w = self.weights[col]
            weight_sums += normed[col].notna().astype(float) * w

        # Recompute composite properly
        composite = pd.Series(0.0, index=normed.index)
        for col in metric_cols:
            w = self.weights[col]
            filled = normed[col].fillna(0.0)
            composite += w * filled

        # Normalise by available weight so methods missing some metrics
        # are not unfairly advantaged.
        weight_sums = weight_sums.replace(0.0, np.nan)
        composite = composite / weight_sums

        # Methods where *all* metrics were NaN get score = NaN
        all_nan_mask = normed[metric_cols].isna().all(axis=1)
        composite[all_nan_mask] = np.nan

        # --- Step 6: rank --------------------------------------------------
        rankings = composite.dropna().sort_values()

        all_rankings = rankings.to_dict()

        if rankings.empty:
            return self._default_result(unique_id, group)

        best_method = rankings.index[0]
        best_score = float(rankings.iloc[0])

        if len(rankings) >= 2:
            runner_up_method = rankings.index[1]
            runner_up_score = float(rankings.iloc[1])
        else:
            runner_up_method = None
            runner_up_score = None

        self.logger.debug(
            "Series '%s': best=%s (%.4f), runner_up=%s",
            unique_id,
            best_method,
            best_score,
            runner_up_method,
        )

        return {
            "unique_id": unique_id,
            "best_method": best_method,
            "best_score": best_score,
            "runner_up_method": runner_up_method,
            "runner_up_score": runner_up_score,
            "all_rankings": all_rankings,
        }

    @staticmethod
    def _default_result(unique_id: str, group: pd.DataFrame) -> Dict:
        """Fallback result when ranking cannot be performed."""
        methods = group["method"].unique()
        first_method = str(methods[0]) if len(methods) > 0 else ""
        return {
            "unique_id": unique_id,
            "best_method": first_method,
            "best_score": np.nan,
            "runner_up_method": None,
            "runner_up_score": None,
            "all_rankings": {first_method: np.nan} if first_method else {},
        }

    @staticmethod
    def _empty_result() -> pd.DataFrame:
        """Return an empty DataFrame with the expected schema."""
        return pd.DataFrame(
            columns=[
                "unique_id",
                "best_method",
                "best_score",
                "runner_up_method",
                "runner_up_score",
                "all_rankings",
            ]
        )

    def _save_output(self, result_df: pd.DataFrame) -> None:
        """Persist the selection results to the database."""
        try:
            from db.db import bulk_insert, get_schema, jsonb_serialize
            config_path = 'config/config.yaml'
            schema = get_schema(config_path)
            cols = list(result_df.columns)
            rows = [
                tuple(jsonb_serialize(v) for v in row)
                for row in result_df.itertuples(index=False, name=None)
            ]
            n = bulk_insert(config_path, f"{schema}.best_method_per_series", cols, rows)
            self.logger.info(
                "Best-method results saved to %s.best_method_per_series (%d series).",
                schema,
                n,
            )
        except Exception as exc:
            self.logger.error(
                "Failed to save best-method results to DB: %s",
                exc,
            )


# -----------------------------------------------------------------------
# Standalone entry point
# -----------------------------------------------------------------------
def main():
    """Example / CLI usage of MethodSelector."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(name)s  %(levelname)s  %(message)s",
    )

    from db.db import load_table, get_schema

    config_path = 'config/config.yaml'
    schema = get_schema(config_path)

    # Load backtest metrics from PostgreSQL
    metrics_df = load_table(config_path, f"{schema}.backtest_metrics")
    if metrics_df.empty:
        print(
            "Backtest metrics table is empty. "
            "Run the evaluation pipeline first."
        )
        return

    print(f"Loaded {len(metrics_df)} backtest metric rows.")

    selector = MethodSelector()
    results = selector.select_best_methods(metrics_df)

    print(f"\nBest methods selected for {len(results)} series:")
    print(
        results[
            ["unique_id", "best_method", "best_score",
             "runner_up_method", "runner_up_score"]
        ].to_string(index=False)
    )


if __name__ == "__main__":
    main()

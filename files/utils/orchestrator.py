"""
Parallel Forecast Orchestrator
Coordinates all forecasting methods across time series using Dask.

Pipeline steps:
  1. ETL: Extract from source, transform, load to PostgreSQL
  2. Characterization: Analyze each series (seasonality, trend, intermittency, complexity)
  3. Forecasting: Statistical + Neural + Foundation + ML models
  4. Backtesting: Rolling-window evaluation with per-origin forecast storage
  5. Best method selection: Composite-score ranking per series
  6. Distribution fitting: Parametric distributions for MEIO
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional
import logging
import uuid
import yaml
from pathlib import Path
import time
from datetime import datetime
from tqdm import tqdm

try:
    from dask.distributed import Client, as_completed
    import dask
    DASK_AVAILABLE = True
except ImportError:
    DASK_AVAILABLE = False
    logging.debug("Dask not available (optional). Install with: pip install dask[complete]")

# Core forecasting modules
from forecasting.statistical_models import StatisticalForecaster
from forecasting.neural_models import NeuralForecaster
from forecasting.foundation_models import FoundationForecaster
from forecasting.ml_models import MLForecaster

# Evaluation and distribution
from evaluation.metrics import ForecastEvaluator
from distribution.fitting import DistributionFitter

# New modules
from etl.etl import ETLPipeline
from characterization.characterization import TimeSeriesCharacterizer
from selection.best_method import MethodSelector
from outlier.detection import OutlierDetector
from utils.process_logger import ProcessLogger, ListHandler


def _dask_forecast_batch(config_path: str,
                         batch_df: pd.DataFrame,
                         batch_chars: pd.DataFrame,
                         config_override: dict = None) -> pd.DataFrame:
    """
    Module-level standalone function submitted to Dask workers.

    Must be a plain function (not a bound method) so it can be pickled.
    Each worker reconstructs the forecasters from the config path; no
    asyncio state is transmitted.

    Args:
        config_path: Path to config.yaml (picklable string).
        batch_df: Time series data slice for this batch.
        batch_chars: Characteristics slice for this batch.
        config_override: Optional dict deep-merged onto config.yaml before
            component construction (parameter-aware pipeline).

    Returns:
        DataFrame with forecast results for the batch.
    """
    import logging
    logger = logging.getLogger(__name__)

    all_forecasts = []

    # Statistical forecasts (always attempted)
    try:
        stat_forecaster = StatisticalForecaster(config_path, config_override=config_override)
        stat_forecasts = stat_forecaster.forecast_multiple_series(
            df=batch_df,
            characteristics_df=batch_chars,
            show_progress=False,
        )
        if not stat_forecasts.empty:
            all_forecasts.append(stat_forecasts)
    except Exception as e:
        logger.warning(f"Statistical forecasting failed in worker: {e}")

    # ML forecasts (LightGBM / XGBoost) — only for eligible series
    # Recompute eligibility from current config thresholds so we don't rely on
    # stale booleans that may have been stored with different threshold settings.
    try:
        import yaml as _yaml
        with open(config_path, 'r') as _fh:
            _cfg = _yaml.safe_load(_fh)
        if config_override:
            from utils.parameter_resolver import ParameterResolver
            _cfg = ParameterResolver.deep_merge(_cfg, config_override)
        _sufficiency = _cfg.get('characterization', {}).get('data_sufficiency', {})
        _min_ml = _sufficiency.get('min_for_ml', 100)
        _min_dl = _sufficiency.get('min_for_deep_learning', 200)
    except Exception:
        _min_ml, _min_dl = 100, 200

    if 'n_observations' in batch_chars.columns:
        ml_eligible = batch_chars[batch_chars['n_observations'] >= _min_ml]
    else:
        ml_eligible = batch_chars[
            batch_chars.get('sufficient_for_ml', pd.Series(dtype=bool)).fillna(False)
        ] if 'sufficient_for_ml' in batch_chars.columns else pd.DataFrame()

    if len(ml_eligible) > 0:
        try:
            ml_forecaster = MLForecaster(config_path, config_override=config_override)
            ml_forecasts = ml_forecaster.forecast_multiple_series(
                df=batch_df,
                characteristics_df=ml_eligible,
                show_progress=False,
            )
            if not ml_forecasts.empty:
                all_forecasts.append(ml_forecasts)
        except Exception as e:
            logger.warning(f"ML forecasting failed in worker: {e}")

    if all_forecasts:
        return pd.concat(all_forecasts, ignore_index=True)
    return pd.DataFrame()


# Statistical methods known to StatisticalForecaster — used for backtest filtering
_STAT_METHODS = {
    'AutoARIMA', 'AutoETS', 'AutoTheta', 'AutoCES', 'MSTL',
    'CrostonOptimized', 'ADIDA', 'IMAPA', 'TSB',
    'SeasonalNaive', 'HistoricAverage', 'Naive', 'SeasonalWindowAverage'
}


def _dask_backtest_batch(config_path: str,
                         batch_df: pd.DataFrame,
                         batch_work: list,
                         bt_overrides_map: dict = None,
                         fc_config_override: dict = None,
                         eval_config_override: dict = None) -> tuple:
    """
    Module-level standalone function submitted to Dask workers for backtesting.

    Processes a *batch* of series in one worker call so that StatisticalForecaster
    and ForecastEvaluator are constructed only once per task, amortising the
    YAML-parse / import overhead across many series.

    Must be a plain function (not a bound method) so it can be pickled.

    Args:
        config_path: Path to config.yaml (picklable string).
        batch_df:    DataFrame rows for all series in this batch (unique_id, date, y).
        batch_work:  List of (unique_id, methods, characteristics_dict) tuples.
        bt_overrides_map: Optional {unique_id: {backtest_horizon, window_size, n_tests}}
                          per-series backtesting overrides loaded from the DB.
        fc_config_override: Optional config override for forecasting components.
        eval_config_override: Optional config override for evaluation component.

    Returns:
        Tuple (metrics_df, origin_forecasts_df) — concatenation of all series
        results; both may be empty DataFrames.
    """
    import logging
    logger = logging.getLogger(__name__)

    stat_forecaster = StatisticalForecaster(config_path, config_override=fc_config_override)
    evaluator = ForecastEvaluator(config_path, config_override=eval_config_override)
    use_with_forecasts = hasattr(evaluator, 'backtest_series_with_forecasts')

    # Lazily construct MLForecaster only when ML methods are present in the batch
    _ml_methods = {'LightGBM', 'XGBoost'}
    _needs_ml = any(
        any(m in _ml_methods for m in methods)
        for _, methods, _ in batch_work
    )
    ml_forecaster = None
    if _needs_ml:
        try:
            ml_forecaster = MLForecaster(config_path, config_override=fc_config_override)
        except Exception as exc:
            logger.warning(f"Could not init MLForecaster in worker: {exc}")

    def _combined_forecast_fn(df, unique_id, methods, characteristics, **kw):
        """Delegate to stat and/or ML forecaster depending on method names."""
        results = []
        stat = [m for m in methods if m in _STAT_METHODS]
        ml = [m for m in methods if m in _ml_methods]
        if stat:
            results.extend(
                stat_forecaster.forecast_single_series(
                    df=df, unique_id=unique_id, methods=stat,
                    characteristics=characteristics, **kw,
                )
            )
        if ml and ml_forecaster is not None:
            results.extend(
                ml_forecaster.forecast_single_series(
                    df=df, unique_id=unique_id, methods=ml,
                    characteristics=characteristics, **kw,
                )
            )
        return results

    # Choose forecast function: combined when ML methods present, stat-only otherwise
    forecast_fn = _combined_forecast_fn if _needs_ml else stat_forecaster.forecast_single_series

    all_metrics = []
    all_origins = []

    for unique_id, methods, characteristics in batch_work:
        series_df = batch_df[batch_df['unique_id'] == unique_id]
        bt_ovr = (bt_overrides_map or {}).get(unique_id, {})
        try:
            if use_with_forecasts:
                metrics_df, origin_df = evaluator.backtest_series_with_forecasts(
                    df=series_df,
                    unique_id=unique_id,
                    forecast_fn=forecast_fn,
                    methods=methods,
                    characteristics=characteristics,
                    backtest_overrides=bt_ovr,
                )
                if not origin_df.empty:
                    all_origins.append(origin_df)
            else:
                metrics_df = evaluator.backtest_series(
                    df=series_df,
                    unique_id=unique_id,
                    forecast_fn=forecast_fn,
                    methods=methods,
                    characteristics=characteristics,
                    backtest_overrides=bt_ovr,
                )
            if not metrics_df.empty:
                all_metrics.append(metrics_df)
        except Exception as exc:
            logger.warning(f"Backtest worker failed for {unique_id}: {exc}")

    metrics_out = pd.concat(all_metrics, ignore_index=True) if all_metrics else pd.DataFrame()
    origins_out = pd.concat(all_origins, ignore_index=True) if all_origins else pd.DataFrame()

    # ---- ML internal validation fallback (per-batch) ----
    _ml_bt = {'LightGBM', 'XGBoost'}
    ml_fallback_work = []

    # 1. ML methods with all-NaN MAE
    if not metrics_out.empty:
        for uid, grp in metrics_out.groupby('unique_id'):
            for mname in grp['method'].unique():
                if mname in _ml_bt and grp.loc[grp['method'] == mname, 'mae'].isna().all():
                    chars = next((c for u, _, c in batch_work if u == uid), None)
                    if chars:
                        ml_fallback_work.append((uid, [mname], chars))

    # 2. ML methods that produced no metric rows at all
    existing_pairs = set()
    if not metrics_out.empty:
        for _, row in metrics_out.iterrows():
            if row['method'] in _ml_bt:
                existing_pairs.add((row['unique_id'], row['method']))
    for uid, methods, chars in batch_work:
        for m in methods:
            if m in _ml_bt and (uid, m) not in existing_pairs:
                ml_fallback_work.append((uid, [m], chars))

    if ml_fallback_work:
        internal_df = _run_ml_internal_validation(config_path, batch_df, ml_fallback_work)
        if not internal_df.empty:
            metrics_out = pd.concat([metrics_out, internal_df], ignore_index=True)

    return metrics_out, origins_out


def _run_ml_internal_validation(
    config_path: str,
    df: pd.DataFrame,
    ml_fallback_work: list,
) -> pd.DataFrame:
    """
    For ML methods that produced no valid backtest metrics, run a single
    full-series ML forecast and extract internal validation metrics from
    the model's 80/20 train/val split.

    Module-level function for Dask picklability.

    Args:
        config_path: Path to config.yaml.
        df: Full time-series data (unique_id, date, y).
        ml_fallback_work: List of (unique_id, [method], characteristics_dict).

    Returns:
        DataFrame of EvaluationMetrics rows from internal validation.
    """
    import logging
    _logger = logging.getLogger(__name__)

    if not ml_fallback_work:
        return pd.DataFrame()

    from evaluation.metrics import ForecastEvaluator

    try:
        ml_forecaster = MLForecaster(config_path)
    except Exception as exc:
        _logger.warning(f"Cannot init MLForecaster for internal validation: {exc}")
        return pd.DataFrame()

    evaluator = ForecastEvaluator(config_path)
    horizon = ml_forecaster.horizon
    _val_split = ml_forecaster.val_split  # config default (e.g. 0.2)
    all_rows = []

    for unique_id, ml_methods, characteristics in ml_fallback_work:
        series_df = df[df['unique_id'] == unique_id]
        if series_df.empty:
            continue

        # Compute the split date for forecast_origin using the configured val_split
        dates = pd.to_datetime(series_df['date'] if 'date' in series_df.columns else series_df.get('ds'))
        dates_sorted = dates.sort_values()
        split_idx = int(len(dates_sorted) * (1 - _val_split))
        origin_date = str(dates_sorted.iloc[min(split_idx, len(dates_sorted) - 1)].date())

        for method in ml_methods:
            try:
                results = ml_forecaster.forecast_single_series(
                    df=series_df,
                    unique_id=unique_id,
                    methods=[method],
                    characteristics=characteristics,
                )
                for result in results:
                    if result.internal_val_metrics is not None:
                        eval_m = evaluator.create_eval_metrics_from_internal_validation(
                            unique_id=unique_id,
                            method=method,
                            horizon=horizon,
                            internal_metrics=result.internal_val_metrics,
                            forecast_origin_date=origin_date,
                        )
                        all_rows.append(eval_m.to_dict())
                        _logger.debug(
                            f"Internal val metrics captured for {unique_id}/{method} "
                            f"(MAE={result.internal_val_metrics.get('mae', '?'):.4f})"
                        )
            except Exception as e:
                _logger.warning(
                    f"Internal validation fallback failed for {unique_id}/{method}: {e}"
                )

    return pd.DataFrame(all_rows) if all_rows else pd.DataFrame()


def _dask_distribution_batch(config_path: str,
                              batch_df: pd.DataFrame) -> pd.DataFrame:
    """
    Module-level standalone function submitted to Dask workers for distribution fitting.

    Constructs DistributionFitter once per task and fits all rows in the batch,
    amortising init overhead across many (unique_id, method) pairs.

    Args:
        config_path: Path to config.yaml (picklable string).
        batch_df:    Slice of forecasts_df for this batch.

    Returns:
        DataFrame with fitted distribution rows; may be empty.
    """
    import logging
    logger = logging.getLogger(__name__)
    try:
        fitter = DistributionFitter(config_path)
        return fitter.fit_forecast_distributions(batch_df)
    except Exception as exc:
        logger.warning(f"Distribution fitting batch failed: {exc}")
        return pd.DataFrame()


class ForecastOrchestrator:
    """
    Orchestrates the full forecasting pipeline from ETL through distribution fitting.
    """

    def __init__(self, config_path: str = "config/config.yaml"):
        """Initialize orchestrator with configuration."""
        self.config_path = config_path
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)

        self.parallel_config = self.config['parallel']
        self.output_config = self.config['output']
        self.logger = logging.getLogger(__name__)

        # Lazy-initialized components
        self._stat_forecaster = None
        self._neural_forecaster = None
        self._foundation_forecaster = None
        self._ml_forecaster = None
        self._evaluator = None
        self._dist_fitter = None
        self._etl = None
        self._characterizer = None
        self._method_selector = None
        self._outlier_detector = None
        self._parameter_resolver = None

        # Dask client
        self.client = None

    # -- Lazy property accessors --

    @property
    def stat_forecaster(self):
        if self._stat_forecaster is None:
            self._stat_forecaster = StatisticalForecaster(self.config_path)
        return self._stat_forecaster

    @property
    def neural_forecaster(self):
        if self._neural_forecaster is None:
            self._neural_forecaster = NeuralForecaster(self.config_path)
        return self._neural_forecaster

    @property
    def foundation_forecaster(self):
        if self._foundation_forecaster is None:
            self._foundation_forecaster = FoundationForecaster(self.config_path)
        return self._foundation_forecaster

    @property
    def ml_forecaster(self):
        if self._ml_forecaster is None:
            self._ml_forecaster = MLForecaster(self.config_path)
        return self._ml_forecaster

    @property
    def evaluator(self):
        if self._evaluator is None:
            self._evaluator = ForecastEvaluator(self.config_path)
        return self._evaluator

    @property
    def dist_fitter(self):
        if self._dist_fitter is None:
            self._dist_fitter = DistributionFitter(self.config_path)
        return self._dist_fitter

    @property
    def etl(self):
        if self._etl is None:
            self._etl = ETLPipeline(self.config_path)
        return self._etl

    @property
    def characterizer(self):
        if self._characterizer is None:
            self._characterizer = TimeSeriesCharacterizer(self.config_path)
        return self._characterizer

    @property
    def method_selector(self):
        if self._method_selector is None:
            self._method_selector = MethodSelector(self.config_path)
        return self._method_selector

    @property
    def outlier_detector(self):
        if self._outlier_detector is None:
            self._outlier_detector = OutlierDetector(self.config_path)
        return self._outlier_detector

    @property
    def parameter_resolver(self):
        """Lazy ParameterResolver.  Returns None when DB/table unavailable."""
        if self._parameter_resolver is None:
            try:
                from utils.parameter_resolver import ParameterResolver
                self._parameter_resolver = ParameterResolver(str(self.config_path))
            except Exception:
                self._parameter_resolver = False  # sentinel – don't retry
        return self._parameter_resolver if self._parameter_resolver is not False else None

    def _get_param_groups(self, unique_ids, business_type):
        """
        Group *unique_ids* by their assigned parameter set for *business_type*.

        Returns a list of ``(config_override | None, [uids])`` tuples.
        Series that have per-SKU hyperparameter overrides are separated into
        their own single-element groups so they get a fully-merged config.

        When the resolver is unavailable (first run, empty table) a single
        ``(None, unique_ids)`` group is returned – identical to today's
        behaviour.
        """
        resolver = self.parameter_resolver
        if resolver is None:
            return [(None, list(unique_ids))]

        groups = resolver.group_series_by_param_set(list(unique_ids), business_type)
        override_uids = resolver.get_series_with_overrides(list(unique_ids), business_type)

        result = []
        for param_id, group_uids in groups.items():
            group_override = resolver.build_group_config_override(param_id, business_type)
            bulk_uids = [u for u in group_uids if u not in override_uids]
            if bulk_uids:
                result.append((group_override, bulk_uids))
            # Each SKU-override series gets its own fully-merged config
            for uid in group_uids:
                if uid in override_uids:
                    uid_override = resolver.build_config_override(uid, business_type)
                    result.append((uid_override, [uid]))

        if not result:
            return [(None, list(unique_ids))]

        n_groups = len(result)
        n_overrides = sum(1 for _, uids in result if len(uids) == 1 and uids[0] in override_uids)
        if n_groups > 1:
            self.logger.info(
                f"Parameter grouping ({business_type}): {n_groups} groups "
                f"({n_overrides} individual overrides)"
            )
        return result

    # -- Dask management --

    def start_dask_client(self):
        """Start Dask client for parallel processing."""
        if not DASK_AVAILABLE:
            return

        dask_config = self.parallel_config['dask']

        if dask_config.get('scheduler') == 'distributed':
            scheduler_address = dask_config.get('scheduler_address')
            if scheduler_address:
                self.client = Client(scheduler_address)
                self.logger.info(f"Connected to Dask scheduler: {scheduler_address}")
            else:
                self.logger.warning("Distributed scheduler address not provided")
                self._start_local_client(dask_config)
        else:
            self._start_local_client(dask_config)

    def _start_local_client(self, dask_config: Dict):
        """Start local Dask client."""
        n_workers = dask_config.get('n_workers')
        threads_per_worker = dask_config.get('threads_per_worker', 1)
        memory_limit = dask_config.get('memory_limit', 'auto')

        self.client = Client(
            n_workers=n_workers,
            threads_per_worker=threads_per_worker,
            memory_limit=memory_limit,
            processes=True
        )

        self.logger.info(f"Started local Dask client: {self.client}")
        self.logger.info(f"Dashboard: {self.client.dashboard_link}")

    def stop_dask_client(self):
        """Stop Dask client."""
        if self.client:
            self.client.close()
            self.client = None

    # -- ProcessLogger helpers --

    def _make_process_logger(self) -> "ProcessLogger":
        """Create a new ProcessLogger for this pipeline run."""
        return ProcessLogger(self.config_path, run_id=str(uuid.uuid4()))

    def _run_step(self, pl: "ProcessLogger", step_name: str, fn, *args, **kwargs):
        """
        Execute *fn(*args, **kwargs)* wrapped with process-log start/end.

        Attaches a ListHandler to the root logger so that all log output
        emitted during *fn* is captured and stored in zcube.process_log.

        Returns:
            The return value of fn, or raises the exception.
        """
        # Attach a list-handler to capture log lines for this step
        handler = ListHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s — %(message)s"))
        root_logger = logging.getLogger()
        root_logger.addHandler(handler)

        log_id = pl.start_step(step_name)
        rows_out = None
        try:
            result = fn(*args, **kwargs)
            # Try to infer row count from result
            if isinstance(result, pd.DataFrame):
                rows_out = len(result)
            elif isinstance(result, tuple):
                for item in result:
                    if isinstance(item, pd.DataFrame) and not item.empty:
                        rows_out = len(item)
                        break
            pl.end_step(log_id, "success", rows=rows_out, log_tail=handler.get_tail())
            return result
        except Exception as exc:
            pl.end_step(log_id, "error", error=str(exc), log_tail=handler.get_tail())
            raise
        finally:
            root_logger.removeHandler(handler)

    # -- Pipeline steps --

    def step_etl(self) -> pd.DataFrame:
        """
        Step 1: Extract data from source, transform, load to PostgreSQL.

        Returns:
            DataFrame with columns [unique_id, date, y, ...]
        """
        self.logger.info("=" * 80)
        self.logger.info("STEP 1: ETL - Extract, Transform, Load")
        self.logger.info("=" * 80)

        df = self.etl.run()

        self.logger.info(f"ETL complete: {len(df)} rows, {df['unique_id'].nunique()} series")
        return df

    def step_outlier_detection(self, df: pd.DataFrame) -> tuple:
        """
        Step 1b: Detect and correct outliers in the time series data.

        Args:
            df: Time series data from ETL

        Returns:
            Tuple of (corrected_df, outliers_df)
        """
        self.logger.info("=" * 80)
        self.logger.info("STEP 1b: Outlier Detection & Correction")
        self.logger.info("=" * 80)

        groups = self._get_param_groups(df['unique_id'].unique(), 'outlier_detection')
        if len(groups) == 1 and groups[0][0] is None:
            corrected_df, outliers_df = self.outlier_detector.detect_and_correct_all(df)
        else:
            all_corrected, all_outliers = [], []
            for config_override, uids in groups:
                detector = OutlierDetector(self.config_path, config_override=config_override)
                group_df = df[df['unique_id'].isin(uids)]
                c, o = detector.detect_and_correct_all(group_df)
                all_corrected.append(c)
                if not o.empty:
                    all_outliers.append(o)
            corrected_df = pd.concat(all_corrected, ignore_index=True)
            outliers_df = pd.concat(all_outliers, ignore_index=True) if all_outliers else pd.DataFrame()

        # Save corrected data to PostgreSQL (update corrected_qty in demand_actuals)
        try:
            from db.db import get_conn, get_schema
            config_path = str(self.config_path)
            schema = get_schema(config_path)
            conn = get_conn(config_path)
            try:
                with conn.cursor() as cur:
                    # Build a mapping of (unique_id, date) → corrected y value
                    updates = []
                    for _, row in corrected_df.iterrows():
                        updates.append((float(row['y']), str(row['unique_id']), str(row['date'])))

                    if updates:
                        from psycopg2.extras import execute_batch
                        update_sql = f"""
                            UPDATE {schema}.demand_actuals
                            SET corrected_qty = %s
                            WHERE unique_id = %s AND date = %s
                        """
                        execute_batch(cur, update_sql, updates, page_size=5000)
                conn.commit()
                self.logger.info(f"Corrected data saved to {schema}.demand_actuals ({len(updates):,} rows updated)")
            finally:
                conn.close()
        except Exception as db_err:
            self.logger.error(f"Could not save corrected data to DB: {db_err}")
            raise

        # Save outlier log to PostgreSQL
        if not outliers_df.empty:
            try:
                from db.db import bulk_insert, get_schema, jsonb_serialize
                config_path = str(self.config_path)
                schema = get_schema(config_path)
                outlier_table = f"{schema}.detected_outliers"

                # Map outlier_df columns to DB columns
                db_cols = ['unique_id', 'date', 'original_value', 'corrected_value',
                           'detection_method', 'correction_method', 'z_score',
                           'lower_bound', 'upper_bound']
                # Build rows from the DataFrame (handle missing columns gracefully)
                rows = []
                for _, row in outliers_df.iterrows():
                    rows.append((
                        str(row.get('unique_id', '')),
                        str(row.get('date', '')),
                        float(row['original_value']) if pd.notna(row.get('original_value')) else None,
                        float(row['corrected_value']) if pd.notna(row.get('corrected_value')) else None,
                        str(row.get('detection_method', '')),
                        str(row.get('correction_method', '')),
                        float(row['z_score']) if pd.notna(row.get('z_score')) else None,
                        float(row['lower_bound']) if pd.notna(row.get('lower_bound')) else None,
                        float(row['upper_bound']) if pd.notna(row.get('upper_bound')) else None,
                    ))
                bulk_insert(config_path, outlier_table, db_cols, rows, truncate=True)
                self.logger.info(f"Outliers saved to {outlier_table} ({len(rows):,} outliers)")
            except Exception as db_err:
                self.logger.error(f"Could not save outliers to DB: {db_err}")
                raise
        else:
            self.logger.info("No outliers detected")

        return corrected_df, outliers_df

    def step_segmentation(self) -> Dict[str, int]:
        """
        Step 1c: Compute ABC classification and assign series to all segments.

        Returns:
            Dict mapping segment_name → count of assigned series
        """
        self.logger.info("=" * 80)
        self.logger.info("STEP 1c: Segmentation — ABC Classification + Segment Assignment")
        self.logger.info("=" * 80)

        from segmentation.segmentation import SegmentationEngine
        engine = SegmentationEngine(self.config_path)
        results = engine.run_all()
        for seg_name, count in results.items():
            self.logger.info(f"  '{seg_name}': {count} series")
        return results

    def step_classification(self) -> list:
        """
        Step 1d: Run all active configurable ABC/XYZ classifications.

        Returns:
            List of summary dicts (one per active configuration)
        """
        self.logger.info("=" * 80)
        self.logger.info("STEP 1d: Configurable Classifications (ABC/XYZ)")
        self.logger.info("=" * 80)

        from classification.abc import ABCClassifier
        classifier = ABCClassifier(self.config_path)
        summaries = classifier.run_all_active()
        for s in summaries:
            self.logger.info(
                "  '%s': %d series — %s",
                s["name"], s["total"],
                ", ".join(f"{k}={v}" for k, v in sorted(s.get("per_class", {}).items())),
            )
        return summaries

    def step_characterize(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Step 2: Analyze time series characteristics.

        Args:
            df: Time series data from ETL

        Returns:
            DataFrame with characteristics per series
        """
        self.logger.info("=" * 80)
        self.logger.info("STEP 2: Time Series Characterization")
        self.logger.info("=" * 80)

        groups = self._get_param_groups(df['unique_id'].unique(), 'characterization')
        if len(groups) == 1 and groups[0][0] is None:
            characteristics_df = self.characterizer.analyze_all(
                df, id_col='unique_id', date_col='date', value_col='y', save=True
            )
        else:
            all_chars = []
            for config_override, uids in groups:
                charzer = TimeSeriesCharacterizer(self.config_path, config_override=config_override)
                group_df = df[df['unique_id'].isin(uids)]
                chars = charzer.analyze_all(
                    group_df, id_col='unique_id', date_col='date', value_col='y', save=False
                )
                all_chars.append(chars)
            characteristics_df = pd.concat(all_chars, ignore_index=True)
            # Save combined results once
            try:
                from db.db import bulk_insert, get_schema, jsonb_serialize
                schema = get_schema(str(self.config_path))
                cols = list(characteristics_df.columns)
                rows = [tuple(jsonb_serialize(v) for v in row)
                        for row in characteristics_df.itertuples(index=False, name=None)]
                bulk_insert(str(self.config_path),
                            f"{schema}.time_series_characteristics", cols, rows, truncate=True)
            except Exception as exc:
                self.logger.error(f"Could not save grouped characteristics: {exc}")
                raise

        # Log summary
        self.logger.info(f"Characterized {len(characteristics_df)} series")
        if 'complexity_level' in characteristics_df.columns:
            self.logger.info(f"Complexity distribution: {characteristics_df['complexity_level'].value_counts().to_dict()}")
        if 'is_intermittent' in characteristics_df.columns:
            self.logger.info(f"Intermittent: {characteristics_df['is_intermittent'].sum()}")

        return characteristics_df

    def forecast_batch(self,
                       df: pd.DataFrame,
                       characteristics_df: pd.DataFrame,
                       methods_filter: Optional[List[str]] = None,
                       config_override: dict = None) -> pd.DataFrame:
        """
        Generate forecasts for a batch of time series across all model families.

        Args:
            df: Time series data
            characteristics_df: Characteristics for this batch
            methods_filter: Optional list to restrict methods
            config_override: Optional config override for this group's components.

        Returns:
            DataFrame with forecast results
        """
        all_forecasts = []

        # Use group-specific component instances when override is provided
        if config_override:
            _stat_fc = StatisticalForecaster(self.config_path, config_override=config_override)
            _ml_fc_fn = lambda: MLForecaster(self.config_path, config_override=config_override)
            _neural_fc_fn = lambda: NeuralForecaster(self.config_path, config_override=config_override)
            _found_fc_fn = lambda: FoundationForecaster(self.config_path, config_override=config_override)
            # Merge override into config for threshold lookups
            from utils.parameter_resolver import ParameterResolver
            _merged_cfg = ParameterResolver.deep_merge(dict(self.config), config_override)
        else:
            _stat_fc = self.stat_forecaster
            _ml_fc_fn = lambda: self.ml_forecaster
            _neural_fc_fn = lambda: self.neural_forecaster
            _found_fc_fn = lambda: self.foundation_forecaster
            _merged_cfg = self.config

        # --- Statistical forecasts ---
        self.logger.info(f"Statistical forecasts for {len(characteristics_df)} series...")
        try:
            stat_forecasts = _stat_fc.forecast_multiple_series(
                df=df,
                characteristics_df=characteristics_df,
                show_progress=False,
            )
            if not stat_forecasts.empty:
                all_forecasts.append(stat_forecasts)
        except Exception as e:
            self.logger.warning(f"Statistical forecasting failed: {e}")

        # --- ML forecasts (LightGBM / XGBoost) ---
        # Recompute eligibility from current config so stale DB booleans are ignored.
        _sufficiency = _merged_cfg.get('characterization', {}).get('data_sufficiency', {})
        _min_ml = _sufficiency.get('min_for_ml', 100)
        _min_dl = _sufficiency.get('min_for_deep_learning', 200)

        if 'n_observations' in characteristics_df.columns:
            ml_eligible = characteristics_df[characteristics_df['n_observations'] >= _min_ml]
        else:
            ml_eligible = characteristics_df[
                characteristics_df.get('sufficient_for_ml', pd.Series(dtype=bool)).fillna(False)
            ] if 'sufficient_for_ml' in characteristics_df.columns else pd.DataFrame()

        if len(ml_eligible) > 0:
            self.logger.info(f"ML forecasts for {len(ml_eligible)} series (min_obs={_min_ml})...")
            try:
                ml_fc = _ml_fc_fn()
                ml_forecasts = ml_fc.forecast_multiple_series(
                    df=df,
                    characteristics_df=ml_eligible,
                    show_progress=False,
                )
                if not ml_forecasts.empty:
                    all_forecasts.append(ml_forecasts)
            except Exception as e:
                self.logger.warning(f"ML forecasting failed: {e}")

        # --- Neural forecasts ---
        if 'n_observations' in characteristics_df.columns:
            neural_eligible = characteristics_df[characteristics_df['n_observations'] >= _min_dl]
        else:
            neural_eligible = characteristics_df[
                characteristics_df.get('sufficient_for_deep_learning', pd.Series(dtype=bool)).fillna(False)
            ] if 'sufficient_for_deep_learning' in characteristics_df.columns else pd.DataFrame()

        if len(neural_eligible) > 0:
            self.logger.info(f"Neural forecasts for {len(neural_eligible)} series (min_obs={_min_dl})...")
            try:
                neural_fc = _neural_fc_fn()
                neural_forecasts = neural_fc.forecast_multiple_series(
                    df=df,
                    characteristics_df=neural_eligible,
                    show_progress=False,
                )
                if not neural_forecasts.empty:
                    all_forecasts.append(neural_forecasts)
            except Exception as e:
                self.logger.warning(f"Neural forecasting failed: {e}")

        # --- Foundation model (TimesFM) ---
        self.logger.info(f"Foundation model forecasts for {len(characteristics_df)} series...")
        try:
            found_fc = _found_fc_fn()
            if found_fc.model is not None:
                foundation_forecasts = found_fc.forecast_multiple_series(
                    df=df,
                    characteristics_df=characteristics_df,
                    show_progress=False,
                )
                if not foundation_forecasts.empty:
                    all_forecasts.append(foundation_forecasts)
        except Exception as e:
            self.logger.warning(f"Foundation model forecasting failed: {e}")

        # Combine
        if all_forecasts:
            combined = pd.concat(all_forecasts, ignore_index=True)
            return combined
        return pd.DataFrame()

    def step_forecast(self,
                      df: pd.DataFrame,
                      characteristics_df: pd.DataFrame,
                      series_subset: bool = False) -> pd.DataFrame:
        """
        Step 3: Generate forecasts using all model families.

        Args:
            df: Time series data
            characteristics_df: Series characteristics
            series_subset: When True, use targeted delete instead of
                           truncating when saving to DB.

        Returns:
            Combined forecast results
        """
        self.logger.info("=" * 80)
        self.logger.info("STEP 3: Forecasting (Statistical + ML + Neural + Foundation)")
        self.logger.info("=" * 80)

        batch_size = self.parallel_config.get('batch_size', 100)

        # Parameter-aware grouping
        param_groups = self._get_param_groups(
            characteristics_df['unique_id'].unique(), 'forecasting'
        )

        if self.client is None or not DASK_AVAILABLE:
            # Serial mode — iterate over parameter groups
            n_series = len(characteristics_df)
            self.logger.info("Running forecasting in serial mode...")
            self.logger.info(f"[FORECAST_PROGRESS] completed=0 total={n_series} batches_done=0 batches_total=1")
            group_results = []
            for config_override, uids in param_groups:
                grp_chars = characteristics_df[characteristics_df['unique_id'].isin(uids)]
                grp_df = df[df['unique_id'].isin(uids)]
                grp_fc = self.forecast_batch(grp_df, grp_chars, config_override=config_override)
                if not grp_fc.empty:
                    group_results.append(grp_fc)
            forecasts_df = pd.concat(group_results, ignore_index=True) if group_results else pd.DataFrame()
            self.logger.info(f"[FORECAST_PROGRESS] completed={n_series} total={n_series} batches_done=1 batches_total=1")
        else:
            # Parallel with Dask — each parameter group is sub-batched separately
            self.logger.info(f"Running parallel forecasting with Dask, batch_size={batch_size}...")
            n_series = len(characteristics_df)

            futures = []
            for config_override, uids in param_groups:
                grp_chars = characteristics_df[characteristics_df['unique_id'].isin(uids)]
                n_grp = len(grp_chars)
                n_batches_grp = (n_grp + batch_size - 1) // batch_size
                for i in range(n_batches_grp):
                    start_idx = i * batch_size
                    end_idx = min((i + 1) * batch_size, n_grp)
                    batch_chars = grp_chars.iloc[start_idx:end_idx]
                    batch_ids = batch_chars['unique_id'].tolist()
                    batch_df = df[df['unique_id'].isin(batch_ids)]
                    future = self.client.submit(
                        _dask_forecast_batch,
                        self.config_path,
                        batch_df,
                        batch_chars,
                        config_override=config_override,
                    )
                    futures.append(future)

            n_batches = len(futures)
            self.logger.info(f"  {n_series} series → {n_batches} batches")
            self.logger.info(f"[FORECAST_PROGRESS] completed=0 total={n_series} batches_done=0 batches_total={n_batches}")

            results = []
            completed_series = 0
            completed_batches = 0
            pbar = tqdm(total=n_batches, desc="  Forecasting (parallel)", unit="batch")
            for future in as_completed(futures):
                completed_batches += 1
                try:
                    result = future.result()
                    if not result.empty:
                        results.append(result)
                        if 'unique_id' in result.columns:
                            completed_series = min(completed_series + result['unique_id'].nunique(), n_series)
                        else:
                            completed_series = min(completed_batches * batch_size, n_series)
                    else:
                        completed_series = min(completed_batches * batch_size, n_series)
                except Exception as e:
                    self.logger.error(f"Batch failed: {e}")
                    completed_series = min(completed_batches * batch_size, n_series)
                pbar.update(1)
                pbar.set_postfix_str(f"{completed_series}/{n_series} series", refresh=False)
                self.logger.info(
                    f"[FORECAST_PROGRESS] completed={completed_series} total={n_series} "
                    f"batches_done={completed_batches} batches_total={n_batches}"
                )
            pbar.close()

            forecasts_df = pd.concat(results, ignore_index=True) if results else pd.DataFrame()
            self.logger.info(f"Generated {len(forecasts_df)} forecasts across {n_batches} batches")

        if forecasts_df.empty:
            self.logger.warning("No forecasts generated!")
            return forecasts_df

        # Save to PostgreSQL
        try:
            from db.db import bulk_insert, get_schema, jsonb_serialize
            schema = get_schema(str(self.config_path))

            # Targeted delete for subset runs (don't wipe all other series)
            _delete_clause = None
            if series_subset and 'unique_id' in forecasts_df.columns:
                _uids = list(forecasts_df['unique_id'].unique())
                _quoted = ", ".join(f"'{s}'" for s in _uids)
                _delete_clause = f"unique_id IN ({_quoted})"
                self.logger.info(f"Targeted save: replacing forecasts for {len(_uids)} series only")

            cols = list(forecasts_df.columns)
            rows = [tuple(jsonb_serialize(v) for v in row)
                    for row in forecasts_df.itertuples(index=False, name=None)]
            n = bulk_insert(str(self.config_path), f"{schema}.forecast_results",
                            cols, rows,
                            truncate=not series_subset,
                            delete_where=_delete_clause)
            self.logger.info(f"Forecasts saved to {schema}.forecast_results ({n} rows)")
        except Exception as db_err:
            self.logger.error(f"Could not save forecasts to DB: {db_err}")
            raise

        return forecasts_df

    def step_backtest(self,
                      df: pd.DataFrame,
                      characteristics_df: pd.DataFrame,
                      all_methods: bool = False) -> tuple:
        """
        Step 4: Rolling-window backtesting with per-origin forecast storage.

        Runs one Dask future per series when a Dask client is available,
        otherwise falls back to sequential execution.

        Args:
            df: Time series data
            characteristics_df: Series characteristics
            all_methods: When True, backtest ML methods (LightGBM, XGBoost)
                         alongside statistical methods.

        Returns:
            Tuple of (metrics_df, forecasts_by_origin_df)
        """
        self.logger.info("=" * 80)
        self.logger.info("STEP 4: Backtesting with Per-Origin Forecast Storage")
        self.logger.info("=" * 80)

        _ml_backtest_methods = {'LightGBM', 'XGBoost'}

        # Build per-series work items: (unique_id, series_df_slice, methods, chars_dict)
        work_items = []
        for _, char_row in characteristics_df.iterrows():
            unique_id = char_row['unique_id']
            methods = char_row.get('recommended_methods', ['AutoETS', 'AutoARIMA', 'AutoTheta'])
            if isinstance(methods, str):
                methods = [methods]
            # Statistical methods for backtest
            stat_methods = [m for m in methods if m in _STAT_METHODS][:5]
            if not stat_methods:
                stat_methods = ['AutoETS', 'AutoARIMA']
            # When all_methods is set, also include ML methods that are in recommended_methods
            if all_methods:
                ml_methods = [m for m in methods if m in _ml_backtest_methods]
                stat_methods = stat_methods + ml_methods
            series_slice = df[df['unique_id'] == unique_id]
            work_items.append((unique_id, series_slice, stat_methods, char_row.to_dict()))

        n_series = len(work_items)
        all_metrics = []
        all_origin_forecasts = []

        # Load per-series backtesting overrides from DB (_backtesting pseudo-method)
        bt_overrides_map = {}
        try:
            from db.db import get_conn, get_schema
            import json as _json
            conn = get_conn(str(self.config_path))
            schema = get_schema(str(self.config_path))
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT unique_id, overrides FROM {schema}.hyperparameter_overrides "
                    f"WHERE method = '_backtesting'"
                )
                for uid, ovr in cur.fetchall():
                    if isinstance(ovr, str):
                        ovr = _json.loads(ovr)
                    bt_overrides_map[uid] = ovr
            conn.close()
        except Exception:
            pass  # No DB or no overrides — use config defaults

        if bt_overrides_map:
            self.logger.info(
                f"Loaded backtesting overrides for {len(bt_overrides_map)} series"
            )

        if self.client is not None and DASK_AVAILABLE:
            # ---- Parallel Dask mode: batched, one future per batch ----
            # Batching amortises StatisticalForecaster + ForecastEvaluator
            # init cost across many series per worker task, keeping CPUs busy.
            batch_size = self.parallel_config.get('batch_size', 130)
            n_batches = (n_series + batch_size - 1) // batch_size
            self.logger.info(
                f"Running parallel backtesting with Dask "
                f"({n_series} series → {n_batches} batches of ~{batch_size})..."
            )
            self.logger.info(f"[BACKTEST_PROGRESS] completed=0 total={n_series} batches_done=0 batches_total={n_batches}")

            futures = []
            for b in range(n_batches):
                batch = work_items[b * batch_size:(b + 1) * batch_size]
                # Collect all unique_ids in this batch to slice the full df once
                batch_ids = [uid for uid, _, _, _ in batch]
                batch_df = df[df['unique_id'].isin(batch_ids)]
                # Work list: (unique_id, methods, chars_dict) — df is passed separately
                batch_work = [(uid, methods, chars) for uid, _, methods, chars in batch]
                # Filter bt overrides to only include series in this batch
                batch_bt = {uid: bt_overrides_map[uid]
                            for uid in batch_ids if uid in bt_overrides_map}
                future = self.client.submit(
                    _dask_backtest_batch,
                    self.config_path,
                    batch_df,
                    batch_work,
                    bt_overrides_map=batch_bt or None,
                )
                futures.append(future)

            completed_series = 0
            completed_batches = 0
            pbar = tqdm(total=n_batches, desc="  Backtesting (parallel)", unit="batch")
            for future in as_completed(futures):
                completed_batches += 1
                try:
                    metrics, origin_forecasts = future.result()
                    if not metrics.empty:
                        all_metrics.append(metrics)
                        completed_series += metrics['unique_id'].nunique() if 'unique_id' in metrics.columns else 0
                    if not origin_forecasts.empty:
                        all_origin_forecasts.append(origin_forecasts)
                except Exception as e:
                    self.logger.warning(f"Backtest batch failed: {e}")
                pbar.update(1)
                pbar.set_postfix_str(f"{completed_series}/{n_series} series", refresh=False)
                self.logger.info(
                    f"[BACKTEST_PROGRESS] completed={completed_series} total={n_series} "
                    f"batches_done={completed_batches} batches_total={n_batches}"
                )
            pbar.close()

            self.logger.info(f"Backtest complete: {completed_series}/{n_series} series processed")

        else:
            # ---- Serial fallback ----
            self.logger.info(f"Running serial backtesting ({n_series} series)...")
            self.logger.info(f"[BACKTEST_PROGRESS] completed=0 total={n_series}")

            # Build a combined forecast function that delegates to the right forecaster
            def _combined_forecast_fn(df, unique_id, methods, characteristics, **kw):
                results = []
                stat = [m for m in methods if m in _STAT_METHODS]
                ml = [m for m in methods if m in _ml_backtest_methods]
                if stat:
                    results.extend(
                        self.stat_forecaster.forecast_single_series(
                            df=df, unique_id=unique_id, methods=stat,
                            characteristics=characteristics, **kw,
                        )
                    )
                if ml:
                    results.extend(
                        self.ml_forecaster.forecast_single_series(
                            df=df, unique_id=unique_id, methods=ml,
                            characteristics=characteristics, **kw,
                        )
                    )
                return results

            forecast_fn = _combined_forecast_fn if all_methods else self.stat_forecaster.forecast_single_series

            pbar = tqdm(enumerate(work_items), total=n_series, desc="  Backtesting", unit="series")
            for idx, (unique_id, series_slice, methods, chars_dict) in pbar:
                pbar.set_postfix_str(unique_id, refresh=False)
                bt_ovr = bt_overrides_map.get(unique_id, {})
                try:
                    if hasattr(self.evaluator, 'backtest_series_with_forecasts'):
                        metrics, origin_forecasts = self.evaluator.backtest_series_with_forecasts(
                            df=series_slice,
                            unique_id=unique_id,
                            forecast_fn=forecast_fn,
                            methods=methods,
                            characteristics=chars_dict,
                            backtest_overrides=bt_ovr,
                        )
                        if not origin_forecasts.empty:
                            all_origin_forecasts.append(origin_forecasts)
                    else:
                        metrics = self.evaluator.backtest_series(
                            df=series_slice,
                            unique_id=unique_id,
                            forecast_fn=forecast_fn,
                            methods=methods,
                            characteristics=chars_dict,
                            backtest_overrides=bt_ovr,
                        )
                    if not metrics.empty:
                        all_metrics.append(metrics)
                except Exception as e:
                    self.logger.warning(f"Backtest failed for {unique_id}: {e}")
                self.logger.info(f"[BACKTEST_PROGRESS] completed={idx+1} total={n_series}")

        # Combine metrics
        metrics_df = pd.concat(all_metrics, ignore_index=True) if all_metrics else pd.DataFrame()

        # Combine per-origin forecasts
        origin_df = pd.concat(all_origin_forecasts, ignore_index=True) if all_origin_forecasts else pd.DataFrame()

        # ---- ML internal validation fallback (serial path) ----
        if all_methods:
            ml_fallback_work = []
            existing_ml_pairs = set()

            if not metrics_df.empty:
                # 1. ML methods with all-NaN MAE
                for uid, grp in metrics_df.groupby('unique_id'):
                    for mname in grp['method'].unique():
                        if mname in _ml_backtest_methods:
                            existing_ml_pairs.add((uid, mname))
                            if grp.loc[grp['method'] == mname, 'mae'].isna().all():
                                chars = next((cd for u, _, _, cd in work_items if u == uid), None)
                                if chars:
                                    ml_fallback_work.append((uid, [mname], chars))

            # 2. ML methods that produced no metric rows at all
            for uid, _, methods_list, chars_d in work_items:
                for m in methods_list:
                    if m in _ml_backtest_methods and (uid, m) not in existing_ml_pairs:
                        ml_fallback_work.append((uid, [m], chars_d))

            if ml_fallback_work:
                self.logger.info(
                    f"Running ML internal validation fallback for "
                    f"{len(ml_fallback_work)} series/method pairs..."
                )
                internal_df = _run_ml_internal_validation(
                    str(self.config_path), df, ml_fallback_work
                )
                if not internal_df.empty:
                    self.logger.info(
                        f"Internal validation produced {len(internal_df)} metric rows"
                    )
                    metrics_df = pd.concat([metrics_df, internal_df], ignore_index=True)

        # Save outputs to PostgreSQL
        from db.db import bulk_insert, get_schema, jsonb_serialize
        schema = get_schema(str(self.config_path))

        # When backtesting a subset (all_methods mode), do a targeted delete
        # for the specific series instead of truncating the entire table.
        _series_ids = list(characteristics_df['unique_id'].unique())
        _is_subset = all_methods and len(_series_ids) > 0
        _delete_clause = None
        if _is_subset:
            _quoted = ", ".join(f"'{s}'" for s in _series_ids)
            _delete_clause = f"unique_id IN ({_quoted})"
            self.logger.info(f"Targeted save: replacing data for {len(_series_ids)} series only")

        if not metrics_df.empty:
            try:
                cols = list(metrics_df.columns)
                rows = [tuple(jsonb_serialize(v) for v in row)
                        for row in metrics_df.itertuples(index=False, name=None)]
                n = bulk_insert(str(self.config_path), f"{schema}.backtest_metrics",
                                cols, rows,
                                truncate=not _is_subset,
                                delete_where=_delete_clause)
                self.logger.info(f"Backtest metrics saved to {schema}.backtest_metrics ({n} rows)")
            except Exception as db_err:
                self.logger.error(f"Could not save backtest metrics to DB: {db_err}")
                raise

            summary = metrics_df.groupby('method')[['mae', 'rmse']].mean()
            self.logger.info(f"Backtest summary by method:\n{summary}")

        if not origin_df.empty:
            try:
                cols = list(origin_df.columns)
                rows = [tuple(jsonb_serialize(v) for v in row)
                        for row in origin_df.itertuples(index=False, name=None)]
                n = bulk_insert(str(self.config_path), f"{schema}.forecasts_by_origin",
                                cols, rows,
                                truncate=not _is_subset,
                                delete_where=_delete_clause)
                self.logger.info(f"Per-origin forecasts saved to {schema}.forecasts_by_origin ({n} rows)")
            except Exception as db_err:
                self.logger.error(f"Could not save origin forecasts to DB: {db_err}")
                raise

        return metrics_df, origin_df

    def step_select_best_methods(self, metrics_df: pd.DataFrame,
                                 series_subset: bool = False) -> pd.DataFrame:
        """
        Step 5: Select best forecasting method per series.

        Args:
            metrics_df: Backtest metrics from step 4
            series_subset: When True, use targeted delete instead of
                           truncating when saving to DB.

        Returns:
            DataFrame with best method per series
        """
        self.logger.info("=" * 80)
        self.logger.info("STEP 5: Best Method Selection")
        self.logger.info("=" * 80)

        if metrics_df.empty:
            self.logger.warning("No metrics available for best method selection")
            return pd.DataFrame()

        best_methods_df = self.method_selector.select_best_methods(metrics_df)

        # Save to PostgreSQL
        try:
            from db.db import bulk_insert, get_schema, jsonb_serialize
            schema = get_schema(str(self.config_path))

            _delete_clause = None
            if series_subset and 'unique_id' in best_methods_df.columns:
                _uids = list(best_methods_df['unique_id'].unique())
                _quoted = ", ".join(f"'{s}'" for s in _uids)
                _delete_clause = f"unique_id IN ({_quoted})"
                self.logger.info(f"Targeted save: replacing best methods for {len(_uids)} series only")

            cols = list(best_methods_df.columns)
            rows = [tuple(jsonb_serialize(v) for v in row)
                    for row in best_methods_df.itertuples(index=False, name=None)]
            n = bulk_insert(str(self.config_path), f"{schema}.best_method_per_series",
                            cols, rows,
                            truncate=not series_subset,
                            delete_where=_delete_clause)
            self.logger.info(f"Best methods saved to {schema}.best_method_per_series ({n} rows)")
        except Exception as db_err:
            self.logger.error(f"Could not save best methods to DB: {db_err}")
            raise

        # Log distribution
        if 'best_method' in best_methods_df.columns:
            dist = best_methods_df['best_method'].value_counts().to_dict()
            self.logger.info(f"Best method distribution: {dist}")

        return best_methods_df

    def step_fit_distributions(self, forecasts_df: pd.DataFrame) -> pd.DataFrame:
        """
        Step 6: Fit parametric distributions for MEIO.

        Args:
            forecasts_df: Forecast results

        Returns:
            DataFrame with fitted distributions
        """
        self.logger.info("=" * 80)
        self.logger.info("STEP 6: Distribution Fitting for MEIO")
        self.logger.info("=" * 80)

        if forecasts_df.empty:
            self.logger.warning("No forecasts available for distribution fitting")
            return pd.DataFrame()

        batch_size = self.parallel_config.get('batch_size', 130)
        n_rows = len(forecasts_df)

        if self.client is not None and DASK_AVAILABLE:
            n_batches = (n_rows + batch_size - 1) // batch_size
            self.logger.info(
                f"Running parallel distribution fitting with Dask "
                f"({n_rows} rows → {n_batches} batches of ~{batch_size})..."
            )
            futures = []
            for b in range(n_batches):
                batch = forecasts_df.iloc[b * batch_size:(b + 1) * batch_size]
                futures.append(self.client.submit(
                    _dask_distribution_batch,
                    self.config_path,
                    batch,
                ))

            results = []
            for future in as_completed(futures):
                try:
                    result = future.result()
                    if not result.empty:
                        results.append(result)
                except Exception as e:
                    self.logger.warning(f"Distribution batch failed: {e}")

            distributions_df = pd.concat(results, ignore_index=True) if results else pd.DataFrame()
        else:
            self.logger.info(f"Running serial distribution fitting ({n_rows} rows)...")
            distributions_df = self.dist_fitter.fit_forecast_distributions(forecasts_df)

        # Save to PostgreSQL
        try:
            from db.db import bulk_insert, get_schema, jsonb_serialize
            schema = get_schema(str(self.config_path))
            cols = list(distributions_df.columns)
            rows = [tuple(jsonb_serialize(v) for v in row)
                    for row in distributions_df.itertuples(index=False, name=None)]
            n = bulk_insert(str(self.config_path), f"{schema}.fitted_distributions",
                            cols, rows, truncate=True)
            self.logger.info(f"Distributions saved to {schema}.fitted_distributions ({n} rows)")
        except Exception as db_err:
            self.logger.error(f"Could not save distributions to DB: {db_err}")
            raise

        return distributions_df

    # -- Full pipeline --

    def run_complete_pipeline(self,
                              skip_etl: bool = False,
                              skip_outlier_detection: bool = False,
                              skip_segmentation: bool = False,
                              skip_classification: bool = False,
                              skip_characterization: bool = False,
                              skip_forecasting: bool = False,
                              skip_backtest: bool = False,
                              skip_best_method: bool = False,
                              skip_distributions: bool = False) -> Dict[str, str]:
        """
        Run the complete forecasting pipeline.
        All data is read from and written to PostgreSQL.

        Args:
            skip_*: Flags to skip individual steps (skipped steps load their
                    inputs from PostgreSQL instead of re-computing them)

        Returns:
            Dictionary describing what each step wrote to (PostgreSQL table names)
        """
        start_time = time.time()
        output_paths = {}

        self.logger.info("=" * 80)
        self.logger.info("STARTING COMPLETE FORECASTING PIPELINE")
        self.logger.info(f"Time: {datetime.now().isoformat()}")
        self.logger.info("=" * 80)

        # Create process logger for this run
        pl = self._make_process_logger()
        self.logger.info(f"Process run_id: {pl.run_id}")

        # Start Dask if configured
        if self.parallel_config['backend'] == 'dask' and DASK_AVAILABLE:
            self.start_dask_client()

        try:
            # Step 1: ETL
            if skip_etl:
                self.logger.info("Skipping ETL, loading demand data from PostgreSQL...")
                from db.db import get_conn, get_schema
                _schema = get_schema(str(self.config_path))
                _table = f"{_schema}.demand_actuals" if _schema != "public" else "demand_actuals"
                _conn = get_conn(str(self.config_path))
                try:
                    df = pd.read_sql(
                        f"SELECT unique_id, date, COALESCE(corrected_qty, qty) AS y FROM {_table} ORDER BY unique_id, date",
                        _conn,
                    )
                finally:
                    _conn.close()
                self.logger.info(f"Loaded {len(df):,} rows from {_table}")
            else:
                df = self._run_step(pl, "etl", self.step_etl)
                output_paths['time_series'] = 'PostgreSQL: demand_actuals'

            self.logger.info(f"Data: {len(df)} rows, {df['unique_id'].nunique()} series")

            # Step 1b: Outlier Detection & Correction
            outlier_config = self.config.get('outlier_detection', {})
            if not skip_outlier_detection and outlier_config.get('enabled', False):
                df, outliers_df = self._run_step(pl, "outlier_detection", self.step_outlier_detection, df)
                output_paths['outliers'] = 'PostgreSQL: detected_outliers'
                output_paths['time_series_corrected'] = 'PostgreSQL: demand_actuals.corrected_qty'
            else:
                self.logger.info("Skipping outlier detection")

            # Step 1c: Segmentation
            if not skip_segmentation:
                self._run_step(pl, "segmentation", self.step_segmentation)
                output_paths['segmentation'] = 'PostgreSQL: segment_membership'
            else:
                self.logger.info("Skipping segmentation step")

            # Step 1d: Classification (configurable ABC/XYZ)
            if not skip_classification:
                self._run_step(pl, "classification", self.step_classification)
                output_paths['classification'] = 'PostgreSQL: abc_results'
            else:
                self.logger.info("Skipping classification step")

            # Step 2: Characterization
            if skip_characterization:
                self.logger.info("Skipping characterization, loading from PostgreSQL...")
                from db.db import load_table, get_schema
                _schema = get_schema(str(self.config_path))
                _table = f"{_schema}.time_series_characteristics" if _schema != "public" else "time_series_characteristics"
                characteristics_df = load_table(str(self.config_path), _table)
                self.logger.info(f"Loaded {len(characteristics_df):,} characteristics from {_table}")
            else:
                characteristics_df = self._run_step(pl, "characterization", self.step_characterize, df)
                output_paths['characteristics'] = 'PostgreSQL: time_series_characteristics'

            # Step 3: Forecasting
            if not skip_forecasting:
                forecasts_df = self._run_step(pl, "forecasting", self.step_forecast, df, characteristics_df)
                output_paths['forecasts'] = 'PostgreSQL: forecast_results'
            else:
                self.logger.info("Skipping forecasting step -- loading from PostgreSQL...")
                from db.db import load_table, get_schema
                _schema = get_schema(str(self.config_path))
                forecasts_df = load_table(str(self.config_path),
                                          f"{_schema}.forecast_results")

            # Step 4: Backtesting
            if not skip_backtest:
                metrics_df, origin_df = self._run_step(pl, "backtesting", self.step_backtest, df, characteristics_df)
                if not metrics_df.empty:
                    output_paths['metrics'] = 'PostgreSQL: backtest_metrics'
                if not origin_df.empty:
                    output_paths['forecasts_by_origin'] = 'PostgreSQL: forecasts_by_origin'
            else:
                self.logger.info("Skipping backtesting step -- loading metrics from PostgreSQL...")
                from db.db import load_table, get_schema
                _schema = get_schema(str(self.config_path))
                metrics_df = load_table(str(self.config_path),
                                        f"{_schema}.backtest_metrics")

            # Step 5: Best method selection
            if not skip_best_method and not metrics_df.empty:
                best_methods_df = self._run_step(pl, "best_method_selection", self.step_select_best_methods, metrics_df)
                output_paths['best_methods'] = 'PostgreSQL: best_method_per_series'
            else:
                self.logger.info("Skipping best method selection")

            # Step 6: Distribution fitting
            if not skip_distributions and not forecasts_df.empty:
                distributions_df = self._run_step(pl, "distribution_fitting", self.step_fit_distributions, forecasts_df)
                output_paths['distributions'] = 'PostgreSQL: fitted_distributions'
            else:
                self.logger.info("Skipping distribution fitting")

            # Pipeline summary
            elapsed = time.time() - start_time
            self.logger.info("=" * 80)
            self.logger.info("PIPELINE COMPLETE")
            self.logger.info(f"Total time: {elapsed:.1f}s ({elapsed/60:.1f}m)")
            self.logger.info("=" * 80)

            summary = {
                'timestamp': datetime.now().isoformat(),
                'n_series': int(df['unique_id'].nunique()),
                'n_observations': int(len(df)),
                'n_forecasts': int(len(forecasts_df)) if not forecasts_df.empty else 0,
                'execution_time_seconds': round(elapsed, 1),
                'output_files': output_paths
            }

            summary_path = output_base / "pipeline_summary.yaml"
            with open(str(summary_path), 'w') as f:
                yaml.dump(summary, f, default_flow_style=False)
            output_paths['summary'] = str(summary_path)

            return output_paths

        finally:
            if self.client:
                self.stop_dask_client()


def main():
    """Run complete pipeline with default settings."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    orchestrator = ForecastOrchestrator()

    output_paths = orchestrator.run_complete_pipeline()

    print("\n" + "=" * 80)
    print("PIPELINE COMPLETE!")
    print("=" * 80)
    print("\nOutput files:")
    for key, path in output_paths.items():
        if path:
            print(f"  {key}: {path}")


if __name__ == "__main__":
    main()

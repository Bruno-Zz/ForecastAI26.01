"""
ForecastAI Pipeline Runner
CLI entry point for running the full or partial forecasting pipeline.

Usage:
    # Full pipeline (ETL -> Characterize -> Forecast -> Backtest -> Best Method -> Distributions)
    python run_pipeline.py

    # Skip ETL (use existing data in PostgreSQL)
    python run_pipeline.py --skip-etl

    # Skip ETL and characterization
    python run_pipeline.py --skip-etl --skip-characterization

    # Only run ETL
    python run_pipeline.py --only etl

    # Only run characterization (requires demand_actuals in DB)
    python run_pipeline.py --only characterize

    # Discover database schema
    python run_pipeline.py --discover-schema
"""

import argparse
import logging
import sys
from pathlib import Path

# Add project root to path so imports work
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from utils.orchestrator import ForecastOrchestrator
from etl.etl import create_etl_pipeline as ETLPipeline  # factory: auto-selects adapter by source_type


def setup_logging(level: str = "INFO", log_file: str = None):
    """Configure logging for the pipeline."""
    handlers = [logging.StreamHandler(sys.stdout)]

    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(str(log_path)))

    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=handlers,
        force=True,  # override any handlers set by imported libraries
    )


def discover_schema(config_path: str):
    """Discover database schema and print table info."""
    print("Discovering database schema...")
    etl = ETLPipeline(config_path)
    schema_info = etl.discover_schema()

    if schema_info:
        for table_name, columns in schema_info.items():
            print(f"\n  {table_name}:")
            for col in columns:
                if isinstance(col, dict):
                    print(f"    - {col['column_name']}: {col.get('data_type', '?')}")
                else:
                    print(f"    - {col}")
    else:
        print("  Could not discover schema. Check database connection.")


def _load_demand_from_db(config_path: str, use_corrected: bool = True):
    """
    Load demand time series from PostgreSQL demand_actuals table.

    Args:
        config_path: Path to config.yaml
        use_corrected: If True, use corrected_qty (fallback to qty); if False, use qty

    Returns:
        DataFrame with columns [unique_id, date, y]
    """
    import pandas as pd
    from db.db import get_conn, get_schema

    schema = get_schema(config_path)
    table = f"{schema}.demand_actuals" if schema != "public" else "demand_actuals"

    if use_corrected:
        value_col = "COALESCE(corrected_qty, qty) AS y"
    else:
        value_col = "qty AS y"

    query = f"SELECT unique_id, date, {value_col} FROM {table} ORDER BY unique_id, date"

    conn = get_conn(config_path)
    try:
        df = pd.read_sql(query, conn)
    finally:
        conn.close()

    print(f"  Loaded {len(df):,} rows, {df['unique_id'].nunique():,} series from {table}")
    return df


def _load_demand_for_scenario(config_path: str, scenario_id: int, use_corrected: bool = True):
    """
    Load demand time series for a specific scenario.
    For scenario_id=1 (base), falls back to standard _load_demand_from_db.
    For other scenarios, uses get_demand_for_scenario() which applies demand_overrides.
    """
    if scenario_id == 1:
        return _load_demand_from_db(config_path, use_corrected=use_corrected)

    from db.db import get_conn, get_schema, get_demand_for_scenario
    schema = get_schema(config_path)
    conn = get_conn(config_path)
    try:
        df = get_demand_for_scenario(schema, scenario_id, conn)
    finally:
        conn.close()
    print(f"  Loaded {len(df):,} rows, {df['unique_id'].nunique():,} series (scenario {scenario_id})")
    return df


def _load_characteristics_from_db(config_path: str):
    """Load time series characteristics from PostgreSQL."""
    import pandas as pd
    from db.db import load_table, get_schema

    schema = get_schema(config_path)
    table = f"{schema}.time_series_characteristics" if schema != "public" else "time_series_characteristics"
    df = load_table(config_path, table)
    print(f"  Loaded {len(df):,} characteristics from {table}")
    return df


def _load_metrics_from_db(config_path: str):
    """Load backtest metrics from PostgreSQL."""
    import pandas as pd
    from db.db import load_table, get_schema

    schema = get_schema(config_path)
    table = f"{schema}.backtest_metrics" if schema != "public" else "backtest_metrics"
    df = load_table(config_path, table)
    print(f"  Loaded {len(df):,} metric rows from {table}")
    return df


def _load_forecasts_from_db(config_path: str):
    """Load forecast results from PostgreSQL."""
    import pandas as pd
    from db.db import load_table, get_schema

    schema = get_schema(config_path)
    table = f"{schema}.forecast_results" if schema != "public" else "forecast_results"
    df = load_table(config_path, table)
    print(f"  Loaded {len(df):,} forecast rows from {table}")
    return df


def _load_characteristics_for_scenario(config_path: str, scenario_id: int):
    """Load characteristics filtered to a specific scenario_id."""
    df = _load_characteristics_from_db(config_path)
    if "scenario_id" in df.columns:
        df = df[df["scenario_id"] == scenario_id]
    return df


def _load_forecasts_for_scenario(config_path: str, scenario_id: int):
    """Load forecast results filtered to a specific scenario_id."""
    df = _load_forecasts_from_db(config_path)
    if "scenario_id" in df.columns:
        df = df[df["scenario_id"] == scenario_id]
    return df


def _load_segment_series(segment_id: int, config_path: str):
    """Query segment_membership for the given segment_id -> return list of unique_ids."""
    from db.db import get_conn, get_schema
    conn = get_conn(config_path)
    schema = get_schema(config_path)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT unique_id FROM {schema}.segment_membership WHERE segment_id = %s",
                (segment_id,)
            )
            ids = [row[0] for row in cur.fetchall()]
        return ids
    finally:
        conn.close()


def _collect_all_methods(config_path: str = None) -> list:
    """Gather the union of ALL methods from every method_selection category in DB config."""
    try:
        if config_path:
            import yaml
            with open(config_path, 'r') as f:
                cfg = yaml.safe_load(f) or {}
        else:
            raise FileNotFoundError
    except (FileNotFoundError, OSError):
        from db.db import load_config_from_db
        cfg = load_config_from_db()
    method_lists = cfg.get('forecasting', {}).get('method_selection', {})
    all_methods = set()
    for methods in method_lists.values():
        if isinstance(methods, list):
            all_methods.update(methods)
    return sorted(all_methods)


def _apply_series_and_method_overrides(
    df, chars_df, *, series_filter=None, all_methods=False, config_path=None
):
    """
    Apply --series filter and --all-methods override to data/characteristics.

    When all_methods is True, the recommended_methods column is replaced with
    the union of every method across all categories in config.yaml, so every
    forecaster will attempt every method it supports.
    """
    if series_filter:
        df = df[df['unique_id'].isin(series_filter)]
        chars_df = chars_df[chars_df['unique_id'].isin(series_filter)]
        print(f"  Series filter applied: {len(series_filter)} series")

    if all_methods and config_path:
        full_methods = _collect_all_methods(config_path)
        print(f"  All-methods mode: overriding recommended_methods -> {full_methods}")
        chars_df = chars_df.copy()
        chars_df['recommended_methods'] = [full_methods] * len(chars_df)

    return df, chars_df


def run_single_step(step: str, config_path: str, segment_id: int = None,
                    series_filter: list = None, all_methods: bool = False,
                    overrides_json: dict = None, scenario_id: int = 1):
    """Run a single pipeline step, loading inputs from PostgreSQL."""
    import uuid as _uuid
    from utils.process_logger import ProcessLogger, ListHandler

    orchestrator = ForecastOrchestrator(config_path, scenario_id=scenario_id)
    pl = ProcessLogger(run_id=str(_uuid.uuid4()))

    import pandas as pd

    if step == 'segmentation':
        from segmentation.segmentation import SegmentationEngine
        engine = SegmentationEngine()
        handler = ListHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s"))
        logging.getLogger().addHandler(handler)
        log_id = pl.start_step('segmentation')
        try:
            results = engine.run_all()
            for seg_name, count in results.items():
                print(f"  '{seg_name}': {count} series assigned")
            total = sum(results.values())
            pl.end_step(log_id, 'success', rows=total, log_tail=handler.get_tail())
        except Exception as exc:
            pl.end_step(log_id, 'error', error=str(exc), log_tail=handler.get_tail())
            raise
        finally:
            logging.getLogger().removeHandler(handler)
        print(f"Segmentation complete: {len(results)} segments processed")
        return

    if step == 'classification':
        from classification.abc import ABCClassifier
        handler = ListHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s"))
        logging.getLogger().addHandler(handler)
        log_id = pl.start_step('classification')
        try:
            classifier = ABCClassifier(config_path)
            summaries = classifier.run_all_active()
            total = sum(s.get("total", 0) for s in summaries)
            for s in summaries:
                print(f"  '{s['name']}': {s['total']} series — {s.get('per_class', {})}")
            pl.end_step(log_id, 'success', rows=total, log_tail=handler.get_tail())
        except Exception as exc:
            pl.end_step(log_id, 'error', error=str(exc), log_tail=handler.get_tail())
            raise
        finally:
            logging.getLogger().removeHandler(handler)
        print(f"Classification complete: {len(summaries)} configurations processed, {total} total results")
        return

    if step == 'etl':
        handler = ListHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s"))
        logging.getLogger().addHandler(handler)
        log_id = pl.start_step('etl')
        try:
            df = orchestrator.step_etl()
            pl.end_step(log_id, 'success', rows=len(df), log_tail=handler.get_tail())
        except Exception as exc:
            pl.end_step(log_id, 'error', error=str(exc), log_tail=handler.get_tail())
            raise
        finally:
            logging.getLogger().removeHandler(handler)
        print(f"ETL complete: {len(df)} rows, {df['unique_id'].nunique()} series")

    elif step == 'outlier-detection':
        print("Loading demand data from PostgreSQL (original qty)...")
        df = _load_demand_for_scenario(config_path, scenario_id, use_corrected=False)
        if segment_id is not None:
            seg_ids = _load_segment_series(segment_id, config_path)
            df = df[df['unique_id'].isin(seg_ids)]
            print(f"  Segment filter applied: {len(seg_ids)} series")
        if series_filter:
            df = df[df['unique_id'].isin(series_filter)]
            print(f"  Series filter applied: {len(series_filter)} series")
        handler = ListHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s"))
        logging.getLogger().addHandler(handler)
        log_id = pl.start_step('outlier-detection')
        try:
            corrected_df, outliers_df = orchestrator.step_outlier_detection(df)
            n_adjusted = outliers_df['unique_id'].nunique() if not outliers_df.empty else 0
            pl.end_step(log_id, 'success', rows=len(outliers_df), log_tail=handler.get_tail())
        except Exception as exc:
            pl.end_step(log_id, 'error', error=str(exc), log_tail=handler.get_tail())
            raise
        finally:
            logging.getLogger().removeHandler(handler)
        print(f"Outlier detection complete: {len(outliers_df)} outliers in {n_adjusted} series")

    elif step == 'characterize':
        print("Loading demand data from PostgreSQL (corrected)...")
        df = _load_demand_for_scenario(config_path, scenario_id, use_corrected=True)
        if segment_id is not None:
            seg_ids = _load_segment_series(segment_id, config_path)
            df = df[df['unique_id'].isin(seg_ids)]
            print(f"  Segment filter applied: {len(seg_ids)} series")
        if series_filter:
            df = df[df['unique_id'].isin(series_filter)]
            print(f"  Series filter applied: {len(series_filter)} series")
        handler = ListHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s"))
        logging.getLogger().addHandler(handler)
        log_id = pl.start_step('characterize')
        try:
            chars_df = orchestrator.step_characterize(df)
            pl.end_step(log_id, 'success', rows=len(chars_df), log_tail=handler.get_tail())
        except Exception as exc:
            pl.end_step(log_id, 'error', error=str(exc), log_tail=handler.get_tail())
            raise
        finally:
            logging.getLogger().removeHandler(handler)
        print(f"Characterization complete: {len(chars_df)} series analyzed")

    elif step == 'forecast':
        print("Loading demand data from PostgreSQL (corrected)...")
        df = _load_demand_for_scenario(config_path, scenario_id, use_corrected=True)
        print("Loading characteristics from PostgreSQL...")
        chars_df = _load_characteristics_for_scenario(config_path, scenario_id)
        if segment_id is not None:
            seg_ids = _load_segment_series(segment_id, config_path)
            df = df[df['unique_id'].isin(seg_ids)]
            chars_df = chars_df[chars_df['unique_id'].isin(seg_ids)]
            print(f"  Segment filter applied: {len(seg_ids)} series")
        df, chars_df = _apply_series_and_method_overrides(
            df, chars_df,
            series_filter=series_filter,
            all_methods=all_methods,
            config_path=config_path,
        )
        # Diagnostic: warn loudly if the series filter left nothing to forecast
        if chars_df.empty:
            print(
                f"  WARNING: characteristics DataFrame is empty after applying "
                f"series_filter={series_filter}. "
                f"Possible causes: unique_id mismatch between time_series_characteristics "
                f"and the filter list, or the series has not been characterised yet. "
                f"Run the 'characterize' step first, then retry."
            )
        else:
            print(f"  Forecasting {len(chars_df)} series: {list(chars_df['unique_id'])}")
        # Pass overrides to the orchestrator for per-series hyper-param tuning
        if overrides_json:
            orchestrator.series_overrides = overrides_json
            print(f"  Hyperparameter overrides loaded for {len(overrides_json)} series: "
                  f"{list(overrides_json.keys())}")

        # --- Forecast step (logged) ---
        handler = ListHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s"))
        logging.getLogger().addHandler(handler)
        log_id = pl.start_step('forecast')
        from utils.orchestrator import DASK_AVAILABLE
        # Only spin up Dask when the workload genuinely needs parallel workers.
        # For small segment / targeted-series runs (fits in one batch), Dask adds
        # overhead and — on Windows — worker-process log output and exceptions are
        # silently dropped because spawned processes don't inherit log handlers.
        # The serial path (client=None) runs all 4 forecasters in-process and
        # propagates errors to the visible job log.
        _n_series_fc = len(chars_df['unique_id'].unique()) if not chars_df.empty else 0
        _batch_size_fc = orchestrator.parallel_config.get('batch_size', 100)
        _use_dask_fc = (
            orchestrator.parallel_config.get('backend') == 'dask'
            and DASK_AVAILABLE
            and _n_series_fc > _batch_size_fc
        )
        if _use_dask_fc:
            orchestrator.start_dask_client()
        try:
            # Use targeted delete (series_subset=True) whenever we are scoping
            # to a subset of all series — either via --series filter OR via
            # --segment-id.  This prevents truncating the ENTIRE forecast_results
            # table when only a small segment is being re-forecast.
            forecasts_df = orchestrator.step_forecast(
                df, chars_df,
                series_subset=bool(series_filter) or (segment_id is not None),
            )
            pl.end_step(log_id, 'success', rows=len(forecasts_df), log_tail=handler.get_tail())
        except Exception as exc:
            pl.end_step(log_id, 'error', error=str(exc), log_tail=handler.get_tail())
            raise
        finally:
            if orchestrator.client:
                orchestrator.stop_dask_client()
            logging.getLogger().removeHandler(handler)
        print(f"Forecasting complete: {len(forecasts_df)} forecasts generated")

        # When running for specific series with all methods, also
        # chain backtest -> best-method so the user gets a full comparison.
        if series_filter and all_methods:
            print("\n--- Chaining: Backtest + Best Method for full comparison ---")

            # Backtest step (logged)
            handler = ListHandler()
            handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s"))
            logging.getLogger().addHandler(handler)
            log_id = pl.start_step('backtest')
            if orchestrator.parallel_config.get('backend') == 'dask' and DASK_AVAILABLE:
                orchestrator.start_dask_client()
            try:
                metrics_df, origin_df = orchestrator.step_backtest(
                    df, chars_df, all_methods=True, series_subset=True,
                )
                pl.end_step(log_id, 'success', rows=len(metrics_df), log_tail=handler.get_tail())
            except Exception as exc:
                pl.end_step(log_id, 'error', error=str(exc), log_tail=handler.get_tail())
                raise
            finally:
                if orchestrator.client:
                    orchestrator.stop_dask_client()
                logging.getLogger().removeHandler(handler)
            print(f"Backtesting complete: {len(metrics_df)} metric rows")

            # Composite scoring now runs automatically inside step_backtest

    elif step == 'backtest':
        print("Loading demand data from PostgreSQL (corrected)...")
        df = _load_demand_for_scenario(config_path, scenario_id, use_corrected=True)
        print("Loading characteristics from PostgreSQL...")
        chars_df = _load_characteristics_for_scenario(config_path, scenario_id)
        if segment_id is not None:
            seg_ids = _load_segment_series(segment_id, config_path)
            df = df[df['unique_id'].isin(seg_ids)]
            chars_df = chars_df[chars_df['unique_id'].isin(seg_ids)]
            print(f"  Segment filter applied: {len(seg_ids)} series")
        df, chars_df = _apply_series_and_method_overrides(
            df, chars_df,
            series_filter=series_filter,
            all_methods=all_methods,
            config_path=config_path,
        )
        handler = ListHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s"))
        logging.getLogger().addHandler(handler)
        log_id = pl.start_step('backtest')
        from utils.orchestrator import DASK_AVAILABLE
        # Only spin up Dask when the workload genuinely needs parallel workers.
        # For small segment / targeted-series runs (fits in one batch), Dask adds
        # overhead and — on Windows — worker-process log output and exceptions are
        # silently dropped because spawned processes don't inherit log handlers.
        # The serial path (client=None) runs all backtesting in-process and
        # propagates errors to the visible job log.
        _n_series_bt = len(chars_df['unique_id'].unique()) if not chars_df.empty else 0
        _batch_size_bt = orchestrator.parallel_config.get('batch_size', 100)
        _use_dask_bt = (
            orchestrator.parallel_config.get('backend') == 'dask'
            and DASK_AVAILABLE
            and _n_series_bt > _batch_size_bt
        )
        if _use_dask_bt:
            orchestrator.start_dask_client()
        try:
            metrics_df, origin_df = orchestrator.step_backtest(
                df, chars_df, all_methods=all_methods,
            )
            pl.end_step(log_id, 'success', rows=len(metrics_df), log_tail=handler.get_tail())
        except Exception as exc:
            pl.end_step(log_id, 'error', error=str(exc), log_tail=handler.get_tail())
            raise
        finally:
            if orchestrator.client:
                orchestrator.stop_dask_client()
            logging.getLogger().removeHandler(handler)
        print(f"Backtesting complete: {len(metrics_df)} metric rows, {len(origin_df)} origin forecast rows")

    elif step == 'distributions':
        print("Loading forecasts from PostgreSQL...")
        forecasts_df = _load_forecasts_for_scenario(config_path, scenario_id)
        handler = ListHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s"))
        logging.getLogger().addHandler(handler)
        log_id = pl.start_step('distributions')
        # Start Dask for parallel distribution fitting (mirrors forecast/backtest steps)
        from utils.orchestrator import DASK_AVAILABLE
        if orchestrator.parallel_config.get('backend') == 'dask' and DASK_AVAILABLE:
            orchestrator.start_dask_client()
        try:
            dist_df = orchestrator.step_fit_distributions(forecasts_df)
            pl.end_step(log_id, 'success', rows=len(dist_df), log_tail=handler.get_tail())
        except Exception as exc:
            pl.end_step(log_id, 'error', error=str(exc), log_tail=handler.get_tail())
            raise
        finally:
            if orchestrator.client:
                orchestrator.stop_dask_client()
            logging.getLogger().removeHandler(handler)
        print(f"Distribution fitting complete: {len(dist_df)} distributions fitted")

    else:
        print(f"Unknown step: {step}")
        print("Available steps: etl, outlier-detection, segmentation, classification, characterize, forecast, backtest, distributions")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description='ForecastAI Pipeline Runner',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_pipeline.py                          # Full pipeline
  python run_pipeline.py --skip-etl               # Skip ETL step
  python run_pipeline.py --only etl               # Only run ETL
  python run_pipeline.py --only characterize      # Only characterize
  python run_pipeline.py --discover-schema        # Inspect database tables
  python run_pipeline.py --skip-etl --skip-characterization  # Start from forecasting
        """
    )

    parser.add_argument(
        '--config', type=str, default=None,
        help='Optional path to a YAML config file (legacy; configuration is loaded from DB by default)'
    )

    # Step control
    parser.add_argument('--skip-etl', action='store_true', help='Skip ETL step')
    parser.add_argument('--skip-outlier-detection', action='store_true', help='Skip outlier detection step')
    parser.add_argument('--skip-segmentation', action='store_true', help='Skip segmentation step')
    parser.add_argument('--skip-classification', action='store_true', help='Skip classification step')
    parser.add_argument('--skip-characterization', action='store_true', help='Skip characterization step')
    parser.add_argument('--skip-forecasting', action='store_true', help='Skip forecasting step')
    parser.add_argument('--skip-backtest', action='store_true', help='Skip backtesting step')
    parser.add_argument('--skip-best-method', action='store_true', help='Skip best method selection')
    parser.add_argument('--skip-distributions', action='store_true', help='Skip distribution fitting')

    # Single step mode
    parser.add_argument(
        '--only', type=str, default=None,
        choices=['etl', 'outlier-detection', 'segmentation', 'classification',
                 'characterize', 'forecast', 'backtest', 'best-method', 'distributions'],
        help='Run only a single step'
    )

    # Segment scoping
    parser.add_argument(
        '--segment-id', type=int, default=None,
        help='Scope step to only series belonging to this segment (by segment.id)'
    )

    # Scenario
    parser.add_argument(
        '--scenario-id', type=int, default=1,
        help='Scenario id for this pipeline run (default: 1 = base scenario)'
    )

    # Series-level run (from Time Series Viewer)
    parser.add_argument(
        '--series', type=str, default=None,
        help='Comma-separated list of unique_ids to restrict the run to'
    )
    parser.add_argument(
        '--all-methods', action='store_true',
        help='Run ALL forecasting methods regardless of recommended_methods'
    )
    parser.add_argument(
        '--overrides-json', type=str, default=None,
        help='JSON string with per-series hyperparameter overrides'
    )

    # Schema discovery
    parser.add_argument('--discover-schema', action='store_true', help='Discover database schema and exit')

    # Logging
    parser.add_argument('--log-level', type=str, default='INFO',
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                        help='Logging level (default: INFO)')
    parser.add_argument('--log-file', type=str, default=None,
                        help='Log file path (default: logs/forecasting.log from config)')

    args = parser.parse_args()

    # Setup logging
    log_file = args.log_file
    if log_file is None:
        try:
            from db.db import load_config_from_db
            cfg = load_config_from_db()
            log_file = cfg.get('logging', {}).get('file')
        except Exception:
            pass
        if not log_file:
            log_file = './logs/forecasting.log'

    setup_logging(args.log_level, log_file)
    logger = logging.getLogger('run_pipeline')

    # Schema discovery mode
    if args.discover_schema:
        discover_schema(args.config)
        return

    # Parse optional series filter and overrides
    series_filter = [s.strip() for s in args.series.split(',')] if args.series else None
    overrides_json = None
    if args.overrides_json:
        import json as _json
        try:
            overrides_json = _json.loads(args.overrides_json)
        except Exception as e:
            logger.warning(f"Failed to parse --overrides-json: {e}")

    # Single step mode
    if args.only:
        logger.info(f"Running single step: {args.only} (scenario_id={args.scenario_id})")
        # Mark scenario as running (non-fatal if DB unavailable)
        if args.scenario_id != 1:
            try:
                from db.db import get_conn, get_schema
                _conn = get_conn()
                _schema = get_schema()
                with _conn.cursor() as _cur:
                    _cur.execute(
                        f"UPDATE {_schema}.forecast_scenarios SET status='running', run_at=NOW() WHERE scenario_id=%s",
                        (args.scenario_id,),
                    )
                _conn.commit()
                _conn.close()
            except Exception as _e:
                logger.warning(f"Could not update scenario status to running: {_e}")
        try:
            run_single_step(
                args.only, args.config,
                segment_id=args.segment_id,
                series_filter=series_filter,
                all_methods=args.all_methods,
                overrides_json=overrides_json,
                scenario_id=args.scenario_id,
            )
        except Exception:
            # Mark scenario as failed
            if args.scenario_id != 1:
                try:
                    from db.db import get_conn, get_schema
                    _conn = get_conn()
                    _schema = get_schema()
                    with _conn.cursor() as _cur:
                        _cur.execute(
                            f"UPDATE {_schema}.forecast_scenarios SET status='failed' WHERE scenario_id=%s",
                            (args.scenario_id,),
                        )
                    _conn.commit()
                    _conn.close()
                except Exception:
                    pass
            raise
        # Mark scenario step as complete (only when scenario_id!=1; base is always 'complete')
        if args.scenario_id != 1:
            try:
                from db.db import get_conn, get_schema
                _conn = get_conn()
                _schema = get_schema()
                with _conn.cursor() as _cur:
                    _cur.execute(
                        f"UPDATE {_schema}.forecast_scenarios SET status='complete' WHERE scenario_id=%s",
                        (args.scenario_id,),
                    )
                _conn.commit()
                _conn.close()
            except Exception as _e:
                logger.warning(f"Could not update scenario status to complete: {_e}")
        return

    # Full pipeline
    logger.info("Running complete pipeline")

    orchestrator = ForecastOrchestrator(args.config)

    output_paths = orchestrator.run_complete_pipeline(
        skip_etl=args.skip_etl,
        skip_outlier_detection=args.skip_outlier_detection,
        skip_segmentation=args.skip_segmentation,
        skip_classification=args.skip_classification,
        skip_characterization=args.skip_characterization,
        skip_forecasting=args.skip_forecasting,
        skip_backtest=args.skip_backtest,
        skip_best_method=args.skip_best_method,
        skip_distributions=args.skip_distributions
    )

    print("\n" + "=" * 80)
    print("PIPELINE COMPLETE!")
    print("=" * 80)
    print("\nOutput files:")
    for key, path in output_paths.items():
        if path:
            print(f"  {key}: {path}")


if __name__ == "__main__":
    main()

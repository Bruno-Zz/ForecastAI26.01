"""
ForecastAI Pipeline Runner
CLI entry point for running the full or partial forecasting pipeline.

Usage:
    # Full pipeline (ETL → Characterize → Forecast → Backtest → Best Method → Distributions)
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
from etl.etl import ETLPipeline


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


def _load_segment_series(segment_id: int, config_path: str):
    """Query segment_membership for the given segment_id → return list of unique_ids."""
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
                    overrides_json: dict = None):
    """Run a single pipeline step, loading inputs from PostgreSQL."""
    import uuid as _uuid
    from utils.process_logger import ProcessLogger, ListHandler

    orchestrator = ForecastOrchestrator(config_path)
    pl = ProcessLogger(config_path, run_id=str(_uuid.uuid4()))

    import pandas as pd

    if step == 'segmentation':
        from segmentation.segmentation import SegmentationEngine
        engine = SegmentationEngine(config_path)
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
        df = _load_demand_from_db(config_path, use_corrected=False)
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
        df = _load_demand_from_db(config_path, use_corrected=True)
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
        df = _load_demand_from_db(config_path, use_corrected=True)
        print("Loading characteristics from PostgreSQL...")
        chars_df = _load_characteristics_from_db(config_path)
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
        # Pass overrides to the orchestrator for per-series hyper-param tuning
        if overrides_json:
            orchestrator._series_overrides = overrides_json

        # --- Forecast step (logged) ---
        handler = ListHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s"))
        logging.getLogger().addHandler(handler)
        log_id = pl.start_step('forecast')
        from utils.orchestrator import DASK_AVAILABLE
        if orchestrator.parallel_config.get('backend') == 'dask' and DASK_AVAILABLE:
            orchestrator.start_dask_client()
        try:
            forecasts_df = orchestrator.step_forecast(
                df, chars_df, series_subset=bool(series_filter),
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
                    df, chars_df, all_methods=True,
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

            # Best method step (logged)
            if not metrics_df.empty:
                log_id = pl.start_step('best-method')
                try:
                    best_df = orchestrator.step_select_best_methods(
                        metrics_df, series_subset=True,
                    )
                    pl.end_step(log_id, 'success', rows=len(best_df))
                except Exception as exc:
                    pl.end_step(log_id, 'error', error=str(exc))
                    raise
                print(f"Best method selection complete: {len(best_df)} series ranked")

    elif step == 'backtest':
        print("Loading demand data from PostgreSQL (corrected)...")
        df = _load_demand_from_db(config_path, use_corrected=True)
        print("Loading characteristics from PostgreSQL...")
        chars_df = _load_characteristics_from_db(config_path)
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
        # Start Dask for parallel backtesting (mirrors forecast step behaviour)
        from utils.orchestrator import DASK_AVAILABLE
        if orchestrator.parallel_config.get('backend') == 'dask' and DASK_AVAILABLE:
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

    elif step == 'best-method':
        print("Loading backtest metrics from PostgreSQL...")
        metrics_df = _load_metrics_from_db(config_path)
        log_id = pl.start_step('best-method')
        try:
            best_df = orchestrator.step_select_best_methods(metrics_df)
            pl.end_step(log_id, 'success', rows=len(best_df))
        except Exception as exc:
            pl.end_step(log_id, 'error', error=str(exc))
            raise
        print(f"Best method selection complete: {len(best_df)} series ranked")

    elif step == 'distributions':
        print("Loading forecasts from PostgreSQL...")
        forecasts_df = _load_forecasts_from_db(config_path)
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
        print("Available steps: etl, outlier-detection, segmentation, classification, characterize, forecast, backtest, best-method, distributions")
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
        logger.info(f"Running single step: {args.only}")
        run_single_step(
            args.only, args.config,
            segment_id=args.segment_id,
            series_filter=series_filter,
            all_methods=args.all_methods,
            overrides_json=overrides_json,
        )
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

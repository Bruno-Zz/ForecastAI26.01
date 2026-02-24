"""
ForecastAI Pipeline Runner
CLI entry point for running the full or partial forecasting pipeline.

Usage:
    # Full pipeline (ETL -> Characterize -> Forecast -> Backtest -> Best Method -> Distributions)
    python run_pipeline.py

    # Skip ETL (use existing data in PostgreSQL)
    python run_pipeline.py --skip-etl

    # Only run ETL
    python run_pipeline.py --only etl

    # Only run characterization (requires data in demand_actuals)
    python run_pipeline.py --only characterize

    # Custom config and output
    python run_pipeline.py --config ./config/config.yaml --output ./output

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
from db.db import load_table, get_schema


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
        handlers=handlers
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


def _load_demand_actuals(config_path: str):
    """Load demand_actuals from DB in the standard (unique_id, date, y) format.

    Only the three canonical columns are returned — extra metadata columns
    (item_id, site_id, channel, …) are intentionally excluded so that
    StatsForecast does not interpret them as exogenous features.
    """
    import pandas as pd
    schema = get_schema(config_path)
    df = load_table(
        config_path,
        f"{schema}.demand_actuals",
        columns="unique_id, date, COALESCE(corrected_qty, qty) AS y",
    )
    df["date"] = pd.to_datetime(df["date"])
    return df


def run_single_step(step: str, config_path: str, output_dir: str = None,
                    series_filter: list = None, all_methods: bool = False):
    """Run a single pipeline step.  All data is read from / written to PostgreSQL.

    Args:
        step: Pipeline step name.
        config_path: Path to config.yaml.
        output_dir: Unused (kept for API compat).
        series_filter: Optional list of unique_ids to restrict processing to.
        all_methods: If True, override recommended_methods with ALL methods
                     from the config (statistical + ML + neural + foundation).
    """
    orchestrator = ForecastOrchestrator(config_path)
    schema = get_schema(config_path)

    import pandas as pd

    if step == 'etl':
        df = orchestrator.step_etl()
        print(f"ETL complete: {len(df)} rows, {df['unique_id'].nunique()} series")

    elif step == 'outlier-detection':
        df = _load_demand_actuals(config_path)
        corrected_df, outliers_df = orchestrator.step_outlier_detection(df)
        n_adjusted = outliers_df['unique_id'].nunique() if not outliers_df.empty else 0
        print(f"Outlier detection complete: {len(outliers_df)} outliers in {n_adjusted} series")

    elif step == 'characterize':
        df = _load_demand_actuals(config_path)
        chars_df = orchestrator.step_characterize(df)
        print(f"Characterization complete: {len(chars_df)} series analyzed")

    elif step == 'forecast':
        df = _load_demand_actuals(config_path)
        chars_df = load_table(config_path, f"{schema}.time_series_characteristics")

        # Filter to specific series if requested
        if series_filter:
            df = df[df['unique_id'].isin(series_filter)]
            chars_df = chars_df[chars_df['unique_id'].isin(series_filter)]
            print(f"Filtered to {len(chars_df)} series: {series_filter}")

        # Override recommended_methods with ALL methods from config
        if all_methods and not chars_df.empty:
            import yaml as _yaml
            with open(config_path, 'r') as _f:
                _cfg = _yaml.safe_load(_f)
            fc = _cfg.get('forecasting', {})
            full_list = (
                fc.get('statsforecast_models', [])
                + fc.get('ml_models', [])
                + fc.get('neuralforecast_models', [])
            )
            if fc.get('timesfm', {}).get('model_name'):
                full_list.append('TimesFM')
            chars_df = chars_df.copy()
            chars_df['recommended_methods'] = [full_list] * len(chars_df)
            print(f"All-methods mode: using {len(full_list)} methods per series")

        # Start Dask for parallel batch processing
        from utils.orchestrator import DASK_AVAILABLE
        if orchestrator.parallel_config.get('backend') == 'dask' and DASK_AVAILABLE:
            orchestrator.start_dask_client()
        try:
            forecasts_df = orchestrator.step_forecast(df, chars_df)
        finally:
            if orchestrator.client:
                orchestrator.stop_dask_client()
        print(f"Forecasting complete: {len(forecasts_df)} forecasts generated")

    elif step == 'backtest':
        df = _load_demand_actuals(config_path)
        chars_df = load_table(config_path, f"{schema}.time_series_characteristics")
        # Start Dask for parallel backtesting
        from utils.orchestrator import DASK_AVAILABLE
        if orchestrator.parallel_config.get('backend') == 'dask' and DASK_AVAILABLE:
            orchestrator.start_dask_client()
        try:
            metrics_df, origin_df = orchestrator.step_backtest(df, chars_df)
        finally:
            if orchestrator.client:
                orchestrator.stop_dask_client()
        print(f"Backtesting complete: {len(metrics_df)} metric rows, {len(origin_df)} origin forecast rows")

    elif step == 'best-method':
        metrics_df = load_table(config_path, f"{schema}.backtest_metrics")
        best_df = orchestrator.step_select_best_methods(metrics_df)
        print(f"Best method selection complete: {len(best_df)} series ranked")

    elif step == 'distributions':
        forecasts_df = load_table(config_path, f"{schema}.forecast_results")
        # Start Dask for parallel distribution fitting
        from utils.orchestrator import DASK_AVAILABLE
        if orchestrator.parallel_config.get('backend') == 'dask' and DASK_AVAILABLE:
            orchestrator.start_dask_client()
        try:
            dist_df = orchestrator.step_fit_distributions(forecasts_df)
        finally:
            if orchestrator.client:
                orchestrator.stop_dask_client()
        print(f"Distribution fitting complete: {len(dist_df)} distributions fitted")

    else:
        print(f"Unknown step: {step}")
        print("Available steps: etl, outlier-detection, characterize, forecast, backtest, best-method, distributions")
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
        '--config', type=str, default='config/config.yaml',
        help='Path to config.yaml (default: config/config.yaml)'
    )
    parser.add_argument(
        '--output', type=str, default=None,
        help='Output directory override (default: from config)'
    )

    # Step control
    parser.add_argument('--skip-etl', action='store_true', help='Skip ETL step')
    parser.add_argument('--skip-outlier-detection', action='store_true', help='Skip outlier detection step')
    parser.add_argument('--skip-characterization', action='store_true', help='Skip characterization step')
    parser.add_argument('--skip-forecasting', action='store_true', help='Skip forecasting step')
    parser.add_argument('--skip-backtest', action='store_true', help='Skip backtesting step')
    parser.add_argument('--skip-best-method', action='store_true', help='Skip best method selection')
    parser.add_argument('--skip-distributions', action='store_true', help='Skip distribution fitting')

    # Single step mode
    parser.add_argument(
        '--only', type=str, default=None,
        choices=['etl', 'outlier-detection', 'characterize', 'forecast', 'backtest', 'best-method', 'distributions'],
        help='Run only a single step'
    )

    # Series filtering and method override (used by the "Run Forecast" UI button)
    parser.add_argument(
        '--series', type=str, default=None,
        help='Comma-separated list of unique_ids to process (e.g., 63530_517,63531_518)'
    )
    parser.add_argument(
        '--all-methods', action='store_true',
        help='Run ALL forecast methods instead of only recommended ones'
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
            import yaml
            with open(args.config, 'r') as f:
                cfg = yaml.safe_load(f)
            log_file = cfg.get('logging', {}).get('file')
        except Exception:
            log_file = './logs/forecasting.log'

    setup_logging(args.log_level, log_file)
    logger = logging.getLogger('run_pipeline')

    # Schema discovery mode
    if args.discover_schema:
        discover_schema(args.config)
        return

    # Single step mode
    if args.only:
        logger.info(f"Running single step: {args.only}")
        series_filter = args.series.split(',') if args.series else None
        run_single_step(
            args.only, args.config, args.output,
            series_filter=series_filter,
            all_methods=args.all_methods,
        )
        return

    # Full pipeline
    logger.info("Running complete pipeline")

    orchestrator = ForecastOrchestrator(args.config)

    output_tables = orchestrator.run_complete_pipeline(
        output_dir=args.output,
        skip_etl=args.skip_etl,
        skip_outlier_detection=args.skip_outlier_detection,
        skip_characterization=args.skip_characterization,
        skip_forecasting=args.skip_forecasting,
        skip_backtest=args.skip_backtest,
        skip_best_method=args.skip_best_method,
        skip_distributions=args.skip_distributions
    )

    print("\n" + "=" * 80)
    print("PIPELINE COMPLETE!")
    print("=" * 80)
    print("\nOutput tables:")
    for key, table in output_tables.items():
        if table:
            print(f"  {key}: {table}")


if __name__ == "__main__":
    main()

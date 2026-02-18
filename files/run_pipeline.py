"""
ForecastAI Pipeline Runner
CLI entry point for running the full or partial forecasting pipeline.

Usage:
    # Full pipeline (ETL → Characterize → Forecast → Backtest → Best Method → Distributions)
    python run_pipeline.py

    # Skip ETL (use existing time_series.parquet)
    python run_pipeline.py --skip-etl

    # Skip ETL and characterization (use existing parquet files)
    python run_pipeline.py --skip-etl --skip-characterization

    # Only run ETL
    python run_pipeline.py --only etl

    # Only run characterization (requires existing time_series.parquet)
    python run_pipeline.py --only characterize

    # Run from specific data files
    python run_pipeline.py --skip-etl --data ./data/time_series.parquet

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


def run_single_step(step: str, config_path: str, data_path: str = None, output_dir: str = None):
    """Run a single pipeline step."""
    orchestrator = ForecastOrchestrator(config_path)

    import pandas as pd

    if step == 'etl':
        df = orchestrator.step_etl()
        print(f"ETL complete: {len(df)} rows, {df['unique_id'].nunique()} series")

    elif step == 'outlier-detection':
        data_file = data_path or orchestrator.config['etl']['output_path']
        df = pd.read_parquet(data_file)
        corrected_df, outliers_df = orchestrator.step_outlier_detection(df)
        n_adjusted = outliers_df['unique_id'].nunique() if not outliers_df.empty else 0
        print(f"Outlier detection complete: {len(outliers_df)} outliers in {n_adjusted} series")

    elif step == 'characterize':
        data_file = data_path or orchestrator.config['etl']['output_path']
        df = pd.read_parquet(data_file)
        chars_df = orchestrator.step_characterize(df)
        print(f"Characterization complete: {len(chars_df)} series analyzed")

    elif step == 'forecast':
        data_file = data_path or orchestrator.config['etl']['output_path']
        output_base = output_dir or orchestrator.config['output']['base_path']
        df = pd.read_parquet(data_file)
        chars_df = pd.read_parquet(str(Path(output_base) / "time_series_characteristics.parquet"))
        forecasts_df = orchestrator.step_forecast(df, chars_df)
        print(f"Forecasting complete: {len(forecasts_df)} forecasts generated")

    elif step == 'backtest':
        data_file = data_path or orchestrator.config['etl']['output_path']
        output_base = output_dir or orchestrator.config['output']['base_path']
        df = pd.read_parquet(data_file)
        chars_df = pd.read_parquet(str(Path(output_base) / "time_series_characteristics.parquet"))
        metrics_df, origin_df = orchestrator.step_backtest(df, chars_df)
        print(f"Backtesting complete: {len(metrics_df)} metric rows, {len(origin_df)} origin forecast rows")

    elif step == 'best-method':
        output_base = output_dir or orchestrator.config['output']['base_path']
        metrics_df = pd.read_parquet(str(Path(output_base) / "backtest_metrics.parquet"))
        best_df = orchestrator.step_select_best_methods(metrics_df)
        print(f"Best method selection complete: {len(best_df)} series ranked")

    elif step == 'distributions':
        output_base = output_dir or orchestrator.config['output']['base_path']
        forecasts_df = pd.read_parquet(str(Path(output_base) / "forecasts_all_methods.parquet"))
        dist_df = orchestrator.step_fit_distributions(forecasts_df)
        print(f"Distribution fitting complete: {len(dist_df)} distributions fitted")

    else:
        print(f"Unknown step: {step}")
        print("Available steps: etl, characterize, forecast, backtest, best-method, distributions")
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
    parser.add_argument(
        '--data', type=str, default=None,
        help='Path to existing time_series.parquet (implies --skip-etl)'
    )
    parser.add_argument(
        '--characteristics', type=str, default=None,
        help='Path to existing characteristics.parquet (implies --skip-characterization)'
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

    # If data file provided, skip ETL
    if args.data:
        args.skip_etl = True

    # If characteristics file provided, skip characterization
    if args.characteristics:
        args.skip_characterization = True

    # Schema discovery mode
    if args.discover_schema:
        discover_schema(args.config)
        return

    # Single step mode
    if args.only:
        logger.info(f"Running single step: {args.only}")
        run_single_step(args.only, args.config, args.data, args.output)
        return

    # Full pipeline
    logger.info("Running complete pipeline")

    orchestrator = ForecastOrchestrator(args.config)

    output_paths = orchestrator.run_complete_pipeline(
        time_series_path=args.data,
        characteristics_path=args.characteristics,
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
    print("\nOutput files:")
    for key, path in output_paths.items():
        if path:
            print(f"  {key}: {path}")


if __name__ == "__main__":
    main()

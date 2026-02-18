"""
Parallel Forecast Orchestrator
Coordinates all forecasting methods across time series using Dask.

Pipeline steps:
  1. ETL: Extract from PostgreSQL, transform, load to Parquet
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
import yaml
from pathlib import Path
import time
from datetime import datetime

try:
    from dask.distributed import Client, as_completed
    import dask
    DASK_AVAILABLE = True
except ImportError:
    DASK_AVAILABLE = False
    logging.warning("Dask not available. Install with: pip install dask[complete]")

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

    # -- Pipeline steps --

    def step_etl(self) -> pd.DataFrame:
        """
        Step 1: Extract data from PostgreSQL, transform, load to Parquet.

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

        corrected_df, outliers_df = self.outlier_detector.detect_and_correct_all(df)

        # Save corrected data
        corrected_path = Path(self.config['etl']['output_path']).parent / "time_series_corrected.parquet"
        corrected_path.parent.mkdir(parents=True, exist_ok=True)
        corrected_df.to_parquet(str(corrected_path), index=False)
        self.logger.info(f"Corrected data saved: {corrected_path} ({len(corrected_df)} rows)")

        # Save outlier log
        if not outliers_df.empty:
            output_base = Path(self.output_config['base_path'])
            output_base.mkdir(parents=True, exist_ok=True)
            outlier_path = output_base / "detected_outliers.parquet"
            outliers_df.to_parquet(str(outlier_path), index=False)
            self.logger.info(f"Outliers saved: {outlier_path} ({len(outliers_df)} outliers)")
        else:
            self.logger.info("No outliers detected")

        return corrected_df, outliers_df

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

        characteristics_df = self.characterizer.analyze_all(
            df, id_col='unique_id', date_col='date', value_col='y', save=True
        )

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
                       methods_filter: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Generate forecasts for a batch of time series across all model families.

        Args:
            df: Time series data
            characteristics_df: Characteristics for this batch
            methods_filter: Optional list to restrict methods

        Returns:
            DataFrame with forecast results
        """
        all_forecasts = []

        # --- Statistical forecasts ---
        self.logger.info(f"Statistical forecasts for {len(characteristics_df)} series...")
        try:
            stat_forecasts = self.stat_forecaster.forecast_multiple_series(
                df=df,
                characteristics_df=characteristics_df
            )
            if not stat_forecasts.empty:
                all_forecasts.append(stat_forecasts)
        except Exception as e:
            self.logger.warning(f"Statistical forecasting failed: {e}")

        # --- ML forecasts (LightGBM / XGBoost) ---
        ml_eligible = characteristics_df[
            characteristics_df.get('sufficient_for_ml', pd.Series(dtype=bool)).fillna(False)
        ] if 'sufficient_for_ml' in characteristics_df.columns else pd.DataFrame()

        if len(ml_eligible) > 0:
            self.logger.info(f"ML forecasts for {len(ml_eligible)} series...")
            try:
                ml_forecasts = self.ml_forecaster.forecast_multiple_series(
                    df=df,
                    characteristics_df=ml_eligible
                )
                if not ml_forecasts.empty:
                    all_forecasts.append(ml_forecasts)
            except Exception as e:
                self.logger.warning(f"ML forecasting failed: {e}")

        # --- Neural forecasts ---
        neural_eligible = characteristics_df[
            characteristics_df.get('sufficient_for_deep_learning', pd.Series(dtype=bool)).fillna(False)
        ] if 'sufficient_for_deep_learning' in characteristics_df.columns else pd.DataFrame()

        if len(neural_eligible) > 0:
            self.logger.info(f"Neural forecasts for {len(neural_eligible)} series...")
            try:
                neural_forecasts = self.neural_forecaster.forecast_multiple_series(
                    df=df,
                    characteristics_df=neural_eligible
                )
                if not neural_forecasts.empty:
                    all_forecasts.append(neural_forecasts)
            except Exception as e:
                self.logger.warning(f"Neural forecasting failed: {e}")

        # --- Foundation model (TimesFM) ---
        self.logger.info(f"Foundation model forecasts for {len(characteristics_df)} series...")
        try:
            if self.foundation_forecaster.model is not None:
                foundation_forecasts = self.foundation_forecaster.forecast_multiple_series(
                    df=df,
                    characteristics_df=characteristics_df
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
                      characteristics_df: pd.DataFrame) -> pd.DataFrame:
        """
        Step 3: Generate forecasts using all model families.

        Args:
            df: Time series data
            characteristics_df: Series characteristics

        Returns:
            Combined forecast results
        """
        self.logger.info("=" * 80)
        self.logger.info("STEP 3: Forecasting (Statistical + ML + Neural + Foundation)")
        self.logger.info("=" * 80)

        batch_size = self.parallel_config.get('batch_size', 100)

        if self.client is None or not DASK_AVAILABLE:
            # Serial mode
            self.logger.info("Running forecasting in serial mode...")
            forecasts_df = self.forecast_batch(df, characteristics_df)
        else:
            # Parallel with Dask
            self.logger.info(f"Running parallel forecasting, batch_size={batch_size}...")
            n_series = len(characteristics_df)
            n_batches = (n_series + batch_size - 1) // batch_size

            futures = []
            for i in range(n_batches):
                start_idx = i * batch_size
                end_idx = min((i + 1) * batch_size, n_series)
                batch_chars = characteristics_df.iloc[start_idx:end_idx]
                batch_ids = batch_chars['unique_id'].tolist()
                batch_df = df[df['unique_id'].isin(batch_ids)]

                future = self.client.submit(
                    self.forecast_batch,
                    batch_df,
                    batch_chars
                )
                futures.append(future)

            results = []
            for future in as_completed(futures):
                try:
                    result = future.result()
                    if not result.empty:
                        results.append(result)
                except Exception as e:
                    self.logger.error(f"Batch failed: {e}")

            forecasts_df = pd.concat(results, ignore_index=True) if results else pd.DataFrame()
            self.logger.info(f"Generated {len(forecasts_df)} forecasts across {n_batches} batches")

        if forecasts_df.empty:
            self.logger.warning("No forecasts generated!")
            return forecasts_df

        # Save
        output_path = Path(self.output_config['base_path']) / "forecasts_all_methods.parquet"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        forecasts_df.to_parquet(str(output_path), index=False)
        self.logger.info(f"Forecasts saved: {output_path} ({len(forecasts_df)} rows)")

        return forecasts_df

    def step_backtest(self,
                      df: pd.DataFrame,
                      characteristics_df: pd.DataFrame) -> tuple:
        """
        Step 4: Rolling-window backtesting with per-origin forecast storage.

        Args:
            df: Time series data
            characteristics_df: Series characteristics

        Returns:
            Tuple of (metrics_df, forecasts_by_origin_df)
        """
        self.logger.info("=" * 80)
        self.logger.info("STEP 4: Backtesting with Per-Origin Forecast Storage")
        self.logger.info("=" * 80)

        all_metrics = []
        all_origin_forecasts = []

        for _, char_row in characteristics_df.iterrows():
            unique_id = char_row['unique_id']

            # Use top recommended methods (limit to avoid excessive compute)
            methods = char_row.get('recommended_methods', ['AutoETS', 'AutoARIMA', 'AutoTheta'])
            if isinstance(methods, str):
                methods = [methods]
            methods = methods[:5]

            self.logger.info(f"Backtesting: {unique_id} with {methods}")

            try:
                # Use the enhanced backtest that also stores per-origin forecasts
                if hasattr(self.evaluator, 'backtest_series_with_forecasts'):
                    metrics, origin_forecasts = self.evaluator.backtest_series_with_forecasts(
                        df=df,
                        unique_id=unique_id,
                        forecast_fn=self.stat_forecaster.forecast_single_series,
                        methods=methods,
                        characteristics=char_row.to_dict()
                    )
                    if not origin_forecasts.empty:
                        all_origin_forecasts.append(origin_forecasts)
                else:
                    metrics = self.evaluator.backtest_series(
                        df=df,
                        unique_id=unique_id,
                        forecast_fn=self.stat_forecaster.forecast_single_series,
                        methods=methods,
                        characteristics=char_row.to_dict()
                    )

                if not metrics.empty:
                    all_metrics.append(metrics)

            except Exception as e:
                self.logger.warning(f"Backtest failed for {unique_id}: {e}")
                continue

        # Combine metrics
        metrics_df = pd.concat(all_metrics, ignore_index=True) if all_metrics else pd.DataFrame()

        # Combine per-origin forecasts
        origin_df = pd.concat(all_origin_forecasts, ignore_index=True) if all_origin_forecasts else pd.DataFrame()

        # Save outputs
        output_base = Path(self.output_config['base_path'])
        output_base.mkdir(parents=True, exist_ok=True)

        if not metrics_df.empty:
            metrics_path = output_base / "backtest_metrics.parquet"
            metrics_df.to_parquet(str(metrics_path), index=False)
            self.logger.info(f"Backtest metrics saved: {metrics_path} ({len(metrics_df)} rows)")

            # Log summary
            summary = metrics_df.groupby('method')[['mae', 'rmse']].mean()
            self.logger.info(f"Backtest summary by method:\n{summary}")

        if not origin_df.empty:
            origin_path = output_base / "forecasts_by_origin.parquet"
            origin_df.to_parquet(str(origin_path), index=False)
            self.logger.info(f"Per-origin forecasts saved: {origin_path} ({len(origin_df)} rows)")

        return metrics_df, origin_df

    def step_select_best_methods(self, metrics_df: pd.DataFrame) -> pd.DataFrame:
        """
        Step 5: Select best forecasting method per series.

        Args:
            metrics_df: Backtest metrics from step 4

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

        # Save
        output_path = Path(self.output_config['base_path']) / "best_method_per_series.parquet"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        best_methods_df.to_parquet(str(output_path), index=False)
        self.logger.info(f"Best methods saved: {output_path} ({len(best_methods_df)} rows)")

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

        distributions_df = self.dist_fitter.fit_forecast_distributions(forecasts_df)

        # Save
        output_path = Path(self.output_config['base_path']) / "fitted_distributions.parquet"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        distributions_df.to_parquet(str(output_path), index=False)
        self.logger.info(f"Distributions saved: {output_path} ({len(distributions_df)} rows)")

        return distributions_df

    # -- Full pipeline --

    def run_complete_pipeline(self,
                              time_series_path: Optional[str] = None,
                              characteristics_path: Optional[str] = None,
                              output_dir: Optional[str] = None,
                              skip_etl: bool = False,
                              skip_outlier_detection: bool = False,
                              skip_characterization: bool = False,
                              skip_forecasting: bool = False,
                              skip_backtest: bool = False,
                              skip_best_method: bool = False,
                              skip_distributions: bool = False) -> Dict[str, str]:
        """
        Run the complete forecasting pipeline.

        Args:
            time_series_path: Path to pre-existing time series parquet (skips ETL)
            characteristics_path: Path to pre-existing characteristics (skips characterization)
            output_dir: Output directory override
            skip_*: Flags to skip individual steps

        Returns:
            Dictionary with output file paths
        """
        start_time = time.time()
        output_base = Path(output_dir or self.output_config['base_path'])
        output_base.mkdir(parents=True, exist_ok=True)
        output_paths = {}

        self.logger.info("=" * 80)
        self.logger.info("STARTING COMPLETE FORECASTING PIPELINE")
        self.logger.info(f"Time: {datetime.now().isoformat()}")
        self.logger.info("=" * 80)

        # Start Dask if configured
        if self.parallel_config['backend'] == 'dask' and DASK_AVAILABLE:
            self.start_dask_client()

        try:
            # Step 1: ETL
            if skip_etl and time_series_path:
                self.logger.info("Skipping ETL, loading from file...")
                df = pd.read_parquet(time_series_path)
            elif skip_etl:
                default_path = self.config['etl']['output_path']
                self.logger.info(f"Skipping ETL, loading from default: {default_path}")
                df = pd.read_parquet(default_path)
            else:
                df = self.step_etl()
                output_paths['time_series'] = self.config['etl']['output_path']

            self.logger.info(f"Data: {len(df)} rows, {df['unique_id'].nunique()} series")

            # Step 1b: Outlier Detection & Correction
            outlier_config = self.config.get('outlier_detection', {})
            if not skip_outlier_detection and outlier_config.get('enabled', False):
                df, outliers_df = self.step_outlier_detection(df)
                output_paths['outliers'] = str(output_base / "detected_outliers.parquet")
                corrected_path = str(Path(self.config['etl']['output_path']).parent / "time_series_corrected.parquet")
                output_paths['time_series_corrected'] = corrected_path
            else:
                self.logger.info("Skipping outlier detection")

            # Step 2: Characterization
            if skip_characterization and characteristics_path:
                self.logger.info("Skipping characterization, loading from file...")
                characteristics_df = pd.read_parquet(characteristics_path)
            elif skip_characterization:
                default_char_path = str(output_base / "time_series_characteristics.parquet")
                self.logger.info(f"Skipping characterization, loading from: {default_char_path}")
                characteristics_df = pd.read_parquet(default_char_path)
            else:
                characteristics_df = self.step_characterize(df)
                output_paths['characteristics'] = str(output_base / "time_series_characteristics.parquet")

            # Step 3: Forecasting
            if not skip_forecasting:
                forecasts_df = self.step_forecast(df, characteristics_df)
                output_paths['forecasts'] = str(output_base / "forecasts_all_methods.parquet")
            else:
                self.logger.info("Skipping forecasting step")
                forecasts_path = output_base / "forecasts_all_methods.parquet"
                forecasts_df = pd.read_parquet(str(forecasts_path)) if forecasts_path.exists() else pd.DataFrame()

            # Step 4: Backtesting
            if not skip_backtest:
                metrics_df, origin_df = self.step_backtest(df, characteristics_df)
                if not metrics_df.empty:
                    output_paths['metrics'] = str(output_base / "backtest_metrics.parquet")
                if not origin_df.empty:
                    output_paths['forecasts_by_origin'] = str(output_base / "forecasts_by_origin.parquet")
            else:
                self.logger.info("Skipping backtesting step")
                metrics_path = output_base / "backtest_metrics.parquet"
                metrics_df = pd.read_parquet(str(metrics_path)) if metrics_path.exists() else pd.DataFrame()

            # Step 5: Best method selection
            if not skip_best_method and not metrics_df.empty:
                best_methods_df = self.step_select_best_methods(metrics_df)
                output_paths['best_methods'] = str(output_base / "best_method_per_series.parquet")
            else:
                self.logger.info("Skipping best method selection")

            # Step 6: Distribution fitting
            if not skip_distributions and not forecasts_df.empty:
                distributions_df = self.step_fit_distributions(forecasts_df)
                output_paths['distributions'] = str(output_base / "fitted_distributions.parquet")
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

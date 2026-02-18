"""
Parallel Forecast Orchestrator
Coordinates all forecasting methods across time series using Dask
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
    import dask.dataframe as dd
    from dask.diagnostics import ProgressBar
    DASK_AVAILABLE = True
except ImportError:
    DASK_AVAILABLE = False
    logging.warning("Dask not available. Install with: pip install dask[complete]")

from forecasting.statistical_models import StatisticalForecaster
from forecasting.neural_models import NeuralForecaster
from forecasting.foundation_models import FoundationForecaster
from evaluation.metrics import ForecastEvaluator
from distribution.fitting import DistributionFitter


class ForecastOrchestrator:
    """
    Orchestrates parallel forecasting across all time series and methods.
    """
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """Initialize orchestrator with configuration."""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.parallel_config = self.config['parallel']
        self.logger = logging.getLogger(__name__)
        
        # Initialize forecasters
        self.stat_forecaster = StatisticalForecaster(config_path)
        self.neural_forecaster = None  # Initialized on demand
        self.foundation_forecaster = None  # Initialized on demand
        
        # Initialize evaluator and fitter
        self.evaluator = ForecastEvaluator(config_path)
        self.dist_fitter = DistributionFitter(config_path)
        
        # Dask client
        self.client = None
        
        if not DASK_AVAILABLE:
            self.logger.warning("Dask not available. Running in serial mode.")
    
    def start_dask_client(self):
        """Start Dask client for parallel processing."""
        if not DASK_AVAILABLE:
            return
        
        dask_config = self.parallel_config['dask']
        
        if dask_config.get('scheduler') == 'distributed':
            # Connect to existing scheduler
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
    
    def forecast_batch(self,
                      df: pd.DataFrame,
                      characteristics_df: pd.DataFrame,
                      methods_filter: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Generate forecasts for a batch of time series.
        
        Args:
            df: Time series data
            characteristics_df: Characteristics for this batch
            methods_filter: Optional list of methods to use (None = use recommended)
            
        Returns:
            DataFrame with forecast results
        """
        all_forecasts = []
        
        # Statistical forecasts
        self.logger.info(f"Generating statistical forecasts for {len(characteristics_df)} series...")
        stat_forecasts = self.stat_forecaster.forecast_multiple_series(
            df=df,
            characteristics_df=characteristics_df
        )
        all_forecasts.append(stat_forecasts)
        
        # Neural forecasts (if sufficient data)
        neural_eligible = characteristics_df[
            characteristics_df['sufficient_for_deep_learning'] == True
        ]
        
        if len(neural_eligible) > 0:
            self.logger.info(f"Generating neural forecasts for {len(neural_eligible)} series...")
            try:
                if self.neural_forecaster is None:
                    self.neural_forecaster = NeuralForecaster(self.config_path)
                
                neural_forecasts = self.neural_forecaster.forecast_multiple_series(
                    df=df,
                    characteristics_df=neural_eligible
                )
                if not neural_forecasts.empty:
                    all_forecasts.append(neural_forecasts)
            except Exception as e:
                self.logger.warning(f"Neural forecasting failed: {e}")
        
        # Foundation model forecasts
        self.logger.info(f"Generating TimesFM forecasts for {len(characteristics_df)} series...")
        try:
            if self.foundation_forecaster is None:
                self.foundation_forecaster = FoundationForecaster(self.config_path)
            
            if self.foundation_forecaster.model is not None:
                foundation_forecasts = self.foundation_forecaster.forecast_multiple_series(
                    df=df,
                    characteristics_df=characteristics_df
                )
                if not foundation_forecasts.empty:
                    all_forecasts.append(foundation_forecasts)
        except Exception as e:
            self.logger.warning(f"Foundation model forecasting failed: {e}")
        
        # Combine all forecasts
        if all_forecasts:
            combined = pd.concat(all_forecasts, ignore_index=True)
            return combined
        else:
            return pd.DataFrame()
    
    def run_parallel_forecasting(self,
                                df: pd.DataFrame,
                                characteristics_df: pd.DataFrame) -> pd.DataFrame:
        """
        Run forecasting in parallel across all time series.
        
        Args:
            df: Time series data
            characteristics_df: Series characteristics
            
        Returns:
            Combined forecast results
        """
        batch_size = self.parallel_config.get('batch_size', 100)
        
        if self.client is None or not DASK_AVAILABLE:
            # Serial processing
            self.logger.info("Running in serial mode...")
            return self.forecast_batch(df, characteristics_df)
        
        # Parallel processing with Dask
        self.logger.info(f"Running parallel forecasting with {batch_size} series per batch...")
        
        # Split characteristics into batches
        n_series = len(characteristics_df)
        n_batches = (n_series + batch_size - 1) // batch_size
        
        batches = []
        for i in range(n_batches):
            start_idx = i * batch_size
            end_idx = min((i + 1) * batch_size, n_series)
            batch_chars = characteristics_df.iloc[start_idx:end_idx]
            batches.append(batch_chars)
        
        # Submit batch jobs
        futures = []
        for batch_chars in batches:
            # Get unique_ids for this batch
            batch_ids = batch_chars['unique_id'].tolist()
            batch_df = df[df['unique_id'].isin(batch_ids)]
            
            future = self.client.submit(
                self.forecast_batch,
                batch_df,
                batch_chars
            )
            futures.append(future)
        
        # Collect results
        results = []
        for future in as_completed(futures):
            try:
                result = future.result()
                if not result.empty:
                    results.append(result)
            except Exception as e:
                self.logger.error(f"Batch failed: {e}")
        
        # Combine all results
        if results:
            combined = pd.concat(results, ignore_index=True)
            self.logger.info(f"Generated {len(combined)} forecasts across {n_batches} batches")
            return combined
        else:
            return pd.DataFrame()
    
    def run_complete_pipeline(self,
                             time_series_path: str,
                             characteristics_path: str,
                             output_dir: str = "./output") -> Dict[str, str]:
        """
        Run complete forecasting pipeline: forecast → evaluate → fit distributions.
        
        Args:
            time_series_path: Path to time series parquet
            characteristics_path: Path to characteristics parquet
            output_dir: Directory for outputs
            
        Returns:
            Dictionary with output file paths
        """
        start_time = time.time()
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        self.logger.info("=" * 80)
        self.logger.info("STARTING COMPLETE FORECASTING PIPELINE")
        self.logger.info("=" * 80)
        
        # Load data
        self.logger.info("Loading data...")
        df = pd.read_parquet(time_series_path)
        characteristics_df = pd.read_parquet(characteristics_path)
        
        self.logger.info(f"Loaded {len(df)} observations for {df['unique_id'].nunique()} series")
        
        # Start Dask if configured
        if self.parallel_config['backend'] == 'dask' and DASK_AVAILABLE:
            self.start_dask_client()
        
        try:
            # Step 1: Generate forecasts
            self.logger.info("\n" + "=" * 80)
            self.logger.info("STEP 1: Generating Forecasts")
            self.logger.info("=" * 80)
            
            forecasts_df = self.run_parallel_forecasting(df, characteristics_df)
            
            if forecasts_df.empty:
                raise ValueError("No forecasts generated")
            
            forecasts_path = output_dir / "forecasts_all_methods.parquet"
            forecasts_df.to_parquet(forecasts_path, index=False)
            self.logger.info(f"Forecasts saved: {forecasts_path}")
            
            # Step 2: Fit distributions for MEIO
            self.logger.info("\n" + "=" * 80)
            self.logger.info("STEP 2: Fitting Distributions for MEIO")
            self.logger.info("=" * 80)
            
            distributions_df = self.dist_fitter.fit_forecast_distributions(forecasts_df)
            
            distributions_path = output_dir / "fitted_distributions.parquet"
            distributions_df.to_parquet(distributions_path, index=False)
            self.logger.info(f"Distributions saved: {distributions_path}")
            
            # Step 3: Backtesting (optional, on subset)
            self.logger.info("\n" + "=" * 80)
            self.logger.info("STEP 3: Backtesting (Sample)")
            self.logger.info("=" * 80)
            
            # Backtest a sample of series
            sample_size = min(10, len(characteristics_df))
            sample_chars = characteristics_df.sample(n=sample_size, random_state=42)
            
            all_metrics = []
            for _, char_row in sample_chars.iterrows():
                unique_id = char_row['unique_id']
                self.logger.info(f"Backtesting: {unique_id}")
                
                metrics = self.evaluator.backtest_series(
                    df=df,
                    unique_id=unique_id,
                    forecast_fn=self.stat_forecaster.forecast_single_series,
                    methods=char_row['recommended_methods'][:3],  # Top 3 methods
                    characteristics=char_row.to_dict()
                )
                
                if not metrics.empty:
                    all_metrics.append(metrics)
            
            if all_metrics:
                metrics_df = pd.concat(all_metrics, ignore_index=True)
                metrics_path = output_dir / "backtest_metrics_sample.parquet"
                metrics_df.to_parquet(metrics_path, index=False)
                self.logger.info(f"Backtest metrics saved: {metrics_path}")
                
                # Summary
                self.logger.info("\nBacktest Summary (by method):")
                summary = metrics_df.groupby('method')[['mae', 'rmse', 'bias', 'coverage_90']].mean()
                self.logger.info(f"\n{summary}")
            
            # Generate summary report
            self.logger.info("\n" + "=" * 80)
            self.logger.info("PIPELINE SUMMARY")
            self.logger.info("=" * 80)
            
            summary = {
                'n_series': df['unique_id'].nunique(),
                'n_forecasts': len(forecasts_df),
                'n_distributions': len(distributions_df),
                'methods_used': forecasts_df['method'].unique().tolist(),
                'distribution_types': distributions_df['distribution_type'].value_counts().to_dict(),
                'execution_time_seconds': time.time() - start_time
            }
            
            for key, value in summary.items():
                self.logger.info(f"{key}: {value}")
            
            # Save summary
            summary_path = output_dir / "pipeline_summary.yaml"
            with open(summary_path, 'w') as f:
                yaml.dump(summary, f, default_flow_style=False)
            
            output_paths = {
                'forecasts': str(forecasts_path),
                'distributions': str(distributions_path),
                'metrics': str(metrics_path) if all_metrics else None,
                'summary': str(summary_path)
            }
            
            return output_paths
            
        finally:
            # Cleanup
            if self.client:
                self.stop_dask_client()


def main():
    """Run complete pipeline."""
    orchestrator = ForecastOrchestrator()
    
    output_paths = orchestrator.run_complete_pipeline(
        time_series_path='./data/time_series.parquet',
        characteristics_path='./output/time_series_characteristics.parquet',
        output_dir='./output'
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

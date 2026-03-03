"""
Foundation Model Forecasting - Google TimesFM
Zero-shot time series forecasting with uncertainty quantification
"""

import pandas as pd
import numpy as np
from typing import Any, Dict, List, Optional
import logging
import yaml
from pathlib import Path
import time
from tqdm import tqdm

# TimesFM imports
try:
    import timesfm
    TIMESFM_AVAILABLE = True
except ImportError:
    TIMESFM_AVAILABLE = False
    logging.debug("TimesFM not available (optional). Install with: pip install timesfm")

from .statistical_models import ForecastResult


class FoundationForecaster:
    """
    Wrapper for Google's TimesFM foundation model.
    Provides zero-shot forecasting with quantile outputs.
    """
    
    def __init__(self, config_path: str = "config/config.yaml", config_override: dict = None):
        """Initialize with configuration."""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        if config_override:
            from utils.parameter_resolver import ParameterResolver
            self.config = ParameterResolver.deep_merge(self.config, config_override)

        self.forecast_config = self.config['forecasting']
        self.logger = logging.getLogger(__name__)
        
        if not TIMESFM_AVAILABLE:
            self.logger.debug("TimesFM not available. Skipping foundation model forecasting.")
            self.model = None
            return
        
        # Extract configuration
        self.horizon = self.forecast_config['horizon']
        self.confidence_levels = self.forecast_config['confidence_levels']
        
        # TimesFM configuration
        timesfm_config = self.forecast_config.get('timesfm', {})
        self.context_length = timesfm_config.get('context_length', 512)
        self.horizon_length = timesfm_config.get('horizon_length', 128)
        
        # Convert confidence levels to quantiles
        self.quantiles = [l/100 for l in self.confidence_levels]
        self.quantiles.append(0.5)  # Add median
        self.quantiles = sorted(list(set(self.quantiles)))
        
        # Initialize model
        try:
            self.model = self._load_model()
            self.logger.info("TimesFM model loaded successfully")
        except Exception as e:
            self.logger.error(f"Failed to load TimesFM model: {e}")
            self.model = None
    
    def _load_model(self):
        """Load pre-trained TimesFM model."""
        # Load the pre-trained model
        # Model sizes: timesfm-1.0-200m (200M parameters)
        tfm = timesfm.TimesFm(
            context_len=self.context_length,
            horizon_len=self.horizon_length,
            input_patch_len=32,
            output_patch_len=128,
            num_layers=20,
            model_dims=1280,
            backend='gpu' if self._check_gpu() else 'cpu'
        )
        
        # Load checkpoint
        tfm.load_from_checkpoint(repo_id="google/timesfm-1.0-200m-pytorch")
        
        return tfm
    
    def _check_gpu(self) -> bool:
        """Check if GPU is available."""
        try:
            import torch
            return torch.cuda.is_available()
        except:
            return False
    
    def forecast_single_series(self,
                              series: np.ndarray,
                              unique_id: str,
                              freq: Optional[int] = None,
                              overrides: Optional[Dict[str, Any]] = None) -> ForecastResult:
        """
        Generate forecast for a single time series using TimesFM.

        Args:
            series: Time series values as numpy array
            unique_id: Series identifier
            freq: Frequency hint (0=high freq, 1=medium, 2=low)
            overrides: Optional user hyperparameter overrides.

        Returns:
            ForecastResult object
        """
        if self.model is None:
            raise RuntimeError("TimesFM model not available")
        
        start_time = time.time()
        
        try:
            # Prepare input
            # TimesFM expects 2D array: [num_series, length]
            input_data = series.reshape(1, -1)
            
            # Generate point forecast
            point_forecast, = self.model.forecast(
                inputs=input_data,
                freq=freq if freq is not None else [0]  # Default to high frequency
            )
            
            # Trim to horizon length
            point_forecast = point_forecast[:self.horizon]
            
            # Generate quantile forecasts
            # TimesFM supports quantile forecasting
            quantiles = {}
            quantiles[0.5] = point_forecast  # Median
            
            for q in self.quantiles:
                if q == 0.5:
                    continue
                
                try:
                    quantile_forecast, = self.model.forecast(
                        inputs=input_data,
                        freq=freq if freq is not None else [0],
                        quantile=q
                    )
                    quantiles[q] = quantile_forecast[:self.horizon]
                except:
                    # If quantile forecasting not supported, use scaled point forecast
                    scale = 1.0 + abs(q - 0.5) * 2
                    if q < 0.5:
                        quantiles[q] = point_forecast / scale
                    else:
                        quantiles[q] = point_forecast * scale
            
            training_time = time.time() - start_time
            
            result = ForecastResult(
                unique_id=unique_id,
                method='TimesFM',
                point_forecast=point_forecast,
                quantiles=quantiles,
                hyperparameters={
                    'context_length': overrides.get('context_length', self.context_length) if overrides else self.context_length,
                    'horizon_length': overrides.get('horizon_length', self.horizon_length) if overrides else self.horizon_length,
                    'freq': freq,
                    'method_family': 'Foundation',
                    'description': 'Google TimesFM foundation model for time series forecasting.',
                    **(({'has_overrides': True, 'overrides_applied': overrides} if overrides else {})),
                },
                training_time=training_time,
                insample_actual=series
            )
            
            self.logger.debug(f"TimesFM forecast complete: {unique_id} ({training_time:.2f}s)")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Failed to forecast {unique_id} with TimesFM: {e}")
            raise
    
    def forecast_multiple_series(self,
                                df: pd.DataFrame,
                                characteristics_df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate TimesFM forecasts for multiple time series.
        
        Args:
            df: DataFrame with time series data
            characteristics_df: DataFrame with characteristics
            
        Returns:
            DataFrame with forecast results
        """
        if self.model is None:
            self.logger.warning("TimesFM model not available")
            return pd.DataFrame()
        
        all_results = []
        
        self.logger.info(f"Generating TimesFM forecasts for {len(characteristics_df)} series")
        
        for _, char_row in tqdm(characteristics_df.iterrows(),
                                total=len(characteristics_df),
                                desc="  Foundation forecasting",
                                unit="series"):
            unique_id = char_row['unique_id']

            # Get series data
            series_data = df[df['unique_id'] == unique_id].sort_values('date')
            series = series_data['y'].values
            
            # Determine frequency hint based on characteristics
            freq = self._get_frequency_hint(char_row.to_dict())
            
            try:
                result = self.forecast_single_series(
                    series=series,
                    unique_id=unique_id,
                    freq=freq
                )
                all_results.append(result)
                
            except Exception as e:
                self.logger.warning(f"Skipping {unique_id}: {e}")
                continue
        
        # Convert to DataFrame
        results_data = [result.to_dict() for result in all_results]
        
        return pd.DataFrame(results_data) if results_data else pd.DataFrame()
    
    def _get_frequency_hint(self, characteristics: Dict) -> int:
        """
        Get frequency hint for TimesFM based on characteristics.
        0 = high frequency (hourly, daily)
        1 = medium frequency (weekly, monthly)
        2 = low frequency (quarterly, yearly)
        """
        # Based on frequency config
        freq_str = self.forecast_config.get('frequency', 'M')
        
        freq_map = {
            'H': 0, 'D': 0, 'B': 0,  # High frequency
            'W': 1, 'M': 1,           # Medium frequency
            'Q': 2, 'Y': 2, 'A': 2    # Low frequency
        }
        
        return freq_map.get(freq_str, 1)


def main():
    """Example usage of foundation forecaster."""
    from db.db import load_table, get_schema, bulk_insert, jsonb_serialize

    config_path = 'config/config.yaml'
    schema = get_schema(config_path)

    # Load data from PostgreSQL
    df = load_table(config_path, f"{schema}.demand_actuals",
                    columns="unique_id, date, COALESCE(corrected_qty, qty) AS y")
    df['date'] = pd.to_datetime(df['date'])
    characteristics_df = load_table(config_path, f"{schema}.time_series_characteristics")

    # Initialize forecaster
    forecaster = FoundationForecaster()

    if forecaster.model is not None:
        # Generate forecasts
        forecasts_df = forecaster.forecast_multiple_series(
            df=df,
            characteristics_df=characteristics_df.head(10)  # Test with first 10
        )

        if not forecasts_df.empty:
            # Save results to PostgreSQL
            cols = list(forecasts_df.columns)
            rows = [
                tuple(jsonb_serialize(v) for v in row)
                for row in forecasts_df.itertuples(index=False, name=None)
            ]
            n = bulk_insert(config_path, f"{schema}.forecast_results", cols, rows, truncate=False)
            print(f"\nTimesFM forecasts saved to {schema}.forecast_results ({n} rows)")
            print(f"Generated {len(forecasts_df)} forecasts")
    else:
        print("\nTimesFM model not available")


if __name__ == "__main__":
    main()

"""
Neural Forecasting Models - Nixtla NeuralForecast
Deep learning methods with quantile regression for probabilistic forecasts
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any
import logging
import yaml
from pathlib import Path
import time

# Nixtla NeuralForecast imports
try:
    from neuralforecast import NeuralForecast
    from neuralforecast.models import (
        NHITS,
        NBEATS,
        PatchTST,
        TFT,
        DeepAR,
        MLP,
        RNN,
        LSTM,
        TCN
    )
    from neuralforecast.losses.pytorch import MQLoss, DistributionLoss
    NEURALFORECAST_AVAILABLE = True
except ImportError:
    NEURALFORECAST_AVAILABLE = False
    logging.debug("NeuralForecast not available (optional). Install with: pip install neuralforecast")

from .statistical_models import ForecastResult


class NeuralForecaster:
    """
    Wrapper for Nixtla NeuralForecast models with hyperparameter tuning.
    Focuses on quantile regression for MEIO requirements.
    """
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """Initialize with configuration."""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.forecast_config = self.config['forecasting']
        self.logger = logging.getLogger(__name__)
        
        if not NEURALFORECAST_AVAILABLE:
            raise ImportError("NeuralForecast is required. Install: pip install neuralforecast")
        
        # Extract configuration
        self.horizon = self.forecast_config['horizon']
        self.frequency = self.forecast_config['frequency']
        self.confidence_levels = self.forecast_config['confidence_levels']
        
        # Convert confidence levels to quantiles
        self.quantiles = [l/100 for l in self.confidence_levels]
        # Add symmetric quantiles
        self.quantiles.extend([0.5])  # Median
        self.quantiles = sorted(list(set(self.quantiles)))
    
    def get_model_hyperparameters(self,
                                  method_name: str,
                                  characteristics: Dict,
                                  overrides: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Get optimized hyperparameters based on data characteristics.

        Args:
            method_name: Name of the neural model
            characteristics: Time series characteristics
            overrides: Optional user overrides to merge on top of defaults.

        Returns:
            Dictionary of hyperparameters
        """
        n_obs = characteristics['n_observations']
        complexity = characteristics['complexity_level']
        has_seasonality = characteristics['has_seasonality']
        
        # Base parameters
        base_params = {
            'h': self.horizon,
            'loss': MQLoss(level=self.quantiles),  # Quantile loss for probabilistic forecasts
            'max_steps': 1000 if n_obs > 200 else 500,
            'val_check_steps': 100,
            'early_stop_patience_steps': 3,
        }
        
        # Method-specific parameters
        if method_name == 'NHITS':
            # N-HiTS: Neural Hierarchical Interpolation for Time Series
            params = {
                **base_params,
                'input_size': min(5 * self.horizon, n_obs // 2),
                'n_blocks': [1, 1, 1] if complexity == 'low' else [3, 3, 3],
                'mlp_units': [[512, 512]] * 3 if complexity == 'high' else [[256, 256]] * 3,
                'n_pool_kernel_size': [2, 2, 1] if has_seasonality else [1, 1, 1],
                'dropout_prob_theta': 0.0,
                'learning_rate': 1e-3,
                'batch_size': 32,
            }
        
        elif method_name == 'NBEATS':
            # N-BEATS: Neural Basis Expansion Analysis
            params = {
                **base_params,
                'input_size': min(5 * self.horizon, n_obs // 2),
                'n_blocks': [3, 3, 3] if complexity == 'high' else [2, 2, 2],
                'mlp_units': [[512, 512]] * 3 if complexity == 'high' else [[256, 256]] * 3,
                'stack_types': ['identity', 'trend', 'seasonality'] if has_seasonality else ['identity', 'trend'],
                'learning_rate': 1e-3,
                'batch_size': 32,
            }
        
        elif method_name == 'PatchTST':
            # PatchTST: Patch Time Series Transformer
            params = {
                **base_params,
                'input_size': min(10 * self.horizon, n_obs // 2),
                'patch_len': 16,
                'stride': 8,
                'n_layers': 3 if complexity == 'high' else 2,
                'd_model': 128 if complexity == 'high' else 64,
                'n_heads': 8 if complexity == 'high' else 4,
                'learning_rate': 1e-4,
                'batch_size': 32,
            }
        
        elif method_name == 'TFT':
            # Temporal Fusion Transformer
            params = {
                **base_params,
                'input_size': min(5 * self.horizon, n_obs // 2),
                'hidden_size': 128 if complexity == 'high' else 64,
                'dropout': 0.1,
                'num_attention_heads': 4,
                'learning_rate': 1e-3,
                'batch_size': 32,
            }
        
        elif method_name == 'DeepAR':
            # DeepAR: Probabilistic forecasting with RNN
            params = {
                **base_params,
                'input_size': min(5 * self.horizon, n_obs // 2),
                'encoder_hidden_size': 128 if complexity == 'high' else 64,
                'encoder_n_layers': 3 if complexity == 'high' else 2,
                'decoder_hidden_size': 128 if complexity == 'high' else 64,
                'decoder_n_layers': 3 if complexity == 'high' else 2,
                'learning_rate': 1e-3,
                'batch_size': 32,
            }
        
        else:
            # Default for other models
            params = {
                **base_params,
                'input_size': min(5 * self.horizon, n_obs // 2),
                'learning_rate': 1e-3,
                'batch_size': 32,
            }
        # Apply user overrides — merge numeric/int/float params
        if overrides:
            _overridable = ('input_size', 'max_steps', 'learning_rate', 'batch_size',
                            'dropout', 'dropout_prob_theta', 'hidden_size',
                            'encoder_hidden_size', 'encoder_n_layers',
                            'decoder_hidden_size', 'decoder_n_layers',
                            'n_layers', 'd_model', 'n_heads', 'patch_len', 'stride',
                            'num_attention_heads', 'val_check_steps', 'early_stop_patience_steps')
            for k in _overridable:
                if k in overrides:
                    params[k] = type(params.get(k, overrides[k]))(overrides[k]) if k in params else overrides[k]
            params['has_overrides'] = True
            params['overrides_applied'] = overrides

        return params

    def get_model_instance(self,
                          method_name: str,
                          characteristics: Dict) -> Any:
        """
        Get configured neural model instance.
        
        Args:
            method_name: Name of the neural model
            characteristics: Time series characteristics
            
        Returns:
            Configured model instance
        """
        params = self.get_model_hyperparameters(method_name, characteristics)
        
        model_classes = {
            'NHITS': NHITS,
            'NBEATS': NBEATS,
            'PatchTST': PatchTST,
            'TFT': TFT,
            'DeepAR': DeepAR,
            'MLP': MLP,
            'LSTM': LSTM,
            'RNN': RNN,
            'TCN': TCN
        }
        
        if method_name not in model_classes:
            raise ValueError(f"Unknown neural method: {method_name}")
        
        model_class = model_classes[method_name]
        
        try:
            return model_class(**params)
        except Exception as e:
            self.logger.warning(f"Failed to initialize {method_name} with params: {e}")
            # Fallback to simpler config
            params['input_size'] = self.horizon * 2
            return model_class(**params)
    
    def forecast_single_series(self,
                              df: pd.DataFrame,
                              unique_id: str,
                              methods: List[str],
                              characteristics: Dict,
                              overrides_map: Dict[str, Dict] = None) -> List[ForecastResult]:
        """
        Generate neural forecasts for a single time series.

        Args:
            df: DataFrame with columns [unique_id, ds, y]
            unique_id: Series identifier
            methods: List of neural methods to use
            characteristics: Time series characteristics
            overrides_map: Optional {method: {param: value}} overrides.

        Returns:
            List of ForecastResult objects
        """
        results = []
        
        # Filter to specific series
        series_df = df[df['unique_id'] == unique_id].copy()
        series_df = series_df.rename(columns={'date': 'ds'})
        
        # Check if sufficient data for neural models
        if characteristics['n_observations'] < 50:
            self.logger.warning(f"Insufficient data for neural models: {unique_id}")
            return results
        
        for method in methods:
            try:
                start_time = time.time()
                
                # Get model instance
                model = self.get_model_instance(method, characteristics)
                
                # Create NeuralForecast instance
                nf = NeuralForecast(
                    models=[model],
                    freq=self.frequency
                )
                
                # Fit and predict
                nf.fit(df=series_df)
                forecast_df = nf.predict()
                
                training_time = time.time() - start_time
                
                # Extract forecasts
                # NeuralForecast outputs columns: {model}-median, {model}-lo-X, {model}-hi-X
                point_forecast = forecast_df[f'{method}-median'].values if f'{method}-median' in forecast_df.columns else forecast_df[method].values
                
                # Extract quantiles
                quantiles = {0.5: point_forecast}  # Median
                
                for q in self.quantiles:
                    if q == 0.5:
                        continue
                    q_col = f'{method}-q-{q}'
                    if q_col in forecast_df.columns:
                        quantiles[q] = forecast_df[q_col].values
                    else:
                        # Try prediction interval format
                        level = int(abs(2 * (q - 0.5)) * 100)
                        if q < 0.5:
                            col = f'{method}-lo-{level}'
                        else:
                            col = f'{method}-hi-{level}'
                        
                        if col in forecast_df.columns:
                            quantiles[q] = forecast_df[col].values
                
                # Get hyperparameters (with overrides merged)
                method_ovr = (overrides_map or {}).get(method, None)
                hyperparams = self.get_model_hyperparameters(method, characteristics, overrides=method_ovr)
                
                # Create result
                result = ForecastResult(
                    unique_id=unique_id,
                    method=method,
                    point_forecast=point_forecast,
                    quantiles=quantiles,
                    hyperparameters=hyperparams,
                    training_time=training_time,
                    insample_actual=series_df['y'].values
                )
                
                results.append(result)
                
                self.logger.debug(f"Neural forecast complete: {unique_id} - {method} ({training_time:.2f}s)")
                
            except Exception as e:
                self.logger.warning(f"Failed to forecast {unique_id} with {method}: {str(e)}")
                continue
        
        return results
    
    def forecast_multiple_series(self,
                                df: pd.DataFrame,
                                characteristics_df: pd.DataFrame,
                                overrides_map: dict = None) -> pd.DataFrame:
        """
        Generate neural forecasts for multiple time series.

        Args:
            df: DataFrame with time series data
            characteristics_df: DataFrame with characteristics and recommended methods
            overrides_map: Optional {unique_id: {method: {param: value}}} overrides.

        Returns:
            DataFrame with all forecast results
        """
        all_results = []
        
        # Filter to series with sufficient data for neural models
        valid_chars = characteristics_df[
            characteristics_df['sufficient_for_deep_learning'] == True
        ]
        
        self.logger.info(f"Generating neural forecasts for {len(valid_chars)} series")
        
        for _, char_row in valid_chars.iterrows():
            unique_id = char_row['unique_id']
            
            # Get neural methods from recommended
            all_methods = char_row['recommended_methods']
            neural_methods = [m for m in all_methods if m in ['NHITS', 'NBEATS', 'PatchTST', 'TFT', 'DeepAR']]
            
            if not neural_methods:
                continue
            
            characteristics = char_row.to_dict()
            
            # Generate forecasts
            series_overrides = (overrides_map or {}).get(unique_id, None)
            results = self.forecast_single_series(
                df=df,
                unique_id=unique_id,
                methods=neural_methods,
                characteristics=characteristics,
                overrides_map=series_overrides,
            )
            
            all_results.extend(results)
        
        # Convert to DataFrame
        results_data = [result.to_dict() for result in all_results]
        
        return pd.DataFrame(results_data) if results_data else pd.DataFrame()


def main():
    """Example usage of neural forecaster."""
    from db.db import load_table, get_schema, bulk_insert, jsonb_serialize

    config_path = 'config/config.yaml'
    schema = get_schema(config_path)

    # Load data from PostgreSQL
    df = load_table(config_path, f"{schema}.demand_actuals",
                    columns="unique_id, date, COALESCE(corrected_qty, qty) AS y")
    df['date'] = pd.to_datetime(df['date'])
    characteristics_df = load_table(config_path, f"{schema}.time_series_characteristics")

    # Initialize forecaster
    forecaster = NeuralForecaster()

    # Generate forecasts (only for series with sufficient data)
    forecasts_df = forecaster.forecast_multiple_series(
        df=df,
        characteristics_df=characteristics_df
    )

    if not forecasts_df.empty:
        # Save results to PostgreSQL
        cols = list(forecasts_df.columns)
        rows = [
            tuple(jsonb_serialize(v) for v in row)
            for row in forecasts_df.itertuples(index=False, name=None)
        ]
        n = bulk_insert(config_path, f"{schema}.forecast_results", cols, rows, truncate=False)
        print(f"\nNeural forecasts saved to {schema}.forecast_results ({n} rows)")
        print(f"Generated {len(forecasts_df)} forecasts")
    else:
        print("\nNo series with sufficient data for neural models")


if __name__ == "__main__":
    main()

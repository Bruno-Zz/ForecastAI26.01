"""
Statistical Forecasting Models - Nixtla StatsForecast
Fast statistical methods with native prediction intervals
"""

import os
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
import logging
from dataclasses import dataclass
import yaml
from pathlib import Path
from tqdm import tqdm

# Nixtla imports
try:
    from statsforecast import StatsForecast
    from statsforecast.models import (
        ARIMA,
        AutoARIMA,
        AutoETS,
        AutoTheta,
        AutoCES,
        MSTL,
        CrostonOptimized,
        ADIDA,
        IMAPA,
        SeasonalNaive,
        HistoricAverage,
        Naive,
        SeasonalWindowAverage,
        TSB  # Teunter-Syntetos-Babai for intermittent
    )
    from statsforecast.utils import ConformalIntervals
    STATSFORECAST_AVAILABLE = True
except ImportError:
    STATSFORECAST_AVAILABLE = False
    logging.warning("StatsForecast not available. Install with: pip install statsforecast")


@dataclass
class ForecastResult:
    """Container for forecast results with probabilistic outputs."""
    unique_id: str
    method: str
    point_forecast: np.ndarray  # Mean forecast
    quantiles: Dict[float, np.ndarray]  # Quantile forecasts for MEIO
    fitted_values: Optional[np.ndarray] = None
    residuals: Optional[np.ndarray] = None
    
    # Model info
    hyperparameters: Optional[Dict[str, Any]] = None
    training_time: Optional[float] = None
    
    # For evaluation
    insample_actual: Optional[np.ndarray] = None

    # ML internal validation metrics (populated by MLForecaster only)
    internal_val_metrics: Optional[Dict[str, Any]] = None
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for storage."""
        return {
            'unique_id': self.unique_id,
            'method': self.method,
            'point_forecast': self.point_forecast.tolist(),
            'quantiles': {str(k): v.tolist() for k, v in self.quantiles.items()},
            'hyperparameters': self.hyperparameters,
            'training_time': self.training_time
        }


class StatisticalForecaster:
    """
    Wrapper for Nixtla StatsForecast models with automatic hyperparameter tuning.
    """
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """Initialize with configuration."""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.forecast_config = self.config['forecasting']
        self.logger = logging.getLogger(__name__)
        
        if not STATSFORECAST_AVAILABLE:
            raise ImportError("StatsForecast is required. Install: pip install statsforecast")
        
        # Extract configuration
        self.horizon = self.forecast_config['horizon']
        self.confidence_levels = self.forecast_config['confidence_levels']

        # Translate config frequency shorthand to the pandas 2.x offset aliases
        # that StatsForecast / utilsforecast expects.  'M' was deprecated in
        # pandas 2.2 in favour of 'ME' (month-end); 'Q' → 'QE', 'Y'/'A' → 'YE'.
        _FREQ_ALIAS = {'M': 'ME', 'Q': 'QE', 'Y': 'YE', 'A': 'YE'}
        raw_freq = self.forecast_config['frequency']
        self.frequency = _FREQ_ALIAS.get(raw_freq, raw_freq)
        
    def get_model_instance(self,
                          method_name: str,
                          characteristics: Dict,
                          season_length: Optional[int] = None,
                          overrides: Optional[Dict[str, Any]] = None) -> Tuple[Any, Dict[str, Any]]:
        """
        Get configured model instance and its hyperparameters.

        Args:
            method_name: Name of the forecasting method
            characteristics: Time series characteristics for hyperparameter tuning
            season_length: Seasonal period length
            overrides: Optional dict of user-edited hyperparameter values to merge.

        Returns:
            Tuple of (configured model instance, hyperparameters dict)
        """
        ovr = overrides or {}

        # Determine season length from characteristics if not provided
        if season_length is None:
            if characteristics.get('has_seasonality'):
                seasonal_periods = characteristics.get('seasonal_periods', [])
                season_length = seasonal_periods[0] if seasonal_periods else 1
            else:
                season_length = 1

        # Clamp season_length to at least 1 — apply override if present
        season_length = max(1, int(ovr.get('season_length', season_length)))

        # For MSTL the trend forecaster must be non-seasonal (season_length=1).
        mstl_trend = AutoARIMA(season_length=1)

        # Conformal prediction intervals for intermittent-demand models
        n_obs = characteristics.get('n_observations', 0)
        _min_for_conformal = 2 * self.horizon + 1  # n_windows=2
        _conformal = ConformalIntervals(n_windows=2, h=self.horizon) if n_obs >= _min_for_conformal else None

        # Common hyperparameters shared by all methods
        _base_hyper = {
            'horizon': self.horizon,
            'frequency': self.frequency,
            'confidence_levels': self.confidence_levels,
            'n_observations': n_obs,
        }

        approximation = ovr.get('approximation',
                                True if characteristics.get('n_observations', 0) > 150 else False)
        decomp_type = ovr.get('decomposition_type',
                              'multiplicative' if characteristics.get('complexity_level') == 'high' else 'additive')
        conformal_enabled = _conformal is not None
        conformal_info = {
            'conformal_intervals': conformal_enabled,
            'conformal_n_windows': 2 if conformal_enabled else None,
            'conformal_h': self.horizon if conformal_enabled else None,
        }

        # ── TSB-specific overridable params ──
        tsb_alpha_d = ovr.get('alpha_d', 0.1)
        tsb_alpha_p = ovr.get('alpha_p', 0.1)

        # ── ETS model override ──
        ets_model = ovr.get('model', 'ZZZ')

        # ── SeasonalWindowAverage window_size override ──
        swa_window = int(ovr.get('window_size', 2))

        # Mark overrides in hyperparams dict for UI display
        _ovr_flag = {'has_overrides': True, 'overrides_applied': ovr} if ovr else {}

        # ── ARIMA: switch to manual ARIMA when user overrides p/d/q ──
        _arima_has_order = any(k in ovr for k in ('p', 'd', 'q', 'P', 'D', 'Q'))
        _arima_order = (int(ovr.get('p', 0)), int(ovr.get('d', 0)), int(ovr.get('q', 0)))
        _arima_seasonal = (int(ovr.get('P', 0)), int(ovr.get('D', 0)), int(ovr.get('Q', 0)))
        _arima_include_mean = ovr.get('include_mean', True)
        _arima_include_drift = ovr.get('include_drift', False)
        _arima_method = ovr.get('method', 'CSS-ML')
        # AutoARIMA search params (when not forcing manual orders)
        _auto_arima_kwargs = {}
        for _aak in ('max_p', 'max_q', 'max_P', 'max_Q', 'max_order', 'max_d', 'max_D',
                      'start_p', 'start_q', 'start_P', 'start_Q',
                      'stationary', 'seasonal', 'stepwise', 'allowdrift', 'allowmean'):
            if _aak in ovr:
                _auto_arima_kwargs[_aak] = ovr[_aak]

        # ── ETS: pass through damped/phi/model overrides ──
        _ets_damped = ovr.get('damped', None)
        _ets_phi = ovr.get('phi', None)

        # ── Theta: model override (Theta, OptimizedTheta, DynamicTheta...) ──
        _theta_model = ovr.get('model', None)  # e.g. 'OptimizedTheta'

        # ── CES: model override (N=simple, P=partial, F=full, Z=auto) ──
        _ces_model = ovr.get('model', None)

        # Model mapping with hyperparameter configuration
        model_configs = {
            'AutoARIMA': lambda: (
                ARIMA(order=_arima_order,
                      seasonal_order=_arima_seasonal,
                      season_length=season_length,
                      include_mean=_arima_include_mean,
                      include_drift=_arima_include_drift,
                      method=_arima_method)
                if _arima_has_order else
                AutoARIMA(season_length=season_length, approximation=approximation,
                          **_auto_arima_kwargs),
                {**_base_hyper, **_ovr_flag, 'season_length': season_length,
                 'approximation': approximation,
                 **_auto_arima_kwargs,
                 'method_family': 'ARIMA',
                 'description': ('Manual ARIMA({},{},{})({},{},{}) with user-specified orders.'
                                 .format(*_arima_order, *_arima_seasonal)
                                 if _arima_has_order else
                                 'Automatic ARIMA model selection via Hyndman-Khandakar algorithm. '
                                 'Searches over (p,d,q)(P,D,Q)m orders and selects by AICc.'),
                 **(dict(p=_arima_order[0], d=_arima_order[1], q=_arima_order[2],
                         P=_arima_seasonal[0], D=_arima_seasonal[1], Q=_arima_seasonal[2],
                         include_mean=_arima_include_mean, include_drift=_arima_include_drift)
                    if _arima_has_order else {})}
            ),
            'AutoETS': lambda: (
                AutoETS(season_length=season_length, model=ets_model,
                        damped=_ets_damped, phi=_ets_phi),
                {**_base_hyper, **_ovr_flag, 'season_length': season_length,
                 'model': ets_model,
                 **({} if _ets_damped is None else {'damped': _ets_damped}),
                 **({} if _ets_phi is None else {'phi': _ets_phi}),
                 'description': 'Automatic Exponential Smoothing (ETS) model selection. '
                                f'{ets_model} = auto-select Error, Trend, and Seasonal components. '
                                'Optimizes smoothing parameters (alpha, beta, gamma, phi) via MLE.',
                 'method_family': 'ETS',
                 'selection_criterion': 'AICc'}
            ),
            'AutoTheta': lambda: (
                AutoTheta(season_length=season_length, decomposition_type=decomp_type,
                          **({'model': _theta_model} if _theta_model else {})),
                {**_base_hyper, **_ovr_flag, 'season_length': season_length,
                 'decomposition_type': decomp_type,
                 **({'model': _theta_model} if _theta_model else {}),
                 'description': f'Automatic Theta method with {decomp_type} decomposition. '
                                'Decomposes series into trend and seasonality, applies Theta lines.',
                 'method_family': 'Theta'}
            ),
            'AutoCES': lambda: (
                AutoCES(season_length=season_length,
                        **({'model': _ces_model} if _ces_model else {})),
                {**_base_hyper, **_ovr_flag, 'season_length': season_length,
                 **({'model': _ces_model} if _ces_model else {}),
                 'description': 'Complex Exponential Smoothing. Auto-selects between '
                                'simple (N), partial (P), full (F) seasonal models.',
                 'method_family': 'CES'}
            ),
            'MSTL': lambda: (
                MSTL(season_length=season_length, trend_forecaster=mstl_trend),
                {**_base_hyper, **_ovr_flag, 'season_length': season_length,
                 'trend_forecaster': 'AutoARIMA(season_length=1)',
                 'description': 'Multiple Seasonal-Trend decomposition using LOESS. '
                                'Decomposes into trend + seasonal + remainder, then forecasts '
                                'trend with AutoARIMA(season_length=1).',
                 'method_family': 'Decomposition'}
            ),
            'CrostonOptimized': lambda: (
                CrostonOptimized(prediction_intervals=_conformal),
                {**_base_hyper, **conformal_info, **_ovr_flag,
                 'description': 'Optimized Croston method for intermittent demand. '
                                'Separately models demand size and inter-arrival intervals. '
                                'Optimizes smoothing parameter via MSE.',
                 'method_family': 'Intermittent'}
            ),
            'ADIDA': lambda: (
                ADIDA(prediction_intervals=_conformal),
                {**_base_hyper, **conformal_info, **_ovr_flag,
                 'description': 'Aggregate-Disaggregate Intermittent Demand Approach. '
                                'Aggregates demand to remove intermittency, forecasts, then disaggregates.',
                 'method_family': 'Intermittent'}
            ),
            'IMAPA': lambda: (
                IMAPA(prediction_intervals=_conformal),
                {**_base_hyper, **conformal_info, **_ovr_flag,
                 'description': 'Intermittent Multiple Aggregation Prediction Algorithm. '
                                'Aggregates at multiple temporal levels and combines forecasts.',
                 'method_family': 'Intermittent'}
            ),
            'TSB': lambda: (
                TSB(alpha_d=tsb_alpha_d, alpha_p=tsb_alpha_p, prediction_intervals=_conformal),
                {**_base_hyper, **conformal_info, **_ovr_flag,
                 'alpha_d': tsb_alpha_d,
                 'alpha_p': tsb_alpha_p,
                 'description': f'Teunter-Syntetos-Babai method for intermittent demand. '
                                f'Smoothes demand probability (alpha_p={tsb_alpha_p}) and demand size (alpha_d={tsb_alpha_d}) separately.',
                 'method_family': 'Intermittent'}
            ),
            'SeasonalNaive': lambda: (
                SeasonalNaive(season_length=season_length),
                {**_base_hyper, **_ovr_flag, 'season_length': season_length,
                 'description': f'Repeats last seasonal cycle (season_length={season_length}). '
                                'Forecast for h steps ahead equals the value from h-season_length steps ago.',
                 'method_family': 'Naive'}
            ),
            'HistoricAverage': lambda: (
                HistoricAverage(),
                {**_base_hyper, **_ovr_flag,
                 'description': 'Flat forecast equal to the mean of all historical observations.',
                 'method_family': 'Naive'}
            ),
            'Naive': lambda: (
                Naive(),
                {**_base_hyper, **_ovr_flag,
                 'description': 'Random walk forecast — repeats the last observed value for all horizons.',
                 'method_family': 'Naive'}
            ),
            'SeasonalWindowAverage': lambda: (
                SeasonalWindowAverage(season_length=season_length, window_size=swa_window),
                {**_base_hyper, **_ovr_flag, 'season_length': season_length, 'window_size': swa_window,
                 'description': f'Average of last {swa_window} seasonal cycles (season_length={season_length}).',
                 'method_family': 'Naive'}
            ),
        }

        if method_name not in model_configs:
            raise ValueError(f"Unknown method: {method_name}")

        return model_configs[method_name]()
    
    def forecast_single_series(self,
                              df: pd.DataFrame,
                              unique_id: str,
                              methods: List[str],
                              characteristics: Dict,
                              overrides_map: Optional[Dict[str, Dict[str, Any]]] = None) -> List[ForecastResult]:
        """
        Generate forecasts for a single time series using multiple methods.

        Args:
            df: DataFrame with columns [unique_id, ds, y]
            unique_id: Series identifier
            methods: List of method names to use
            characteristics: Time series characteristics
            overrides_map: Optional {method: {param: value}} overrides for this series.

        Returns:
            List of ForecastResult objects
        """
        import time
        
        results = []
        
        # Filter to specific series
        series_df = df[df['unique_id'] == unique_id].copy()
        series_df = series_df.rename(columns={'date': 'ds'})
        
        # Ensure required columns
        if 'ds' not in series_df.columns or 'y' not in series_df.columns:
            raise ValueError("DataFrame must have columns: unique_id, ds, y")
        
        # Determine season length
        season_length = None
        if characteristics.get('has_seasonality'):
            seasonal_periods = characteristics.get('seasonal_periods', [])
            if seasonal_periods:
                # Map frequency to season length
                freq_map = {'D': 7, 'W': 52, 'M': 12, 'ME': 12, 'Q': 4, 'QE': 4, 'Y': 1, 'YE': 1, 'A': 1}
                season_length = seasonal_periods[0] if seasonal_periods else freq_map.get(self.frequency, 1)

        if season_length is None:
            freq_map = {'D': 7, 'W': 52, 'M': 12, 'ME': 12, 'Q': 4, 'QE': 4, 'Y': 1, 'YE': 1, 'A': 1}
            season_length = freq_map.get(self.frequency, 1)
        
        n_obs = len(series_df)

        for method in methods:
            # Pre-flight guard: SeasonalNaive and SeasonalWindowAverage require
            # at least season_length observations; StatsForecast raises
            # "number sections must be larger than 0" otherwise.
            if method in ('SeasonalNaive', 'SeasonalWindowAverage') and n_obs < season_length:
                self.logger.debug(
                    f"{unique_id}: skipping {method} — only {n_obs} obs, need >= {season_length}"
                )
                continue

            # HistoricAverage also raises the same error when n_obs < 2
            if method == 'HistoricAverage' and n_obs < 2:
                self.logger.debug(
                    f"{unique_id}: skipping HistoricAverage — only {n_obs} obs, need >= 2"
                )
                continue

            # MSTL requires at least 2 full seasonal cycles to decompose;
            # with fewer observations the internal np.min on an empty array
            # raises "zero-size array to reduction operation minimum".
            if method == 'MSTL' and n_obs < 2 * season_length + 1:
                self.logger.debug(
                    f"{unique_id}: skipping MSTL — only {n_obs} obs, need >= {2 * season_length + 1}"
                )
                continue

            # AutoETS, AutoARIMA, AutoTheta, AutoCES need enough data to
            # fit; models raise "tiny datasets" or similar errors when there
            # are fewer than ~2*season_length observations.
            if method in ('AutoETS', 'AutoARIMA', 'AutoTheta', 'AutoCES') and n_obs < max(2 * season_length, 10):
                self.logger.debug(
                    f"{unique_id}: skipping {method} — only {n_obs} obs, need >= {max(2 * season_length, 10)}"
                )
                continue

            try:
                start_time = time.time()

                # Get model instance AND its hyperparameters
                method_ovr = (overrides_map or {}).get(method, {})
                model, method_hyperparams = self.get_model_instance(
                    method, characteristics, season_length, overrides=method_ovr
                )

                # Create StatsForecast instance
                sf = StatsForecast(
                    models=[model],
                    freq=self.frequency,
                    n_jobs=1  # Single job per series (parallelization at series level)
                )

                # Intermittent-demand models (CrostonOptimized, ADIDA, IMAPA, TSB)
                # don't produce native prediction intervals.  They can only produce
                # PIs via ConformalIntervals, which needs enough observations.
                # When the series is too short we call forecast() WITHOUT level=
                # so the model produces point forecasts only.
                _INTERMITTENT_MODELS = {'CrostonOptimized', 'ADIDA', 'IMAPA', 'TSB'}
                _conformal_available = n_obs >= 2 * self.horizon + 1
                _skip_level = method in _INTERMITTENT_MODELS and not _conformal_available

                # Generate forecast with prediction intervals
                forecast_kwargs = dict(df=series_df, h=self.horizon)
                if not _skip_level:
                    forecast_kwargs['level'] = self.confidence_levels
                forecast_df = sf.forecast(**forecast_kwargs)

                training_time = time.time() - start_time

                # Extract point forecast (mean)
                point_forecast = forecast_df[method].values

                # Extract quantiles from prediction intervals
                quantiles = {}
                for level in self.confidence_levels:
                    # StatsForecast uses lo-{level} and hi-{level} columns
                    lo_col = f'{method}-lo-{level}'
                    hi_col = f'{method}-hi-{level}'

                    if lo_col in forecast_df.columns and hi_col in forecast_df.columns:
                        # Calculate quantile from prediction interval
                        # lo corresponds to (100-level)/2 percentile
                        # hi corresponds to 100-(100-level)/2 percentile
                        lower_q = (100 - level) / 200
                        upper_q = 1 - lower_q

                        quantiles[lower_q] = forecast_df[lo_col].values
                        quantiles[upper_q] = forecast_df[hi_col].values

                # Add median (point forecast)
                quantiles[0.5] = point_forecast

                # ---- Extract fitted model parameters ----
                # After fitting, some StatsForecast models expose their estimated
                # parameters (e.g. ARIMA orders, ETS smoothing weights).
                try:
                    fitted_model_obj = sf.fitted_[0, 0]  # first model, first series
                    if hasattr(fitted_model_obj, 'model_'):
                        _inner = fitted_model_obj.model_
                        _fitted = getattr(_inner, '__dict__', {})

                        # Generic extraction — JSON-serialisable primitives
                        for k, v in _fitted.items():
                            if k.startswith('_'):
                                continue
                            if isinstance(v, (int, float, bool, str)):
                                method_hyperparams[f'fitted_{k}'] = v
                            elif isinstance(v, (np.integer,)):
                                method_hyperparams[f'fitted_{k}'] = int(v)
                            elif isinstance(v, (np.floating,)):
                                method_hyperparams[f'fitted_{k}'] = float(v)
                            elif isinstance(v, np.ndarray) and v.size <= 10:
                                method_hyperparams[f'fitted_{k}'] = v.tolist()

                        # ── ARIMA-specific: extract (p,d,q)(P,D,Q,s) ──
                        if method in ('AutoARIMA',):
                            arma = getattr(_inner, 'arma', None)
                            if arma is not None and hasattr(arma, '__len__') and len(arma) >= 7:
                                method_hyperparams['fitted_p'] = int(arma[0])
                                method_hyperparams['fitted_q'] = int(arma[1])
                                method_hyperparams['fitted_P'] = int(arma[2])
                                method_hyperparams['fitted_Q'] = int(arma[3])
                                method_hyperparams['fitted_s'] = int(arma[4])
                                method_hyperparams['fitted_d'] = int(arma[5])
                                method_hyperparams['fitted_D'] = int(arma[6])
                            # Fallback: look for individual attributes
                            for attr in ('p', 'd', 'q', 'P', 'D', 'Q', 's'):
                                if f'fitted_{attr}' not in method_hyperparams:
                                    val = getattr(_inner, attr, None)
                                    if val is not None:
                                        method_hyperparams[f'fitted_{attr}'] = int(val) if isinstance(val, (int, np.integer)) else val
                            # Also grab AIC / AICc / BIC if available
                            for attr in ('aic', 'aicc', 'bic'):
                                val = getattr(_inner, attr, None)
                                if val is not None and isinstance(val, (int, float, np.floating)):
                                    method_hyperparams[f'fitted_{attr}'] = round(float(val), 4)

                        # ── ETS-specific: extract alpha, beta, gamma, phi ──
                        if method in ('AutoETS',):
                            for attr in ('alpha', 'beta', 'gamma', 'phi', 'sigma2'):
                                val = getattr(_inner, attr, None)
                                if val is not None and isinstance(val, (int, float, np.integer, np.floating)):
                                    method_hyperparams[f'fitted_{attr}'] = round(float(val), 6)
                            # ETS model type string (e.g. 'MAM', 'ANA')
                            for attr in ('model', 'errortype', 'trendtype', 'seasontype', 'damped'):
                                val = getattr(_inner, attr, None)
                                if val is not None:
                                    if isinstance(val, str):
                                        method_hyperparams[f'fitted_{attr}'] = val
                                    elif isinstance(val, bool):
                                        method_hyperparams[f'fitted_{attr}'] = val
                            for attr in ('aic', 'aicc', 'bic'):
                                val = getattr(_inner, attr, None)
                                if val is not None and isinstance(val, (int, float, np.floating)):
                                    method_hyperparams[f'fitted_{attr}'] = round(float(val), 4)

                        # ── Theta-specific ──
                        if method in ('AutoTheta',):
                            for attr in ('theta', 'alpha', 'drift'):
                                val = getattr(_inner, attr, None)
                                if val is not None and isinstance(val, (int, float, np.integer, np.floating)):
                                    method_hyperparams[f'fitted_{attr}'] = round(float(val), 6)

                        # ── CES-specific ──
                        if method in ('AutoCES',):
                            for attr in ('alpha', 'beta', 'model'):
                                val = getattr(_inner, attr, None)
                                if val is not None:
                                    if isinstance(val, (int, float, np.integer, np.floating)):
                                        method_hyperparams[f'fitted_{attr}'] = round(float(val), 6)
                                    elif isinstance(val, str):
                                        method_hyperparams[f'fitted_{attr}'] = val

                except Exception:
                    pass  # Not all models expose fitted internals

                method_hyperparams['training_time_seconds'] = round(training_time, 4)
                method_hyperparams['prediction_intervals_available'] = not _skip_level

                # Get fitted values if available
                fitted_values = None
                residuals = None
                try:
                    fitted_df = sf.forecast_fitted_values()
                    if method in fitted_df.columns:
                        fitted_values = fitted_df[method].values
                        residuals = series_df['y'].values - fitted_values
                except:
                    pass

                # Create result
                result = ForecastResult(
                    unique_id=unique_id,
                    method=method,
                    point_forecast=point_forecast,
                    quantiles=quantiles,
                    fitted_values=fitted_values,
                    residuals=residuals,
                    hyperparameters=method_hyperparams,
                    training_time=training_time,
                    insample_actual=series_df['y'].values
                )
                
                results.append(result)
                
                self.logger.debug(f"Forecast complete: {unique_id} - {method} ({training_time:.2f}s)")
                
            except Exception as e:
                # Downgrade "Unknown method" to debug — expected when non-statistical
                # methods (TimesFM, LightGBM…) are passed in without pre-filtering.
                if "Unknown method" in str(e):
                    self.logger.debug(f"{unique_id}: skipping {method} — {e}")
                else:
                    self.logger.warning(f"Failed to forecast {unique_id} with {method}: {str(e)}")
                continue
        
        return results
    
    def forecast_multiple_series(self,
                                df: pd.DataFrame,
                                characteristics_df: pd.DataFrame,
                                parallel: bool = False,
                                overrides_map: dict = None) -> pd.DataFrame:
        """
        Generate forecasts for a batch of time series sequentially.
        Parallelism across series is handled externally by the Dask orchestrator,
        which submits batches of this method as separate tasks.

        Args:
            df: DataFrame with time series data
            characteristics_df: DataFrame with characteristics and recommended methods
            parallel: Unused – kept for API compatibility
            overrides_map: Optional {unique_id: {method: {param: value}}} overrides.

        Returns:
            DataFrame with all forecast results
        """
        all_results = []

        # Methods this forecaster knows how to handle
        KNOWN_METHODS = {
            'AutoARIMA', 'AutoETS', 'AutoTheta', 'AutoCES', 'MSTL',
            'CrostonOptimized', 'ADIDA', 'IMAPA', 'TSB',
            'SeasonalNaive', 'HistoricAverage', 'Naive', 'SeasonalWindowAverage'
        }

        for _, char_row in tqdm(characteristics_df.iterrows(),
                                total=len(characteristics_df),
                                desc="  Statistical forecasting",
                                unit="series"):
            unique_id = char_row['unique_id']
            all_methods = char_row['recommended_methods']

            # Filter to only methods this statistical forecaster supports
            methods = [m for m in all_methods if m in KNOWN_METHODS]
            skipped = [m for m in all_methods if m not in KNOWN_METHODS]
            if skipped:
                self.logger.debug(
                    f"{unique_id}: skipping non-statistical methods: {skipped}"
                )
            if not methods:
                self.logger.warning(
                    f"{unique_id}: no supported statistical methods found in {all_methods}, skipping"
                )
                continue

            # Convert characteristics to dict
            characteristics = char_row.to_dict()

            # Extract per-series overrides (if any)
            series_overrides = (overrides_map or {}).get(unique_id, None)

            # Generate forecasts
            results = self.forecast_single_series(
                df=df,
                unique_id=unique_id,
                methods=methods,
                characteristics=characteristics,
                overrides_map=series_overrides,
            )

            all_results.extend(results)

        # Convert to DataFrame
        results_data = [result.to_dict() for result in all_results]
        return pd.DataFrame(results_data)
    
    def generate_features(self, 
                         series: pd.Series, 
                         characteristics: Dict) -> pd.DataFrame:
        """
        Generate features for ML models based on time series characteristics.
        
        Args:
            series: Time series data
            characteristics: Time series characteristics
            
        Returns:
            DataFrame with engineered features
        """
        features = pd.DataFrame(index=series.index)
        
        # Lagged features
        if characteristics.get('has_seasonality'):
            seasonal_periods = characteristics.get('seasonal_periods', [])
            for period in seasonal_periods[:3]:  # Top 3 seasonal periods
                features[f'lag_{period}'] = series.shift(period)
                # Rolling statistics at seasonal lag
                features[f'rolling_mean_{period}'] = series.rolling(window=period).mean()
                features[f'rolling_std_{period}'] = series.rolling(window=period).std()
        
        # Recent lags
        for lag in [1, 2, 3, 7, 14, 30]:
            if lag < len(series):
                features[f'lag_{lag}'] = series.shift(lag)
        
        # Rolling features
        for window in [7, 14, 30]:
            if window < len(series):
                features[f'rolling_mean_{window}'] = series.rolling(window=window).mean()
                features[f'rolling_std_{window}'] = series.rolling(window=window).std()
                features[f'rolling_min_{window}'] = series.rolling(window=window).min()
                features[f'rolling_max_{window}'] = series.rolling(window=window).max()
        
        # Trend features
        if characteristics.get('has_trend'):
            features['time_idx'] = np.arange(len(series))
            features['time_idx_squared'] = features['time_idx'] ** 2
        
        # Calendar features (if datetime index)
        if isinstance(series.index, pd.DatetimeIndex):
            features['month'] = series.index.month
            features['quarter'] = series.index.quarter
            features['day_of_week'] = series.index.dayofweek
            features['day_of_month'] = series.index.day
            features['week_of_year'] = series.index.isocalendar().week
        
        # Exponential weighted features
        features['ewm_mean'] = series.ewm(span=12).mean()
        features['ewm_std'] = series.ewm(span=12).std()
        
        # Drop NaN rows
        features = features.dropna()
        
        return features


def main():
    """Example usage of statistical forecaster."""
    from db.db import load_table, get_schema, bulk_insert, jsonb_serialize

    config_path = 'config/config.yaml'
    schema = get_schema(config_path)

    # Load data from PostgreSQL
    df = load_table(config_path, f"{schema}.demand_actuals",
                    columns="unique_id, date, COALESCE(corrected_qty, qty) AS y")
    df['date'] = pd.to_datetime(df['date'])
    characteristics_df = load_table(config_path, f"{schema}.time_series_characteristics")

    # Initialize forecaster
    forecaster = StatisticalForecaster()

    # Generate forecasts
    forecasts_df = forecaster.forecast_multiple_series(
        df=df,
        characteristics_df=characteristics_df.head(5)  # Test with first 5 series
    )

    # Save results to PostgreSQL
    if not forecasts_df.empty:
        cols = list(forecasts_df.columns)
        rows = [
            tuple(jsonb_serialize(v) for v in row)
            for row in forecasts_df.itertuples(index=False, name=None)
        ]
        n = bulk_insert(config_path, f"{schema}.forecast_results", cols, rows, truncate=False)
        print(f"\nForecasts saved to {schema}.forecast_results ({n} rows)")
    print(f"Generated {len(forecasts_df)} forecasts")


if __name__ == "__main__":
    main()

"""
Backtesting and Evaluation Module
Rolling window cross-validation with comprehensive metrics
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
import logging
import yaml
from pathlib import Path
from dataclasses import dataclass, asdict
import warnings
warnings.filterwarnings('ignore')


@dataclass
class EvaluationMetrics:
    """Container for evaluation metrics."""
    unique_id: str
    method: str
    forecast_origin: str
    horizon: int
    
    # Point forecast metrics
    mae: float
    rmse: float
    mape: float
    smape: float
    mase: float
    bias: float
    
    # Probabilistic metrics
    crps: Optional[float] = None
    winkler_score: Optional[float] = None
    coverage_50: Optional[float] = None
    coverage_80: Optional[float] = None
    coverage_90: Optional[float] = None
    coverage_95: Optional[float] = None
    quantile_loss: Optional[float] = None
    
    # Model selection criteria
    aic: Optional[float] = None
    bic: Optional[float] = None
    aicc: Optional[float] = None

    # Source of the metrics: 'rolling_window' (default) or 'internal_validation'
    metric_source: str = 'rolling_window'

    def to_dict(self) -> Dict:
        """Convert to dictionary."""
        return asdict(self)


class ForecastEvaluator:
    """
    Comprehensive forecast evaluation with backtesting.
    Implements rolling window cross-validation.
    """
    
    def __init__(self, config_path: str = "config/config.yaml", config_override: dict = None):
        """Initialize with configuration."""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        if config_override:
            from utils.parameter_resolver import ParameterResolver
            self.config = ParameterResolver.deep_merge(self.config, config_override)

        self.forecast_config = self.config['forecasting']
        self.backtesting_config = self.forecast_config['backtesting']
        self.logger = logging.getLogger(__name__)
        
        # Extract configuration
        self.horizon = self.forecast_config['horizon']

        # New backtesting params (with backward compat for old n_windows/step_size)
        _bt_horizon = self.backtesting_config.get('backtest_horizon', None)
        _bt_window = self.backtesting_config.get('window_size', None)
        _bt_n_tests = self.backtesting_config.get('n_tests', None)

        if _bt_horizon is None:
            # Old config keys — derive new-style params from them
            _old_n_windows = self.backtesting_config.get('n_windows', 3)
            _old_step_size = self.backtesting_config.get('step_size', 4)
            self.window_size = _bt_window if _bt_window is not None else self.horizon
            self.n_tests = _bt_n_tests if _bt_n_tests is not None else _old_n_windows
            self.backtest_horizon = self.window_size + max(0, _old_n_windows - 1) * _old_step_size
        else:
            self.backtest_horizon = _bt_horizon
            self.window_size = _bt_window if _bt_window is not None else self.horizon
            self.n_tests = _bt_n_tests if _bt_n_tests is not None else 3

        self.min_train_size = self.backtesting_config.get('min_train_size', 24)

        # Frequency (used to derive periods-per-year when date calc is unavailable)
        self.frequency = self.forecast_config.get('frequency', 'M')

    def create_rolling_windows(self,
                               series: pd.Series,
                               dates: pd.Series,
                               backtest_horizon: int = None,
                               window_size: int = None,
                               n_tests: int = None) -> List[Tuple[int, pd.Series, pd.Series]]:
        """
        Create rolling window splits for backtesting.

        The backtest zone is the last *backtest_horizon* periods of the series.
        Within that zone, *n_tests* test origins are placed, each evaluating a
        forecast of *window_size* periods.

        When n_tests=0 or exceeds the maximum possible, the window slides by 1
        period each time.  For n_tests>=2 the step between origins is derived:
            step = (backtest_horizon - window_size) / (n_tests - 1)

        Per-series overrides can be passed via the keyword arguments; when not
        supplied, the values from the config (self.*) are used.

        Args:
            series: Time series values
            dates: Corresponding dates
            backtest_horizon: Override for self.backtest_horizon
            window_size: Override for self.window_size
            n_tests: Override for self.n_tests

        Returns:
            List of (origin_idx, train_series, test_series) tuples
        """
        n = len(series)
        windows = []

        if n < 2:
            self.logger.debug(f"Series too short ({n} obs) for backtesting — need at least 2")
            return windows

        # Use overrides or config defaults
        _horizon = backtest_horizon if backtest_horizon is not None else self.backtest_horizon
        _window  = window_size if window_size is not None else self.window_size
        _n_tests = n_tests if n_tests is not None else self.n_tests

        # Enforce constraints
        _horizon = min(_horizon, n - 1)            # Can't exceed series length - 1
        _window  = min(_window, _horizon)           # Window can't exceed backtest horizon
        _window  = min(_window, self.horizon)        # Window can't exceed forecast horizon

        # First origin = start of backtest zone (ensure minimum training data)
        first_origin = max(self.min_train_size, n - _horizon)

        # Last possible origin where a full window still fits
        last_possible_origin = n - _window
        available_range = last_possible_origin - first_origin

        if available_range < 0:
            # Not enough data — use midpoint fallback
            first_origin = max(1, n // 2)
            last_possible_origin = n - 1
            available_range = last_possible_origin - first_origin
            _window = min(_window, n - first_origin)

        if available_range < 0:
            self.logger.debug(
                f"Could not construct any backtest windows "
                f"(n={n}, first_origin={first_origin}, "
                f"backtest_horizon={_horizon}, window_size={_window})"
            )
            return windows

        # Compute step and actual number of tests
        max_possible = available_range + 1
        if _n_tests <= 0 or _n_tests > max_possible:
            step = 1
            actual_n_tests = max_possible
        elif _n_tests == 1:
            step = 0
            actual_n_tests = 1
        else:
            step = max(1, available_range // (_n_tests - 1))
            actual_n_tests = _n_tests

        for i in range(actual_n_tests):
            origin_idx = first_origin + i * step

            if origin_idx > last_possible_origin:
                break

            train_series = series.iloc[:origin_idx]
            test_end = min(origin_idx + _window, n)
            test_series = series.iloc[origin_idx:test_end]

            if len(train_series) == 0 or len(test_series) == 0:
                continue

            windows.append((origin_idx, train_series, test_series))

        if not windows:
            self.logger.debug(
                f"Could not construct any backtest windows "
                f"(n={n}, first_origin={first_origin}, "
                f"backtest_horizon={_horizon}, window_size={_window}, n_tests={_n_tests})"
            )

        return windows
    
    def calculate_point_metrics(self,
                               actual: np.ndarray,
                               forecast: np.ndarray,
                               naive_forecast: Optional[np.ndarray] = None) -> Dict[str, float]:
        """
        Calculate point forecast accuracy metrics.
        
        Args:
            actual: Actual values
            forecast: Forecasted values
            naive_forecast: Naive forecast for MASE calculation
            
        Returns:
            Dictionary of metrics
        """
        # Ensure same length
        min_len = min(len(actual), len(forecast))
        actual = actual[:min_len]
        forecast = forecast[:min_len]
        
        if len(actual) == 0:
            return {}
        
        errors = actual - forecast
        abs_errors = np.abs(errors)
        squared_errors = errors ** 2
        
        # MAE (Mean Absolute Error)
        mae = np.mean(abs_errors)
        
        # RMSE (Root Mean Squared Error)
        rmse = np.sqrt(np.mean(squared_errors))
        
        # MAPE (Mean Absolute Percentage Error)
        with np.errstate(divide='ignore', invalid='ignore'):
            mape = np.mean(np.abs(errors / actual)) * 100
            mape = mape if np.isfinite(mape) else np.nan
        
        # sMAPE (Symmetric Mean Absolute Percentage Error)
        with np.errstate(divide='ignore', invalid='ignore'):
            denominator = (np.abs(actual) + np.abs(forecast)) / 2
            smape = np.mean(np.abs(errors) / denominator) * 100
            smape = smape if np.isfinite(smape) else np.nan
        
        # Bias (Mean Error)
        bias = np.mean(errors)
        
        # MASE (Mean Absolute Scaled Error)
        if naive_forecast is not None and len(naive_forecast) > 0:
            naive_errors = np.abs(actual - naive_forecast[:len(actual)])
            mae_naive = np.mean(naive_errors) if len(naive_errors) > 0 else 1.0
            mase = mae / mae_naive if mae_naive > 0 else np.nan
        else:
            # Use in-sample MAE of naive forecast
            if len(actual) > 1:
                naive_mae = np.mean(np.abs(np.diff(actual)))
                mase = mae / naive_mae if naive_mae > 0 else np.nan
            else:
                mase = np.nan
        
        return {
            'mae': float(mae),
            'rmse': float(rmse),
            'mape': float(mape),
            'smape': float(smape),
            'bias': float(bias),
            'mase': float(mase)
        }
    
    def calculate_probabilistic_metrics(self,
                                       actual: np.ndarray,
                                       quantiles: Dict[float, np.ndarray],
                                       point_forecast: np.ndarray) -> Dict[str, float]:
        """
        Calculate probabilistic forecast metrics.
        
        Args:
            actual: Actual values
            quantiles: Dictionary of quantile forecasts
            point_forecast: Point forecast (mean)
            
        Returns:
            Dictionary of probabilistic metrics
        """
        metrics = {}
        
        # CRPS (Continuous Ranked Probability Score)
        try:
            crps = self._calculate_crps(actual, quantiles, point_forecast)
            metrics['crps'] = float(crps)
        except:
            metrics['crps'] = np.nan
        
        # Coverage rates for different prediction intervals
        coverage_levels = {
            50: (0.25, 0.75),
            80: (0.10, 0.90),
            90: (0.05, 0.95),
            95: (0.025, 0.975)
        }
        
        for level, (lower_q, upper_q) in coverage_levels.items():
            if lower_q in quantiles and upper_q in quantiles:
                lower = quantiles[lower_q][:len(actual)]
                upper = quantiles[upper_q][:len(actual)]
                coverage = np.mean((actual >= lower) & (actual <= upper))
                metrics[f'coverage_{level}'] = float(coverage)
                
                # Winkler score for this interval
                alpha = 1 - (level / 100)
                winkler = self._calculate_winkler_score(actual, lower, upper, alpha)
                if level == 90:  # Report 90% interval Winkler score
                    metrics['winkler_score'] = float(winkler)
        
        # Quantile loss (pinball loss)
        quantile_losses = []
        for q, q_forecast in quantiles.items():
            q_forecast = q_forecast[:len(actual)]
            ql = self._quantile_loss(actual, q_forecast, q)
            quantile_losses.append(ql)
        
        if quantile_losses:
            metrics['quantile_loss'] = float(np.mean(quantile_losses))
        
        return metrics
    
    def _calculate_crps(self,
                       actual: np.ndarray,
                       quantiles: Dict[float, np.ndarray],
                       point_forecast: np.ndarray) -> float:
        """
        Calculate Continuous Ranked Probability Score.
        Approximated using quantile forecasts.
        """
        crps_values = []
        
        for i, y_true in enumerate(actual):
            # Get quantiles for this horizon
            q_values = []
            q_forecasts = []
            
            for q in sorted(quantiles.keys()):
                if i < len(quantiles[q]):
                    q_values.append(q)
                    q_forecasts.append(quantiles[q][i])
            
            if len(q_values) < 2:
                continue
            
            # Approximate CRPS using quantile integration
            crps = 0.0
            for j in range(len(q_values) - 1):
                q1, q2 = q_values[j], q_values[j + 1]
                f1, f2 = q_forecasts[j], q_forecasts[j + 1]
                
                # Trapezoidal integration
                indicator = 1.0 if y_true <= (f1 + f2) / 2 else 0.0
                crps += (q2 - q1) * ((f1 + f2) / 2 - y_true) * (indicator - (q1 + q2) / 2)
            
            crps_values.append(abs(crps))
        
        return np.mean(crps_values) if crps_values else np.nan
    
    def _calculate_winkler_score(self,
                                 actual: np.ndarray,
                                 lower: np.ndarray,
                                 upper: np.ndarray,
                                 alpha: float) -> float:
        """
        Calculate Winkler score for prediction intervals.
        Lower is better.
        """
        interval_width = upper - lower
        
        # Penalty for observations outside interval
        penalty_lower = (2 / alpha) * (lower - actual) * (actual < lower)
        penalty_upper = (2 / alpha) * (actual - upper) * (actual > upper)
        
        winkler = interval_width + penalty_lower + penalty_upper
        
        return np.mean(winkler)
    
    def _quantile_loss(self,
                      actual: np.ndarray,
                      q_forecast: np.ndarray,
                      quantile: float) -> float:
        """
        Calculate pinball loss for a specific quantile.
        """
        errors = actual - q_forecast
        loss = np.where(errors >= 0,
                       quantile * errors,
                       (quantile - 1) * errors)
        return np.mean(loss)
    
    def calculate_information_criteria(self,
                                      residuals: np.ndarray,
                                      n_params: int,
                                      n_obs: int) -> Dict[str, float]:
        """
        Calculate AIC, BIC, AICc for model selection.
        
        Args:
            residuals: Model residuals
            n_params: Number of model parameters
            n_obs: Number of observations
            
        Returns:
            Dictionary with AIC, BIC, AICc
        """
        if len(residuals) == 0 or n_obs == 0:
            return {'aic': np.nan, 'bic': np.nan, 'aicc': np.nan}
        
        # Log-likelihood (assuming Gaussian errors)
        sigma2 = np.var(residuals)
        if sigma2 <= 0:
            return {'aic': np.nan, 'bic': np.nan, 'aicc': np.nan}
        
        log_likelihood = -0.5 * n_obs * (np.log(2 * np.pi * sigma2) + 1)
        
        # AIC
        aic = -2 * log_likelihood + 2 * n_params
        
        # BIC
        bic = -2 * log_likelihood + n_params * np.log(n_obs)
        
        # AICc (corrected AIC for small samples)
        if n_obs - n_params - 1 > 0:
            aicc = aic + (2 * n_params * (n_params + 1)) / (n_obs - n_params - 1)
        else:
            aicc = np.nan
        
        return {
            'aic': float(aic),
            'bic': float(bic),
            'aicc': float(aicc)
        }

    # ------------------------------------------------------------------
    # Internal validation metrics converter
    # ------------------------------------------------------------------

    @staticmethod
    def create_eval_metrics_from_internal_validation(
        unique_id: str,
        method: str,
        horizon: int,
        internal_metrics: Dict,
        forecast_origin_date: str,
    ) -> 'EvaluationMetrics':
        """
        Convert an internal-validation metrics dict (from ML models) into an
        EvaluationMetrics instance that can be stored in backtest_metrics.

        Args:
            unique_id: Series identifier.
            method: Method name (e.g. 'LightGBM').
            horizon: Forecast horizon.
            internal_metrics: Dict produced by MLForecaster._compute_internal_val_metrics().
            forecast_origin_date: Date string for the 80/20 split point.

        Returns:
            EvaluationMetrics with metric_source='internal_validation'.
        """
        _g = internal_metrics.get

        return EvaluationMetrics(
            unique_id=unique_id,
            method=method,
            forecast_origin=forecast_origin_date,
            horizon=horizon,
            mae=float(_g('mae', np.nan)),
            rmse=float(_g('rmse', np.nan)),
            mape=float(_g('mape', np.nan)),
            smape=float(_g('smape', np.nan)),
            mase=float(_g('mase', np.nan)),
            bias=float(_g('bias', np.nan)),
            crps=_g('crps'),
            winkler_score=_g('winkler_score'),
            coverage_50=_g('coverage_50'),
            coverage_80=_g('coverage_80'),
            coverage_90=_g('coverage_90'),
            coverage_95=_g('coverage_95'),
            quantile_loss=_g('quantile_loss'),
            aic=_g('aic'),
            bic=_g('bic'),
            aicc=_g('aicc'),
            metric_source='internal_validation',
        )

    def evaluate_forecast(self,
                         actual: pd.Series,
                         forecast_result: Dict,
                         forecast_origin: str,
                         naive_forecast: Optional[np.ndarray] = None) -> EvaluationMetrics:
        """
        Evaluate a single forecast against actuals.
        
        Args:
            actual: Actual values for test period
            forecast_result: Dictionary with forecast data
            forecast_origin: Date string of forecast origin
            naive_forecast: Naive forecast for MASE
            
        Returns:
            EvaluationMetrics object
        """
        unique_id = forecast_result['unique_id']
        method = forecast_result['method']
        
        # Extract forecasts
        point_forecast = np.array(forecast_result['point_forecast'])
        quantiles = {float(k): np.array(v) for k, v in forecast_result['quantiles'].items()}
        
        # Calculate point metrics
        point_metrics = self.calculate_point_metrics(
            actual=actual.values,
            forecast=point_forecast,
            naive_forecast=naive_forecast
        )
        
        # Calculate probabilistic metrics
        prob_metrics = self.calculate_probabilistic_metrics(
            actual=actual.values,
            quantiles=quantiles,
            point_forecast=point_forecast
        )
        
        # Information criteria (if residuals available)
        ic_metrics = {'aic': None, 'bic': None, 'aicc': None}
        if 'residuals' in forecast_result and forecast_result['residuals'] is not None:
            residuals = np.array(forecast_result['residuals'])
            n_params = len(forecast_result.get('hyperparameters', {}))
            n_obs = len(residuals)
            ic_metrics = self.calculate_information_criteria(residuals, n_params, n_obs)
        
        # Create metrics object
        metrics = EvaluationMetrics(
            unique_id=unique_id,
            method=method,
            forecast_origin=forecast_origin,
            horizon=self.horizon,
            **point_metrics,
            **prob_metrics,
            **ic_metrics
        )
        
        return metrics
    
    def backtest_series(self,
                       df: pd.DataFrame,
                       unique_id: str,
                       forecast_fn,
                       methods: List[str],
                       characteristics: Dict,
                       backtest_overrides: Dict = None) -> pd.DataFrame:
        """
        Perform rolling window backtesting for a single series.

        Args:
            df: DataFrame with time series data
            unique_id: Series identifier
            forecast_fn: Function to generate forecasts
            methods: List of methods to test
            characteristics: Series characteristics
            backtest_overrides: Optional dict with per-series backtesting
                overrides (backtest_horizon, window_size, n_tests)

        Returns:
            DataFrame with evaluation metrics for all windows and methods
        """
        # Get series data
        series_df = df[df['unique_id'] == unique_id].sort_values('date')
        series = series_df['y']
        dates = series_df['date']

        # Create rolling windows (with optional per-series overrides)
        bt_ovr = backtest_overrides or {}
        windows = self.create_rolling_windows(
            series, dates,
            backtest_horizon=bt_ovr.get('backtest_horizon'),
            window_size=bt_ovr.get('window_size'),
            n_tests=bt_ovr.get('n_tests'),
        )

        if not windows:
            self.logger.debug(f"No valid windows for {unique_id} — skipping backtest")
            return pd.DataFrame()

        # Effective window size for naive forecast length
        _effective_window = min(
            bt_ovr.get('window_size', self.window_size),
            self.horizon
        )

        all_metrics = []

        for window_idx, (origin_idx, train_series, test_series) in enumerate(windows):
            forecast_origin = dates.iloc[origin_idx]

            self.logger.debug(f"Window {window_idx + 1}/{len(windows)} for {unique_id} at {forecast_origin}")

            # Generate forecasts for this window
            train_df = series_df.iloc[:origin_idx].copy()

            try:
                forecast_results = forecast_fn(
                    df=train_df,
                    unique_id=unique_id,
                    methods=methods,
                    characteristics=characteristics
                )

                # Naive forecast for MASE
                naive_forecast = np.repeat(train_series.iloc[-1], _effective_window)

                # Evaluate each method
                for forecast_result in forecast_results:
                    metrics = self.evaluate_forecast(
                        actual=test_series,
                        forecast_result=forecast_result.to_dict() if hasattr(forecast_result, 'to_dict') else forecast_result,
                        forecast_origin=str(forecast_origin),
                        naive_forecast=naive_forecast
                    )
                    all_metrics.append(metrics.to_dict())

            except Exception as e:
                self.logger.warning(f"Failed window {window_idx} for {unique_id}: {e}")
                continue

        return pd.DataFrame(all_metrics)

    def backtest_series_with_forecasts(self,
                                       df: pd.DataFrame,
                                       unique_id: str,
                                       forecast_fn,
                                       methods: List[str],
                                       characteristics: Dict,
                                       backtest_overrides: Dict = None) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Perform rolling window backtesting and store both metrics AND forecast values per origin.

        Uses the same rolling window logic as backtest_series, but additionally
        records the per-step point forecast and actual value for every
        (unique_id, method, forecast_origin, horizon_step) combination.

        Args:
            df: DataFrame with time series data (must contain unique_id, date, y)
            unique_id: Series identifier
            forecast_fn: Function to generate forecasts.  Called as
                forecast_fn(df=train_df, unique_id=unique_id,
                            methods=methods, characteristics=characteristics)
                and expected to return a list of forecast result dicts (or objects
                with a .to_dict() method) each containing at least
                'unique_id', 'method', 'point_forecast', and 'quantiles'.
            methods: List of method names to evaluate
            characteristics: Series characteristics dict
            backtest_overrides: Optional dict with per-series backtesting
                overrides (backtest_horizon, window_size, n_tests)

        Returns:
            Tuple of (metrics_df, forecasts_by_origin_df)
                metrics_df: Same structure as backtest_series output – one row per
                    (unique_id, method, forecast_origin) with aggregate metrics.
                forecasts_by_origin_df: One row per
                    (unique_id, method, forecast_origin, horizon_step) with columns:
                        unique_id        – series identifier
                        method           – forecasting method name
                        forecast_origin  – origin date (string)
                        horizon_step     – 1-based step index within forecast horizon
                        point_forecast   – predicted value at this step
                        actual_value     – observed value at this step
        """
        # Get series data
        series_df = df[df['unique_id'] == unique_id].sort_values('date')
        series = series_df['y']
        dates = series_df['date']

        # Create rolling windows (with optional per-series overrides)
        bt_ovr = backtest_overrides or {}
        windows = self.create_rolling_windows(
            series, dates,
            backtest_horizon=bt_ovr.get('backtest_horizon'),
            window_size=bt_ovr.get('window_size'),
            n_tests=bt_ovr.get('n_tests'),
        )

        if not windows:
            self.logger.debug(f"No valid windows for {unique_id} — skipping backtest")
            return pd.DataFrame(), pd.DataFrame()

        # Effective window size for naive forecast length
        _effective_window = min(
            bt_ovr.get('window_size', self.window_size),
            self.horizon
        )

        all_metrics = []
        all_forecasts = []

        for window_idx, (origin_idx, train_series, test_series) in enumerate(windows):
            forecast_origin = dates.iloc[origin_idx]

            self.logger.debug(
                f"Window {window_idx + 1}/{len(windows)} for {unique_id} at {forecast_origin}"
            )

            # Build training subset up to the origin
            train_df = series_df.iloc[:origin_idx].copy()

            try:
                forecast_results = forecast_fn(
                    df=train_df,
                    unique_id=unique_id,
                    methods=methods,
                    characteristics=characteristics
                )

                # Naive forecast for MASE calculation
                naive_forecast = np.repeat(train_series.iloc[-1], _effective_window)

                # Evaluate each method and collect per-step forecasts
                for forecast_result in forecast_results:
                    result_dict = (
                        forecast_result.to_dict()
                        if hasattr(forecast_result, 'to_dict')
                        else forecast_result
                    )

                    # --- metrics (same as backtest_series) ---
                    metrics = self.evaluate_forecast(
                        actual=test_series,
                        forecast_result=result_dict,
                        forecast_origin=str(forecast_origin),
                        naive_forecast=naive_forecast
                    )
                    all_metrics.append(metrics.to_dict())

                    # --- per-origin forecast values ---
                    point_forecast_arr = np.array(result_dict['point_forecast'])
                    actual_arr = test_series.values

                    # Align lengths (forecast may be longer/shorter than actuals)
                    n_steps = min(len(point_forecast_arr), len(actual_arr))

                    method_name = result_dict['method']
                    origin_str = str(forecast_origin)

                    for step in range(n_steps):
                        all_forecasts.append({
                            'unique_id': unique_id,
                            'method': method_name,
                            'forecast_origin': origin_str,
                            'horizon_step': step + 1,        # 1-based
                            'point_forecast': float(point_forecast_arr[step]),
                            'actual_value': float(actual_arr[step]),
                        })

            except Exception as e:
                self.logger.warning(f"Failed window {window_idx} for {unique_id}: {e}")
                continue

        metrics_df = pd.DataFrame(all_metrics)
        forecasts_by_origin_df = pd.DataFrame(all_forecasts)

        return metrics_df, forecasts_by_origin_df


def main():
    """Example usage of evaluator."""
    from forecasting.statistical_models import StatisticalForecaster
    from db.db import load_table, get_schema, bulk_insert

    config_path = 'config/config.yaml'
    schema = get_schema(config_path)

    # Load data from PostgreSQL
    df = load_table(config_path, f"{schema}.demand_actuals",
                    columns="unique_id, date, COALESCE(corrected_qty, qty) AS y")
    df['date'] = pd.to_datetime(df['date'])
    characteristics_df = load_table(config_path, f"{schema}.time_series_characteristics")

    # Initialize
    evaluator = ForecastEvaluator()
    forecaster = StatisticalForecaster()

    # Backtest first series
    first_id = characteristics_df['unique_id'].iloc[0]
    first_char = characteristics_df[characteristics_df['unique_id'] == first_id].iloc[0]

    metrics_df = evaluator.backtest_series(
        df=df,
        unique_id=first_id,
        forecast_fn=forecaster.forecast_single_series,
        methods=first_char['recommended_methods'][:3],  # Test top 3 methods
        characteristics=first_char.to_dict()
    )

    # Save results to PostgreSQL
    if not metrics_df.empty:
        cols = list(metrics_df.columns)
        rows = [tuple(row) for row in metrics_df.itertuples(index=False, name=None)]
        n = bulk_insert(config_path, f"{schema}.backtest_metrics", cols, rows, truncate=False)
        print(f"\nBacktest metrics saved to {schema}.backtest_metrics ({n} rows)")
    print(f"\nSummary by method:")
    print(metrics_df.groupby('method')[['mae', 'rmse', 'bias', 'coverage_90']].mean())


if __name__ == "__main__":
    main()

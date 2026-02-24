"""
ML Forecasting Models - LightGBM and XGBoost
Gradient boosting methods with engineered features and quantile regression
for direct multi-step probabilistic forecasting.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any
import logging
import yaml
from pathlib import Path
import time
import warnings

# LightGBM import
try:
    import lightgbm as lgb
    LIGHTGBM_AVAILABLE = True
except ImportError:
    LIGHTGBM_AVAILABLE = False
    logging.warning("LightGBM not available. Install with: pip install lightgbm")

# XGBoost import
try:
    import xgboost as xgb
    XGBOOST_AVAILABLE = True
except ImportError:
    XGBOOST_AVAILABLE = False
    logging.warning("XGBoost not available. Install with: pip install xgboost")

# sklearn for train/test splitting
try:
    from sklearn.model_selection import train_test_split
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False
    logging.warning("scikit-learn not available. Install with: pip install scikit-learn")

from .statistical_models import ForecastResult


class MLForecaster:
    """
    Gradient boosting forecaster using LightGBM and XGBoost.

    Uses direct multi-step forecasting (one model per horizon step) with
    engineered lag, rolling, calendar, trend, and EWM features. Produces
    probabilistic outputs via quantile regression.
    """

    # Minimum observations required to fit ML models
    MIN_OBSERVATIONS = 50

    # Supported methods
    SUPPORTED_METHODS = ['LightGBM', 'XGBoost']

    def __init__(self, config_path: str = "config/config.yaml"):
        """
        Initialize with configuration.

        Args:
            config_path: Path to the YAML configuration file.
        """
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)

        self.forecast_config = self.config['forecasting']
        self.logger = logging.getLogger(__name__)

        # Extract configuration
        self.horizon = self.forecast_config['horizon']
        self.frequency = self.forecast_config['frequency']
        self.confidence_levels = self.forecast_config['confidence_levels']

        # Convert confidence levels (e.g. [10, 25, 50, 75, 90, 95, 99]) to
        # quantile floats [0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99]
        self.quantile_levels = sorted(list(set(
            [level / 100.0 for level in self.confidence_levels]
        )))

        # Default frequency-to-season-length mapping
        self._freq_season_map = {
            'H': 24,
            'D': 7,
            'W': 52,
            'M': 12,
            'Q': 4,
            'Y': 1,
            'B': 5,
        }

    # ------------------------------------------------------------------
    # Feature engineering
    # ------------------------------------------------------------------

    def generate_features(self,
                          series: pd.Series,
                          characteristics: Dict) -> pd.DataFrame:
        """
        Generate engineered features from a univariate time series.

        Features produced:
            - Lag features at periods 1, 2, 3, 6, 12
            - Seasonal lag (if has_seasonality) at the dominant seasonal period
            - Rolling statistics (mean, std, min, max) for windows 3, 6, 12
            - Calendar features (month, quarter, day_of_week) when datetime index
            - Trend features: time_idx and time_idx_squared
            - EWM features: exponentially weighted mean and std with span=12

        Args:
            series: Time series values with a DatetimeIndex (preferred) or
                    integer index.
            characteristics: Dictionary of time series characteristics.

        Returns:
            DataFrame of features aligned to the series index. Rows with
            NaN values created by lagging / rolling are dropped.
        """
        features = pd.DataFrame(index=series.index)

        # --- Lag features ---
        for lag in [1, 2, 3, 6, 12]:
            if lag < len(series):
                features[f'lag_{lag}'] = series.shift(lag)

        # --- Seasonal lag ---
        has_seasonality = characteristics.get('has_seasonality', False)
        if has_seasonality:
            seasonal_periods = characteristics.get('seasonal_periods', [])
            if seasonal_periods:
                sp = int(seasonal_periods[0])
            else:
                sp = self._freq_season_map.get(self.frequency, 12)
            if sp < len(series):
                features[f'seasonal_lag_{sp}'] = series.shift(sp)

        # --- Rolling statistics ---
        for window in [3, 6, 12]:
            if window < len(series):
                features[f'rolling_mean_{window}'] = series.rolling(window=window).mean()
                features[f'rolling_std_{window}'] = series.rolling(window=window).std()
                features[f'rolling_min_{window}'] = series.rolling(window=window).min()
                features[f'rolling_max_{window}'] = series.rolling(window=window).max()

        # --- Calendar features ---
        if isinstance(series.index, pd.DatetimeIndex):
            features['month'] = series.index.month
            features['quarter'] = series.index.quarter
            features['day_of_week'] = series.index.dayofweek

        # --- Trend features ---
        features['time_idx'] = np.arange(len(series))
        features['time_idx_squared'] = features['time_idx'] ** 2

        # --- Exponentially weighted features ---
        features['ewm_mean'] = series.ewm(span=12, min_periods=1).mean()
        features['ewm_std'] = series.ewm(span=12, min_periods=1).std()

        # Drop rows that have NaN values introduced by lags / rolling
        features = features.dropna()

        return features

    # ------------------------------------------------------------------
    # Model helpers
    # ------------------------------------------------------------------

    def _get_lgb_params(self, quantile: Optional[float] = None,
                        overrides: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Return LightGBM parameters.  When *quantile* is provided the model
        is configured for quantile regression; otherwise squared-error
        regression is used.  User overrides are merged on top of defaults.
        """
        params: Dict[str, Any] = {
            'n_estimators': 300,
            'learning_rate': 0.05,
            'max_depth': 6,
            'num_leaves': 31,
            'subsample': 0.8,
            'colsample_bytree': 0.8,
            'min_child_samples': 10,
            'random_state': 42,
            'verbose': -1,
            'n_jobs': -1,
        }
        # Apply user overrides
        if overrides:
            for k in ('n_estimators', 'learning_rate', 'max_depth', 'num_leaves',
                       'subsample', 'colsample_bytree', 'min_child_samples', 'random_state'):
                if k in overrides:
                    params[k] = type(params[k])(overrides[k])  # coerce to original type
        if quantile is not None:
            params['objective'] = 'quantile'
            params['alpha'] = quantile
        else:
            params['objective'] = 'regression'
        return params

    def _get_xgb_params(self, quantile: Optional[float] = None,
                        overrides: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Return XGBoost parameters.  When *quantile* is provided the model
        uses the ``reg:quantileerror`` objective.  User overrides are merged.
        """
        params: Dict[str, Any] = {
            'n_estimators': 300,
            'learning_rate': 0.05,
            'max_depth': 6,
            'subsample': 0.8,
            'colsample_bytree': 0.8,
            'min_child_weight': 10,
            'random_state': 42,
            'verbosity': 0,
            'n_jobs': -1,
        }
        # Apply user overrides
        if overrides:
            for k in ('n_estimators', 'learning_rate', 'max_depth',
                       'subsample', 'colsample_bytree', 'min_child_weight', 'random_state'):
                if k in overrides:
                    params[k] = type(params[k])(overrides[k])
        if quantile is not None:
            params['objective'] = 'reg:quantileerror'
            params['quantile_alpha'] = quantile
        else:
            params['objective'] = 'reg:squarederror'
        return params

    def _build_model(self, method: str, quantile: Optional[float] = None,
                     overrides: Optional[Dict[str, Any]] = None):
        """
        Instantiate a LightGBM or XGBoost regressor.

        Args:
            method: 'LightGBM' or 'XGBoost'.
            quantile: If provided, builds a quantile regression model for
                      the given quantile level.
            overrides: Optional user hyperparameter overrides.

        Returns:
            An unfitted scikit-learn-compatible regressor.
        """
        if method == 'LightGBM':
            if not LIGHTGBM_AVAILABLE:
                raise ImportError("LightGBM is required. Install: pip install lightgbm")
            params = self._get_lgb_params(quantile, overrides=overrides)
            return lgb.LGBMRegressor(**params)

        if method == 'XGBoost':
            if not XGBOOST_AVAILABLE:
                raise ImportError("XGBoost is required. Install: pip install xgboost")
            params = self._get_xgb_params(quantile, overrides=overrides)
            return xgb.XGBRegressor(**params)

        raise ValueError(f"Unknown ML method: {method}. Supported: {self.SUPPORTED_METHODS}")

    # ------------------------------------------------------------------
    # Supervised dataset construction
    # ------------------------------------------------------------------

    @staticmethod
    def _build_supervised_dataset(features_df: pd.DataFrame,
                                  target: pd.Series,
                                  horizon_step: int):
        """
        Create an (X, y) pair for a specific horizon step.

        The target is shifted *horizon_step* periods into the future so that
        the features at time *t* predict the value at *t + horizon_step*.

        Args:
            features_df: Feature matrix.
            target: Target series aligned to features_df.
            horizon_step: Number of steps ahead (1-based).

        Returns:
            Tuple (X, y) with aligned, NaN-free arrays.
        """
        y_shifted = target.shift(-horizon_step)
        combined = features_df.copy()
        combined['__target__'] = y_shifted
        combined = combined.dropna()
        X = combined.drop(columns=['__target__'])
        y = combined['__target__']
        return X, y

    # ------------------------------------------------------------------
    # Core forecasting
    # ------------------------------------------------------------------

    def _forecast_method(self,
                         series: pd.Series,
                         characteristics: Dict,
                         method: str,
                         overrides: Optional[Dict[str, Any]] = None) -> ForecastResult:
        """
        Train and forecast a single series with one ML method using direct
        multi-step quantile regression.

        For each horizon step h in [1 .. self.horizon], a separate model is
        trained for the point forecast (median) and for every requested
        quantile level.

        Args:
            series: Univariate time series with DatetimeIndex.
            characteristics: Time series characteristics dict.
            method: 'LightGBM' or 'XGBoost'.
            overrides: Optional user hyperparameter overrides for this method.

        Returns:
            A ForecastResult with point forecasts and quantile forecasts.
        """
        unique_id = characteristics.get('unique_id', 'unknown')
        start_time = time.time()

        # --- Feature engineering ---
        features_df = self.generate_features(series, characteristics)

        # Align the target to the feature rows
        target = series.loc[features_df.index]

        # Prepare containers
        point_forecasts = np.empty(self.horizon)
        quantile_forecasts: Dict[float, np.ndarray] = {
            q: np.empty(self.horizon) for q in self.quantile_levels
        }

        # The last row of features represents the most recent available
        # observation.  It will be used as the input for prediction at each
        # horizon step.
        last_features = features_df.iloc[[-1]]

        for h in range(1, self.horizon + 1):
            X_train, y_train = self._build_supervised_dataset(
                features_df, target, horizon_step=h
            )

            if len(X_train) < 10:
                self.logger.warning(
                    f"{unique_id}/{method}: Not enough supervised samples "
                    f"for horizon step {h} (got {len(X_train)}). Filling with NaN."
                )
                point_forecasts[h - 1] = np.nan
                for q in self.quantile_levels:
                    quantile_forecasts[q][h - 1] = np.nan
                continue

            # Optional train/val split for early stopping (use last 20 %)
            if SKLEARN_AVAILABLE and len(X_train) >= 30:
                X_tr, X_val, y_tr, y_val = train_test_split(
                    X_train, y_train, test_size=0.2, shuffle=False
                )
            else:
                X_tr, y_tr = X_train, y_train
                X_val, y_val = None, None

            # --- Point forecast (median, q=0.5) ---
            try:
                model_point = self._build_model(method, quantile=0.5, overrides=overrides)
                if X_val is not None and method == 'LightGBM':
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore")
                        model_point.fit(
                            X_tr, y_tr,
                            eval_set=[(X_val, y_val)],
                            callbacks=[lgb.early_stopping(stopping_rounds=30, verbose=False)]
                        )
                elif X_val is not None and method == 'XGBoost':
                    model_point.fit(
                        X_tr, y_tr,
                        eval_set=[(X_val, y_val)],
                        verbose=False,
                    )
                else:
                    model_point.fit(X_tr, y_tr)

                point_forecasts[h - 1] = float(model_point.predict(last_features)[0])
            except Exception as e:
                self.logger.warning(
                    f"{unique_id}/{method}: Point forecast failed at h={h}: {e}"
                )
                point_forecasts[h - 1] = np.nan

            # --- Quantile forecasts ---
            for q in self.quantile_levels:
                try:
                    model_q = self._build_model(method, quantile=q, overrides=overrides)
                    if X_val is not None and method == 'LightGBM':
                        with warnings.catch_warnings():
                            warnings.simplefilter("ignore")
                            model_q.fit(
                                X_tr, y_tr,
                                eval_set=[(X_val, y_val)],
                                callbacks=[lgb.early_stopping(stopping_rounds=30, verbose=False)]
                            )
                    elif X_val is not None and method == 'XGBoost':
                        model_q.fit(
                            X_tr, y_tr,
                            eval_set=[(X_val, y_val)],
                            verbose=False,
                        )
                    else:
                        model_q.fit(X_tr, y_tr)

                    quantile_forecasts[q][h - 1] = float(
                        model_q.predict(last_features)[0]
                    )
                except Exception as e:
                    self.logger.warning(
                        f"{unique_id}/{method}: Quantile {q} failed at h={h}: {e}"
                    )
                    quantile_forecasts[q][h - 1] = np.nan

        # --- Post-processing: enforce quantile monotonicity ---
        quantile_forecasts = self._enforce_quantile_monotonicity(quantile_forecasts)

        training_time = time.time() - start_time

        # Collect hyper-parameters for traceability
        hyperparameters: Dict[str, Any] = {
            'method': method,
            'method_family': 'ML',
            'horizon': self.horizon,
            'n_features': features_df.shape[1],
            'n_train_rows': len(features_df),
            'description': f'{method} gradient boosting with direct multi-step quantile regression.',
        }
        if method == 'LightGBM':
            hyperparameters.update(self._get_lgb_params(overrides=overrides))
        else:
            hyperparameters.update(self._get_xgb_params(overrides=overrides))
        if overrides:
            hyperparameters['has_overrides'] = True
            hyperparameters['overrides_applied'] = overrides

        result = ForecastResult(
            unique_id=unique_id,
            method=method,
            point_forecast=point_forecasts,
            quantiles=quantile_forecasts,
            fitted_values=None,
            residuals=None,
            hyperparameters=hyperparameters,
            training_time=training_time,
            insample_actual=series.values,
        )

        self.logger.debug(
            f"ML forecast complete: {unique_id} - {method} ({training_time:.2f}s)"
        )

        return result

    @staticmethod
    def _enforce_quantile_monotonicity(
            quantile_forecasts: Dict[float, np.ndarray]
    ) -> Dict[float, np.ndarray]:
        """
        Ensure that quantile forecasts are non-decreasing across quantile
        levels at every horizon step.  If a lower quantile exceeds a higher
        one the values are sorted.
        """
        sorted_keys = sorted(quantile_forecasts.keys())
        if len(sorted_keys) <= 1:
            return quantile_forecasts

        horizon = len(next(iter(quantile_forecasts.values())))
        for h in range(horizon):
            values = np.array([quantile_forecasts[q][h] for q in sorted_keys])
            sorted_values = np.sort(values)
            for idx, q in enumerate(sorted_keys):
                quantile_forecasts[q][h] = sorted_values[idx]

        return quantile_forecasts

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def forecast_single_series(self,
                               df: pd.DataFrame,
                               unique_id: str,
                               methods: List[str],
                               characteristics: Dict,
                               overrides_map: Optional[Dict[str, Dict[str, Any]]] = None) -> List[ForecastResult]:
        """
        Generate ML forecasts for a single time series using the requested
        methods.

        Args:
            df: DataFrame with columns [unique_id, ds, y] (or date instead
                of ds).
            unique_id: Series identifier.
            methods: List of method names ('LightGBM', 'XGBoost').
            characteristics: Time series characteristics dictionary.  Must
                             include at least 'n_observations'.
            overrides_map: Optional {method: {param: value}} overrides for this series.

        Returns:
            List of ForecastResult objects (one per method that succeeded).
        """
        results: List[ForecastResult] = []

        # --- Check minimum data requirement ---
        n_obs = characteristics.get('n_observations', 0)
        if n_obs < self.MIN_OBSERVATIONS:
            self.logger.warning(
                f"Insufficient data for ML models ({unique_id}): "
                f"{n_obs} observations < {self.MIN_OBSERVATIONS} required."
            )
            return results

        # --- Prepare series ---
        series_df = df[df['unique_id'] == unique_id].copy()

        # Normalise column names (the rest of the codebase uses 'date' and 'ds')
        if 'date' in series_df.columns and 'ds' not in series_df.columns:
            series_df = series_df.rename(columns={'date': 'ds'})

        if 'ds' not in series_df.columns or 'y' not in series_df.columns:
            raise ValueError(
                "DataFrame must contain columns: unique_id, ds (or date), y"
            )

        series_df['ds'] = pd.to_datetime(series_df['ds'])
        series_df = series_df.sort_values('ds')
        series = series_df.set_index('ds')['y']

        # Ensure characteristics has unique_id for internal use
        characteristics = dict(characteristics)
        characteristics['unique_id'] = unique_id

        # --- Run each requested method ---
        ml_methods = [m for m in methods if m in self.SUPPORTED_METHODS]

        for method in ml_methods:
            # Check library availability
            if method == 'LightGBM' and not LIGHTGBM_AVAILABLE:
                self.logger.warning(
                    f"Skipping LightGBM for {unique_id}: library not installed."
                )
                continue
            if method == 'XGBoost' and not XGBOOST_AVAILABLE:
                self.logger.warning(
                    f"Skipping XGBoost for {unique_id}: library not installed."
                )
                continue

            try:
                method_ovr = (overrides_map or {}).get(method, None)
                result = self._forecast_method(series, characteristics, method, overrides=method_ovr)
                results.append(result)
            except Exception as e:
                self.logger.warning(
                    f"Failed to forecast {unique_id} with {method}: {e}"
                )
                continue

        return results

    def forecast_multiple_series(self,
                                 df: pd.DataFrame,
                                 characteristics_df: pd.DataFrame,
                                 overrides_map: dict = None) -> pd.DataFrame:
        """
        Generate ML forecasts for multiple time series.

        Args:
            df: DataFrame with time series data (columns: unique_id,
                ds/date, y).
            characteristics_df: DataFrame where each row contains at least
                'unique_id', 'recommended_methods', and the fields expected
                by ``generate_features`` / ``forecast_single_series``.
            overrides_map: Optional {unique_id: {method: {param: value}}} overrides.

        Returns:
            DataFrame with all forecast results (one row per
            unique_id / method combination).
        """
        all_results: List[ForecastResult] = []

        # Filter to series with enough data
        valid_chars = characteristics_df[
            characteristics_df['n_observations'] >= self.MIN_OBSERVATIONS
        ]

        self.logger.info(
            f"Generating ML forecasts for {len(valid_chars)} series "
            f"(skipped {len(characteristics_df) - len(valid_chars)} with "
            f"< {self.MIN_OBSERVATIONS} observations)"
        )

        for _, char_row in valid_chars.iterrows():
            unique_id = char_row['unique_id']

            # Extract ML methods from recommended methods
            all_methods = char_row.get('recommended_methods', [])
            if isinstance(all_methods, str):
                all_methods = [m.strip() for m in all_methods.split(',')]
            ml_methods = [m for m in all_methods if m in self.SUPPORTED_METHODS]

            if not ml_methods:
                continue

            characteristics = char_row.to_dict()

            try:
                series_overrides = (overrides_map or {}).get(unique_id, None)
                results = self.forecast_single_series(
                    df=df,
                    unique_id=unique_id,
                    methods=ml_methods,
                    characteristics=characteristics,
                    overrides_map=series_overrides,
                )
                all_results.extend(results)
            except Exception as e:
                self.logger.warning(f"Skipping {unique_id}: {e}")
                continue

        # Convert to DataFrame
        results_data = [result.to_dict() for result in all_results]

        return pd.DataFrame(results_data) if results_data else pd.DataFrame()


def main():
    """Example usage of the ML forecaster."""
    from db.db import load_table, get_schema, bulk_insert, jsonb_serialize

    config_path = 'config/config.yaml'
    schema = get_schema(config_path)

    # Load data from PostgreSQL
    df = load_table(config_path, f"{schema}.demand_actuals",
                    columns="unique_id, date, COALESCE(corrected_qty, qty) AS y")
    df['date'] = pd.to_datetime(df['date'])
    characteristics_df = load_table(config_path, f"{schema}.time_series_characteristics")

    # Initialize forecaster
    forecaster = MLForecaster()

    # Generate forecasts
    forecasts_df = forecaster.forecast_multiple_series(
        df=df,
        characteristics_df=characteristics_df.head(5),
    )

    if not forecasts_df.empty:
        cols = list(forecasts_df.columns)
        rows = [
            tuple(jsonb_serialize(v) for v in row)
            for row in forecasts_df.itertuples(index=False, name=None)
        ]
        n = bulk_insert(config_path, f"{schema}.forecast_results", cols, rows, truncate=False)
        print(f"\nML forecasts saved to {schema}.forecast_results ({n} rows)")
        print(f"Generated {len(forecasts_df)} forecasts")
    else:
        print("\nNo ML forecasts generated (insufficient data or no ML methods)")


if __name__ == "__main__":
    main()

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
from tqdm import tqdm

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

    # Fallback minimum observations for ML models (used if config is missing)
    _DEFAULT_MIN_OBSERVATIONS = 50

    # Supported methods
    SUPPORTED_METHODS = ['LightGBM', 'XGBoost']

    def __init__(self, config_path: str = None, config_override: dict = None):
        """
        Initialize with configuration.

        Args:
            config_path: Optional path to a YAML config file (legacy). When
                omitted the configuration is loaded from the database.
            config_override: Optional dict to deep-merge on top of the loaded config.
        """
        try:
            if config_path:
                with open(config_path, 'r') as f:
                    self.config = yaml.safe_load(f) or {}
            else:
                raise FileNotFoundError
        except (FileNotFoundError, OSError):
            from db.db import load_config_from_db
            self.config = load_config_from_db()
        if config_override:
            from utils.parameter_resolver import ParameterResolver
            self.config = ParameterResolver.deep_merge(self.config, config_override)

        self.forecast_config = self.config['forecasting']
        self.logger = logging.getLogger(__name__)

        # Read min_for_ml from config, fall back to class default
        _sufficiency = self.config.get('characterization', {}).get('data_sufficiency', {})
        self.MIN_OBSERVATIONS = _sufficiency.get('min_for_ml', self._DEFAULT_MIN_OBSERVATIONS)

        # Extract configuration
        self.horizon = self.forecast_config['horizon']
        self.frequency = self.forecast_config['frequency']
        self.confidence_levels = self.forecast_config['confidence_levels']
        self.val_split = self.forecast_config.get('ml_val_split', 0.2)

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

        # Frequency-aware feature window configuration.
        # Lags, rolling windows and EWM span are expressed in *periods* so
        # they capture the same "semantic" durations regardless of granularity.
        # e.g. for weekly data lag_52 = 1 year; for monthly data lag_12 = 1 year.
        self._freq_feature_cfg = {
            'H':  {'lags': [1, 2, 3, 6, 12, 24],      'windows': [6, 12, 24],     'ewm_span': 6},
            'D':  {'lags': [1, 2, 3, 7, 14, 30],       'windows': [7, 14, 30],     'ewm_span': 7},
            'W':  {'lags': [1, 2, 4, 8, 13, 26, 52],   'windows': [4, 8, 13, 26],  'ewm_span': 13},
            'M':  {'lags': [1, 2, 3, 6, 12],           'windows': [3, 6, 12],      'ewm_span': 12},
            'ME': {'lags': [1, 2, 3, 6, 12],           'windows': [3, 6, 12],      'ewm_span': 12},
            'Q':  {'lags': [1, 2, 4],                  'windows': [2, 4],          'ewm_span': 4},
            'QE': {'lags': [1, 2, 4],                  'windows': [2, 4],          'ewm_span': 4},
            'Y':  {'lags': [1, 2, 3],                  'windows': [2, 3],          'ewm_span': 3},
            'YE': {'lags': [1, 2, 3],                  'windows': [2, 3],          'ewm_span': 3},
            'B':  {'lags': [1, 2, 5, 10, 20],          'windows': [5, 10, 20],     'ewm_span': 5},
        }
        # Fall back to monthly config when frequency is unrecognised
        self._default_feature_cfg = {'lags': [1, 2, 3, 6, 12], 'windows': [3, 6, 12], 'ewm_span': 12}

    # ------------------------------------------------------------------
    # Feature engineering
    # ------------------------------------------------------------------

    def generate_features(self,
                          series: pd.Series,
                          characteristics: Dict) -> pd.DataFrame:
        """
        Generate engineered features from a univariate time series.

        Features produced (window sizes are frequency-aware, e.g. weekly → 4/8/13/26):
            - Lag features at frequency-scaled periods (short + medium + long range)
            - Seasonal lag (if has_seasonality) at the dominant seasonal period
            - Rolling statistics (mean, std, min, max) for frequency-scaled windows
            - Calendar features (month, quarter, day_of_week) when datetime index
            - Trend features: time_idx and time_idx_squared
            - EWM features: exponentially weighted mean and std with frequency-scaled span

        Args:
            series: Time series values with a DatetimeIndex (preferred) or
                    integer index.
            characteristics: Dictionary of time series characteristics.

        Returns:
            DataFrame of features aligned to the series index. Rows with
            NaN values created by lagging / rolling are dropped.
        """
        features = pd.DataFrame(index=series.index)

        # Resolve frequency-aware window sizes
        feat_cfg = self._freq_feature_cfg.get(self.frequency, self._default_feature_cfg)
        lag_periods  = feat_cfg['lags']
        window_sizes = feat_cfg['windows']
        ewm_span     = feat_cfg['ewm_span']

        # --- Lag features ---
        for lag in lag_periods:
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
            if sp < len(series) and sp not in lag_periods:
                features[f'seasonal_lag_{sp}'] = series.shift(sp)

        # --- Rolling statistics ---
        for window in window_sizes:
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

        # --- Exponentially weighted features (span scaled to frequency) ---
        features['ewm_mean'] = series.ewm(span=ewm_span, min_periods=1).mean()
        features['ewm_std'] = series.ewm(span=ewm_span, min_periods=1).std()

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

        # Containers for internal validation metrics capture
        val_actuals_per_h: list = []
        val_predictions_per_h: list = []
        val_quantile_preds_per_h: Dict[float, list] = {
            q: [] for q in self.quantile_levels
        }
        train_residuals_all: list = []
        n_params_total = 0
        has_validation = False

        # The last row of features represents the most recent available
        # observation.  It will be used as the input for prediction at each
        # horizon step.
        last_features = features_df.iloc[[-1]]

        # Early skip: if features can't support at least half the horizon
        # with the minimum 10 supervised samples, skip entirely.
        min_supervised = 10
        max_trainable_h = max(len(features_df) - min_supervised, 0)
        if max_trainable_h < self.horizon // 2:
            self.logger.warning(
                f"{unique_id}/{method}: Only {len(features_df)} usable feature rows "
                f"after engineering — can train at most {max_trainable_h}/{self.horizon} "
                f"horizon steps. Skipping method (need at least {self.horizon // 2})."
            )
            # Return a result with all-NaN forecasts so downstream knows it was attempted
            return ForecastResult(
                unique_id=unique_id,
                method=method,
                point_forecast=point_forecasts * np.nan,
                quantiles={q: arr * np.nan for q, arr in quantile_forecasts.items()},
                fitted_values=None,
                residuals=None,
                hyperparameters={'method': method, 'method_family': 'ML', 'skipped': True,
                                 'skip_reason': f'insufficient features ({len(features_df)} rows) for horizon {self.horizon}'},
                training_time=time.time() - start_time,
                insample_actual=series.values,
            )

        # Validation split ratio — per-series override or config default
        _val_split = (overrides or {}).get('val_split', self.val_split)

        # Track horizon steps skipped due to insufficient samples
        skipped_steps = []

        for h in range(1, self.horizon + 1):
            X_train, y_train = self._build_supervised_dataset(
                features_df, target, horizon_step=h
            )

            if len(X_train) < min_supervised:
                skipped_steps.append((h, len(X_train)))
                point_forecasts[h - 1] = np.nan
                for q in self.quantile_levels:
                    quantile_forecasts[q][h - 1] = np.nan
                continue

            # Optional train/val split for early stopping
            if SKLEARN_AVAILABLE and len(X_train) >= 30 and _val_split > 0:
                X_tr, X_val, y_tr, y_val = train_test_split(
                    X_train, y_train, test_size=_val_split, shuffle=False
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

                # --- Capture validation data for internal metrics ---
                if X_val is not None:
                    has_validation = True
                    val_pred_h = model_point.predict(X_val)
                    val_actual_h = y_val.values if hasattr(y_val, 'values') else np.asarray(y_val)
                    val_actuals_per_h.append(val_actual_h)
                    val_predictions_per_h.append(val_pred_h)

                    # Training residuals for AIC/BIC
                    train_actual_h = y_tr.values if hasattr(y_tr, 'values') else np.asarray(y_tr)
                    train_pred_h = model_point.predict(X_tr)
                    train_residuals_all.extend(train_actual_h - train_pred_h)

                    # Parameter count proxy: best_iteration for LightGBM, n_estimators for XGBoost
                    if hasattr(model_point, 'best_iteration_') and model_point.best_iteration_ > 0:
                        n_params_total += model_point.best_iteration_
                    elif hasattr(model_point, 'n_estimators'):
                        n_params_total += model_point.n_estimators
                    else:
                        n_params_total += 100  # fallback

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

                    # Capture quantile validation predictions
                    if X_val is not None:
                        val_quantile_preds_per_h[q].append(model_q.predict(X_val))

                except Exception as e:
                    self.logger.warning(
                        f"{unique_id}/{method}: Quantile {q} failed at h={h}: {e}"
                    )
                    quantile_forecasts[q][h - 1] = np.nan

        # --- Log a single condensed warning for all skipped horizon steps ---
        if skipped_steps:
            first_h = skipped_steps[0][0]
            last_h = skipped_steps[-1][0]
            self.logger.warning(
                f"{unique_id}/{method}: Insufficient supervised samples for "
                f"{len(skipped_steps)}/{self.horizon} horizon steps "
                f"(h={first_h}..{last_h}). Filled with NaN."
            )

        # --- Post-processing: enforce quantile monotonicity ---
        quantile_forecasts = self._enforce_quantile_monotonicity(quantile_forecasts)

        # --- Compute internal validation metrics ---
        internal_val_metrics = None
        residuals_arr = None
        if has_validation and val_actuals_per_h:
            try:
                internal_val_metrics = self._compute_internal_val_metrics(
                    val_actuals_per_h,
                    val_predictions_per_h,
                    val_quantile_preds_per_h,
                    train_residuals_all,
                    n_params_total,
                )
            except Exception as e:
                self.logger.warning(
                    f"{unique_id}/{method}: Failed to compute internal val metrics: {e}"
                )
            if train_residuals_all:
                residuals_arr = np.array(train_residuals_all)

        training_time = time.time() - start_time

        # Collect hyper-parameters for traceability
        hyperparameters: Dict[str, Any] = {
            'method': method,
            'method_family': 'ML',
            'horizon': self.horizon,
            'n_features': features_df.shape[1],
            'n_train_rows': len(features_df),
            'val_split': _val_split,
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
            residuals=residuals_arr,
            hyperparameters=hyperparameters,
            training_time=training_time,
            insample_actual=series.values,
            internal_val_metrics=internal_val_metrics,
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
    # Internal validation metrics
    # ------------------------------------------------------------------

    def _compute_internal_val_metrics(
        self,
        val_actuals_per_h: list,
        val_predictions_per_h: list,
        val_quantile_preds_per_h: Dict[float, list],
        train_residuals_all: list,
        n_params_total: int,
    ) -> Dict[str, Any]:
        """
        Compute aggregate validation metrics across all horizon steps.

        For each horizon step h, the validation set (last 20 % of supervised
        samples) provides genuine out-of-sample predictions.  Per-step metrics
        are averaged across all H horizon steps, mirroring how rolling-window
        backtest metrics are averaged across forecast origins.
        """
        all_mae, all_rmse, all_bias = [], [], []
        all_mape, all_smape, all_mase = [], [], []

        for actual, pred in zip(val_actuals_per_h, val_predictions_per_h):
            if len(actual) == 0:
                continue
            errors = actual - pred
            abs_errors = np.abs(errors)

            all_mae.append(float(np.mean(abs_errors)))
            all_rmse.append(float(np.sqrt(np.mean(errors ** 2))))
            all_bias.append(float(np.mean(errors)))

            with np.errstate(divide='ignore', invalid='ignore'):
                mape = np.mean(np.abs(errors / actual)) * 100
                all_mape.append(float(mape) if np.isfinite(mape) else np.nan)

                denom = (np.abs(actual) + np.abs(pred)) / 2
                smape = np.mean(np.abs(errors) / denom) * 100
                all_smape.append(float(smape) if np.isfinite(smape) else np.nan)

            # MASE — naive = one-step diff within the validation set
            if len(actual) > 1:
                naive_mae = np.mean(np.abs(np.diff(actual)))
                mase = float(np.mean(abs_errors)) / naive_mae if naive_mae > 0 else np.nan
            else:
                mase = np.nan
            all_mase.append(mase)

        metrics: Dict[str, Any] = {
            'mae':   float(np.nanmean(all_mae))   if all_mae   else np.nan,
            'rmse':  float(np.nanmean(all_rmse))  if all_rmse  else np.nan,
            'bias':  float(np.nanmean(all_bias))  if all_bias  else np.nan,
            'mape':  float(np.nanmean(all_mape))  if all_mape  else np.nan,
            'smape': float(np.nanmean(all_smape)) if all_smape else np.nan,
            'mase':  float(np.nanmean(all_mase))  if all_mase  else np.nan,
        }

        # ---- Probabilistic metrics from quantile validation predictions ----
        coverage_levels = {
            50: (0.25, 0.75), 80: (0.10, 0.90),
            90: (0.05, 0.95), 95: (0.025, 0.975),
        }
        for level, (lower_q, upper_q) in coverage_levels.items():
            lower_list = val_quantile_preds_per_h.get(lower_q, [])
            upper_list = val_quantile_preds_per_h.get(upper_q, [])
            if lower_list and upper_list:
                coverages = []
                for h_idx, actual in enumerate(val_actuals_per_h):
                    if h_idx < len(lower_list) and h_idx < len(upper_list):
                        lower = lower_list[h_idx][:len(actual)]
                        upper = upper_list[h_idx][:len(actual)]
                        cov = np.mean((actual >= lower) & (actual <= upper))
                        coverages.append(float(cov))
                if coverages:
                    metrics[f'coverage_{level}'] = float(np.nanmean(coverages))

        # Winkler score (90 % interval)
        lower_90 = val_quantile_preds_per_h.get(0.05, [])
        upper_90 = val_quantile_preds_per_h.get(0.95, [])
        if lower_90 and upper_90:
            winkler_values = []
            alpha = 0.10
            for h_idx, actual in enumerate(val_actuals_per_h):
                if h_idx < len(lower_90) and h_idx < len(upper_90):
                    lower = lower_90[h_idx][:len(actual)]
                    upper = upper_90[h_idx][:len(actual)]
                    width = upper - lower
                    pen_low = (2 / alpha) * (lower - actual) * (actual < lower)
                    pen_up  = (2 / alpha) * (actual - upper) * (actual > upper)
                    winkler_values.extend((width + pen_low + pen_up).tolist())
            if winkler_values:
                metrics['winkler_score'] = float(np.nanmean(winkler_values))

        # CRPS approximation
        sorted_qs = sorted(val_quantile_preds_per_h.keys())
        if len(sorted_qs) >= 2:
            crps_values = []
            for h_idx, actual in enumerate(val_actuals_per_h):
                for i, y_true in enumerate(actual):
                    q_vals, q_preds = [], []
                    for q in sorted_qs:
                        preds_list = val_quantile_preds_per_h[q]
                        if h_idx < len(preds_list) and i < len(preds_list[h_idx]):
                            q_vals.append(q)
                            q_preds.append(preds_list[h_idx][i])
                    if len(q_vals) >= 2:
                        crps = 0.0
                        for j in range(len(q_vals) - 1):
                            q1, q2 = q_vals[j], q_vals[j + 1]
                            f1, f2 = q_preds[j], q_preds[j + 1]
                            indicator = 1.0 if y_true <= (f1 + f2) / 2 else 0.0
                            crps += (q2 - q1) * ((f1 + f2) / 2 - y_true) * (indicator - (q1 + q2) / 2)
                        crps_values.append(abs(crps))
            if crps_values:
                metrics['crps'] = float(np.nanmean(crps_values))

        # Quantile loss (pinball loss)
        if sorted_qs:
            all_ql = []
            for q in sorted_qs:
                q_losses = []
                for h_idx, actual in enumerate(val_actuals_per_h):
                    preds_list = val_quantile_preds_per_h[q]
                    if h_idx < len(preds_list):
                        q_pred = preds_list[h_idx][:len(actual)]
                        err = actual - q_pred
                        loss = np.where(err >= 0, q * err, (q - 1) * err)
                        q_losses.append(float(np.mean(loss)))
                if q_losses:
                    all_ql.append(float(np.nanmean(q_losses)))
            if all_ql:
                metrics['quantile_loss'] = float(np.nanmean(all_ql))

        # ---- AIC / BIC from training residuals ----
        if train_residuals_all:
            residuals = np.array(train_residuals_all)
            n_obs = len(residuals)
            # Average n_params per horizon model
            n_steps_with_val = len(val_actuals_per_h) or 1
            n_params = max(n_params_total // n_steps_with_val, 1)
            sigma2 = np.var(residuals)
            if sigma2 > 0 and n_obs > 0:
                log_lik = -0.5 * n_obs * (np.log(2 * np.pi * sigma2) + 1)
                metrics['aic'] = float(-2 * log_lik + 2 * n_params)
                metrics['bic'] = float(-2 * log_lik + n_params * np.log(n_obs))
                if n_obs - n_params - 1 > 0:
                    metrics['aicc'] = float(
                        metrics['aic'] + (2 * n_params * (n_params + 1)) / (n_obs - n_params - 1)
                    )

        metrics['metric_source'] = 'internal_validation'
        return metrics

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
                                 overrides_map: dict = None,
                                 show_progress: bool = True) -> pd.DataFrame:
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

        for _, char_row in tqdm(valid_chars.iterrows(),
                                total=len(valid_chars),
                                desc="  ML forecasting",
                                unit="series",
                                disable=not show_progress):
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

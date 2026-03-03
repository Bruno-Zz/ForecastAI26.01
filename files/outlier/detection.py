"""
Outlier Detection and Correction Module

Detects and optionally corrects outliers in time series demand data.
Supports three detection methods (IQR, Z-Score, STL Residuals) and
four correction methods (clip, interpolate, median, none).

All parameters are configurable via config.yaml under 'outlier_detection'.
"""

import pandas as pd
import numpy as np
from typing import Tuple, Optional, Dict, Any
from pathlib import Path
import logging
import yaml

logger = logging.getLogger(__name__)


class OutlierDetector:
    """
    Configurable outlier detection and correction for time series data.

    Detection methods:
        - iqr: Interquartile Range (flag values outside Q1-k*IQR / Q3+k*IQR)
        - zscore: Modified Z-Score using median/MAD (or mean/std)
        - stl_residuals: STL decomposition residuals exceeding threshold

    Correction methods:
        - clip: Cap at upper/lower bounds
        - interpolate: Linear interpolation from neighbours
        - median: Replace with rolling-window median
        - none: Flag only, no correction
    """

    def __init__(self, config_path: str = "config/config.yaml", config_override: dict = None):
        with open(config_path, 'r') as f:
            full_config = yaml.safe_load(f)
        if config_override:
            from utils.parameter_resolver import ParameterResolver
            full_config = ParameterResolver.deep_merge(full_config, config_override)

        self.config = full_config.get('outlier_detection', {})
        self.enabled = self.config.get('enabled', True)
        self.detection_method = self.config.get('detection_method', 'iqr')
        self.correction_method = self.config.get('correction_method', 'clip')
        self.min_observations = self.config.get('min_observations', 6)

        # Method-specific parameters
        self.iqr_config = self.config.get('iqr', {})
        self.zscore_config = self.config.get('zscore', {})
        self.stl_config = self.config.get('stl_residuals', {})
        self.correction_config = self.config.get('correction', {})

        logger.info(
            f"OutlierDetector initialized: detection={self.detection_method}, "
            f"correction={self.correction_method}, enabled={self.enabled}"
        )

    # =========================================================================
    # Detection methods
    # =========================================================================

    def _detect_iqr(self, values: np.ndarray) -> Tuple[np.ndarray, float, float]:
        """
        IQR-based outlier detection.

        Returns:
            (is_outlier mask, lower_bound, upper_bound)
        """
        k = self.iqr_config.get('multiplier', 1.5)
        q1 = np.nanpercentile(values, 25)
        q3 = np.nanpercentile(values, 75)
        iqr = q3 - q1

        lower = q1 - k * iqr
        upper = q3 + k * iqr

        is_outlier = (values < lower) | (values > upper)
        return is_outlier, lower, upper

    def _detect_zscore(self, values: np.ndarray) -> Tuple[np.ndarray, float, float]:
        """
        Modified Z-Score outlier detection (using median/MAD or mean/std).

        Returns:
            (is_outlier mask, lower_bound, upper_bound)
        """
        threshold = self.zscore_config.get('threshold', 3.0)
        use_mad = self.zscore_config.get('use_mad', True)

        if use_mad:
            median = np.nanmedian(values)
            mad = np.nanmedian(np.abs(values - median))
            # Scale factor for normal distribution consistency
            mad_scaled = mad * 1.4826 if mad > 0 else np.nanstd(values)
            if mad_scaled == 0:
                return np.zeros(len(values), dtype=bool), -np.inf, np.inf
            z_scores = (values - median) / mad_scaled
            center = median
            spread = mad_scaled
        else:
            mean = np.nanmean(values)
            std = np.nanstd(values)
            if std == 0:
                return np.zeros(len(values), dtype=bool), -np.inf, np.inf
            z_scores = (values - mean) / std
            center = mean
            spread = std

        is_outlier = np.abs(z_scores) > threshold
        lower = center - threshold * spread
        upper = center + threshold * spread

        return is_outlier, lower, upper

    def _detect_stl_residuals(self, values: np.ndarray) -> Tuple[np.ndarray, float, float]:
        """
        STL decomposition residual-based detection.
        Falls back to IQR if series is too short for STL.

        Returns:
            (is_outlier mask, lower_bound, upper_bound)
        """
        seasonal_period = self.stl_config.get('seasonal_period', 12)
        residual_threshold = self.stl_config.get('residual_threshold', 3.0)

        # STL requires at least 2 full seasonal cycles
        if len(values) < 2 * seasonal_period:
            logger.debug("Series too short for STL, falling back to IQR")
            return self._detect_iqr(values)

        try:
            from statsmodels.tsa.seasonal import STL

            # Handle NaNs by interpolating temporarily
            series = pd.Series(values)
            if series.isna().any():
                series = series.interpolate(method='linear').fillna(method='bfill').fillna(method='ffill')

            stl = STL(series, period=seasonal_period, robust=True)
            result = stl.fit()
            residuals = result.resid.values

            # Detect outliers in residuals using modified z-score
            median_r = np.nanmedian(residuals)
            mad_r = np.nanmedian(np.abs(residuals - median_r))
            mad_scaled = mad_r * 1.4826 if mad_r > 0 else np.nanstd(residuals)

            if mad_scaled == 0:
                return np.zeros(len(values), dtype=bool), -np.inf, np.inf

            z_scores = np.abs((residuals - median_r) / mad_scaled)
            is_outlier = z_scores > residual_threshold

            # Bounds are approximate (in original scale)
            lower = np.nanpercentile(values, 1)
            upper = np.nanpercentile(values, 99)

            return is_outlier, lower, upper

        except Exception as e:
            logger.warning(f"STL failed: {e}, falling back to IQR")
            return self._detect_iqr(values)

    def _detect(self, values: np.ndarray) -> Tuple[np.ndarray, float, float]:
        """Route to configured detection method."""
        if self.detection_method == 'iqr':
            return self._detect_iqr(values)
        elif self.detection_method == 'zscore':
            return self._detect_zscore(values)
        elif self.detection_method == 'stl_residuals':
            return self._detect_stl_residuals(values)
        else:
            logger.warning(f"Unknown detection method '{self.detection_method}', using IQR")
            return self._detect_iqr(values)

    # =========================================================================
    # Correction methods
    # =========================================================================

    def _correct_clip(self, values: np.ndarray, is_outlier: np.ndarray,
                      lower: float, upper: float) -> np.ndarray:
        """Clip outlier values to bounds."""
        corrected = values.copy()
        corrected[is_outlier & (values < lower)] = lower
        corrected[is_outlier & (values > upper)] = upper
        return corrected

    def _correct_interpolate(self, values: np.ndarray, is_outlier: np.ndarray,
                             lower: float, upper: float) -> np.ndarray:
        """Replace outliers with linear interpolation from neighbours."""
        method = self.correction_config.get('interpolation_method', 'linear')
        corrected = pd.Series(values.copy())
        corrected[is_outlier] = np.nan
        corrected = corrected.interpolate(method=method)
        # Fill edges if first/last values are outliers
        corrected = corrected.fillna(method='bfill').fillna(method='ffill')
        return corrected.values

    def _correct_median(self, values: np.ndarray, is_outlier: np.ndarray,
                        lower: float, upper: float) -> np.ndarray:
        """Replace outliers with rolling-window median."""
        window = self.correction_config.get('median_window', 5)
        rolling_med = pd.Series(values).rolling(window=window, center=True, min_periods=1).median()
        corrected = values.copy()
        corrected[is_outlier] = rolling_med.values[is_outlier]
        return corrected

    def _correct(self, values: np.ndarray, is_outlier: np.ndarray,
                 lower: float, upper: float) -> np.ndarray:
        """Route to configured correction method."""
        if self.correction_method == 'none':
            return values.copy()
        elif self.correction_method == 'clip':
            return self._correct_clip(values, is_outlier, lower, upper)
        elif self.correction_method == 'interpolate':
            return self._correct_interpolate(values, is_outlier, lower, upper)
        elif self.correction_method == 'median':
            return self._correct_median(values, is_outlier, lower, upper)
        else:
            logger.warning(f"Unknown correction method '{self.correction_method}', using clip")
            return self._correct_clip(values, is_outlier, lower, upper)

    # =========================================================================
    # Compute z-scores for reporting (regardless of detection method)
    # =========================================================================

    def _compute_zscores(self, values: np.ndarray) -> np.ndarray:
        """Compute z-scores for reporting purposes."""
        median = np.nanmedian(values)
        mad = np.nanmedian(np.abs(values - median))
        mad_scaled = mad * 1.4826 if mad > 0 else np.nanstd(values)
        if mad_scaled == 0:
            return np.zeros(len(values))
        return (values - median) / mad_scaled

    # =========================================================================
    # Main entry point
    # =========================================================================

    def detect_and_correct_series(self, series_values: np.ndarray,
                                  unique_id: str,
                                  dates: np.ndarray) -> Tuple[np.ndarray, pd.DataFrame]:
        """
        Detect and correct outliers for a single series.

        Args:
            series_values: Array of demand values
            unique_id: Series identifier
            dates: Array of dates corresponding to values

        Returns:
            (corrected_values, outliers_df)
        """
        n = len(series_values)

        # Skip very short series
        if n < self.min_observations:
            return series_values.copy(), pd.DataFrame()

        # Detect
        is_outlier, lower_bound, upper_bound = self._detect(series_values)

        if not np.any(is_outlier):
            return series_values.copy(), pd.DataFrame()

        # Correct
        corrected = self._correct(series_values, is_outlier, lower_bound, upper_bound)

        # Compute z-scores for reporting
        z_scores = self._compute_zscores(series_values)

        # Build outlier log
        outlier_indices = np.where(is_outlier)[0]
        outlier_records = []
        for idx in outlier_indices:
            outlier_records.append({
                'unique_id': unique_id,
                'date': dates[idx],
                'original_value': float(series_values[idx]),
                'corrected_value': float(corrected[idx]),
                'detection_method': self.detection_method,
                'correction_method': self.correction_method,
                'z_score': float(z_scores[idx]),
                'lower_bound': float(lower_bound),
                'upper_bound': float(upper_bound),
            })

        outliers_df = pd.DataFrame(outlier_records)
        return corrected, outliers_df

    def detect_and_correct_all(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Detect and correct outliers across all series in the DataFrame.

        Args:
            df: DataFrame with columns [unique_id, date, y]

        Returns:
            (corrected_df, outliers_df)
                corrected_df: Same shape, with y values adjusted where flagged
                outliers_df: Log of all detections with columns:
                    [unique_id, date, original_value, corrected_value,
                     detection_method, correction_method, z_score,
                     lower_bound, upper_bound]
        """
        if not self.enabled:
            logger.info("Outlier detection is disabled in config")
            return df.copy(), pd.DataFrame()

        logger.info(
            f"Running outlier detection on {df['unique_id'].nunique()} series "
            f"(method={self.detection_method}, correction={self.correction_method})"
        )

        corrected_df = df.copy()
        all_outliers = []
        adjusted_count = 0

        for uid in df['unique_id'].unique():
            mask = df['unique_id'] == uid
            series_data = df.loc[mask].sort_values('date')
            values = series_data['y'].values.astype(float)
            dates = series_data['date'].values

            corrected_values, outliers = self.detect_and_correct_series(values, uid, dates)

            if not outliers.empty:
                all_outliers.append(outliers)
                adjusted_count += 1
                # Update the corrected DataFrame
                corrected_df.loc[mask, 'y'] = corrected_values[
                    np.argsort(series_data.index.values)
                ] if not mask.sum() == len(corrected_values) else corrected_values

                # Safer approach: match by sorted date order
                sorted_idx = series_data.sort_values('date').index
                corrected_df.loc[sorted_idx, 'y'] = corrected_values

        outliers_df = pd.concat(all_outliers, ignore_index=True) if all_outliers else pd.DataFrame()

        total_outliers = len(outliers_df)
        logger.info(
            f"Outlier detection complete: {total_outliers} outliers detected "
            f"across {adjusted_count} series "
            f"(of {df['unique_id'].nunique()} total)"
        )

        return corrected_df, outliers_df

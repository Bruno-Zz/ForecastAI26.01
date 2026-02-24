"""
Time Series Characterization Module
Analyzes each time series and classifies its patterns for forecasting method selection.

Detects: seasonality, trend, intermittency, stationarity, complexity, data sufficiency.
Outputs recommended forecasting methods per series based on detected characteristics.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
import logging
import yaml
from pathlib import Path
from dataclasses import dataclass, asdict, field
import warnings

warnings.filterwarnings('ignore')

# Statistical testing imports
try:
    from statsmodels.tsa.stattools import acf, adfuller
    STATSMODELS_AVAILABLE = True
except ImportError:
    STATSMODELS_AVAILABLE = False
    logging.warning("statsmodels not available. Install with: pip install statsmodels")

try:
    from scipy import stats as scipy_stats
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False
    logging.warning("scipy not available. Install with: pip install scipy")


@dataclass
class SeriesCharacteristics:
    """Container for all characteristics of a single time series."""
    # Identity
    unique_id: str

    # Basic statistics
    n_observations: int = 0
    date_range_start: Optional[str] = None
    date_range_end: Optional[str] = None
    mean: float = np.nan
    std: float = np.nan

    # Seasonality
    has_seasonality: bool = False
    seasonal_periods: List[int] = field(default_factory=list)
    seasonal_strength: float = 0.0

    # Trend
    has_trend: bool = False
    trend_direction: str = 'none'  # 'up', 'down', 'none'
    trend_strength: float = 0.0

    # Intermittency
    is_intermittent: bool = False
    zero_ratio: float = 0.0
    adi: float = 0.0  # Average Demand Interval
    cov: float = 0.0  # Coefficient of Variation

    # Stationarity
    is_stationary: bool = False
    adf_pvalue: float = np.nan

    # Complexity
    complexity_score: float = 0.0
    complexity_level: str = 'low'  # 'low', 'medium', 'high'

    # Data sufficiency
    sufficient_for_ml: bool = False
    sufficient_for_deep_learning: bool = False

    # Method recommendation
    recommended_methods: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for DataFrame construction."""
        return asdict(self)


def _mann_kendall_test(values: np.ndarray, significance_level: float = 0.05) -> Tuple[bool, str, float]:
    """
    Perform the Mann-Kendall trend test.

    The Mann-Kendall test is a non-parametric test for monotonic trend in a
    time series. It computes the S statistic (sum of signs of differences
    between all later and earlier values), derives its variance under the null
    hypothesis of no trend, and converts to a z-score for hypothesis testing.

    Args:
        values: Time series values as a 1-D numpy array.
        significance_level: Threshold for the two-sided p-value.

    Returns:
        Tuple of (has_trend, direction, strength):
            has_trend -- True if the null hypothesis of no trend is rejected.
            direction -- 'up', 'down', or 'none'.
            strength  -- Kendall's tau correlation coefficient in [0, 1].
    """
    n = len(values)
    if n < 10:
        return False, 'none', 0.0

    # Compute the S statistic
    s = 0
    for k in range(n - 1):
        for j in range(k + 1, n):
            diff = values[j] - values[k]
            if diff > 0:
                s += 1
            elif diff < 0:
                s -= 1

    # Number of possible pairs
    n_pairs = n * (n - 1) / 2

    # Kendall's tau as a normalised trend strength
    tau = abs(s) / n_pairs if n_pairs > 0 else 0.0

    # Variance of S (corrected for ties)
    unique_vals, tie_counts = np.unique(values, return_counts=True)
    tie_correction = 0
    for t in tie_counts:
        if t > 1:
            tie_correction += t * (t - 1) * (2 * t + 5)

    var_s = (n * (n - 1) * (2 * n + 5) - tie_correction) / 18

    if var_s == 0:
        return False, 'none', 0.0

    # z-score
    if s > 0:
        z = (s - 1) / np.sqrt(var_s)
    elif s < 0:
        z = (s + 1) / np.sqrt(var_s)
    else:
        z = 0.0

    # Two-sided p-value from standard normal
    p_value = 2 * (1 - scipy_stats.norm.cdf(abs(z))) if SCIPY_AVAILABLE else 1.0

    has_trend = p_value < significance_level
    if has_trend:
        direction = 'up' if s > 0 else 'down'
    else:
        direction = 'none'

    return has_trend, direction, tau


class TimeSeriesCharacterizer:
    """
    Analyzes a collection of time series and classifies each one's patterns
    (seasonality, trend, intermittency, stationarity, complexity) to drive
    automatic forecasting method selection.

    Expected input DataFrame format (Nixtla convention):
        unique_id | ds (datetime) | y (numeric)
    """

    def __init__(self, config_path: str = "config/config.yaml"):
        """
        Initialize the characterizer from a YAML configuration file.

        Args:
            config_path: Path to the project config.yaml.
        """
        self.config_path = config_path
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)

        self.char_config = self.config['characterization']
        self.method_selection = self.config['forecasting']['method_selection']
        self.output_config = self.config.get('output', {})
        self.logger = logging.getLogger(__name__)

        # Unpack sub-configs for convenience
        self.seasonality_cfg = self.char_config['seasonality']
        self.trend_cfg = self.char_config['trend']
        self.intermittency_cfg = self.char_config['intermittency']
        self.stationarity_cfg = self.char_config['stationarity']
        self.sufficiency_cfg = self.char_config['data_sufficiency']

        # Validate that required libraries are present
        if not STATSMODELS_AVAILABLE:
            raise ImportError(
                "statsmodels is required for characterization. "
                "Install with: pip install statsmodels"
            )
        if not SCIPY_AVAILABLE:
            raise ImportError(
                "scipy is required for characterization. "
                "Install with: pip install scipy"
            )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def analyze_all(self,
                    df: pd.DataFrame,
                    id_col: str = 'unique_id',
                    date_col: str = 'ds',
                    value_col: str = 'y',
                    save: bool = True) -> pd.DataFrame:
        """
        Analyze every time series in the DataFrame.

        Args:
            df: Long-format DataFrame with columns [id_col, date_col, value_col].
            id_col: Column containing the series identifier.
            date_col: Column containing the date / timestamp.
            value_col: Column containing the numeric values.
            save: If True, persist results to the time_series_characteristics DB table.

        Returns:
            DataFrame where each row describes one time series.
        """
        unique_ids = df[id_col].unique()
        n_series = len(unique_ids)
        self.logger.info(f"Starting characterization of {n_series} time series")

        results: List[Dict[str, Any]] = []

        for idx, uid in enumerate(unique_ids):
            if (idx + 1) % 100 == 0 or (idx + 1) == n_series:
                self.logger.info(
                    f"  Characterizing series {idx + 1}/{n_series} ({uid})"
                )

            series_df = (
                df[df[id_col] == uid]
                .sort_values(date_col)
                .reset_index(drop=True)
            )

            chars = self.analyze_single(
                series_df=series_df,
                unique_id=uid,
                date_col=date_col,
                value_col=value_col,
            )
            results.append(chars.to_dict())

        characteristics_df = pd.DataFrame(results)

        # Logging summary
        self._log_summary(characteristics_df)

        # Persist to PostgreSQL
        if save:
            try:
                from db.db import bulk_insert, jsonb_serialize, get_schema
                schema = get_schema(self.config_path)
                c_cols = [
                    "unique_id", "n_observations", "date_range_start", "date_range_end",
                    "mean", "std", "has_seasonality", "seasonal_periods", "seasonal_strength",
                    "has_trend", "trend_direction", "trend_strength",
                    "is_intermittent", "zero_ratio", "adi", "cov",
                    "is_stationary", "adf_pvalue",
                    "complexity_score", "complexity_level",
                    "sufficient_for_ml", "sufficient_for_deep_learning",
                    "recommended_methods",
                ]
                c_rows = []
                for _, r in characteristics_df.iterrows():
                    row = []
                    for c in c_cols:
                        v = r.get(c)
                        if c in ("seasonal_periods", "recommended_methods"):
                            row.append(jsonb_serialize(v))
                        elif c in ("has_seasonality", "has_trend", "is_intermittent",
                                   "is_stationary", "sufficient_for_ml", "sufficient_for_deep_learning"):
                            row.append(bool(v) if pd.notna(v) else None)
                        elif c in ("n_observations",):
                            row.append(int(v) if pd.notna(v) else None)
                        elif c in ("unique_id", "date_range_start", "date_range_end",
                                   "trend_direction", "complexity_level"):
                            row.append(str(v) if v is not None else None)
                        else:
                            row.append(float(v) if pd.notna(v) else None)
                    c_rows.append(tuple(row))
                bulk_insert(self.config_path, f"{schema}.time_series_characteristics", c_cols, c_rows)
                self.logger.info(f"Characteristics saved to DB ({len(c_rows)} rows)")
            except Exception as e:
                self.logger.warning(f"DB save failed, skipping: {e}")

        return characteristics_df

    def analyze_single(self,
                       series_df: pd.DataFrame,
                       unique_id: str,
                       date_col: str = 'ds',
                       value_col: str = 'y') -> SeriesCharacteristics:
        """
        Analyze a single time series and return its characteristics.

        Args:
            series_df: DataFrame for one series with at least [date_col, value_col].
            unique_id: Identifier for this series.
            date_col: Name of the date column.
            value_col: Name of the value column.

        Returns:
            A populated SeriesCharacteristics dataclass.
        """
        chars = SeriesCharacteristics(unique_id=unique_id)

        values = series_df[value_col].values.astype(float)
        dates = pd.to_datetime(series_df[date_col])

        # ---- Basic statistics ----
        chars.n_observations = len(values)
        chars.date_range_start = str(dates.min())
        chars.date_range_end = str(dates.max())
        chars.mean = float(np.nanmean(values))
        chars.std = float(np.nanstd(values))

        # Guard: skip deeper analysis if series is too short
        if chars.n_observations < 4:
            chars.recommended_methods = self._recommend_methods(chars)
            return chars

        # ---- Seasonality ----
        self._detect_seasonality(values, chars, dates)

        # ---- Trend ----
        self._detect_trend(values, chars)

        # ---- Intermittency ----
        self._detect_intermittency(values, chars)

        # ---- Stationarity ----
        self._detect_stationarity(values, chars)

        # ---- Complexity ----
        self._compute_complexity(values, chars)

        # ---- Data sufficiency ----
        self._assess_sufficiency(chars)

        # ---- Method recommendation ----
        chars.recommended_methods = self._recommend_methods(chars)

        return chars

    # ------------------------------------------------------------------
    # Seasonality
    # ------------------------------------------------------------------

    def _detect_seasonality(
        self,
        values: np.ndarray,
        chars: SeriesCharacteristics,
        dates: pd.Series,
    ) -> None:
        """
        Detect seasonality using the autocorrelation function.

        Seasonality is only tested when the date range spans at least 2 years
        (i.e. last observation is at least 2 years after the first). For shorter
        histories there is not enough data to distinguish seasonal cycles from
        noise, so has_seasonality is left False.

        For each candidate period in config.seasonality.test_periods, compute
        the ACF at that lag. If the ACF value exceeds min_strength, the period
        is considered seasonal. The overall seasonal_strength is the maximum
        ACF among detected seasonal lags.
        """
        # Gate: require at least 2 years of history
        date_span_days = (dates.max() - dates.min()).days
        min_days_for_seasonality = 2 * 365
        if date_span_days < min_days_for_seasonality:
            chars.has_seasonality = False
            chars.seasonal_periods = []
            chars.seasonal_strength = 0.0
            return

        test_periods: List[int] = self.seasonality_cfg['test_periods']
        min_strength: float = self.seasonality_cfg['min_strength']

        n = len(values)
        detected_periods: List[int] = []
        max_strength: float = 0.0

        # We need at least 2 * max_lag observations
        max_testable_lag = n // 2

        for period in test_periods:
            if period >= max_testable_lag or period < 2:
                continue

            try:
                nlags = min(period + 1, n - 1)
                acf_values = acf(values, nlags=nlags, fft=True, missing='conservative')
                strength = abs(acf_values[period])

                if strength > min_strength:
                    detected_periods.append(period)
                    max_strength = max(max_strength, strength)
            except Exception as e:
                self.logger.debug(f"ACF failed for period {period}: {e}")
                continue

        chars.has_seasonality = len(detected_periods) > 0
        chars.seasonal_periods = sorted(detected_periods)
        chars.seasonal_strength = float(max_strength)

    # ------------------------------------------------------------------
    # Trend
    # ------------------------------------------------------------------

    def _detect_trend(self, values: np.ndarray, chars: SeriesCharacteristics) -> None:
        """
        Detect monotonic trend using the Mann-Kendall test.

        Stores has_trend, trend_direction ('up'/'down'/'none'), and
        trend_strength (Kendall's tau, 0-1).
        """
        significance = self.trend_cfg.get('significance_level', 0.05)
        method = self.trend_cfg.get('method', 'mann_kendall')

        if method == 'mann_kendall':
            has_trend, direction, strength = _mann_kendall_test(
                values, significance_level=significance
            )
        elif method == 'linear_regression':
            has_trend, direction, strength = self._linear_regression_trend(
                values, significance_level=significance
            )
        else:
            self.logger.warning(f"Unknown trend method '{method}', defaulting to mann_kendall")
            has_trend, direction, strength = _mann_kendall_test(
                values, significance_level=significance
            )

        chars.has_trend = has_trend
        chars.trend_direction = direction
        chars.trend_strength = float(strength)

    @staticmethod
    def _linear_regression_trend(
        values: np.ndarray, significance_level: float = 0.05
    ) -> Tuple[bool, str, float]:
        """
        Fallback trend detection via ordinary least-squares linear regression.

        Returns:
            (has_trend, direction, r_squared)
        """
        n = len(values)
        if n < 10:
            return False, 'none', 0.0

        x = np.arange(n, dtype=float)
        slope, intercept, r_value, p_value, std_err = scipy_stats.linregress(x, values)

        has_trend = p_value < significance_level
        if has_trend:
            direction = 'up' if slope > 0 else 'down'
        else:
            direction = 'none'

        return has_trend, direction, float(r_value ** 2)

    # ------------------------------------------------------------------
    # Intermittency
    # ------------------------------------------------------------------

    def _detect_intermittency(self, values: np.ndarray, chars: SeriesCharacteristics) -> None:
        """
        Detect intermittent demand patterns.

        Computes:
            zero_ratio  -- proportion of zero values.
            ADI         -- average number of periods between non-zero demands.
            CoV         -- coefficient of variation of non-zero demands.

        is_intermittent is flagged when the total number of periods (weeks)
        with positive demand across the complete horizon is fewer than 5.
        """
        n = len(values)
        n_zeros = int(np.sum(values == 0))
        chars.zero_ratio = float(n_zeros / n) if n > 0 else 0.0

        # ADI: average interval between consecutive non-zero observations
        nonzero_indices = np.where(values != 0)[0]
        if len(nonzero_indices) >= 2:
            intervals = np.diff(nonzero_indices).astype(float)
            chars.adi = float(np.mean(intervals))
        elif len(nonzero_indices) == 1:
            # Only one non-zero value: ADI is essentially the whole series length
            chars.adi = float(n)
        else:
            # All zeros
            chars.adi = float(n)

        # CoV of non-zero demand values
        nonzero_values = values[values != 0]
        if len(nonzero_values) > 1 and np.mean(nonzero_values) != 0:
            chars.cov = float(np.std(nonzero_values, ddof=1) / np.mean(nonzero_values))
        else:
            chars.cov = 0.0

        # Sparsity rule: sparse (intermittent) when fewer than 5 periods have
        # positive demand over the complete horizon.
        n_periods_with_demand = int(np.sum(values > 0))
        chars.is_intermittent = n_periods_with_demand < 5

    # ------------------------------------------------------------------
    # Stationarity
    # ------------------------------------------------------------------

    def _detect_stationarity(self, values: np.ndarray, chars: SeriesCharacteristics) -> None:
        """
        Test for stationarity using the Augmented Dickey-Fuller test.

        The ADF null hypothesis is that a unit root is present (non-stationary).
        A p-value below significance_level rejects the null, meaning the series
        is stationary.
        """
        significance = self.stationarity_cfg.get('significance_level', 0.05)

        try:
            # maxlag=None lets statsmodels choose automatically
            result = adfuller(values, autolag='AIC')
            adf_statistic = result[0]
            p_value = result[1]

            chars.adf_pvalue = float(p_value)
            chars.is_stationary = p_value < significance

        except Exception as e:
            self.logger.debug(f"ADF test failed for {chars.unique_id}: {e}")
            chars.adf_pvalue = np.nan
            chars.is_stationary = False

    # ------------------------------------------------------------------
    # Complexity scoring
    # ------------------------------------------------------------------

    def _compute_complexity(self, values: np.ndarray, chars: SeriesCharacteristics) -> None:
        """
        Compute a composite complexity score in [0, 1] from multiple factors.

        Factors (each normalised to 0-1):
            1. Coefficient of variation (higher = more complex)
            2. Entropy of the first differences (approximation of predictability)
            3. Turning points ratio (fraction of local extrema)
            4. Non-stationarity indicator
            5. Seasonal strength (higher seasonality can mean more complex modelling)
            6. Intermittency presence

        Weights are fixed but calibrated to provide reasonable separation.
        """
        n = len(values)

        # Factor 1: Normalised coefficient of variation (cap at 1)
        overall_mean = np.mean(values)
        overall_std = np.std(values)
        if overall_mean != 0:
            cv_score = min(abs(overall_std / overall_mean), 3.0) / 3.0
        else:
            cv_score = 1.0  # constant zero series are trivially complex

        # Factor 2: Approximate sample entropy of first differences
        if n > 2:
            diffs = np.diff(values)
            diff_std = np.std(diffs)
            if diff_std > 0:
                # Normalise diffs and compute histogram-based entropy
                normed = diffs / diff_std
                hist_counts, _ = np.histogram(normed, bins=min(20, max(5, n // 5)))
                hist_probs = hist_counts / hist_counts.sum()
                hist_probs = hist_probs[hist_probs > 0]
                entropy = -np.sum(hist_probs * np.log2(hist_probs))
                # Normalise by log2(n_bins) to get [0, 1]
                max_entropy = np.log2(len(hist_counts))
                entropy_score = float(entropy / max_entropy) if max_entropy > 0 else 0.0
            else:
                entropy_score = 0.0  # constant differences = low complexity
        else:
            entropy_score = 0.0

        # Factor 3: Turning points ratio
        if n > 2:
            diffs = np.diff(values)
            sign_changes = np.sum(diffs[:-1] * diffs[1:] < 0)
            max_possible = n - 2
            turning_ratio = float(sign_changes / max_possible) if max_possible > 0 else 0.0
        else:
            turning_ratio = 0.0

        # Factor 4: Non-stationarity (binary, already computed)
        stationarity_score = 0.0 if chars.is_stationary else 1.0

        # Factor 5: Seasonal strength (already [0, 1])
        seasonal_score = min(chars.seasonal_strength, 1.0)

        # Factor 6: Intermittency (binary)
        intermittency_score = 1.0 if chars.is_intermittent else 0.0

        # Weighted composite
        weights = {
            'cv': 0.20,
            'entropy': 0.25,
            'turning': 0.15,
            'stationarity': 0.15,
            'seasonal': 0.10,
            'intermittency': 0.15,
        }

        composite = (
            weights['cv'] * cv_score
            + weights['entropy'] * entropy_score
            + weights['turning'] * turning_ratio
            + weights['stationarity'] * stationarity_score
            + weights['seasonal'] * seasonal_score
            + weights['intermittency'] * intermittency_score
        )

        chars.complexity_score = float(np.clip(composite, 0.0, 1.0))

        # Classification
        if chars.complexity_score < 0.35:
            chars.complexity_level = 'low'
        elif chars.complexity_score < 0.65:
            chars.complexity_level = 'medium'
        else:
            chars.complexity_level = 'high'

    # ------------------------------------------------------------------
    # Data sufficiency
    # ------------------------------------------------------------------

    def _assess_sufficiency(self, chars: SeriesCharacteristics) -> None:
        """
        Determine whether the series has enough observations for various
        modelling families.
        """
        min_ml = self.sufficiency_cfg.get('min_for_ml', 100)
        min_dl = self.sufficiency_cfg.get('min_for_deep_learning', 200)

        chars.sufficient_for_ml = chars.n_observations >= min_ml
        chars.sufficient_for_deep_learning = chars.n_observations >= min_dl

    # ------------------------------------------------------------------
    # Method recommendation
    # ------------------------------------------------------------------

    def _recommend_methods(self, chars: SeriesCharacteristics) -> List[str]:
        """
        Select forecasting methods based on detected characteristics.

        Decision logic (in priority order):
            1. Sparse data (< 5 obs/year on average) -> sparse_data methods.
            2. Intermittent demand -> intermittent methods.
            3. High complexity -> complex methods (filtered by data sufficiency).
            4. Seasonal patterns -> seasonal methods (filtered by data sufficiency).
            5. Otherwise -> standard methods.

        Deep-learning and ML methods are excluded when data is insufficient.
        """
        sparse_obs_per_year = self.sufficiency_cfg.get('sparse_obs_per_year', 5)

        # Methods that require substantial data
        ml_methods = {'LightGBM', 'XGBoost'}
        dl_methods = {'NHITS', 'NBEATS', 'PatchTST', 'TFT', 'DeepAR'}

        # ----- 1. Sparse data: fewer than 5 observations per year on average -----
        # Compute the span in years from the stored date range; fall back to
        # a period-count estimate if dates are unavailable.
        is_sparse = False
        try:
            if chars.date_range_start and chars.date_range_end:
                start = pd.Timestamp(chars.date_range_start)
                end   = pd.Timestamp(chars.date_range_end)
                span_years = max((end - start).days / 365.25, 1 / 12)
                obs_per_year = chars.n_observations / span_years
                is_sparse = obs_per_year < sparse_obs_per_year
            else:
                # Fallback: assume monthly — fewer than 5 obs means < 5 months
                is_sparse = chars.n_observations < sparse_obs_per_year
        except Exception:
            is_sparse = chars.n_observations < sparse_obs_per_year

        if is_sparse:
            methods = list(self.method_selection.get('sparse_data', []))
            return self._filter_by_sufficiency(methods, chars, ml_methods, dl_methods)

        # ----- 2. Intermittent demand -----
        if chars.is_intermittent:
            methods = list(self.method_selection.get('intermittent', []))
            return self._filter_by_sufficiency(methods, chars, ml_methods, dl_methods)

        # ----- 3. High complexity -----
        if chars.complexity_level == 'high':
            methods = list(self.method_selection.get('complex', []))
            return self._filter_by_sufficiency(methods, chars, ml_methods, dl_methods)

        # ----- 4. Seasonal -----
        if chars.has_seasonality:
            methods = list(self.method_selection.get('seasonal', []))
            return self._filter_by_sufficiency(methods, chars, ml_methods, dl_methods)

        # ----- 5. Standard -----
        methods = list(self.method_selection.get('standard', []))
        return self._filter_by_sufficiency(methods, chars, ml_methods, dl_methods)

    @staticmethod
    def _filter_by_sufficiency(
        methods: List[str],
        chars: SeriesCharacteristics,
        ml_methods: set,
        dl_methods: set,
    ) -> List[str]:
        """Remove methods whose data requirements are not met."""
        filtered: List[str] = []
        for m in methods:
            if m in dl_methods and not chars.sufficient_for_deep_learning:
                continue
            if m in ml_methods and not chars.sufficient_for_ml:
                continue
            filtered.append(m)

        # Guarantee at least one method
        if not filtered:
            filtered = ['HistoricAverage']

        return filtered

    # ------------------------------------------------------------------
    # Logging helpers
    # ------------------------------------------------------------------

    def _log_summary(self, df: pd.DataFrame) -> None:
        """Log a human-readable summary of the characterization results."""
        n = len(df)
        self.logger.info("=" * 70)
        self.logger.info("TIME SERIES CHARACTERIZATION SUMMARY")
        self.logger.info("=" * 70)
        self.logger.info(f"Total series analyzed        : {n}")
        self.logger.info(
            f"With seasonality             : {df['has_seasonality'].sum()} "
            f"({df['has_seasonality'].mean() * 100:.1f}%)"
        )
        self.logger.info(
            f"With trend                   : {df['has_trend'].sum()} "
            f"({df['has_trend'].mean() * 100:.1f}%)"
        )
        self.logger.info(
            f"Intermittent                 : {df['is_intermittent'].sum()} "
            f"({df['is_intermittent'].mean() * 100:.1f}%)"
        )
        self.logger.info(
            f"Stationary                   : {df['is_stationary'].sum()} "
            f"({df['is_stationary'].mean() * 100:.1f}%)"
        )
        self.logger.info(
            f"Sufficient for ML (>={self.sufficiency_cfg.get('min_for_ml', 100)})  : "
            f"{df['sufficient_for_ml'].sum()} "
            f"({df['sufficient_for_ml'].mean() * 100:.1f}%)"
        )
        self.logger.info(
            f"Sufficient for DL (>={self.sufficiency_cfg.get('min_for_deep_learning', 200)}) : "
            f"{df['sufficient_for_deep_learning'].sum()} "
            f"({df['sufficient_for_deep_learning'].mean() * 100:.1f}%)"
        )

        # Complexity distribution
        if 'complexity_level' in df.columns:
            complexity_counts = df['complexity_level'].value_counts()
            self.logger.info("Complexity distribution:")
            for level in ['low', 'medium', 'high']:
                count = complexity_counts.get(level, 0)
                pct = count / n * 100 if n > 0 else 0
                self.logger.info(f"  {level:>8s}: {count:>5d} ({pct:.1f}%)")

        self.logger.info("=" * 70)


# ======================================================================
# Convenience entry point
# ======================================================================

def main():
    """
    Standalone entry point: load time series from DB, characterize,
    and save results.
    """
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    )
    logger = logging.getLogger(__name__)

    # Paths (relative to project root)
    config_path = 'config/config.yaml'

    # Load configuration
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    etl_config = config.get('etl', {})
    query_config = etl_config.get('query', {})
    id_col = query_config.get('id_column', 'unique_id')
    date_col = query_config.get('date_column', 'ds')
    value_col = query_config.get('value_column', 'y')

    # Load data from PostgreSQL
    from db.db import load_table, get_schema
    schema = get_schema(config_path)
    logger.info(f"Loading time series data from {schema}.demand_actuals")
    df = load_table(
        config_path,
        f"{schema}.demand_actuals",
        columns="unique_id, date, COALESCE(corrected_qty, qty) AS y",
    )
    df['date'] = pd.to_datetime(df['date'])
    logger.info(
        f"Loaded {len(df)} rows, "
        f"{df['unique_id'].nunique()} unique series"
    )

    # Rename columns to canonical names if needed
    rename_map = {}
    if id_col != 'unique_id':
        rename_map[id_col] = 'unique_id'
    if date_col != 'ds':
        rename_map[date_col] = 'ds'
    if value_col != 'y':
        rename_map[value_col] = 'y'
    if rename_map:
        df = df.rename(columns=rename_map)

    # Characterize
    characterizer = TimeSeriesCharacterizer(config_path=config_path)
    characteristics_df = characterizer.analyze_all(
        df=df,
        id_col='unique_id',
        date_col='ds',
        value_col='y',
        save=True,
    )

    # Print quick summary to console
    print(f"\nCharacterization complete for {len(characteristics_df)} time series.")
    print(characteristics_df[
        ['unique_id', 'n_observations', 'has_seasonality', 'has_trend',
         'is_intermittent', 'complexity_level', 'recommended_methods']
    ].head(20).to_string(index=False))


if __name__ == '__main__':
    main()

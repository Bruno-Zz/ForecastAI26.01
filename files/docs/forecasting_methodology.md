# ForecastAI -- Forecasting Methodology
## Detailed Technical Reference

**Version**: 2026.01
**Date**: March 2026

---

## Table of Contents

1. [Pipeline Overview](#1-pipeline-overview)
2. [Time Series Characterization](#2-time-series-characterization)
3. [Method Selection Logic](#3-method-selection-logic)
4. [Hyperparameter Selection](#4-hyperparameter-selection)
   - 4.1 Statistical Models
   - 4.2 Machine Learning Models
   - 4.3 Neural Network Models
   - 4.4 Foundation Models
5. [Backtesting Methodology](#5-backtesting-methodology)
6. [Best Method Selection](#6-best-method-selection)
7. [Parameter Override System](#7-parameter-override-system)
8. [Configuration Reference](#8-configuration-reference)
9. [Edge Cases and Special Handling](#9-edge-cases-and-special-handling)

---

## 1. Pipeline Overview

The forecasting pipeline executes the following steps in strict order. Each step reads from and writes to PostgreSQL.

| Step | Name | Input | Output Table |
|------|------|-------|-------------|
| 1 | ETL | Source database | `demand_actuals` |
| 1b | Outlier Detection | `demand_actuals` | `demand_actuals.corrected_qty`, `detected_outliers` |
| 1c | Segmentation | `demand_actuals` | `segment_membership` |
| 1d | Classification | `demand_actuals` | `abc_results` |
| 2 | Characterization | `demand_actuals` | `time_series_characteristics` |
| 3 | Forecasting | `demand_actuals` + characteristics | `forecast_results` |
| 4 | Backtesting | `demand_actuals` + characteristics | `backtest_metrics`, `forecasts_by_origin` |
| 5 | Best Method Selection | `backtest_metrics` | `best_method_per_series` |
| 6 | Distribution Fitting | `forecast_results` | `fitted_distributions` |

**Parallelization**: Steps 3, 4, and 6 are parallelized via Dask. Series are batched (default: 100 per batch) and submitted as futures to Dask workers. The orchestrator tracks progress via `as_completed()`.

**Parameter-Aware Grouping**: Before steps 1b, 2, 3, and 4, the orchestrator groups series by their assigned parameter sets (from the `series_parameter_assignment` table). Each group is processed with its own component instance constructed with the appropriate configuration override. Series with per-SKU hyperparameter overrides get their own single-element group.

---

## 2. Time Series Characterization

The `TimeSeriesCharacterizer` analyzes each series through six detection stages. The results drive method selection.

### 2.1 Seasonality Detection

**Prerequisite**: The series must span at least 2 years (730 days) of date history. If shorter, `has_seasonality = False` immediately.

**Algorithm**:
1. Test ACF (autocorrelation function) at configured candidate lag periods: `[4, 13, 26, 52]`
2. A period is only testable if `period < n / 2` and `period >= 2`
3. Uses `statsmodels.tsa.stattools.acf(values, nlags=max_period, fft=True, missing='conservative')`
4. A period is seasonal if `|acf_value[period]| > min_strength`
5. `seasonal_strength` = maximum ACF value across all detected periods

| Config Key | Default | Description |
|-----------|---------|-------------|
| `characterization.seasonality.test_periods` | `[4, 13, 26, 52]` | Lag periods to test |
| `characterization.seasonality.min_strength` | `0.3` | Minimum ACF threshold |

### 2.2 Trend Detection

**Method**: Mann-Kendall test (default) or linear regression.

**Mann-Kendall Algorithm**:
1. Requires at least 10 observations; otherwise returns `has_trend = False`
2. Computes S statistic = sum of signs of all pairwise differences `sign(x[j] - x[i])` for all `j > i`
3. Computes variance with tie correction
4. Z-score from normal distribution, two-sided p-value
5. `has_trend = (p_value < significance_level)`
6. `trend_strength` = Kendall's tau = `|S| / (n * (n - 1) / 2)`
7. `trend_direction` = `'increasing'` if S > 0, `'decreasing'` if S < 0

**Linear Regression Fallback**: Uses `scipy.stats.linregress`. Strength = R-squared.

| Config Key | Default | Description |
|-----------|---------|-------------|
| `characterization.trend.method` | `'mann_kendall'` | Detection method |
| `characterization.trend.significance_level` | `0.05` | P-value threshold |

### 2.3 Intermittency Detection

**Computed Metrics**:
- `zero_ratio` = proportion of zero values in the series
- `ADI` (Average Demand Interval) = mean gap between consecutive non-zero observations
- `CoV` = coefficient of variation of non-zero demand values (`std / mean` with `ddof=1`)

**Decision Rule**: `is_intermittent = True` when the total number of periods with positive demand is less than 5.

### 2.4 Stationarity Detection

**Algorithm**: Augmented Dickey-Fuller test via `statsmodels.tsa.stattools.adfuller(values, autolag='AIC')`.

`is_stationary = (p_value < significance_level)`

| Config Key | Default | Description |
|-----------|---------|-------------|
| `characterization.stationarity.significance_level` | `0.05` | P-value threshold |

### 2.5 Complexity Scoring

Six factors, each normalized to [0, 1], combined with fixed weights:

| Factor | Weight | Computation |
|--------|--------|-------------|
| CV (coefficient of variation) | 0.20 | `min(|std / mean|, 3.0) / 3.0` |
| Entropy of first differences | 0.25 | Histogram-based entropy (5-20 bins), normalized by `log2(n_bins)` |
| Turning points ratio | 0.15 | Count of sign changes in differences / `(n - 2)` |
| Non-stationarity | 0.15 | `1.0` if not stationary, `0.0` if stationary |
| Seasonal strength | 0.10 | Clamped to [0, 1] |
| Intermittency | 0.15 | `1.0` if intermittent, else `0.0` |

**Complexity Score** = weighted sum of all factors.

**Classification Thresholds**:
- `complexity_score < 0.35` = **low**
- `0.35 <= complexity_score < 0.65` = **medium**
- `complexity_score >= 0.65` = **high**

### 2.6 Data Sufficiency

| Config Key | Default | Description |
|-----------|---------|-------------|
| `characterization.data_sufficiency.min_for_ml` | `20` | Minimum observations for ML models |
| `characterization.data_sufficiency.min_for_deep_learning` | `30` | Minimum observations for neural models |
| `characterization.data_sufficiency.sparse_obs_per_year` | `5` | Threshold for sparse data classification |

---

## 3. Method Selection Logic

After characterization, each series receives a list of `recommended_methods` based on a strict priority decision tree. The first matching condition determines the category.

### Decision Tree

```
1. SPARSE DATA: if observations_per_year < sparse_obs_per_year (5)
   Methods: SeasonalNaive, HistoricAverage, TimesFM

2. INTERMITTENT: if is_intermittent == True
   Methods: CrostonOptimized, ADIDA, IMAPA, TimesFM

3. HIGH COMPLEXITY: if complexity_level == 'high'
   Methods: NHITS, NBEATS, TFT, PatchTST, AutoETS, LightGBM, XGBoost

4. SEASONAL: if has_seasonality == True
   Methods: MSTL, AutoETS, NHITS, PatchTST, TimesFM, LightGBM

5. STANDARD (default -- none of the above):
   Methods: AutoETS, AutoARIMA, AutoTheta, NHITS, TimesFM, LightGBM
```

`observations_per_year = n_observations / ((end_date - start_date).days / 365.25)`

### Data Sufficiency Filtering

After category selection, methods are filtered based on data sufficiency:

- Methods in `{LightGBM, XGBoost}` are **removed** if `sufficient_for_ml == False`
- Methods in `{NHITS, NBEATS, PatchTST, TFT, DeepAR}` are **removed** if `sufficient_for_deep_learning == False`
- If all methods are filtered out, the fallback is `[HistoricAverage]`

### Configurable Method Lists

All five category lists are configurable in `config.yaml` under `forecasting.method_selection`:

```yaml
forecasting:
  method_selection:
    sparse_data: [SeasonalNaive, HistoricAverage, TimesFM]
    intermittent: [CrostonOptimized, ADIDA, IMAPA, TimesFM]
    seasonal: [MSTL, AutoETS, NHITS, PatchTST, TimesFM, LightGBM]
    complex: [NHITS, NBEATS, TFT, PatchTST, AutoETS, LightGBM, XGBoost]
    standard: [AutoETS, AutoARIMA, AutoTheta, NHITS, TimesFM, LightGBM]
```

---

## 4. Hyperparameter Selection

### 4.1 Statistical Models

All statistical models use the StatsForecast library from Nixtla. Hyperparameters are a combination of **auto-tuning** (model-internal optimization) and **characteristic-driven** defaults.

#### AutoARIMA

**Auto-tuning**: Uses the Hyndman-Khandakar algorithm to search over ARIMA orders `(p,d,q)(P,D,Q)m`, selecting by AICc (corrected Akaike Information Criterion).

| Parameter | Value | Source |
|-----------|-------|--------|
| `season_length` | First detected seasonal period, or frequency map (`W`=52, `M`=12, etc.) | Characteristics |
| `approximation` | `True` if `n_observations > 150`, else `False` | Adaptive |
| `(p,d,q,P,D,Q)` | Auto-selected by AICc minimization | Auto-tuned |

**Overridable parameters**: `p`, `d`, `q`, `P`, `D`, `Q`, `max_p`, `max_q`, `max_P`, `max_Q`, `max_order`, `max_d`, `max_D`, `start_p`, `start_q`, `start_P`, `start_Q`, `stationary`, `seasonal`, `stepwise`, `allowdrift`, `allowmean`. When manual orders are provided, the model switches from `AutoARIMA()` to fixed `ARIMA()`.

**Extracted fitted parameters**: `arma` array (p, q, P, Q, s, d, D), AIC, AICc, BIC.

#### AutoETS (Error-Trend-Seasonal)

**Auto-tuning**: Automatically selects the optimal combination of Error (Additive/Multiplicative), Trend (None/Additive/Damped), and Seasonal (None/Additive/Multiplicative) components by minimizing AICc.

| Parameter | Value | Source |
|-----------|-------|--------|
| `model` | `'ZZZ'` (auto-select all components) | Default |
| `season_length` | Same as AutoARIMA | Characteristics |

**Overridable**: `model`, `damped`, `phi`.
**Extracted fitted parameters**: `alpha`, `beta`, `gamma`, `phi`, `sigma2`, `errortype`, `trendtype`, `seasontype`, `damped`, `aic`, `aicc`, `bic`.

#### AutoTheta

| Parameter | Value | Source |
|-----------|-------|--------|
| `decomposition_type` | `'multiplicative'` if `complexity_level == 'high'`, else `'additive'` | Characteristics |
| `season_length` | Same as AutoARIMA | Characteristics |

**Overridable**: `model` (e.g., `'OptimizedTheta'`, `'DynamicTheta'`).
**Extracted fitted parameters**: `theta`, `alpha`, `drift`.

#### AutoCES (Complex Exponential Smoothing)

| Parameter | Value | Source |
|-----------|-------|--------|
| `season_length` | Same as AutoARIMA | Characteristics |
| `model` | `'Z'` (auto-select) by default | Default |

**Overridable**: `model` (`'N'`=simple, `'P'`=partial, `'F'`=full, `'Z'`=auto).

#### MSTL (Multiple Seasonal-Trend Decomposition using LOESS)

| Parameter | Value | Source |
|-----------|-------|--------|
| `season_length` | Same as AutoARIMA | Characteristics |
| `trend_forecaster` | `AutoARIMA(season_length=1)` | Fixed |

**Minimum data requirement**: `2 * season_length + 1` observations.

#### Intermittent Demand Models

**CrostonOptimized / ADIDA / IMAPA**:
- Uses `ConformalIntervals(n_windows=2, h=horizon)` for prediction intervals when `n_obs >= 2 * horizon + 1`
- Without conformal, produces point forecasts only (no prediction intervals)

**TSB (Teunter-Syntetos-Babai)**:
- `alpha_d = 0.1` (demand smoothing), `alpha_p = 0.1` (probability smoothing)
- Both overridable

#### Simple Benchmark Models

| Model | Parameters |
|-------|-----------|
| SeasonalNaive | `season_length` only |
| SeasonalWindowAverage | `season_length` + `window_size` (default: 2) |
| HistoricAverage | None |
| Naive | None |

#### Pre-Flight Guards

Before fitting, each model checks minimum data requirements:

| Model | Minimum Observations |
|-------|---------------------|
| SeasonalNaive, SeasonalWindowAverage | `>= season_length` |
| HistoricAverage | `>= 2` |
| MSTL | `>= 2 * season_length + 1` |
| AutoETS, AutoARIMA, AutoTheta, AutoCES | `>= max(2 * season_length, 10)` |

If the check fails, the method is skipped for that series with a logged warning.

### 4.2 Machine Learning Models (LightGBM / XGBoost)

**Architecture**: Direct multi-step quantile regression. One model is trained per horizon step, per quantile level.

#### Default Hyperparameters

**LightGBM**:

| Parameter | Default |
|-----------|---------|
| `n_estimators` | 300 |
| `learning_rate` | 0.05 |
| `max_depth` | 6 |
| `num_leaves` | 31 |
| `subsample` | 0.8 |
| `colsample_bytree` | 0.8 |
| `min_child_samples` | 10 |
| `random_state` | 42 |
| `objective` | `'quantile'` (quantile models) or `'regression'` (point) |

**XGBoost**:

| Parameter | Default |
|-----------|---------|
| `n_estimators` | 300 |
| `learning_rate` | 0.05 |
| `max_depth` | 6 |
| `subsample` | 0.8 |
| `colsample_bytree` | 0.8 |
| `min_child_weight` | 10 |
| `random_state` | 42 |
| `objective` | `'reg:quantileerror'` (quantile) or `'reg:squarederror'` (point) |

All parameters are individually overridable via the per-series override system.

#### Feature Engineering

Features are frequency-aware. For weekly data (`W`):

| Feature Type | Details |
|-------------|---------|
| **Lags** | `[1, 2, 4, 8, 13, 26, 52]` + dominant seasonal period lag |
| **Rolling statistics** | Windows: `[4, 8, 13, 26]`; stats: mean, std, min, max |
| **EWM** | Span = 13; stats: mean, std |
| **Calendar** | month, quarter, day_of_week |
| **Trend** | `time_idx`, `time_idx_squared` |

#### Training Process

For each horizon step `h` (1 to `horizon`):
1. Target = `series.shift(-h)` (direct multi-step)
2. Point forecast model: trained with quantile = 0.5 (median)
3. Separate quantile model for each configured quantile level
4. **Early stopping**: `stopping_rounds = 30` for LightGBM with validation set
5. **Validation split**: `ml_val_split: 0.2` from config (overridable per series)
6. Split only applied if `len(X_train) >= 30` and `val_split > 0`
7. Minimum supervised samples per step: 10

**Early Skip Logic**: If `max_trainable_steps < horizon / 2`, the entire method is skipped with a single warning message.

**Post-processing**: Quantile monotonicity enforcement sorts quantile values at each horizon step to ensure lower quantiles never exceed higher ones.

#### Internal Validation Metrics

When a validation set is available, the model computes: MAE, RMSE, bias, MAPE, sMAPE, MASE, coverage at 50/80/90/95%, Winkler score, CRPS, quantile loss, AIC, BIC, AICc from training residuals.

### 4.3 Neural Network Models

All neural models use `MQLoss(level=quantiles)` for multi-quantile regression via the NeuralForecast library from Nixtla.

#### Base Parameters (common to all)

| Parameter | Value |
|-----------|-------|
| `h` | `horizon` (52) |
| `loss` | `MQLoss(level=quantiles)` |
| `max_steps` | `1000` if `n_obs > 200`, else `500` |
| `val_check_steps` | `100` |
| `early_stop_patience_steps` | `3` |

#### NHITS (Neural Hierarchical Interpolation for Time Series)

| Parameter | Low/Medium Complexity | High Complexity |
|-----------|----------------------|-----------------|
| `input_size` | `min(5 * horizon, n_obs // 2)` | Same |
| `n_blocks` | `[1, 1, 1]` | `[3, 3, 3]` |
| `mlp_units` | `[[256, 256]] * 3` | `[[512, 512]] * 3` |
| `n_pool_kernel_size` | `[1, 1, 1]` (non-seasonal) | `[2, 2, 1]` (seasonal) |
| `learning_rate` | 1e-3 | 1e-3 |
| `batch_size` | 32 | 32 |

#### NBEATS (Neural Basis Expansion Analysis)

| Parameter | Low/Medium Complexity | High Complexity |
|-----------|----------------------|-----------------|
| `input_size` | `min(5 * horizon, n_obs // 2)` | Same |
| `n_blocks` | `[2, 2, 2]` | `[3, 3, 3]` |
| `stack_types` | `['identity', 'trend']` (non-seasonal) | `['identity', 'trend', 'seasonality']` (seasonal) |
| `learning_rate` | 1e-3 | 1e-3 |
| `batch_size` | 32 | 32 |

#### PatchTST (Patch Time Series Transformer)

| Parameter | Low/Medium Complexity | High Complexity |
|-----------|----------------------|-----------------|
| `input_size` | `min(10 * horizon, n_obs // 2)` | Same |
| `patch_len` | 16 | 16 |
| `stride` | 8 | 8 |
| `n_layers` | 2 | 3 |
| `d_model` | 64 | 128 |
| `n_heads` | 4 | 8 |
| `learning_rate` | 1e-4 | 1e-4 |

#### TFT (Temporal Fusion Transformer)

| Parameter | Low/Medium Complexity | High Complexity |
|-----------|----------------------|-----------------|
| `input_size` | `min(5 * horizon, n_obs // 2)` | Same |
| `hidden_size` | 64 | 128 |
| `dropout` | 0.1 | 0.1 |
| `num_attention_heads` | 4 | 4 |
| `learning_rate` | 1e-3 | 1e-3 |

#### DeepAR

| Parameter | Low/Medium Complexity | High Complexity |
|-----------|----------------------|-----------------|
| `input_size` | `min(5 * horizon, n_obs // 2)` | Same |
| `encoder_hidden_size` | 64 | 128 |
| `decoder_hidden_size` | 64 | 128 |
| `encoder_n_layers` | 2 | 3 |
| `decoder_n_layers` | 2 | 3 |
| `learning_rate` | 1e-3 | 1e-3 |

All neural model parameters are overridable. Overridable keys include: `input_size`, `max_steps`, `learning_rate`, `batch_size`, `dropout`, `hidden_size`, `encoder_hidden_size`, `encoder_n_layers`, `decoder_hidden_size`, `decoder_n_layers`, `n_layers`, `d_model`, `n_heads`, `patch_len`, `stride`, `num_attention_heads`, `val_check_steps`, `early_stop_patience_steps`.

**Minimum data**: 50 observations (hardcoded); additionally filtered by `sufficient_for_deep_learning` threshold.

### 4.4 Foundation Models (TimesFM)

| Parameter | Value |
|-----------|-------|
| Model | `google/timesfm-1.0-200m-pytorch` (200M parameters) |
| Architecture | Pre-trained transformer, zero-shot (no per-series training) |
| `context_length` | 512 |
| `horizon_length` | 128 |
| `input_patch_len` | 32 |
| `output_patch_len` | 128 |
| `num_layers` | 20 |
| `model_dims` | 1280 |
| Backend | GPU if CUDA available, else CPU |

**Frequency hint**: 0 = high frequency (H/D/B), 1 = medium (W/M), 2 = low (Q/Y/A).

**Quantile fallback**: If native quantile forecasting fails, uses scaling: `point_forecast * (1 + |q - 0.5| * 2)`.

---

## 5. Backtesting Methodology

### 5.1 Configuration

| Config Key | Default | Description |
|-----------|---------|-------------|
| `forecasting.backtesting.backtest_horizon` | `60` | Total periods reserved for backtesting |
| `forecasting.backtesting.window_size` | `8` | Forecast window per test (steps ahead) |
| `forecasting.backtesting.n_tests` | `4` | Number of forecast origins |
| `forecasting.backtesting.min_train_size` | `24` | Minimum training data before first origin |

### 5.2 Rolling Window Algorithm

Given a series of length `n`, the algorithm creates `n_tests` forecast origins:

```
Step 1: Clamp parameters
  _horizon = min(backtest_horizon, n - 1)
  _window  = min(window_size, backtest_horizon, forecast_horizon)

Step 2: Compute origin boundaries
  first_origin = max(min_train_size, n - backtest_horizon)
  last_possible_origin = n - window_size
  available_range = last_possible_origin - first_origin

Step 3: Compute step size
  If n_tests <= 0 or n_tests > max_possible: step = 1 (test all)
  If n_tests == 1: single origin at first_origin
  Otherwise: step = max(1, available_range / (n_tests - 1))

Step 4: Generate origins
  origins = [first_origin, first_origin + step, first_origin + 2*step, ...]

Fallback: If available_range < 0: first_origin = max(1, n / 2)
```

Each window produces:
- `train_series = series[:origin_idx]` (expanding window)
- `test_series = series[origin_idx : origin_idx + window_size]`

**Visual representation** (example with n=100, backtest_horizon=60, window_size=8, n_tests=4):

```
|-------- Training --------|-- Test 1 --|
|----------- Training -----------|-- Test 2 --|
|-------------- Training --------------|-- Test 3 --|
|-----------------  Training -------------------|-- Test 4 --|
 0                  40     53     66     79  87  92  100
                    ^      ^      ^      ^
                    |      |      |      |
                 Origin1 Origin2 Origin3 Origin4
```

### 5.3 Per-Series Backtesting Overrides

The orchestrator loads per-series overrides from the `hyperparameter_overrides` table where `method = '_backtesting'`. The overrides JSON can contain `backtest_horizon`, `window_size`, and `n_tests`, allowing different backtesting parameters for individual series.

### 5.4 Which Methods Are Backtested

- **Default**: Only statistical methods from `recommended_methods`, limited to the top 5
- **All-methods mode** (`--all-methods`): Also includes ML methods (LightGBM, XGBoost)

Neural network and foundation models are **not** backtested (too computationally expensive). Their selection is based on forward forecasting only.

### 5.5 Metric Computation

#### Point Forecast Metrics

| Metric | Formula | Notes |
|--------|---------|-------|
| **MAE** | `mean(\|actual - forecast\|)` | Scale-dependent |
| **RMSE** | `sqrt(mean((actual - forecast)^2))` | Penalizes large errors |
| **MAPE** | `mean(\|errors / actual\|) * 100` | Percentage; undefined for zero actuals |
| **sMAPE** | `mean(\|errors\| / ((\|actual\| + \|forecast\|) / 2)) * 100` | Symmetric; bounded [0, 200] |
| **Bias** | `mean(actual - forecast)` | Positive = under-forecasting |
| **MASE** | `MAE / mean(\|diff(actual)\|)` | Scale-free; uses naive one-step differences |

#### Probabilistic Metrics

| Metric | Formula | Notes |
|--------|---------|-------|
| **Coverage_50** | Fraction of actuals in [Q0.25, Q0.75] | Nominal = 50% |
| **Coverage_80** | Fraction of actuals in [Q0.10, Q0.90] | Nominal = 80% |
| **Coverage_90** | Fraction of actuals in [Q0.05, Q0.95] | Nominal = 90% |
| **Coverage_95** | Fraction of actuals in [Q0.025, Q0.975] | Nominal = 95% |
| **Winkler Score** | `mean(width + penalty_lower + penalty_upper)` | For 90% interval; `penalty = (2/alpha) * overshoot` |
| **CRPS** | Trapezoidal integration over quantile pairs | Lower = better; measures full distributional accuracy |
| **Quantile Loss** | `mean(q * max(a - f, 0) + (1-q) * max(f - a, 0))` | Pinball loss, averaged across all quantile levels |

#### Information Criteria

Computed from in-sample residuals assuming Gaussian distribution:

| Criterion | Formula |
|-----------|---------|
| **AIC** | `-2 * log_likelihood + 2 * n_params` |
| **BIC** | `-2 * log_likelihood + n_params * log(n)` |
| **AICc** | `AIC + (2 * n_params * (n_params + 1)) / (n - n_params - 1)` |

Where `log_likelihood = -0.5 * n * (log(2 * pi * sigma^2) + 1)` and `sigma^2 = mean(residuals^2)`.

### 5.6 ML Internal Validation Fallback

When ML methods produce no valid backtest metrics (common when the series is too short for rolling-window cross-validation), the system activates a fallback:

1. Runs the ML forecaster on the full series
2. Extracts internal 80/20 train/val split metrics from the model
3. Converts them to `EvaluationMetrics` with `metric_source = 'internal_validation'`
4. Appends to the backtest metrics table

This ensures ML methods are never left without metrics for best-method selection.

### 5.7 Per-Origin Forecast Storage

In addition to metrics, the backtester stores every individual forecast at every origin:

```
(unique_id, method, forecast_origin, horizon_step, point_forecast, actual_value)
```

This enables the "forecasts by origin" visualization in the frontend Time Series Viewer.

---

## 6. Best Method Selection

### 6.1 Scoring Weights

| Metric | Weight | Optimization Direction |
|--------|--------|----------------------|
| MAE | 0.40 | Lower is better |
| RMSE | 0.20 | Lower is better |
| Bias (absolute) | 0.15 | Lower is better |
| Coverage_90 deviation | 0.15 | Closer to 0.90 is better |
| MASE | 0.10 | Lower is better |

Weights are configurable in `config.yaml` under `best_method.weights`.

### 6.2 Ranking Algorithm

For each series (independently):

**Step 1 -- Average across forecast origins**: For each method, compute the mean of each metric column across all backtest windows.

**Step 2 -- Transform bias**: Convert `bias` to `|bias|` so both positive and negative bias are penalized equally.

**Step 3 -- Transform coverage_90**: Convert to `|coverage_90 - 0.9|` so deviation from the nominal 90% level is penalized. Overcoverage and undercoverage are treated symmetrically.

**Step 4 -- Min-max normalize to [0, 1]**: For each metric column across all methods for this series:

```
normalized[col] = (value[col] - min[col]) / (max[col] - min[col])
```

If all methods have the same value for a metric: `normalized[col] = 0.0` (no discriminating power).

**Step 5 -- Weighted composite score**:

```
composite = sum(weight[col] * normalized[col]) for col in metrics
```

Then re-normalize by the sum of weights of non-NaN metrics per method:

```
composite = composite / sum(weight[col] for col where metric is not NaN)
```

This ensures methods missing some metrics (e.g., intermittent models without coverage data, or ML models with only internal validation) are compared fairly.

**Step 6 -- Rank**: Sort by composite score **ascending**. **Lower score = better**.

### 6.3 Output

For each series, the algorithm produces:

| Field | Description |
|-------|-------------|
| `best_method` | Name of the winning method |
| `best_score` | Composite score of the winner |
| `runner_up_method` | Second-best method |
| `runner_up_score` | Composite score of the runner-up |
| `all_rankings` | JSON array with all methods ranked |

### 6.4 Fallback

If no usable metric columns exist or all metrics are NaN, the first method in the group is returned as the default best method.

---

## 7. Parameter Override System

### 7.1 Resolution Hierarchy

Parameters are resolved from highest to lowest priority:

1. **hyperparameter_overrides table**: Per-SKU, per-method partial overrides (most specific)
2. **parameters table** via **series_parameter_assignment**: Segment-based parameter sets
3. **Default parameter set**: `is_default = TRUE` in the parameters table
4. **config.yaml**: Base configuration (least specific)

### 7.2 Database Tables

| Table | Purpose |
|-------|---------|
| `series_parameter_assignment` | Maps `unique_id` to parameter IDs for each business type |
| `parameters` | Stores `(id, parameter_type, parameters_set JSONB, is_default)` |
| `parameter_segment` | Links parameters to segments |
| `hyperparameter_overrides` | Stores `(unique_id, method, overrides JSONB)` |

### 7.3 Business Types

| Business Type | Config Section | Purpose |
|--------------|---------------|---------|
| `forecasting` | `forecasting` | Forecasting model parameters |
| `outlier_detection` | `outlier_detection` | Outlier detection parameters |
| `characterization` | `characterization` | Characterization parameters |
| `evaluation` | `forecasting` | Backtesting parameters |
| `best_method` | `best_method` | Best method selection weights |

### 7.4 Deep Merge

Overrides are applied via recursive deep merge: override keys win over base keys. Non-dict values are replaced entirely. New keys are added. The merge produces a new dict without mutating the original.

### 7.5 Per-Series Hyperparameter Overrides

The `hyperparameter_overrides` table stores JSON overrides keyed by `(unique_id, method)`. Special method values:

- Any forecasting method name (e.g., `'AutoETS'`, `'LightGBM'`): overrides model hyperparameters
- `'_backtesting'`: overrides backtesting parameters (`backtest_horizon`, `window_size`, `n_tests`)

---

## 8. Configuration Reference

```yaml
characterization:
  seasonality:
    test_periods: [4, 13, 26, 52]
    significance_level: 0.05
    min_strength: 0.3
  trend:
    method: mann_kendall
    significance_level: 0.05
  intermittency:
    zero_threshold: 0.5
    adi_threshold: 1.32
    cov_threshold: 0.49
  stationarity:
    test: adf
    significance_level: 0.05
  data_sufficiency:
    min_for_ml: 20
    min_for_deep_learning: 30
    sparse_obs_per_year: 5

forecasting:
  horizon: 52
  frequency: W
  backtesting:
    backtest_horizon: 60
    window_size: 8
    n_tests: 4
    min_train_size: 24
  confidence_levels: [10, 25, 50, 75, 90, 95, 99]
  method_selection:
    sparse_data: [SeasonalNaive, HistoricAverage, TimesFM]
    intermittent: [CrostonOptimized, ADIDA, IMAPA, TimesFM]
    seasonal: [MSTL, AutoETS, NHITS, PatchTST, TimesFM, LightGBM]
    complex: [NHITS, NBEATS, TFT, PatchTST, AutoETS, LightGBM, XGBoost]
    standard: [AutoETS, AutoARIMA, AutoTheta, NHITS, TimesFM, LightGBM]
  ml_val_split: 0.2
  timesfm:
    model_name: timesfm-1.0-200m
    context_length: 512
    horizon_length: 128
    quantiles: [0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99]

evaluation:
  metrics:
    point_forecast: [mae, rmse, mape, smape, bias, mase]
    probabilistic: [winkler_score, crps, coverage, quantile_loss]
    information_criteria: [aic, bic, aicc]

best_method:
  weights:
    mae: 0.4
    rmse: 0.2
    bias: 0.15
    coverage_90: 0.15
    mase: 0.1

parallel:
  backend: dask
  dask:
    scheduler: processes
    n_workers: 64
    threads_per_worker: 2
  batch_size: 100
```

---

## 9. Edge Cases and Special Handling

1. **Series with < 4 observations**: Characterization may be incomplete; method recommendation falls through to sparse/intermittent/standard based on minimal available data.

2. **Seasonality gating by date span**: Series with less than 2 years of date history always receive `has_seasonality = False`, even if the data has periodic patterns at shorter ranges.

3. **Conformal intervals for intermittent models**: Only available when `n_obs >= 2 * horizon + 1`. Without this, CrostonOptimized/ADIDA/IMAPA/TSB produce point forecasts only.

4. **ML horizon steps with insufficient data**: Individual horizon steps with fewer than 10 supervised samples are filled with NaN. If more than half the horizon steps cannot be trained, the entire ML method is skipped for that series.

5. **Quantile monotonicity enforcement**: After ML quantile regression, quantile values are sorted at each horizon step to ensure lower quantiles never exceed higher quantiles (e.g., Q10 <= Q25 <= Q50 <= Q75 <= Q90).

6. **ML internal validation fallback**: When rolling-window backtesting produces no metrics for ML methods, the system falls back to the model's internal 80/20 train/val split metrics, tagged with `metric_source = 'internal_validation'`.

7. **Best method NaN handling**: Composite scores divide by the sum of weights of non-NaN metrics per method, ensuring methods with missing probabilistic metrics (e.g., intermittent models without coverage) are still comparable to methods with full metric sets.

8. **Frequency alias mapping**: Pandas 2.x deprecated frequency aliases (`'M'` to `'ME'`, `'Q'` to `'QE'`, `'Y'` to `'YE'`). The StatisticalForecaster applies this mapping before passing to StatsForecast.

9. **Parameter group isolation**: Series with per-SKU hyperparameter overrides are processed in their own single-element batch with a fully-merged config, ensuring their custom parameters do not leak to other series in the same batch.

10. **Targeted saves for subset runs**: When re-running individual series (e.g., from the Time Series Viewer), the orchestrator uses `DELETE WHERE unique_id IN (...)` instead of truncating entire tables, preserving results from other series.

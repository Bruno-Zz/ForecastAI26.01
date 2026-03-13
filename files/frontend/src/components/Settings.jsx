import { useState, useEffect, useCallback, useRef, useMemo } from 'react';
import { createPortal } from 'react-dom';
import { useTheme } from '../contexts/ThemeContext';
import { useLocale, LOCALE_PRESETS } from '../contexts/LocaleContext';
import { formatDate, formatNumber, formatDateTime } from '../utils/formatting';
import DateInput from './DateInput';
import api from '../utils/api';

/* ─── Known option sets for enum parameters ─── */

const ALL_FORECASTING_METHODS = [
  'AutoARIMA', 'AutoETS', 'AutoTheta', 'AutoCES', 'MSTL',
  'CrostonOptimized', 'ADIDA', 'IMAPA', 'HistoricAverage', 'SeasonalNaive',
  'NHITS', 'NBEATS', 'PatchTST', 'TFT', 'DeepAR',
  'LightGBM', 'XGBoost',
  'TimesFM',
];

/** Single-value parameters → dropdown <select> */
const PARAM_OPTIONS = {
  'data_source.type':                                  ['postgres', 's3', 'azure', 'csv'],
  'etl.aggregation.frequency':                         ['D', 'W', 'M', 'Q', 'Y'],
  'etl.aggregation.method':                            ['sum', 'mean', 'median', 'first', 'last', 'min', 'max'],
  'outlier_detection.detection_method':                 ['iqr', 'zscore', 'stl_residuals'],
  'outlier_detection.correction_method':                ['clip', 'median', 'interpolation', 'remove'],
  'outlier_detection.correction.interpolation_method':  ['linear', 'nearest', 'cubic', 'spline'],
  'characterization.trend.method':                      ['mann_kendall', 'ols', 'spearman'],
  'characterization.stationarity.test':                 ['adf', 'kpss', 'pp'],
  'forecasting.frequency':                              ['D', 'W', 'M', 'Q', 'Y'],
  'meio.fitting_method':                                ['mle', 'quantile_matching', 'mom'],
  'parallel.backend':                                   ['dask', 'sequential', 'joblib'],
  'parallel.dask.scheduler':                            ['processes', 'threads', 'synchronous'],
  'output.formats.forecasts':                           ['postgres', 'parquet', 'csv'],
  'output.formats.metrics':                             ['postgres', 'parquet', 'csv'],
  'output.formats.plots':                               ['png', 'svg', 'html'],
  'logging.level':                                      ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
  'forecasting.method_selection_strategy':             ['auto', 'best_fit'],
};

/** Array parameters → dropdown per item row in the modal editor */
const ARRAY_ITEM_OPTIONS = {
  'forecasting.ml_models':                        ALL_FORECASTING_METHODS,
  'forecasting.statsforecast_models':             ALL_FORECASTING_METHODS,
  'forecasting.neuralforecast_models':            ALL_FORECASTING_METHODS,
  'forecasting.method_selection.sparse_data':     ALL_FORECASTING_METHODS,
  'forecasting.method_selection.intermittent':    ALL_FORECASTING_METHODS,
  'forecasting.method_selection.seasonal':        ALL_FORECASTING_METHODS,
  'forecasting.method_selection.complex':         ALL_FORECASTING_METHODS,
  'forecasting.method_selection.standard':        ALL_FORECASTING_METHODS,
  'forecasting.best_fit_methods':                 ALL_FORECASTING_METHODS,
  'meio.distributions':                           ['normal', 'gamma', 'negative_binomial', 'lognormal', 'poisson', 'weibull'],
  'evaluation.metrics.point_forecast':            ['mae', 'rmse', 'mape', 'smape', 'bias', 'mase'],
  'evaluation.metrics.probabilistic':             ['winkler_score', 'crps', 'coverage', 'quantile_loss'],
  'evaluation.metrics.information_criteria':      ['aic', 'bic', 'aicc'],
  'hierarchical.reconciliation_methods':          ['BottomUp', 'TopDown', 'MinTrace', 'ERM'],
};

/**
 * Nullable method override dropdowns (strategy=auto, per demand group).
 * Value null means "Auto – run all group methods".
 */
const NULLABLE_METHOD_OPTIONS = {
  'forecasting.method_overrides.sparse_data':    ALL_FORECASTING_METHODS,
  'forecasting.method_overrides.intermittent':   ALL_FORECASTING_METHODS,
  'forecasting.method_overrides.seasonal':       ALL_FORECASTING_METHODS,
  'forecasting.method_overrides.complex':        ALL_FORECASTING_METHODS,
  'forecasting.method_overrides.standard':       ALL_FORECASTING_METHODS,
};

/** Static compatibility hints shown as an amber ⚠ next to a method choice */
const METHOD_WARNINGS = {
  NHITS:            'Requires deep learning data sufficiency (≥30 obs)',
  NBEATS:           'Requires deep learning data sufficiency (≥30 obs)',
  PatchTST:         'Requires deep learning data sufficiency (≥30 obs)',
  TFT:              'Requires deep learning data sufficiency (≥30 obs)',
  DeepAR:           'Requires deep learning data sufficiency (≥30 obs)',
  LightGBM:         'Requires ML data sufficiency (≥20 obs)',
  XGBoost:          'Requires ML data sufficiency (≥20 obs)',
  CrostonOptimized: 'Designed for intermittent demand only',
  ADIDA:            'Designed for intermittent demand only',
  IMAPA:            'Designed for intermittent demand only',
  MSTL:             'Best with seasonal data',
};

/* ─── Hover tooltips for every parameter field ─── */
const PARAM_TOOLTIPS = {
  // ── data_source ──
  'data_source.type': 'Source connector: postgres reads from a PostgreSQL table, s3 from an S3 bucket, azure from Azure Blob Storage, csv from a local file.',
  'data_source.postgres.host': 'Hostname or IP of the PostgreSQL demand database.',
  'data_source.postgres.port': 'TCP port for the PostgreSQL demand database (default 5432).',
  'data_source.postgres.database': 'Database name on the PostgreSQL server.',
  'data_source.postgres.schema': 'Schema that contains the demand table.',
  'data_source.postgres.table': 'Table name holding the demand history.',
  'data_source.postgres.user': 'PostgreSQL username. Supports ${ENV_VAR} substitution.',
  'data_source.postgres.password': 'PostgreSQL password.',
  'data_source.source_db.host': 'Hostname of the source ERP/WMS database used by the ETL.',
  'data_source.source_db.port': 'TCP port of the source database.',
  'data_source.source_db.dbname': 'Database name on the source server.',
  'data_source.source_db.user': 'Username for the source database connection.',
  'data_source.source_db.password': 'Password for the source database connection.',
  'data_source.source_db.sslmode': 'SSL mode for the source connection (disable / require / verify-full).',
  'data_source.source_db.columns.item_id': 'Column in the source table that holds the item / SKU identifier.',
  'data_source.source_db.columns.site_id': 'Column in the source table that holds the site / location identifier.',
  'data_source.source_db.columns.date': 'Column in the source table that holds the transaction date.',
  'data_source.source_db.columns.qty': 'Column in the source table that holds the demand quantity.',
  'data_source.source_db.demand_table': 'Fully-qualified source table name (schema.table) queried by the ETL.',
  'data_source.s3.bucket': 'AWS S3 bucket name containing the demand data files.',
  'data_source.s3.prefix': 'S3 key prefix (folder path) to filter demand files.',
  'data_source.s3.region': 'AWS region where the S3 bucket resides.',
  'data_source.azure.container': 'Azure Blob Storage container name.',
  'data_source.azure.account_name': 'Azure Storage account name.',
  'data_source.azure.account_key': 'Azure Storage account access key.',
  // ── etl ──
  'etl.output_path': 'File path where the ETL saves an intermediate Parquet snapshot for auditing and debugging.',
  'etl.partition_by': 'Columns used to partition the Parquet output. Useful for large datasets when you want fast filtering.',
  'etl.query.date_column': 'Name of the date column in the source query result.',
  'etl.query.value_column': 'Name of the demand / quantity column in the source query result.',
  'etl.query.id_column': 'Name of the identifier column (item / SKU) in the source query result.',
  'etl.query.hierarchy_columns': 'Additional columns pulled from source to carry hierarchical groupings (e.g. category, region).',
  'etl.query.min_date': 'Earliest date to include (ISO format YYYY-MM-DD). Leave null for no lower bound.',
  'etl.query.max_date': 'Latest date to include (ISO format YYYY-MM-DD). Leave null to default to today.',
  'etl.query.min_observations': 'Series with fewer observations than this threshold after aggregation are dropped during ETL.',
  'etl.aggregation.frequency': 'Target time frequency after aggregation: D=Daily, W=Weekly, M=Monthly, Q=Quarterly, Y=Yearly.',
  'etl.aggregation.method': 'How to combine demand values when multiple rows fall within the same time bucket.',
  // ── outlier_detection ──
  'outlier_detection.enabled': 'Enable or disable automatic outlier detection and correction for all series.',
  'outlier_detection.detection_method': 'Algorithm used to identify outliers. IQR: interquartile range fences. Z-score: standard deviation from mean. STL residuals: seasonal decomposition residuals.',
  'outlier_detection.correction_method': 'How detected outliers are replaced: clip to fence, replace with rolling median, interpolate from neighbours, or remove the point entirely.',
  'outlier_detection.iqr.multiplier': 'IQR fence multiplier k. Points beyond median ± k×IQR are flagged. Standard value: 1.5. Use 3.0 for a more lenient filter.',
  'outlier_detection.zscore.threshold': 'Number of standard deviations from the mean beyond which a point is flagged as an outlier.',
  'outlier_detection.zscore.use_mad': 'Use Median Absolute Deviation instead of standard deviation — more robust on skewed or heavy-tailed series.',
  'outlier_detection.stl_residuals.seasonal_period': 'Seasonal period (in observations) used in the STL decomposition for residual-based detection.',
  'outlier_detection.stl_residuals.residual_threshold': 'Number of sigma in the STL residual beyond which a point is flagged as an outlier.',
  'outlier_detection.correction.interpolation_method': 'Interpolation strategy when correction_method is "interpolation": linear, nearest-neighbour, cubic, or polynomial spline.',
  'outlier_detection.correction.median_window': 'Rolling window size (periods) used to compute the median replacement value.',
  'outlier_detection.min_observations': 'Minimum observations required before outlier detection is applied to a series.',
  // ── characterization ──
  'characterization.seasonality.test_periods': 'Candidate seasonal periods (in observations) tested per series. For weekly data: [52]. For daily: [7, 30, 365].',
  'characterization.seasonality.significance_level': 'P-value threshold for the seasonality test. Lower values → fewer series classified as seasonal.',
  'characterization.seasonality.min_strength': 'Minimum seasonal strength (0–1) from STL decomposition required to classify a series as seasonal.',
  'characterization.trend.method': 'Statistical test for trend detection: Mann-Kendall (rank-based, robust to outliers), OLS regression, or Spearman correlation.',
  'characterization.trend.significance_level': 'P-value threshold for the trend test. Lower values → fewer series classified as trending.',
  'characterization.intermittency.zero_threshold': 'Fraction of zero-demand periods above which a series is classified as intermittent.',
  'characterization.intermittency.adi_threshold': 'Average Demand Interval threshold. ADI ≥ this value marks the series as intermittent.',
  'characterization.intermittency.cov_threshold': 'Coefficient of Variation of inter-demand intervals. Used in the Syntetos-Boylan classification.',
  'characterization.intermittency.min_positive_periods': 'Minimum number of non-zero demand periods required before the intermittency test is applied.',
  'characterization.stationarity.test': 'Unit-root / stationarity test: ADF (Augmented Dickey-Fuller), KPSS, or Phillips-Perron. Guides differencing in ARIMA models.',
  'characterization.stationarity.significance_level': 'P-value threshold for the stationarity test.',
  'characterization.data_sufficiency.min_for_ml': 'Minimum observations needed to enable ML models (LightGBM, XGBoost) for a series.',
  'characterization.data_sufficiency.min_for_deep_learning': 'Minimum observations needed to enable deep learning models (NHITS, NBEATS, TFT, PatchTST, DeepAR) for a series.',
  'characterization.data_sufficiency.sparse_obs_per_year': 'Average observations per year below which a series is classified as sparse data.',
  'characterization.complexity.low_threshold': 'Composite complexity score below this value → low complexity → series assigned to the "standard" demand group.',
  'characterization.complexity.high_threshold': 'Composite complexity score above this value → high complexity → series assigned to the "complex" demand group.',
  // ── forecasting ──
  'forecasting.horizon': 'Number of future periods to forecast ahead. Should match your planning horizon (e.g. 13 for a 13-week view).',
  'forecasting.frequency': 'Time frequency of the demand data: D=Daily, W=Weekly, M=Monthly, Q=Quarterly, Y=Yearly. Must match the ETL aggregation frequency.',
  'forecasting.confidence_levels': 'Prediction interval confidence levels (%) to compute and store. These determine which quantiles appear in the forecast output.',
  'forecasting.method_selection_strategy': '"auto": the system selects methods per demand group from the method_selection lists. "best_fit": you supply a fixed list evaluated against all series.',
  'forecasting.method_selection.sparse_data': 'Candidate methods evaluated for series classified as sparse data (very few non-zero periods per year).',
  'forecasting.method_selection.intermittent': 'Candidate methods evaluated for intermittent demand series (frequent zeros, irregular non-zero demand).',
  'forecasting.method_selection.seasonal': 'Candidate methods evaluated for seasonal series without high complexity.',
  'forecasting.method_selection.complex': 'Candidate methods evaluated for high-complexity series (multiple seasonalities, high variability, non-stationarity).',
  'forecasting.method_selection.standard': 'Candidate methods evaluated for standard series (low complexity, no strong intermittency or unusual seasonality).',
  'forecasting.method_overrides.sparse_data': 'Pin a single method for all sparse-data series instead of running the full candidate list. Leave on Auto for competitive selection.',
  'forecasting.method_overrides.intermittent': 'Pin a single method for all intermittent series instead of running the full candidate list. Leave on Auto for competitive selection.',
  'forecasting.method_overrides.seasonal': 'Pin a single method for all seasonal series. Leave on Auto for competitive selection.',
  'forecasting.method_overrides.complex': 'Pin a single method for all complex series. Leave on Auto for competitive selection.',
  'forecasting.method_overrides.standard': 'Pin a single method for all standard series. Leave on Auto for competitive selection.',
  'forecasting.best_fit_methods': 'Methods evaluated against every series when strategy is "best_fit". The method with the best composite backtest score wins per series.',
  'forecasting.statsforecast_models': 'Statistical models active in the pipeline. Fast and interpretable models from the StatsForecast library.',
  'forecasting.neuralforecast_models': 'Deep learning models active in the pipeline. Require sufficient historical data and compute resources.',
  'forecasting.ml_models': 'Machine learning tree-based models active in the pipeline.',
  'forecasting.timesfm.model_name': 'TimesFM model variant to load (e.g. timesfm-1.0-200m).',
  'forecasting.timesfm.context_length': 'Number of historical periods fed to TimesFM as input context.',
  'forecasting.timesfm.horizon_length': 'Maximum horizon TimesFM can forecast. Must be ≥ forecasting.horizon.',
  // ── evaluation ──
  'evaluation.metrics.point_forecast': 'Point-forecast accuracy metrics computed during backtesting and shown in the accuracy chart.',
  'evaluation.metrics.probabilistic': 'Probabilistic metrics evaluating the quality of prediction intervals (e.g. Winkler Score, CRPS).',
  'evaluation.metrics.information_criteria': 'Model fit criteria extracted where available: AIC (penalises complexity), BIC (stronger penalty), AICc (corrected for small samples).',
  // ── backtesting ──
  'backtesting.n_windows': 'Number of rolling backtest windows. More windows → more reliable accuracy estimate but proportionally more compute time.',
  'backtesting.step_size': 'Step in periods between consecutive backtest windows. 1 = fully overlapping; larger values reduce overlap and compute cost.',
  'backtesting.min_train_size': 'Minimum observations in the training window. Series too short to satisfy this are skipped in backtesting.',
  'backtesting.weights.mae': 'Weight of MAE in the composite method-ranking score. Higher = penalise average absolute error more. All weights should sum to 1.',
  'backtesting.weights.rmse': 'Weight of RMSE in the composite score. Penalises large individual errors more heavily than MAE.',
  'backtesting.weights.mape': 'Weight of MAPE in the composite score. Scale-independent but sensitive to near-zero actuals.',
  'backtesting.weights.bias': 'Weight of Bias (mean signed error) in the composite score. Detects systematic over- or under-forecasting.',
  'backtesting.weights.coverage_90': 'Weight of 90% prediction interval coverage in the composite score. Measures how well forecast uncertainty is calibrated.',
  'backtesting.weights.direction_accuracy': 'Weight of directional accuracy (did the forecast predict the correct direction of change?) in the composite score.',
  // ── hierarchical ──
  'hierarchical.enabled': 'Enable hierarchical reconciliation so that forecasts at every level sum to a coherent total.',
  'hierarchical.reconciliation_methods': 'Reconciliation algorithms: BottomUp aggregates from the leaf level; TopDown disaggregates; MinTrace minimises total forecast variance; ERM uses empirical risk minimisation.',
  'hierarchical.structure.levels': 'Ordered list of column names defining the hierarchy from the top-most aggregate level down to the leaf.',
  // ── meio ──
  'meio.enabled': 'Enable Multi-Echelon Inventory Optimisation. Fits demand distributions and computes safety stock at each service level target.',
  'meio.distributions': 'Probability distributions fitted to demand data for safety stock calculation. The best-fitting distribution is selected per series.',
  'meio.fitting_method': 'Parameter estimation method for distribution fitting: MLE (maximum likelihood), quantile matching, or method of moments.',
  'meio.service_levels': 'Target service levels (fill rates, 0–1) for which safety stock quantities are computed.',
  // ── causal forecasting ──
  'causal.mdfh.mdfh_mean': 'Mean Demand per Flight Hour (or cycle / landing). Multiply by utilisation to get expected removals per period.',
  'causal.mdfh.mdfh_stddev': 'Standard deviation of MDFH across the historical sample. Drives demand uncertainty in the MEIO optimizer.',
  'causal.bom.qty_per_asset': 'Number of this part installed per asset. Used to scale removal demand when a BOM item fails.',
  'causal.bom.removal_driver': 'Utilisation metric that drives removals for this part: hours, cycles, landings, or calendar days.',
  'causal.effectivity.effective': 'Whether this part is installed on this specific asset instance. Non-effective parts are excluded from demand generation for that tail.',
  'causal.fleet_plan.util_hours': 'Planned flight hours for this asset in the planning period. Used as the utilisation scalar when removal_driver is "hours".',
  'causal.fleet_plan.util_cycles': 'Planned flight cycles for this asset in the planning period. Used when removal_driver is "cycles".',
  'causal.scenarios.fleet_overrides.utilization_multiplier': 'Scale all utilisation metrics for this scenario by this factor (e.g. 0.8 for a demand-reduction scenario, 1.2 for a surge scenario).',
  'causal.scenarios.mdfh_overrides': 'Override individual part MDFH values for this scenario. Useful for stress-testing specific components.',
  'causal.scenarios.linked_meio_scenario_id': 'When set, the causal demand output is automatically fed as input to this MEIO scenario for end-to-end optimisation.',
  // ── parallel ──
  'parallel.backend': 'Parallelism backend: dask (distributed, multi-process), sequential (single-threaded, good for debugging), joblib (thread/process pools).',
  'parallel.batch_size': 'Number of series processed per parallel batch. Larger batches reduce scheduling overhead but increase peak memory per worker.',
  'parallel.dask.scheduler': 'Dask scheduler: processes (separate CPUs, avoids GIL), threads (shared memory, lighter overhead), synchronous (no parallelism, easiest to debug).',
  'parallel.dask.n_workers': 'Number of Dask worker processes. Leave null to use all available CPU cores.',
  'parallel.dask.threads_per_worker': 'Threads per Dask worker. Keep at 1 for CPU-bound statistical models to avoid contention.',
  'parallel.dask.memory_limit': 'RAM limit per worker (e.g. "4GB"). "auto" lets Dask determine a limit based on total system memory.',
  'parallel.dask.distributed.scheduler_address': 'Address of an existing Dask distributed scheduler (e.g. tcp://192.168.1.10:8786). Leave null to spin up a local cluster.',
  // ── output ──
  'output.base_path': 'Root directory for all file-based outputs (Parquet files, CSVs, plot images).',
  'output.save.forecasts': 'Save forecast results to the configured output format.',
  'output.save.characteristics': 'Save series characterisation results (demand group classification, seasonality flags, etc.).',
  'output.save.metrics': 'Save backtesting accuracy metrics for each series and method.',
  'output.save.distributions': 'Save fitted demand distributions (used by MEIO for safety stock calculations).',
  'output.save.hyperparameters': 'Save the fitted model hyperparameters (e.g. ARIMA orders, ETS smoothing weights) for each series.',
  'output.save.plots': 'Generate and save forecast visualisation plots for each series.',
  'output.formats.forecasts': 'Output format for forecast data: postgres (write directly to DB), parquet, or csv.',
  'output.formats.metrics': 'Output format for accuracy metrics.',
  'output.formats.plots': 'Image format for forecast plots: png (static), svg (vector), or html (interactive Plotly).',
  'output.compression': 'Compression codec for Parquet files: snappy (fast), gzip (smaller), zstd (best ratio), or none.',
  // ── auth ──
  'auth.jwt_secret': 'Secret key used to sign JWT tokens. Must be changed before deploying to production. Use a long random string.',
  'auth.jwt_algorithm': 'JWT signing algorithm. HS256 (HMAC-SHA256) is recommended for symmetric shared-secret authentication.',
  'auth.access_token_expire_minutes': 'Minutes before an access token expires and the user must re-authenticate or use a refresh token.',
  'auth.refresh_token_expire_days': 'Days before a refresh token expires and the user must log in again.',
  // ── logging ──
  'logging.level': 'Minimum severity level to emit. DEBUG = all messages; INFO = normal operation; WARNING = problems; ERROR = failures; CRITICAL = crashes only.',
  'logging.format': 'Python log format string applied to every log entry. See Python logging documentation for available fields.',
  'logging.file': 'Path to the log file. Messages are written here in addition to (or instead of) the console.',
  'logging.console': 'Also print log messages to stdout / stderr in addition to the log file.',
  // ── performance ──
  'n_jobs': 'Number of parallel workers for the forecasting loop. 1 = fully sequential (safest, default). -1 = all CPU threads. Values > 1 use joblib thread pools.',
  'incremental_processing': 'When enabled, only series whose demand data changed since the last run are re-forecasted. Greatly speeds up incremental pipeline runs on large datasets.',
};

/* ─── Parameter type → tab assignment ─── */
const BUSINESS_PARAM_TYPES = new Set([
  'backtesting', 'characterization', 'evaluation', 'forecasting', 'outlier_detection',
]);

/** Desired display order for Business Config sub-tabs (pipeline execution order) */
const BUSINESS_PARAM_TYPE_ORDER = [
  'characterization', 'outlier_detection', 'forecasting', 'evaluation', 'backtesting',
];

const SYSTEM_PARAM_TYPES = new Set([
  'data_source', 'etl', 'hierarchical', 'meio', 'parallel', 'output', 'auth', 'logging', 'segmentation', 'performance',
]);

const TABS = [
  { id: 'appearance', label: 'Appearance', icon: (
    <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
        d="M7 21a4 4 0 01-4-4V5a2 2 0 012-2h4a2 2 0 012 2v12a4 4 0 01-4 4zm0 0h12a2 2 0 002-2v-4a2 2 0 00-2-2h-2.343M11 7.343l1.657-1.657a2 2 0 012.828 0l2.829 2.829a2 2 0 010 2.828l-8.486 8.485M7 17h.01" />
    </svg>
  )},
  { id: 'locale', label: 'Locale & Formatting', icon: (
    <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
        d="M3.055 11H5a2 2 0 012 2v1a2 2 0 002 2 2 2 0 012 2v2.945M8 3.935V5.5A2.5 2.5 0 0010.5 8h.5a2 2 0 012 2 2 2 0 104 0 2 2 0 012-2h1.064M15 20.488V18a2 2 0 012-2h3.064M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
    </svg>
  )},
  { id: 'business', label: 'Business Config', icon: (
    <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
        d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
    </svg>
  )},
  { id: 'config', label: 'System Config', icon: (
    <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
        d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.066 2.573c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.573 1.066c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.066-2.573c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z" />
      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
    </svg>
  )},
];

export default function Settings() {
  const [activeTab, setActiveTab] = useState('appearance');

  return (
    <div id="settings-page" className="p-4 sm:p-6 max-w-4xl mx-auto">
      <h1 className="text-2xl sm:text-3xl font-bold mb-6 text-gray-900 dark:text-white">Settings</h1>

      {/* Tab bar - responsive: icons on mobile, full labels on sm+ */}
      <div id="settings-tabs" className="flex gap-1 mb-6 border-b border-gray-200 dark:border-gray-700 overflow-x-auto">
        {TABS.map(tab => (
          <button
            key={tab.id}
            onClick={() => setActiveTab(tab.id)}
            className={`flex items-center gap-2 px-3 sm:px-4 py-2.5 text-sm font-medium border-b-2 transition-colors whitespace-nowrap flex-shrink-0
              ${activeTab === tab.id
                ? 'border-blue-600 text-blue-600 dark:text-blue-400 dark:border-blue-400'
                : 'border-transparent text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300'
              }`}
          >
            {tab.icon}
            <span className="hidden sm:inline">{tab.label}</span>
          </button>
        ))}
      </div>

      <div id="settings-content">
        {activeTab === 'appearance' && <AppearanceTab />}
        {activeTab === 'locale' && <LocaleTab />}
        {activeTab === 'business' && <ConfigTab filterTypes={BUSINESS_PARAM_TYPES} title="Business Configuration" subtitle="Forecasting, evaluation, and method selection parameters. Create versions to use different settings per segment." />}
        {activeTab === 'config' && <ConfigTab filterTypes={SYSTEM_PARAM_TYPES} title="System Configuration" subtitle="Infrastructure, ETL, and runtime parameters." />}
      </div>
    </div>
  );
}

/* ─── Appearance Tab ─── */
function AppearanceTab() {
  const { theme, setTheme } = useTheme();

  const options = [
    { value: 'light', label: 'Light', desc: 'Bright background with dark text',
      icon: (
        <svg className="w-8 h-8" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5}
            d="M12 3v1m0 16v1m9-9h-1M4 12H3m15.364 6.364l-.707-.707M6.343 6.343l-.707-.707m12.728 0l-.707.707M6.343 17.657l-.707.707M16 12a4 4 0 11-8 0 4 4 0 018 0z" />
        </svg>
      ),
      previewBg: 'bg-gray-50', previewBorder: 'border-gray-200',
    },
    { value: 'dark', label: 'Dark', desc: 'Dark background, easier on the eyes',
      icon: (
        <svg className="w-8 h-8" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5}
            d="M20.354 15.354A9 9 0 018.646 3.646 9.003 9.003 0 0012 21a9.003 9.003 0 008.354-5.646z" />
        </svg>
      ),
      previewBg: 'bg-gray-800', previewBorder: 'border-gray-600',
    },
  ];

  return (
    <div className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900/50 p-6">
      <h2 className="text-lg font-semibold mb-2 text-gray-900 dark:text-white">Theme</h2>
      <p className="text-sm text-gray-500 dark:text-gray-400 mb-5">
        Choose your preferred color scheme. Your choice is saved automatically.
      </p>
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
        {options.map(opt => (
          <button
            key={opt.value}
            onClick={() => setTheme(opt.value)}
            className={`flex items-center gap-4 p-4 rounded-xl border-2 transition-all text-left
              ${theme === opt.value
                ? 'border-blue-500 bg-blue-50 dark:bg-blue-900/30 ring-1 ring-blue-500/20'
                : 'border-gray-200 dark:border-gray-600 hover:border-gray-300 dark:hover:border-gray-500'
              }`}
          >
            {/* Mini preview */}
            <div className={`w-16 h-12 rounded-lg ${opt.previewBg} ${opt.previewBorder} border flex items-center justify-center flex-shrink-0`}>
              <span className={opt.value === 'dark' ? 'text-gray-300' : 'text-gray-600'}>{opt.icon}</span>
            </div>
            <div>
              <div className="font-semibold text-sm text-gray-900 dark:text-white">{opt.label}</div>
              <div className="text-xs text-gray-500 dark:text-gray-400 mt-0.5">{opt.desc}</div>
            </div>
            {theme === opt.value && (
              <svg className="w-5 h-5 text-blue-500 ml-auto flex-shrink-0" fill="currentColor" viewBox="0 0 20 20">
                <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" clipRule="evenodd" />
              </svg>
            )}
          </button>
        ))}
      </div>
    </div>
  );
}

/* ─── Locale Tab ─── */
function LocaleTab() {
  const { locale, setLocale, numberDecimals, setNumberDecimals, preset } = useLocale();
  const [testDate, setTestDate] = useState('');

  // Live preview values
  const sampleDate = '2026-02-24';
  const sampleDatetime = '2026-02-24T14:30:45Z';
  const sampleNumber = 1234567.891;
  const sampleSmall = 0.0456;

  return (
    <div className="space-y-6">
      {/* Locale selector */}
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900/50 p-6">
        <h2 className="text-lg font-semibold mb-2 text-gray-900 dark:text-white">Regional Format</h2>
        <p className="text-sm text-gray-500 dark:text-gray-400 mb-4">
          Controls how dates, times, and numbers are displayed throughout the application.
        </p>
        <div className="grid grid-cols-2 sm:grid-cols-3 gap-3">
          {Object.entries(LOCALE_PRESETS).map(([key, p]) => (
            <button
              key={key}
              onClick={() => setLocale(key)}
              className={`text-left p-3 rounded-lg border-2 transition-all
                ${locale === key
                  ? 'border-blue-500 bg-blue-50 dark:bg-blue-900/30 ring-1 ring-blue-500/20'
                  : 'border-gray-200 dark:border-gray-600 hover:border-gray-300 dark:hover:border-gray-500'
                }`}
            >
              <div className="font-medium text-sm text-gray-900 dark:text-white">{p.label}</div>
              <div className="text-xs text-gray-500 dark:text-gray-400 mt-1 font-mono">{p.dateExample}</div>
              <div className="text-[10px] text-gray-400 dark:text-gray-500 mt-0.5">{p.dateFormat}</div>
            </button>
          ))}
        </div>
      </div>

      {/* Number precision */}
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900/50 p-6">
        <h2 className="text-lg font-semibold mb-2 text-gray-900 dark:text-white">Number Precision</h2>
        <p className="text-sm text-gray-500 dark:text-gray-400 mb-3">
          Default number of decimal places for numeric values in tables and charts.
        </p>
        <select
          value={numberDecimals}
          onChange={e => setNumberDecimals(Number(e.target.value))}
          className="border border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-white rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-400"
        >
          {[0, 1, 2, 3, 4].map(n => (
            <option key={n} value={n}>{n} decimal{n !== 1 ? 's' : ''}</option>
          ))}
        </select>
      </div>

      {/* Date input test */}
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900/50 p-6">
        <h2 className="text-lg font-semibold mb-2 text-gray-900 dark:text-white">Date Input Test</h2>
        <p className="text-sm text-gray-500 dark:text-gray-400 mb-3">
          Try typing a date in your regional format ({preset.dateFormat}) to see how it is parsed.
        </p>
        <div className="flex flex-col sm:flex-row items-start sm:items-center gap-3">
          <DateInput
            value={testDate}
            onChange={setTestDate}
            className="w-full sm:w-56"
          />
          {testDate && (
            <div className="text-sm">
              <span className="text-gray-500 dark:text-gray-400">Parsed (ISO):</span>{' '}
              <span className="font-mono text-blue-600 dark:text-blue-400">{testDate}</span>
            </div>
          )}
        </div>
      </div>

      {/* Live preview */}
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900/50 p-6">
        <h2 className="text-lg font-semibold mb-3 text-gray-900 dark:text-white">Formatting Preview</h2>
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-4 text-sm">
          <PreviewRow label="Date" value={formatDate(sampleDate, locale)} />
          <PreviewRow label="Date + Time" value={formatDateTime(sampleDatetime, locale)} />
          <PreviewRow label="Large number" value={formatNumber(sampleNumber, locale, numberDecimals)} />
          <PreviewRow label="Small number" value={formatNumber(sampleSmall, locale, 4)} />
          <PreviewRow label="Integer" value={formatNumber(42195, locale, 0)} />
          <PreviewRow label="Date input format" value={preset.dateFormat} />
        </div>
      </div>
    </div>
  );
}

function PreviewRow({ label, value }) {
  return (
    <div className="flex items-center justify-between gap-2 py-1.5 border-b border-gray-100 dark:border-gray-700 last:border-0">
      <span className="text-gray-500 dark:text-gray-400">{label}</span>
      <span className="font-mono text-gray-900 dark:text-white">{value}</span>
    </div>
  );
}

/* ─── JSON Table Modal (popup for editing arrays / objects as tables) ─── */
function JsonTableModal({ path, value, onSave, onClose, allowedValues }) {
  const isArray = Array.isArray(value);
  const [rows, setRows] = useState(() => {
    if (isArray) return value.map((v, i) => ({ key: String(i), value: v }));
    // object → key-value pairs
    return Object.entries(value).map(([k, v]) => ({ key: k, value: v }));
  });
  const [error, setError] = useState(null);
  const backdropRef = useRef(null);
  const [dragRow, setDragRow] = useState(null);
  const [dragOverRow, setDragOverRow] = useState(null);

  const handleRowDragStart = (idx) => setDragRow(idx);
  const handleRowDragOver = (e, idx) => { e.preventDefault(); setDragOverRow(idx); };
  const handleRowDrop = (e, toIdx) => {
    e.preventDefault();
    if (dragRow === null || dragRow === toIdx) { setDragRow(null); setDragOverRow(null); return; }
    setRows(prev => {
      const next = [...prev];
      const [moved] = next.splice(dragRow, 1);
      next.splice(toIdx, 0, moved);
      if (isArray) return next.map((r, i) => ({ ...r, key: String(i) }));
      return next;
    });
    setDragRow(null);
    setDragOverRow(null);
  };
  const handleRowDragEnd = () => { setDragRow(null); setDragOverRow(null); };

  // Close on Escape
  useEffect(() => {
    const handler = (e) => { if (e.key === 'Escape') onClose(); };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [onClose]);

  // Detect the dominant scalar type in the existing values
  const guessType = useCallback(() => {
    const vals = rows.map(r => r.value).filter(v => v !== null && v !== undefined && v !== '');
    if (vals.length === 0) return 'string';
    if (vals.every(v => typeof v === 'number')) return 'number';
    if (vals.every(v => typeof v === 'boolean')) return 'boolean';
    return 'string';
  }, [rows]);

  const addRow = () => {
    let defaultVal;
    if (allowedValues && allowedValues.length > 0) {
      // Pick the first option not already used, or fall back to the first option
      const usedVals = new Set(rows.map(r => r.value));
      defaultVal = allowedValues.find(v => !usedVals.has(v)) ?? allowedValues[0];
    } else {
      const type = guessType();
      defaultVal = type === 'number' ? 0 : type === 'boolean' ? false : '';
    }
    if (isArray) {
      setRows(prev => [...prev, { key: String(prev.length), value: defaultVal }]);
    } else {
      setRows(prev => [...prev, { key: '', value: defaultVal }]);
    }
  };

  const removeRow = (idx) => {
    setRows(prev => {
      const next = prev.filter((_, i) => i !== idx);
      if (isArray) return next.map((r, i) => ({ ...r, key: String(i) }));
      return next;
    });
  };

  const moveRow = (idx, dir) => {
    setRows(prev => {
      const next = [...prev];
      const target = idx + dir;
      if (target < 0 || target >= next.length) return prev;
      [next[idx], next[target]] = [next[target], next[idx]];
      if (isArray) return next.map((r, i) => ({ ...r, key: String(i) }));
      return next;
    });
  };

  const updateRowValue = (idx, newVal) => {
    setRows(prev => prev.map((r, i) => i === idx ? { ...r, value: newVal } : r));
  };

  const updateRowKey = (idx, newKey) => {
    setRows(prev => prev.map((r, i) => i === idx ? { ...r, key: newKey } : r));
  };

  const handleSave = () => {
    setError(null);
    try {
      if (isArray) {
        const result = rows.map(r => r.value);
        onSave(path, result);
      } else {
        // Check for duplicate keys
        const keys = rows.map(r => r.key.trim()).filter(Boolean);
        if (new Set(keys).size !== keys.length) {
          setError('Duplicate keys found');
          return;
        }
        const result = {};
        for (const r of rows) {
          if (r.key.trim()) result[r.key.trim()] = r.value;
        }
        onSave(path, result);
      }
      onClose();
    } catch (e) {
      setError(e.message);
    }
  };

  /** Parse a raw string from the input into the right JS type */
  const parseVal = (raw, currentVal) => {
    if (raw === 'true') return true;
    if (raw === 'false') return false;
    if (raw === 'null') return null;
    // If current value is a number, try to parse as number
    if (typeof currentVal === 'number' || (raw !== '' && !isNaN(Number(raw)))) {
      const n = Number(raw);
      if (!isNaN(n) && raw.trim() !== '') return n;
    }
    return raw;
  };

  const pathLabel = path.split('.').pop();
  const typeLabel = isArray ? `Array [${rows.length}]` : `Object {${rows.length}}`;

  return createPortal(
    <div
      ref={backdropRef}
      onClick={e => { if (e.target === backdropRef.current) onClose(); }}
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm p-4"
    >
      <div className="bg-white dark:bg-gray-800 rounded-xl shadow-2xl dark:shadow-black/40 w-full max-w-lg max-h-[80vh] flex flex-col overflow-hidden border border-gray-200 dark:border-gray-700">
        {/* Header */}
        <div className="flex items-center justify-between px-5 py-4 border-b border-gray-200 dark:border-gray-700">
          <div>
            <h3 className="text-sm font-bold text-gray-900 dark:text-white">{pathLabel}</h3>
            <span className="text-[10px] text-gray-400 dark:text-gray-500 font-mono">{path} &mdash; {typeLabel}</span>
          </div>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-600 dark:hover:text-gray-300 p-1">
            <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>

        {/* Table body */}
        <div className="flex-1 overflow-y-auto px-5 py-3">
          {error && (
            <div className="mb-3 text-xs text-red-600 dark:text-red-400 bg-red-50 dark:bg-red-900/20 rounded px-3 py-2">
              {error}
            </div>
          )}

          <table className="w-full text-xs">
            <thead>
              <tr className="text-left text-gray-400 dark:text-gray-500 border-b border-gray-100 dark:border-gray-700">
                {isArray && allowedValues && <th className="py-1.5 pr-1 w-5 font-medium" />}
                {isArray
                  ? <th className="py-1.5 pr-2 w-8 font-medium">#</th>
                  : <th className="py-1.5 pr-2 font-medium">Key</th>
                }
                <th className="py-1.5 pr-2 font-medium">Value</th>
                <th className="py-1.5 w-16 font-medium text-right">Actions</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row, idx) => (
                <tr
                  key={idx}
                  draggable={isArray && !!allowedValues}
                  onDragStart={() => isArray && allowedValues && handleRowDragStart(idx)}
                  onDragOver={e => isArray && allowedValues && handleRowDragOver(e, idx)}
                  onDrop={e => isArray && allowedValues && handleRowDrop(e, idx)}
                  onDragEnd={handleRowDragEnd}
                  className={`border-b border-gray-50 dark:border-gray-700/40 group transition-colors
                    ${dragOverRow === idx && dragRow !== idx ? 'bg-blue-50 dark:bg-blue-900/20' : ''}
                    ${dragRow === idx ? 'opacity-40' : ''}
                    ${isArray && allowedValues ? 'cursor-grab active:cursor-grabbing' : ''}`}
                >
                  {/* Drag handle — only for draggable method arrays */}
                  {isArray && allowedValues && (
                    <td className="py-1.5 pr-1 align-middle text-gray-300 dark:text-gray-600 select-none">
                      <svg className="w-3 h-3" fill="currentColor" viewBox="0 0 20 20">
                        <path d="M7 2a2 2 0 1 0 .001 4.001A2 2 0 0 0 7 2zm0 6a2 2 0 1 0 .001 4.001A2 2 0 0 0 7 8zm0 6a2 2 0 1 0 .001 4.001A2 2 0 0 0 7 14zm6-8a2 2 0 1 0-.001-4.001A2 2 0 0 0 13 6zm0 2a2 2 0 1 0 .001 4.001A2 2 0 0 0 13 8zm0 6a2 2 0 1 0 .001 4.001A2 2 0 0 0 13 14z"/>
                      </svg>
                    </td>
                  )}
                  {/* Key / index column */}
                  <td className="py-1.5 pr-2 align-middle">
                    {isArray ? (
                      <span className="text-gray-300 dark:text-gray-600 font-mono tabular-nums">{idx}</span>
                    ) : (
                      <input
                        type="text"
                        value={row.key}
                        onChange={e => updateRowKey(idx, e.target.value)}
                        className="w-full font-mono bg-transparent border-b border-dashed border-gray-200 dark:border-gray-600 text-gray-700 dark:text-gray-300 focus:outline-none focus:border-blue-400 py-0.5"
                        placeholder="key"
                      />
                    )}
                  </td>
                  {/* Value column */}
                  <td className="py-1.5 pr-2 align-middle">
                    {typeof row.value === 'boolean' ? (
                      <button
                        onClick={() => updateRowValue(idx, !row.value)}
                        className={`px-2 py-0.5 rounded text-[10px] font-semibold ${row.value ? 'bg-blue-100 dark:bg-blue-900/40 text-blue-700 dark:text-blue-300' : 'bg-gray-100 dark:bg-gray-700 text-gray-500 dark:text-gray-400'}`}
                      >
                        {String(row.value)}
                      </button>
                    ) : allowedValues && allowedValues.length > 0 ? (
                      <select
                        value={String(row.value)}
                        onChange={e => updateRowValue(idx, parseVal(e.target.value, row.value))}
                        className="w-full font-mono bg-gray-50 dark:bg-gray-700/60 border border-gray-200 dark:border-gray-600 rounded px-2 py-1 text-gray-700 dark:text-gray-300 focus:outline-none focus:ring-1 focus:ring-blue-400 cursor-pointer"
                      >
                        {/* Include current value if it's not in the predefined list */}
                        {!allowedValues.includes(String(row.value)) && row.value !== null && (
                          <option value={String(row.value)}>{String(row.value)}</option>
                        )}
                        {allowedValues.map(opt => (
                          <option key={opt} value={opt}>{opt}</option>
                        ))}
                      </select>
                    ) : (
                      <input
                        type="text"
                        value={row.value === null ? 'null' : String(row.value)}
                        onChange={e => updateRowValue(idx, parseVal(e.target.value, row.value))}
                        className="w-full font-mono bg-gray-50 dark:bg-gray-700/60 border border-gray-200 dark:border-gray-600 rounded px-2 py-1 text-gray-700 dark:text-gray-300 focus:outline-none focus:ring-1 focus:ring-blue-400"
                      />
                    )}
                  </td>
                  {/* Actions column */}
                  <td className="py-1.5 align-middle">
                    <div className="flex items-center justify-end gap-0.5 opacity-40 group-hover:opacity-100 transition-opacity">
                      {/* Show up/down buttons only when drag-and-drop is not available */}
                      {isArray && !allowedValues && (
                        <>
                          <button onClick={() => moveRow(idx, -1)} disabled={idx === 0}
                            className="p-0.5 text-gray-400 dark:text-gray-500 hover:text-gray-700 dark:hover:text-gray-200 disabled:opacity-20" title="Move up">
                            <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 15l7-7 7 7"/></svg>
                          </button>
                          <button onClick={() => moveRow(idx, 1)} disabled={idx === rows.length - 1}
                            className="p-0.5 text-gray-400 dark:text-gray-500 hover:text-gray-700 dark:hover:text-gray-200 disabled:opacity-20" title="Move down">
                            <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7"/></svg>
                          </button>
                        </>
                      )}
                      <button onClick={() => removeRow(idx)}
                        className="p-0.5 text-red-400 hover:text-red-600 dark:hover:text-red-300" title="Remove">
                        <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16"/></svg>
                      </button>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>

          {rows.length === 0 && (
            <div className="py-6 text-center text-gray-400 dark:text-gray-500 text-xs italic">
              Empty &mdash; click &ldquo;Add row&rdquo; to begin
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="flex items-center justify-between px-5 py-3 border-t border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-800/80">
          <button
            onClick={addRow}
            className="flex items-center gap-1 text-xs text-blue-600 dark:text-blue-400 hover:text-blue-800 dark:hover:text-blue-300 font-medium"
          >
            <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 4v16m8-8H4"/></svg>
            Add row
          </button>
          <div className="flex items-center gap-2">
            <button onClick={onClose} className="px-3 py-1.5 text-xs rounded-lg border border-gray-300 dark:border-gray-600 text-gray-600 dark:text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-700">
              Cancel
            </button>
            <button onClick={handleSave} className="px-4 py-1.5 text-xs rounded-lg bg-blue-600 text-white hover:bg-blue-700 font-medium shadow-sm">
              Save
            </button>
          </div>
        </div>
      </div>
    </div>,
    document.body
  );
}


/* ─── Editable parameter field ─── */
function ParamField({ label, path, value, onChange, saving, parametersSet, tooltip }) {
  const [modalOpen, setModalOpen] = useState(false);
  const isBool = typeof value === 'boolean';
  const isNumber = typeof value === 'number';
  const isNull = value === null || value === undefined;
  const isArray = Array.isArray(value);
  const isObj = !isNull && !isArray && typeof value === 'object';
  const isSensitive = /password|secret|token|account_key/i.test(label);

  // ── Strategy-aware visibility ──
  // Hide method_overrides + per-group method_selection rows when strategy = best_fit
  if (
    (path.startsWith('forecasting.method_overrides.') ||
     path.startsWith('forecasting.method_selection.')) &&
    parametersSet?.method_selection_strategy === 'best_fit'
  ) return null;
  // Hide best_fit_methods row when strategy = auto (or unset)
  if (
    path === 'forecasting.best_fit_methods' &&
    parametersSet?.method_selection_strategy !== 'best_fit'
  ) return null;

  // ── Label with optional ⓘ tooltip badge ──
  const LabelEl = (
    <label className="text-xs text-gray-500 dark:text-gray-400 min-w-0 flex items-center gap-1">
      <span className="break-all">{label}</span>
      {tooltip && (
        <span
          title={tooltip}
          aria-label={tooltip}
          className="flex-shrink-0 inline-flex items-center justify-center w-3.5 h-3.5 rounded-full bg-gray-200 dark:bg-gray-600 text-gray-500 dark:text-gray-300 text-[9px] font-bold cursor-help select-none leading-none"
        >?</span>
      )}
    </label>
  );

  if (isSensitive) {
    return (
      <div className="flex items-center justify-between gap-3 py-2 border-b border-gray-100 dark:border-gray-700/50 last:border-0">
        {LabelEl}
        <span className="text-xs font-mono text-gray-400 dark:text-gray-500 italic">{'********'}</span>
      </div>
    );
  }

  if (isBool) {
    return (
      <div className="flex items-center justify-between gap-3 py-2 border-b border-gray-100 dark:border-gray-700/50 last:border-0">
        {LabelEl}
        <button
          onClick={() => onChange(path, !value)}
          disabled={saving === path}
          className={`relative w-9 h-5 rounded-full transition-colors flex-shrink-0 ${value ? 'bg-blue-500' : 'bg-gray-300 dark:bg-gray-600'}`}
        >
          <span className={`absolute top-0.5 w-4 h-4 rounded-full bg-white shadow transition-transform ${value ? 'translate-x-4' : 'translate-x-0.5'}`} />
        </button>
      </div>
    );
  }

  if (isArray || isObj) {
    const count = isArray ? value.length : Object.keys(value).length;
    const preview = isArray
      ? value.slice(0, 3).map(v => typeof v === 'string' ? v : JSON.stringify(v)).join(', ') + (value.length > 3 ? ` +${value.length - 3}` : '')
      : Object.entries(value).slice(0, 2).map(([k,v]) => `${k}: ${typeof v === 'string' ? v : JSON.stringify(v)}`).join(', ') + (Object.keys(value).length > 2 ? ' ...' : '');

    return (
      <div className="flex items-center justify-between gap-3 py-2 border-b border-gray-100 dark:border-gray-700/50 last:border-0">
        {LabelEl}
        <button
          onClick={() => setModalOpen(true)}
          className="flex items-center gap-1.5 text-xs font-mono bg-gray-50 dark:bg-gray-700 border border-gray-200 dark:border-gray-600 rounded px-2 py-1 max-w-[220px] text-left text-gray-600 dark:text-gray-300 hover:border-blue-400 dark:hover:border-blue-500 hover:bg-blue-50 dark:hover:bg-blue-900/20 transition-colors group"
        >
          <span className="text-[10px] font-semibold text-gray-400 dark:text-gray-500 flex-shrink-0">
            {isArray ? `[${count}]` : `{${count}}`}
          </span>
          <span className="truncate">{preview || (isArray ? '[]' : '{}')}</span>
          <svg className="w-3 h-3 text-gray-400 group-hover:text-blue-500 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15.232 5.232l3.536 3.536m-2.036-5.036a2.5 2.5 0 113.536 3.536L6.5 21.036H3v-3.572L16.732 3.732z" />
          </svg>
        </button>
        {modalOpen && (
          <JsonTableModal
            path={path}
            value={value}
            onSave={onChange}
            onClose={() => setModalOpen(false)}
            allowedValues={ARRAY_ITEM_OPTIONS[path] || null}
          />
        )}
      </div>
    );
  }

  // Nullable method override dropdown (auto mode: pin one method per demand group)
  const nullableOpts = NULLABLE_METHOD_OPTIONS[path];
  if (nullableOpts) {
    const warn = value ? METHOD_WARNINGS[value] : null;
    return (
      <div className="flex flex-col gap-0.5 py-2 border-b border-gray-100 dark:border-gray-700/50 last:border-0">
        <div className="flex items-center justify-between gap-3">
          {LabelEl}
          <select
            value={value ?? '__auto__'}
            onChange={e => onChange(path, e.target.value === '__auto__' ? null : e.target.value)}
            disabled={saving === path}
            className="text-xs font-mono bg-gray-50 dark:bg-gray-700 border border-gray-200 dark:border-gray-600 rounded px-2 py-1 w-52 text-right text-gray-700 dark:text-gray-300 focus:outline-none focus:ring-1 focus:ring-blue-400 cursor-pointer"
          >
            <option value="__auto__">— Auto (run all) —</option>
            {nullableOpts.map(opt => (
              <option key={opt} value={opt}>{opt}</option>
            ))}
          </select>
        </div>
        {warn && (
          <div className="flex items-start gap-1 text-[10px] text-amber-600 dark:text-amber-400 justify-end pr-0.5">
            <svg className="w-3 h-3 mt-0.5 flex-shrink-0" fill="currentColor" viewBox="0 0 20 20">
              <path fillRule="evenodd" d="M8.485 2.495c.673-1.167 2.357-1.167 3.03 0l6.28 10.875c.673 1.167-.17 2.625-1.516 2.625H3.72c-1.347 0-2.189-1.458-1.515-2.625L8.485 2.495zM10 5a.75.75 0 01.75.75v3.5a.75.75 0 01-1.5 0v-3.5A.75.75 0 0110 5zm0 9a1 1 0 100-2 1 1 0 000 2z" clipRule="evenodd" />
            </svg>
            <span>{warn}</span>
          </div>
        )}
      </div>
    );
  }

  // Dropdown for scalar params with a known option set
  const dropdownOpts = PARAM_OPTIONS[path];
  if (dropdownOpts && !isNull) {
    // Build option list: include the current value even if it's not predefined (avoid data loss)
    const allOpts = dropdownOpts.includes(String(value)) ? dropdownOpts : [String(value), ...dropdownOpts];
    return (
      <div className="flex items-center justify-between gap-3 py-2 border-b border-gray-100 dark:border-gray-700/50 last:border-0">
        {LabelEl}
        <select
          value={String(value)}
          onChange={e => onChange(path, isNumber ? Number(e.target.value) : e.target.value)}
          disabled={saving === path}
          className="text-xs font-mono bg-gray-50 dark:bg-gray-700 border border-gray-200 dark:border-gray-600 rounded px-2 py-1 w-48 text-right text-gray-700 dark:text-gray-300 focus:outline-none focus:ring-1 focus:ring-blue-400 cursor-pointer"
        >
          {allOpts.map(opt => (
            <option key={opt} value={opt}>{opt}</option>
          ))}
        </select>
      </div>
    );
  }

  return (
    <div className="flex items-center justify-between gap-3 py-2 border-b border-gray-100 dark:border-gray-700/50 last:border-0">
      <label className="text-xs text-gray-500 dark:text-gray-400 min-w-0 break-all">{label}</label>
      <input
        type={isNumber ? 'number' : 'text'}
        defaultValue={isNull ? '' : String(value)}
        onBlur={e => {
          let v = e.target.value;
          if (isNumber) v = v.includes('.') ? parseFloat(v) : parseInt(v, 10);
          if (v === '' && isNull) return;
          if (v === 'null') v = null;
          onChange(path, v);
        }}
        placeholder={isNull ? 'null' : ''}
        className="text-xs font-mono bg-gray-50 dark:bg-gray-700 border border-gray-200 dark:border-gray-600 rounded px-2 py-1 w-48 text-right text-gray-700 dark:text-gray-300 focus:outline-none focus:ring-1 focus:ring-blue-400"
      />
    </div>
  );
}

/* ─── Flatten nested config into dot-path entries for a section ─── */
/* Each entry gets a `group` string when nested >1 level deep,           */
/* so we can render sub-headers like "Method Selection", "Backtesting".  */
function flattenConfig(obj, prefix = '', depth = 0) {
  const entries = [];
  if (!obj || typeof obj !== 'object') return entries;
  for (const [k, v] of Object.entries(obj)) {
    const path = prefix ? `${prefix}.${k}` : k;
    if (v !== null && typeof v === 'object' && !Array.isArray(v)) {
      // Tag child entries with the group name when we recurse past depth 0
      const children = flattenConfig(v, path, depth + 1);
      children.forEach(c => { if (depth >= 0 && !c.group) c.group = k; });
      entries.push(...children);
    } else {
      entries.push({ key: k, path, value: v, group: null });
    }
  }
  return entries;
}

/* Group flat entries by their `group` field (preserving order). */
/* Returns: [{ group: string|null, label: string|null, entries: [...] }] */
function groupEntries(entries) {
  const groups = [];
  let currentGroup = null;
  let currentBucket = [];

  for (const entry of entries) {
    const g = entry.group || null;
    if (g !== currentGroup) {
      if (currentBucket.length > 0) {
        groups.push({ group: currentGroup, label: currentGroup ? fmtGroupLabel(currentGroup) : null, entries: currentBucket });
      }
      currentGroup = g;
      currentBucket = [];
    }
    currentBucket.push(entry);
  }
  if (currentBucket.length > 0) {
    groups.push({ group: currentGroup, label: currentGroup ? fmtGroupLabel(currentGroup) : null, entries: currentBucket });
  }
  return groups;
}

function fmtGroupLabel(raw) {
  return raw.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
}

/* ─── Version Create/Edit Modal ─── */
function VersionModal({ version, segments, onSave, onClose }) {
  const isNew = !version?.id || version._isNew;
  const [name, setName] = useState(version?.name ?? '');
  const [description, setDescription] = useState(version?.description ?? '');
  const [selectedSegmentIds, setSelectedSegmentIds] = useState(version?.segment_ids ?? []);
  const [saving, setSavingState] = useState(false);
  const [error, setError] = useState('');
  const backdropRef = useRef(null);

  useEffect(() => {
    const handler = (e) => { if (e.key === 'Escape') onClose(); };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [onClose]);

  const toggleSegment = (sid) => {
    setSelectedSegmentIds(prev =>
      prev.includes(sid) ? prev.filter(id => id !== sid) : [...prev, sid]
    );
  };

  async function handleSave() {
    if (!name.trim()) { setError('Name is required'); return; }
    setSavingState(true);
    setError('');
    try {
      let savedId;
      if (isNew) {
        const res = await api.post('/parameters', {
          parameter_type: version.parameter_type,
          name: name.trim(),
          description: description || null,
          parameters_set: version.parameters_set || {},
          clone_from_id: version.clone_from_id || null,
        });
        savedId = res.data.id;
      } else {
        await api.put(`/parameters/${version.id}`, {
          name: name.trim(),
          description: description || null,
        });
        savedId = version.id;
      }
      // Set segment associations
      await api.put(`/parameters/${savedId}/segments`, {
        segment_ids: selectedSegmentIds,
      });
      onSave();
    } catch (e) {
      setError(e.response?.data?.detail || e.message);
    } finally {
      setSavingState(false);
    }
  }

  return createPortal(
    <div
      ref={backdropRef}
      onClick={e => { if (e.target === backdropRef.current) onClose(); }}
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm p-4"
    >
      <div className="bg-white dark:bg-gray-800 rounded-xl shadow-2xl dark:shadow-black/40 w-full max-w-md flex flex-col overflow-hidden border border-gray-200 dark:border-gray-700">
        {/* Header */}
        <div className="flex items-center justify-between px-5 py-4 border-b border-gray-200 dark:border-gray-700">
          <h3 className="text-sm font-bold text-gray-900 dark:text-white">
            {isNew ? 'New Version' : 'Edit Version'}
            <span className="ml-2 text-[10px] font-normal text-gray-400 dark:text-gray-500">
              {version?.parameter_type}
            </span>
          </h3>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-600 dark:hover:text-gray-300 p-1">
            <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>

        {/* Body */}
        <div className="px-5 py-4 space-y-4 max-h-[60vh] overflow-y-auto">
          {error && (
            <div className="text-xs text-red-600 dark:text-red-400 bg-red-50 dark:bg-red-900/20 rounded px-3 py-2">
              {error}
            </div>
          )}
          <div>
            <label className="block text-xs font-medium text-gray-700 dark:text-gray-300 mb-1">Name *</label>
            <input
              type="text"
              value={name}
              onChange={e => setName(e.target.value)}
              placeholder="e.g. High Accuracy"
              className="w-full border border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-white rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-400"
              autoFocus
            />
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-700 dark:text-gray-300 mb-1">Description</label>
            <input
              type="text"
              value={description}
              onChange={e => setDescription(e.target.value)}
              placeholder="Optional description"
              className="w-full border border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-white rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-400"
            />
          </div>
          {/* Segment association */}
          <div>
            <label className="block text-xs font-medium text-gray-700 dark:text-gray-300 mb-2">
              Segments
              <span className="ml-1 font-normal text-gray-400">({selectedSegmentIds.length} selected)</span>
            </label>
            <div className="space-y-1 max-h-40 overflow-y-auto border border-gray-200 dark:border-gray-600 rounded-lg p-2">
              {segments.filter(s => !s.is_default).length === 0 ? (
                <div className="text-xs text-gray-400 dark:text-gray-500 italic py-2 text-center">No segments available</div>
              ) : (
                segments.filter(s => !s.is_default).map(seg => (
                  <label key={seg.id} className="flex items-center gap-2 py-1 px-2 rounded hover:bg-gray-50 dark:hover:bg-gray-700/50 cursor-pointer">
                    <input
                      type="checkbox"
                      checked={selectedSegmentIds.includes(seg.id)}
                      onChange={() => toggleSegment(seg.id)}
                      className="rounded border-gray-300 dark:border-gray-600 text-blue-600 focus:ring-blue-500"
                    />
                    <span className="text-xs text-gray-700 dark:text-gray-300">{seg.name}</span>
                    {seg.member_count != null && (
                      <span className="text-[10px] text-gray-400 dark:text-gray-500 ml-auto">{seg.member_count} members</span>
                    )}
                  </label>
                ))
              )}
            </div>
          </div>
        </div>

        {/* Footer */}
        <div className="flex items-center justify-end gap-2 px-5 py-3 border-t border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-800/80">
          <button onClick={onClose} className="px-3 py-1.5 text-xs rounded-lg border border-gray-300 dark:border-gray-600 text-gray-600 dark:text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-700">
            Cancel
          </button>
          <button
            onClick={handleSave}
            disabled={saving}
            className="px-4 py-1.5 text-xs rounded-lg bg-blue-600 text-white hover:bg-blue-700 font-medium shadow-sm disabled:opacity-50"
          >
            {saving ? 'Saving...' : isNew ? 'Create' : 'Save'}
          </button>
        </div>
      </div>
    </div>,
    document.body
  );
}


/* ─── Config Tab (sub-tabbed, DB-backed, versioned) ─── */
function ConfigTab({ filterTypes, title = 'System Configuration', subtitle = 'Edit pipeline parameters below.' }) {
  const [sections, setSections] = useState([]);
  const [segments, setSegments] = useState([]);
  const [activeSection, setActiveSection] = useState(null);
  const [activeVersion, setActiveVersion] = useState(null);
  const [versionModal, setVersionModal] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [searchTerm, setSearchTerm] = useState('');
  const [yamlOpen, setYamlOpen] = useState(false);
  const [rawYaml, setRawYaml] = useState('');
  const [saving, setSaving] = useState(null);
  const [saveMsg, setSaveMsg] = useState(null);
  const [deleting, setDeleting] = useState(null);
  const [dragIdx, setDragIdx] = useState(null);
  const [dragOverIdx, setDragOverIdx] = useState(null);

  const loadAll = useCallback(() => {
    setLoading(true);
    Promise.all([
      api.get('/parameters'),
      api.get('/config'),
      api.get('/segments'),
    ])
      .then(([sectRes, cfgRes, segRes]) => {
        const allSects = sectRes.data;
        // Filter to only the parameter types this tab cares about
        const sects = filterTypes ? allSects.filter(s => filterTypes.has(s.parameter_type)) : allSects;
        setSections(sects);
        setSegments(segRes.data);
        // Set active section to first type if not already set
        setActiveSection(prev => {
          const firstType = sects[0]?.parameter_type;
          return prev && sects.some(s => s.parameter_type === prev) ? prev : (firstType ?? null);
        });
        // Set active version
        setActiveVersion(prev => {
          if (prev && sects.some(s => s.id === prev)) return prev;
          const firstType = sects[0]?.parameter_type;
          const def = sects.find(s => s.parameter_type === firstType && s.is_default);
          return def?.id ?? sects[0]?.id ?? null;
        });
        setRawYaml(cfgRes.data.config_yaml ?? '');
        setLoading(false);
      })
      .catch(err => {
        setError(err.response?.data?.detail || err.message);
        setLoading(false);
      });
  }, [filterTypes]);

  useEffect(() => { loadAll(); }, [loadAll]);

  // Group sections by parameter_type, dedupe type tabs
  const typeGroups = useMemo(() => {
    const map = {};
    for (const s of sections) {
      if (!map[s.parameter_type]) map[s.parameter_type] = [];
      map[s.parameter_type].push(s);
    }
    return map;
  }, [sections]);

  const paramTypes = useMemo(() => {
    const seen = new Set();
    const raw = sections
      .map(s => s.parameter_type)
      .filter(pt => { if (seen.has(pt)) return false; seen.add(pt); return true; });
    // Sort by predefined pipeline execution order; unknowns go at the end
    return raw.sort((a, b) => {
      const ai = BUSINESS_PARAM_TYPE_ORDER.indexOf(a);
      const bi = BUSINESS_PARAM_TYPE_ORDER.indexOf(b);
      return (ai === -1 ? 999 : ai) - (bi === -1 ? 999 : bi);
    });
  }, [sections]);

  // Versions for the active type, sorted by priority (sort_order ASC)
  const versionsForType = useMemo(() => {
    const vers = typeGroups[activeSection] ?? [];
    return [...vers].sort((a, b) => (a.sort_order ?? 9999) - (b.sort_order ?? 9999));
  }, [typeGroups, activeSection]);
  const activeVersionObj = sections.find(s => s.id === activeVersion);

  // When activeSection changes, switch to default version of that type
  const handleTypeChange = (pt) => {
    setActiveSection(pt);
    const vers = typeGroups[pt] ?? [];
    const def = vers.find(v => v.is_default);
    setActiveVersion(def?.id ?? vers[0]?.id ?? null);
    setSearchTerm('');
  };

  const handleParamChange = async (path, value) => {
    const sect = sections.find(s => s.id === activeVersion);
    if (!sect) return;

    const parts = path.split('.').slice(1); // strip section prefix
    const updatedConfig = JSON.parse(JSON.stringify(sect.parameters_set));
    let node = updatedConfig;
    for (let i = 0; i < parts.length - 1; i++) {
      if (node[parts[i]] === undefined) node[parts[i]] = {};
      node = node[parts[i]];
    }
    node[parts[parts.length - 1]] = value;

    setSaving(path);
    setSaveMsg(null);
    try {
      await api.put(`/parameters/${sect.id}`, { parameters_set: updatedConfig });
      setSections(prev => prev.map(s =>
        s.id === sect.id ? { ...s, parameters_set: updatedConfig } : s
      ));
      setSaveMsg({ type: 'ok', text: `${path} updated` });
      setTimeout(() => setSaveMsg(null), 3000);
    } catch (err) {
      setSaveMsg({ type: 'err', text: `Failed: ${err.response?.data?.detail || err.message}` });
    } finally {
      setSaving(null);
    }
  };

  const handleClone = (ver) => {
    setVersionModal({
      _isNew: true,
      parameter_type: ver.parameter_type,
      name: `${ver.name} (Copy)`,
      description: ver.description || '',
      parameters_set: ver.parameters_set,
      clone_from_id: ver.id,
      segment_ids: [],
    });
  };

  const handleDeleteVersion = async (ver) => {
    if (!window.confirm(`Delete version "${ver.name}" for ${ver.label}?`)) return;
    setDeleting(ver.id);
    try {
      await api.delete(`/parameters/${ver.id}`);
      setSections(prev => prev.filter(s => s.id !== ver.id));
      // Switch to default version of same type
      const def = sections.find(s => s.parameter_type === ver.parameter_type && s.is_default && s.id !== ver.id);
      if (def) setActiveVersion(def.id);
      setSaveMsg({ type: 'ok', text: `Version "${ver.name}" deleted` });
      setTimeout(() => setSaveMsg(null), 3000);
    } catch (err) {
      setSaveMsg({ type: 'err', text: err.response?.data?.detail || err.message });
    } finally {
      setDeleting(null);
    }
  };

  const handleVersionModalSave = () => {
    setVersionModal(null);
    loadAll();
  };

  const handleDragDrop = async (fromIdx, toIdx) => {
    if (fromIdx === toIdx) return;
    const reordered = [...versionsForType];
    const [moved] = reordered.splice(fromIdx, 1);
    reordered.splice(toIdx, 0, moved);
    // Default must remain last
    const defIdx = reordered.findIndex(v => v.is_default);
    if (defIdx !== -1 && defIdx !== reordered.length - 1) {
      const [def] = reordered.splice(defIdx, 1);
      reordered.push(def);
    }
    const ordered_ids = reordered.map(v => v.id);
    try {
      await api.put('/parameters/reorder', {
        parameter_type: activeSection,
        ordered_ids,
      });
      loadAll();
    } catch (err) {
      setSaveMsg({ type: 'err', text: err.response?.data?.detail || err.message });
    }
  };

  const entries = activeVersionObj ? flattenConfig(activeVersionObj.parameters_set, activeVersionObj.parameter_type) : [];
  const filteredEntries = searchTerm
    ? entries.filter(e =>
        e.key.toLowerCase().includes(searchTerm.toLowerCase()) ||
        e.path.toLowerCase().includes(searchTerm.toLowerCase()) ||
        String(e.value).toLowerCase().includes(searchTerm.toLowerCase()))
    : entries;
  const groups = groupEntries(filteredEntries);

  return (
    <div className="space-y-4">
      {/* Header */}
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900/50 p-6">
        <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3 mb-1">
          <div>
            <h2 className="text-lg font-semibold text-gray-900 dark:text-white">{title}</h2>
            <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
              {subtitle}
            </p>
          </div>
          {saveMsg && (
            <span className={`self-start text-xs px-2.5 py-1 rounded-full font-medium ${saveMsg.type === 'ok' ? 'bg-emerald-100 dark:bg-emerald-900/30 text-emerald-700 dark:text-emerald-400' : 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400'}`}>
              {saveMsg.text}
            </span>
          )}
        </div>
        <div className="mt-4">
          <input
            type="text"
            value={searchTerm}
            onChange={e => setSearchTerm(e.target.value)}
            placeholder="Filter parameters..."
            className="w-full sm:w-72 border border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-white rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-400"
          />
        </div>
      </div>

      {loading && (
        <div className="flex items-center gap-2 text-gray-400 dark:text-gray-500 py-8 justify-center">
          <svg className="animate-spin h-5 w-5" viewBox="0 0 24 24">
            <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" fill="none" />
            <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
          </svg>
          Loading configuration...
        </div>
      )}
      {error && (
        <div className="text-red-500 dark:text-red-400 bg-red-50 dark:bg-red-900/20 rounded-lg p-4 text-sm">
          Error loading config: {error}
        </div>
      )}

      {/* Sub-tab bar — one pill per parameter type */}
      {paramTypes.length > 0 && (
        <div className="flex gap-1 overflow-x-auto pb-1 border-b border-gray-200 dark:border-gray-700">
          {paramTypes.map(pt => {
            const first = typeGroups[pt]?.[0];
            return (
              <button
                key={pt}
                onClick={() => handleTypeChange(pt)}
                className={`px-3 py-2 text-xs font-medium border-b-2 whitespace-nowrap flex-shrink-0 transition-colors
                  ${activeSection === pt
                    ? 'border-blue-600 text-blue-600 dark:text-blue-400 dark:border-blue-400'
                    : 'border-transparent text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300'
                  }`}
              >
                {first?.label ?? pt}
              </button>
            );
          })}
        </div>
      )}

      {/* Version bar (drag-and-drop reorderable) */}
      {versionsForType.length > 0 && (
        <div className="flex items-center gap-2 flex-wrap">
          {versionsForType.map((ver, idx) => (
            <button
              key={ver.id}
              draggable={!ver.is_default}
              onDragStart={e => { if (ver.is_default) { e.preventDefault(); return; } setDragIdx(idx); e.dataTransfer.effectAllowed = 'move'; }}
              onDragOver={e => { e.preventDefault(); e.dataTransfer.dropEffect = 'move'; setDragOverIdx(idx); }}
              onDragLeave={() => setDragOverIdx(null)}
              onDrop={e => { e.preventDefault(); if (dragIdx !== null) handleDragDrop(dragIdx, idx); setDragIdx(null); setDragOverIdx(null); }}
              onDragEnd={() => { setDragIdx(null); setDragOverIdx(null); }}
              onClick={() => setActiveVersion(ver.id)}
              className={`px-3 py-1.5 text-xs rounded-full border transition-colors flex items-center gap-1.5
                ${dragIdx === idx ? 'opacity-40' : ''}
                ${dragOverIdx === idx && dragIdx !== idx ? 'ring-2 ring-blue-400 dark:ring-blue-500' : ''}
                ${activeVersion === ver.id
                  ? 'bg-blue-100 dark:bg-blue-900/40 border-blue-400 dark:border-blue-500 text-blue-700 dark:text-blue-300 font-medium'
                  : 'border-gray-200 dark:border-gray-600 text-gray-500 dark:text-gray-400 hover:border-gray-300 dark:hover:border-gray-500'
                }`}
            >
              {!ver.is_default && (
                <svg className="w-3 h-3 opacity-40 cursor-grab flex-shrink-0" viewBox="0 0 24 24" fill="currentColor">
                  <circle cx="8" cy="4" r="2"/><circle cx="16" cy="4" r="2"/>
                  <circle cx="8" cy="12" r="2"/><circle cx="16" cy="12" r="2"/>
                  <circle cx="8" cy="20" r="2"/><circle cx="16" cy="20" r="2"/>
                </svg>
              )}
              {ver.name}
              {ver.is_default && <span className="text-[10px] opacity-60">(default)</span>}
              {ver.segment_ids?.length > 0 && (
                <span className="bg-emerald-100 dark:bg-emerald-900/30 text-emerald-700 dark:text-emerald-400 text-[10px] px-1.5 rounded-full">
                  {ver.segment_ids.length} seg
                </span>
              )}
            </button>
          ))}
          <button
            onClick={() => {
              const def = versionsForType.find(v => v.is_default);
              setVersionModal({
                _isNew: true,
                parameter_type: activeSection,
                name: '',
                description: '',
                parameters_set: def?.parameters_set || {},
                clone_from_id: def?.id || null,
                segment_ids: [],
              });
            }}
            className="px-3 py-1.5 text-xs rounded-full border border-dashed border-blue-300 dark:border-blue-600 text-blue-500 dark:text-blue-400 hover:bg-blue-50 dark:hover:bg-blue-900/20 transition-colors"
          >
            + New Version
          </button>
        </div>
      )}

      {/* Active version header with actions */}
      {activeVersionObj && (
        <div className="flex items-center justify-between bg-gray-50 dark:bg-gray-700/30 rounded-lg px-4 py-2.5">
          <div className="flex items-center gap-2 flex-wrap min-w-0">
            <span className="font-medium text-sm text-gray-900 dark:text-white">{activeVersionObj.name}</span>
            {activeVersionObj.is_default && (
              <span className="text-[10px] bg-blue-100 dark:bg-blue-900/30 text-blue-600 dark:text-blue-400 px-1.5 py-0.5 rounded-full font-medium">default</span>
            )}
            {activeVersionObj.description && (
              <span className="text-xs text-gray-500 dark:text-gray-400 truncate">{activeVersionObj.description}</span>
            )}
            {activeVersionObj.segment_ids?.map(sid => {
              const seg = segments.find(s => s.id === sid);
              return seg ? (
                <span key={sid} className="text-[10px] bg-emerald-50 dark:bg-emerald-900/20 text-emerald-600 dark:text-emerald-400 px-1.5 py-0.5 rounded font-medium">
                  {seg.name}
                </span>
              ) : null;
            })}
          </div>
          <div className="flex gap-1.5 flex-shrink-0 ml-2">
            <button
              onClick={() => setVersionModal(activeVersionObj)}
              className="text-xs px-2 py-1 rounded border border-gray-200 dark:border-gray-600 text-gray-500 dark:text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-700 transition-colors"
            >
              Edit
            </button>
            <button
              onClick={() => handleClone(activeVersionObj)}
              className="text-xs px-2 py-1 rounded border border-gray-200 dark:border-gray-600 text-gray-500 dark:text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-700 transition-colors"
            >
              Clone
            </button>
            {!activeVersionObj.is_default && (
              <button
                onClick={() => handleDeleteVersion(activeVersionObj)}
                disabled={deleting === activeVersionObj.id}
                className="text-xs px-2 py-1 rounded border border-red-200 dark:border-red-800 text-red-500 dark:text-red-400 hover:bg-red-50 dark:hover:bg-red-900/20 transition-colors disabled:opacity-50"
              >
                {deleting === activeVersionObj.id ? 'Deleting...' : 'Delete'}
              </button>
            )}
          </div>
        </div>
      )}

      {/* Active version parameters */}
      {activeVersionObj && (
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900/50 px-5 py-4">
          {groups.map((grp, gi) => (
            <div key={grp.group || gi}>
              {grp.label && (
                <div className="mt-3 mb-1 flex items-center gap-2">
                  <span className="text-[10px] font-bold uppercase tracking-wider text-blue-500 dark:text-blue-400">{grp.label}</span>
                  <div className="flex-1 h-px bg-blue-100 dark:bg-blue-900/40" />
                </div>
              )}
              {grp.entries.map(({ key, path, value }) => (
                <ParamField
                  key={path}
                  label={key}
                  path={path}
                  value={value}
                  onChange={handleParamChange}
                  saving={saving}
                  parametersSet={activeVersionObj?.parameters_set}
                  tooltip={PARAM_TOOLTIPS[path]}
                />
              ))}
            </div>
          ))}
          {filteredEntries.length === 0 && searchTerm && (
            <div className="py-6 text-center text-gray-400 dark:text-gray-500 text-xs italic">
              No parameters match &ldquo;{searchTerm}&rdquo;
            </div>
          )}
          {filteredEntries.length === 0 && !searchTerm && (
            <div className="py-6 text-center text-gray-400 dark:text-gray-500 text-xs italic">
              No parameters in this version
            </div>
          )}
        </div>
      )}

      {/* Collapsible raw YAML (system config only) */}
      {rawYaml && filterTypes?.has('data_source') && (
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900/50 overflow-hidden">
          <button
            onClick={() => setYamlOpen(o => !o)}
            className="flex items-center justify-between w-full px-5 py-3.5 text-left hover:bg-gray-50 dark:hover:bg-gray-700/50 transition-colors"
          >
            <div className="flex items-center gap-2">
              <svg className="w-4 h-4 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 20l4-16m4 4l4 4-4 4M6 16l-4-4 4-4" />
              </svg>
              <span className="text-sm font-semibold text-gray-900 dark:text-white">Raw YAML</span>
              <span className="text-[10px] bg-gray-100 dark:bg-gray-700 text-gray-500 dark:text-gray-400 px-1.5 py-0.5 rounded-full">config.yaml (default version)</span>
            </div>
            <svg className={`w-4 h-4 text-gray-400 transition-transform flex-shrink-0 ${yamlOpen ? 'rotate-180' : ''}`}
              fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
            </svg>
          </button>
          {yamlOpen && (
            <div className="px-5 pb-5">
              <pre className="bg-gray-900 dark:bg-gray-950 text-gray-300 rounded-lg p-4 text-xs font-mono overflow-x-auto max-h-[600px] overflow-y-auto leading-5 border border-gray-700 dark:border-gray-600 selection:bg-blue-800">
                {rawYaml}
              </pre>
            </div>
          )}
        </div>
      )}

      {/* Version modal */}
      {versionModal && (
        <VersionModal
          version={versionModal}
          segments={segments}
          onSave={handleVersionModalSave}
          onClose={() => setVersionModal(null)}
        />
      )}
    </div>
  );
}

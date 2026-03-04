"""
Generate a professional PDF from the Forecasting Methodology document.
Uses reportlab for high-quality output with tables, headers, and formatted text.
"""
import os
import sys
from pathlib import Path

from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm, cm
from reportlab.lib.colors import HexColor, black, white, Color
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_JUSTIFY
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    PageBreak, KeepTogether, HRFlowable, ListFlowable, ListItem
)
from reportlab.platypus.tableofcontents import TableOfContents
from reportlab.lib.fonts import addMapping

# ── Colors ──
BLUE = HexColor('#1e40af')
DARK_BLUE = HexColor('#1e3a5f')
LIGHT_BLUE = HexColor('#dbeafe')
MED_BLUE = HexColor('#3b82f6')
GRAY = HexColor('#6b7280')
LIGHT_GRAY = HexColor('#f3f4f6')
DARK_GRAY = HexColor('#374151')
GREEN = HexColor('#059669')
ORANGE = HexColor('#d97706')
TABLE_HEADER_BG = HexColor('#1e40af')
TABLE_ALT_BG = HexColor('#f0f4ff')

WIDTH, HEIGHT = A4

# ── Styles ──
styles = getSampleStyleSheet()
styles.add(ParagraphStyle(
    'DocTitle', parent=styles['Title'], fontSize=26, textColor=DARK_BLUE,
    spaceAfter=6, fontName='Helvetica-Bold', alignment=TA_LEFT
))
styles.add(ParagraphStyle(
    'DocSubtitle', parent=styles['Normal'], fontSize=12, textColor=GRAY,
    spaceAfter=20, fontName='Helvetica'
))
styles.add(ParagraphStyle(
    'H1', parent=styles['Heading1'], fontSize=18, textColor=DARK_BLUE,
    spaceBefore=24, spaceAfter=10, fontName='Helvetica-Bold'
))
styles.add(ParagraphStyle(
    'H2', parent=styles['Heading2'], fontSize=14, textColor=BLUE,
    spaceBefore=16, spaceAfter=8, fontName='Helvetica-Bold'
))
styles.add(ParagraphStyle(
    'H3', parent=styles['Heading3'], fontSize=12, textColor=MED_BLUE,
    spaceBefore=12, spaceAfter=6, fontName='Helvetica-Bold'
))
styles.add(ParagraphStyle(
    'Body', parent=styles['Normal'], fontSize=9.5, textColor=DARK_GRAY,
    spaceAfter=6, fontName='Helvetica', leading=13, alignment=TA_JUSTIFY
))
styles.add(ParagraphStyle(
    'CodeBlock', parent=styles['Normal'], fontSize=8.5, textColor=HexColor('#1f2937'),
    fontName='Courier', backColor=LIGHT_GRAY, spaceBefore=4, spaceAfter=4,
    leftIndent=12, rightIndent=12, leading=11, borderPadding=4
))
styles.add(ParagraphStyle(
    'TableCell', parent=styles['Normal'], fontSize=8.5, textColor=DARK_GRAY,
    fontName='Helvetica', leading=11
))
styles.add(ParagraphStyle(
    'TableHeader', parent=styles['Normal'], fontSize=8.5, textColor=white,
    fontName='Helvetica-Bold', leading=11
))
styles.add(ParagraphStyle(
    'BulletItem', parent=styles['Normal'], fontSize=9.5, textColor=DARK_GRAY,
    fontName='Helvetica', leading=13, bulletIndent=12, leftIndent=24
))
styles.add(ParagraphStyle(
    'Note', parent=styles['Normal'], fontSize=9, textColor=HexColor('#92400e'),
    fontName='Helvetica-Oblique', leftIndent=12, rightIndent=12,
    backColor=HexColor('#fffbeb'), spaceBefore=6, spaceAfter=6, leading=12,
    borderPadding=6
))

def make_table(headers, rows, col_widths=None):
    """Create a styled table."""
    header_cells = [Paragraph(h, styles['TableHeader']) for h in headers]
    data = [header_cells]
    for row in rows:
        data.append([Paragraph(str(cell), styles['TableCell']) for cell in row])

    avail = WIDTH - 2 * 2 * cm
    if col_widths is None:
        col_widths = [avail / len(headers)] * len(headers)
    else:
        total = sum(col_widths)
        col_widths = [w / total * avail for w in col_widths]

    t = Table(data, colWidths=col_widths, repeatRows=1)
    style_cmds = [
        ('BACKGROUND', (0, 0), (-1, 0), TABLE_HEADER_BG),
        ('TEXTCOLOR', (0, 0), (-1, 0), white),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 8.5),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 6),
        ('TOPPADDING', (0, 0), (-1, 0), 6),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ('FONTSIZE', (0, 1), (-1, -1), 8.5),
        ('TOPPADDING', (0, 1), (-1, -1), 4),
        ('BOTTOMPADDING', (0, 1), (-1, -1), 4),
        ('GRID', (0, 0), (-1, -1), 0.5, HexColor('#d1d5db')),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [white, TABLE_ALT_BG]),
    ]
    t.setStyle(TableStyle(style_cmds))
    return t


def build_pdf(output_path):
    doc = SimpleDocTemplate(
        output_path, pagesize=A4,
        leftMargin=2*cm, rightMargin=2*cm,
        topMargin=2*cm, bottomMargin=2*cm
    )
    story = []

    # ── Title Page ──
    story.append(Spacer(1, 4 * cm))
    story.append(Paragraph("ForecastAI", styles['DocTitle']))
    story.append(Paragraph("Forecasting Methodology", ParagraphStyle(
        'SubT', parent=styles['DocTitle'], fontSize=20, textColor=MED_BLUE
    )))
    story.append(Spacer(1, 1 * cm))
    story.append(Paragraph("Detailed Technical Reference", styles['DocSubtitle']))
    story.append(HRFlowable(width="50%", color=MED_BLUE, thickness=2))
    story.append(Spacer(1, 1 * cm))
    story.append(Paragraph("Version 2026.01 &mdash; March 2026", styles['Body']))
    story.append(Paragraph("This document describes in detail how time series are characterized, "
                          "how forecasting methods are selected and parameterized, how backtesting "
                          "is performed, and how the best method is chosen for each series.", styles['Body']))
    story.append(PageBreak())

    # ══════════════════════════════════════════════════════════════════════
    # 1. PIPELINE OVERVIEW
    # ══════════════════════════════════════════════════════════════════════
    story.append(Paragraph("1. Pipeline Overview", styles['H1']))
    story.append(Paragraph(
        "The forecasting pipeline executes the following steps in strict order. Each step reads from "
        "and writes to PostgreSQL. Steps 3, 4, and 6 are parallelized via Dask with configurable "
        "worker counts and batch sizes.", styles['Body']))

    story.append(make_table(
        ['Step', 'Name', 'Input', 'Output Table'],
        [
            ['1', 'ETL', 'Source database', 'demand_actuals'],
            ['1b', 'Outlier Detection', 'demand_actuals', 'demand_actuals.corrected_qty, detected_outliers'],
            ['1c', 'Segmentation', 'demand_actuals', 'segment_membership'],
            ['1d', 'Classification', 'demand_actuals', 'abc_results'],
            ['2', 'Characterization', 'demand_actuals', 'time_series_characteristics'],
            ['3', 'Forecasting', 'demand_actuals + characteristics', 'forecast_results'],
            ['4', 'Backtesting', 'demand_actuals + characteristics', 'backtest_metrics, forecasts_by_origin'],
            ['5', 'Best Method Selection', 'backtest_metrics', 'best_method_per_series'],
            ['6', 'Distribution Fitting', 'forecast_results', 'fitted_distributions'],
        ],
        col_widths=[1, 3, 4, 5]
    ))
    story.append(Spacer(1, 8))

    story.append(Paragraph(
        "<b>Parameter-Aware Grouping</b>: Before steps 1b, 2, 3, and 4, the orchestrator groups "
        "series by their assigned parameter sets (from the series_parameter_assignment table). Each "
        "group is processed with its own component instance constructed with the appropriate "
        "configuration override. Series with per-SKU hyperparameter overrides get their own "
        "single-element group.", styles['Body']))

    # ══════════════════════════════════════════════════════════════════════
    # 2. TIME SERIES CHARACTERIZATION
    # ══════════════════════════════════════════════════════════════════════
    story.append(PageBreak())
    story.append(Paragraph("2. Time Series Characterization", styles['H1']))
    story.append(Paragraph(
        "The TimeSeriesCharacterizer analyzes each series through six detection stages. "
        "The results drive method selection and hyperparameter adaptation.", styles['Body']))

    # 2.1 Seasonality
    story.append(Paragraph("2.1 Seasonality Detection", styles['H2']))
    story.append(Paragraph(
        "<b>Prerequisite</b>: The series must span at least 2 years (730 days) of date history. "
        "If shorter, has_seasonality = False immediately.", styles['Body']))
    story.append(Paragraph("Algorithm:", styles['Body']))
    for step in [
        "Test ACF (autocorrelation function) at configured candidate lag periods: [4, 13, 26, 52]",
        "A period is only testable if period &lt; n/2 and period &ge; 2",
        "Uses statsmodels.tsa.stattools.acf(values, nlags=max_period, fft=True, missing='conservative')",
        "A period is seasonal if |acf_value[period]| &gt; min_strength (default: 0.3)",
        "seasonal_strength = maximum ACF value across all detected periods",
    ]:
        story.append(Paragraph(f"&bull; {step}", styles['BulletItem']))

    story.append(make_table(
        ['Config Key', 'Default', 'Description'],
        [
            ['characterization.seasonality.test_periods', '[4, 13, 26, 52]', 'Lag periods to test'],
            ['characterization.seasonality.min_strength', '0.3', 'Minimum ACF threshold'],
        ],
        col_widths=[5, 2, 4]
    ))

    # 2.2 Trend
    story.append(Paragraph("2.2 Trend Detection", styles['H2']))
    story.append(Paragraph(
        "<b>Mann-Kendall test</b> (default): Requires at least 10 observations. "
        "Computes S statistic (sum of signs of all pairwise differences), variance with tie "
        "correction, Z-score, and two-sided p-value from normal distribution. "
        "has_trend = (p_value &lt; significance_level). "
        "Strength = Kendall's tau = |S| / (n*(n-1)/2). "
        "Trend direction: 'increasing' if S &gt; 0, 'decreasing' if S &lt; 0.", styles['Body']))
    story.append(Paragraph(
        "<b>Linear regression fallback</b>: Uses scipy.stats.linregress. Strength = R-squared.",
        styles['Body']))

    # 2.3 Intermittency
    story.append(Paragraph("2.3 Intermittency Detection", styles['H2']))
    story.append(Paragraph("Computed metrics:", styles['Body']))
    for item in [
        "<b>zero_ratio</b> = proportion of zero values in the series",
        "<b>ADI</b> (Average Demand Interval) = mean gap between consecutive non-zero observations",
        "<b>CoV</b> = coefficient of variation of non-zero demand values (std/mean with ddof=1)",
    ]:
        story.append(Paragraph(f"&bull; {item}", styles['BulletItem']))
    story.append(Paragraph(
        "<b>Decision rule</b>: is_intermittent = True when the total number of periods with "
        "positive demand is less than 5.", styles['Body']))

    # 2.4 Stationarity
    story.append(Paragraph("2.4 Stationarity Detection", styles['H2']))
    story.append(Paragraph(
        "Augmented Dickey-Fuller test via statsmodels.tsa.stattools.adfuller(values, autolag='AIC'). "
        "is_stationary = (p_value &lt; significance_level), default significance = 0.05.",
        styles['Body']))

    # 2.5 Complexity
    story.append(Paragraph("2.5 Complexity Scoring", styles['H2']))
    story.append(Paragraph(
        "Six factors, each normalized to [0, 1], combined with fixed weights:",
        styles['Body']))
    story.append(make_table(
        ['Factor', 'Weight', 'Computation'],
        [
            ['CV (coefficient of variation)', '0.20', 'min(|std/mean|, 3.0) / 3.0'],
            ['Entropy of first differences', '0.25', 'Histogram entropy (5-20 bins), normalized by log2(n_bins)'],
            ['Turning points ratio', '0.15', 'Count of sign changes in differences / (n - 2)'],
            ['Non-stationarity', '0.15', '1.0 if not stationary, 0.0 if stationary'],
            ['Seasonal strength', '0.10', 'Clamped to [0, 1]'],
            ['Intermittency', '0.15', '1.0 if intermittent, else 0.0'],
        ],
        col_widths=[4, 1.5, 6]
    ))
    story.append(Spacer(1, 6))
    story.append(Paragraph(
        "<b>Classification thresholds</b>: score &lt; 0.35 = low, "
        "0.35 &le; score &lt; 0.65 = medium, score &ge; 0.65 = high.", styles['Body']))

    # 2.6 Data Sufficiency
    story.append(Paragraph("2.6 Data Sufficiency", styles['H2']))
    story.append(make_table(
        ['Config Key', 'Default', 'Description'],
        [
            ['data_sufficiency.min_for_ml', '20', 'Minimum observations for ML models'],
            ['data_sufficiency.min_for_deep_learning', '30', 'Minimum observations for neural models'],
            ['data_sufficiency.sparse_obs_per_year', '5', 'Threshold for sparse data classification'],
        ],
        col_widths=[5, 1.5, 5]
    ))

    # ══════════════════════════════════════════════════════════════════════
    # 3. METHOD SELECTION
    # ══════════════════════════════════════════════════════════════════════
    story.append(PageBreak())
    story.append(Paragraph("3. Method Selection Logic", styles['H1']))
    story.append(Paragraph(
        "After characterization, each series receives a list of recommended_methods based on a "
        "strict priority decision tree. The first matching condition determines the category. "
        "All five category lists are configurable in config.yaml.", styles['Body']))

    story.append(Paragraph("3.1 Decision Tree", styles['H2']))
    story.append(make_table(
        ['Priority', 'Condition', 'Methods'],
        [
            ['1', 'SPARSE: obs_per_year < 5', 'SeasonalNaive, HistoricAverage, TimesFM'],
            ['2', 'INTERMITTENT: is_intermittent == True', 'CrostonOptimized, ADIDA, IMAPA, TimesFM'],
            ['3', 'HIGH COMPLEXITY: complexity == "high"', 'NHITS, NBEATS, TFT, PatchTST, AutoETS, LightGBM, XGBoost'],
            ['4', 'SEASONAL: has_seasonality == True', 'MSTL, AutoETS, NHITS, PatchTST, TimesFM, LightGBM'],
            ['5', 'STANDARD (default)', 'AutoETS, AutoARIMA, AutoTheta, NHITS, TimesFM, LightGBM'],
        ],
        col_widths=[1.5, 5, 7]
    ))
    story.append(Spacer(1, 6))
    story.append(Paragraph(
        "obs_per_year = n_observations / ((end_date - start_date).days / 365.25)",
        styles['CodeBlock']))

    story.append(Paragraph("3.2 Data Sufficiency Filtering", styles['H2']))
    story.append(Paragraph(
        "After category selection, methods are filtered based on data sufficiency:", styles['Body']))
    for item in [
        "Methods in {LightGBM, XGBoost} are <b>removed</b> if sufficient_for_ml == False",
        "Methods in {NHITS, NBEATS, PatchTST, TFT, DeepAR} are <b>removed</b> if sufficient_for_deep_learning == False",
        "If all methods are filtered out, fallback = [HistoricAverage]",
    ]:
        story.append(Paragraph(f"&bull; {item}", styles['BulletItem']))

    # ══════════════════════════════════════════════════════════════════════
    # 4. HYPERPARAMETER SELECTION
    # ══════════════════════════════════════════════════════════════════════
    story.append(PageBreak())
    story.append(Paragraph("4. Hyperparameter Selection", styles['H1']))

    # 4.1 Statistical
    story.append(Paragraph("4.1 Statistical Models", styles['H2']))
    story.append(Paragraph(
        "All statistical models use the StatsForecast library. Hyperparameters combine "
        "auto-tuning (model-internal optimization) and characteristic-driven defaults.",
        styles['Body']))

    story.append(Paragraph("AutoARIMA", styles['H3']))
    story.append(Paragraph(
        "Uses the Hyndman-Khandakar algorithm to search over ARIMA orders (p,d,q)(P,D,Q)m, "
        "selecting by AICc. season_length is derived from the first detected seasonal period "
        "or from the frequency map (W=52, M=12, etc.). Approximation is enabled when "
        "n_observations &gt; 150. When manual orders (p, d, q, P, D, Q) are provided via "
        "overrides, the model switches from AutoARIMA to fixed ARIMA.", styles['Body']))
    story.append(Paragraph(
        "<b>Overridable</b>: p, d, q, P, D, Q, max_p, max_q, max_P, max_Q, max_order, max_d, "
        "max_D, start_p, start_q, start_P, start_Q, stationary, seasonal, stepwise, "
        "allowdrift, allowmean.", styles['Body']))
    story.append(Paragraph(
        "<b>Extracted fitted params</b>: arma array (p, q, P, Q, s, d, D), AIC, AICc, BIC.",
        styles['Body']))

    story.append(Paragraph("AutoETS (Error-Trend-Seasonal)", styles['H3']))
    story.append(Paragraph(
        "Automatically selects the optimal Error (A/M), Trend (N/A/Damped), and Seasonal "
        "(N/A/M) components by AICc minimization. Default: model='ZZZ' (auto-select all). "
        "Extracted fitted params: alpha, beta, gamma, phi, sigma2, errortype, trendtype, "
        "seasontype, damped, aic, aicc, bic.", styles['Body']))

    story.append(Paragraph("AutoTheta", styles['H3']))
    story.append(Paragraph(
        "decomposition_type = 'multiplicative' if complexity=='high', else 'additive'. "
        "Overridable: model (e.g., 'OptimizedTheta', 'DynamicTheta'). "
        "Extracted: theta, alpha, drift.", styles['Body']))

    story.append(Paragraph("MSTL", styles['H3']))
    story.append(Paragraph(
        "Multiple Seasonal-Trend decomposition using LOESS. Trend forecaster: AutoARIMA(season_length=1). "
        "Minimum data: 2 * season_length + 1 observations.", styles['Body']))

    story.append(Paragraph("Intermittent Models", styles['H3']))
    story.append(Paragraph(
        "<b>CrostonOptimized / ADIDA / IMAPA</b>: Uses ConformalIntervals(n_windows=2, h=horizon) "
        "for prediction intervals when n_obs &ge; 2*horizon + 1. Without this threshold, "
        "only point forecasts are produced. "
        "<b>TSB</b>: alpha_d=0.1 (demand smoothing), alpha_p=0.1 (probability smoothing), both overridable.",
        styles['Body']))

    story.append(Paragraph("Pre-Flight Guards", styles['H3']))
    story.append(make_table(
        ['Model', 'Minimum Observations'],
        [
            ['SeasonalNaive, SeasonalWindowAverage', '>= season_length'],
            ['HistoricAverage', '>= 2'],
            ['MSTL', '>= 2 * season_length + 1'],
            ['AutoETS, AutoARIMA, AutoTheta, AutoCES', '>= max(2 * season_length, 10)'],
        ],
        col_widths=[6, 5]
    ))

    # 4.2 ML Models
    story.append(PageBreak())
    story.append(Paragraph("4.2 Machine Learning Models (LightGBM / XGBoost)", styles['H2']))
    story.append(Paragraph(
        "<b>Architecture</b>: Direct multi-step quantile regression. One model is trained per "
        "horizon step, per quantile level. All parameters are individually overridable.",
        styles['Body']))

    story.append(Paragraph("Default Hyperparameters", styles['H3']))
    story.append(make_table(
        ['Parameter', 'LightGBM', 'XGBoost'],
        [
            ['n_estimators', '300', '300'],
            ['learning_rate', '0.05', '0.05'],
            ['max_depth', '6', '6'],
            ['num_leaves / -', '31', '-'],
            ['subsample', '0.8', '0.8'],
            ['colsample_bytree', '0.8', '0.8'],
            ['min_child_samples / min_child_weight', '10', '10'],
            ['random_state', '42', '42'],
            ['objective (point)', 'regression', 'reg:squarederror'],
            ['objective (quantile)', 'quantile', 'reg:quantileerror'],
        ],
        col_widths=[5, 3, 3]
    ))
    story.append(Spacer(1, 6))

    story.append(Paragraph("Feature Engineering (weekly data)", styles['H3']))
    story.append(make_table(
        ['Feature Type', 'Details'],
        [
            ['Lags', '[1, 2, 4, 8, 13, 26, 52] + dominant seasonal period lag'],
            ['Rolling statistics', 'Windows: [4, 8, 13, 26]; stats: mean, std, min, max'],
            ['EWM', 'Span = 13; stats: mean, std'],
            ['Calendar', 'month, quarter, day_of_week'],
            ['Trend', 'time_idx, time_idx_squared'],
        ],
        col_widths=[3, 8]
    ))
    story.append(Spacer(1, 6))

    story.append(Paragraph("Training Process", styles['H3']))
    for item in [
        "For each horizon step h (1 to horizon): target = series.shift(-h) (direct multi-step)",
        "Point forecast model: trained with quantile = 0.5 (median)",
        "Separate quantile model for each configured quantile level",
        "Early stopping: stopping_rounds = 30 for LightGBM with validation set",
        "Validation split: ml_val_split = 0.2 (configurable, overridable per series)",
        "Split only applied if len(X_train) &ge; 30 and val_split &gt; 0",
        "Minimum supervised samples per step: 10",
        "<b>Early skip</b>: if max_trainable_steps &lt; horizon/2, entire method is skipped",
        "<b>Post-processing</b>: quantile monotonicity enforcement (sorts across quantile levels)",
    ]:
        story.append(Paragraph(f"&bull; {item}", styles['BulletItem']))

    # 4.3 Neural
    story.append(Paragraph("4.3 Neural Network Models", styles['H2']))
    story.append(Paragraph(
        "All neural models use MQLoss(level=quantiles) for multi-quantile regression "
        "via the NeuralForecast library. Base parameters: max_steps = 1000 if n_obs &gt; 200, "
        "else 500; val_check_steps = 100; early_stop_patience_steps = 3.",
        styles['Body']))

    story.append(make_table(
        ['Parameter', 'NHITS', 'NBEATS', 'PatchTST', 'TFT', 'DeepAR'],
        [
            ['input_size', '5h', '5h', '10h', '5h', '5h'],
            ['key arch param', 'n_blocks [1,1,1] / [3,3,3]', 'n_blocks [2,2,2] / [3,3,3]',
             'n_layers 2/3, d_model 64/128', 'hidden 64/128', 'enc_hidden 64/128'],
            ['learning_rate', '1e-3', '1e-3', '1e-4', '1e-3', '1e-3'],
            ['batch_size', '32', '32', '32', '32', '32'],
            ['complexity adapt.', 'mlp_units size', 'stack_types', 'n_heads 4/8', 'dropout 0.1', 'n_layers 2/3'],
        ],
        col_widths=[2.5, 2.5, 2.5, 2.5, 2, 2]
    ))
    story.append(Spacer(1, 4))
    story.append(Paragraph(
        "Note: values shown as X/Y indicate low-medium / high complexity settings. "
        "input_size is capped at n_obs // 2. All parameters are overridable.", styles['Note']))

    # 4.4 Foundation
    story.append(Paragraph("4.4 Foundation Models (TimesFM)", styles['H2']))
    story.append(make_table(
        ['Parameter', 'Value'],
        [
            ['Model', 'google/timesfm-1.0-200m-pytorch (200M parameters)'],
            ['Architecture', 'Pre-trained transformer, zero-shot (no per-series training)'],
            ['context_length', '512'],
            ['horizon_length', '128'],
            ['input_patch_len', '32'],
            ['output_patch_len', '128'],
            ['num_layers', '20'],
            ['model_dims', '1280'],
            ['Backend', 'GPU if CUDA available, else CPU'],
        ],
        col_widths=[3, 8]
    ))
    story.append(Spacer(1, 4))
    story.append(Paragraph(
        "Frequency hint: 0 = high (H/D/B), 1 = medium (W/M), 2 = low (Q/Y/A). "
        "Quantile fallback: if native quantile forecasting fails, uses point_forecast * (1 + |q - 0.5| * 2).",
        styles['Body']))

    # ══════════════════════════════════════════════════════════════════════
    # 5. BACKTESTING
    # ══════════════════════════════════════════════════════════════════════
    story.append(PageBreak())
    story.append(Paragraph("5. Backtesting Methodology", styles['H1']))

    story.append(Paragraph("5.1 Configuration", styles['H2']))
    story.append(make_table(
        ['Config Key', 'Default', 'Description'],
        [
            ['backtest_horizon', '60', 'Total periods reserved for backtesting'],
            ['window_size', '8', 'Forecast window per test (steps ahead)'],
            ['n_tests', '4', 'Number of forecast origins'],
            ['min_train_size', '24', 'Minimum training data before first origin'],
        ],
        col_widths=[4, 1.5, 6]
    ))

    story.append(Paragraph("5.2 Rolling Window Algorithm", styles['H2']))
    story.append(Paragraph("Given a series of length n:", styles['Body']))
    for step in [
        "<b>Step 1 &ndash; Clamp</b>: _horizon = min(backtest_horizon, n-1); _window = min(window_size, backtest_horizon, forecast_horizon)",
        "<b>Step 2 &ndash; Boundaries</b>: first_origin = max(min_train_size, n - backtest_horizon); last_possible_origin = n - window_size",
        "<b>Step 3 &ndash; Step size</b>: If n_tests &le; 0: step=1. If n_tests==1: single origin. Otherwise: step = max(1, available_range / (n_tests - 1))",
        "<b>Step 4 &ndash; Generate origins</b>: origins = [first_origin, first_origin+step, first_origin+2*step, ...]",
    ]:
        story.append(Paragraph(f"&bull; {step}", styles['BulletItem']))
    story.append(Paragraph(
        "Each window: train = series[:origin], test = series[origin : origin+window_size]. "
        "Fallback: if available_range &lt; 0, uses midpoint: first_origin = max(1, n/2).",
        styles['Body']))

    story.append(Paragraph("5.3 Which Methods Are Backtested", styles['H2']))
    for item in [
        "<b>Default</b>: Only statistical methods from recommended_methods, limited to top 5",
        "<b>All-methods mode</b> (--all-methods): Also includes ML methods",
        "Neural network and foundation models are <b>not</b> backtested (too expensive)",
    ]:
        story.append(Paragraph(f"&bull; {item}", styles['BulletItem']))

    story.append(Paragraph("5.4 Point Forecast Metrics", styles['H2']))
    story.append(make_table(
        ['Metric', 'Formula', 'Notes'],
        [
            ['MAE', 'mean(|actual - forecast|)', 'Scale-dependent'],
            ['RMSE', 'sqrt(mean((actual - forecast)^2))', 'Penalizes large errors'],
            ['MAPE', 'mean(|errors / actual|) * 100', 'Undefined for zero actuals'],
            ['sMAPE', 'mean(|e| / ((|a| + |f|)/2)) * 100', 'Symmetric, bounded [0, 200]'],
            ['Bias', 'mean(actual - forecast)', 'Positive = under-forecasting'],
            ['MASE', 'MAE / mean(|diff(actual)|)', 'Scale-free; naive = 1-step diff'],
        ],
        col_widths=[2, 5, 4]
    ))

    story.append(Paragraph("5.5 Probabilistic Metrics", styles['H2']))
    story.append(make_table(
        ['Metric', 'Description'],
        [
            ['Coverage_50', 'Fraction of actuals in [Q0.25, Q0.75]; nominal = 50%'],
            ['Coverage_80', 'Fraction of actuals in [Q0.10, Q0.90]; nominal = 80%'],
            ['Coverage_90', 'Fraction of actuals in [Q0.05, Q0.95]; nominal = 90%'],
            ['Coverage_95', 'Fraction of actuals in [Q0.025, Q0.975]; nominal = 95%'],
            ['Winkler Score', 'mean(width + penalties) for 90% interval; penalty = (2/alpha) * overshoot'],
            ['CRPS', 'Trapezoidal integration over quantile pairs; lower = better'],
            ['Quantile Loss', 'Pinball loss averaged across all quantile levels'],
        ],
        col_widths=[3, 8]
    ))

    story.append(Paragraph("5.6 Information Criteria", styles['H2']))
    story.append(Paragraph(
        "Computed from in-sample residuals assuming Gaussian distribution:", styles['Body']))
    for item in [
        "log_likelihood = -0.5 * n * (log(2*pi*sigma^2) + 1)",
        "AIC = -2 * log_likelihood + 2 * n_params",
        "BIC = -2 * log_likelihood + n_params * log(n)",
        "AICc = AIC + (2 * n_params * (n_params + 1)) / (n - n_params - 1)",
    ]:
        story.append(Paragraph(f"&bull; {item}", styles['BulletItem']))

    story.append(Paragraph("5.7 ML Internal Validation Fallback", styles['H2']))
    story.append(Paragraph(
        "When rolling-window backtesting produces no metrics for ML methods (common with short "
        "series), the system activates a fallback: (1) Runs the ML forecaster on the full series. "
        "(2) Extracts internal 80/20 train/val split metrics. (3) Converts them to "
        "EvaluationMetrics with metric_source = 'internal_validation'. (4) Appends to backtest "
        "metrics table. This ensures ML methods always have metrics for best-method selection.",
        styles['Body']))

    # ══════════════════════════════════════════════════════════════════════
    # 6. BEST METHOD SELECTION
    # ══════════════════════════════════════════════════════════════════════
    story.append(PageBreak())
    story.append(Paragraph("6. Best Method Selection", styles['H1']))

    story.append(Paragraph("6.1 Scoring Weights", styles['H2']))
    story.append(make_table(
        ['Metric', 'Weight', 'Optimization Direction'],
        [
            ['MAE', '0.40', 'Lower is better'],
            ['RMSE', '0.20', 'Lower is better'],
            ['|Bias|', '0.15', 'Lower is better'],
            ['|Coverage_90 - 0.9|', '0.15', 'Closer to 90% is better'],
            ['MASE', '0.10', 'Lower is better'],
        ],
        col_widths=[4, 2, 5]
    ))
    story.append(Spacer(1, 4))
    story.append(Paragraph(
        "Weights are configurable in config.yaml under best_method.weights.", styles['Body']))

    story.append(Paragraph("6.2 Ranking Algorithm", styles['H2']))
    for step in [
        "<b>Step 1 &ndash; Average across origins</b>: For each method, compute the mean of each metric column across all backtest windows (forecast origins).",
        "<b>Step 2 &ndash; Transform bias</b>: Convert bias to |bias| so both positive and negative bias are penalized equally.",
        "<b>Step 3 &ndash; Transform coverage</b>: Convert coverage_90 to |coverage_90 - 0.9| so deviation from the nominal 90% level is penalized. Overcoverage and undercoverage are treated symmetrically.",
        "<b>Step 4 &ndash; Min-max normalize</b>: For each metric column across all methods for this series, normalize to [0, 1]. If all methods have the same value: normalized = 0.0 (no discriminating power).",
        "<b>Step 5 &ndash; Weighted composite</b>: composite = sum(weight[col] * normalized[col]). Re-normalize by the sum of weights of non-NaN metrics per method, ensuring methods with missing metrics are compared fairly.",
        "<b>Step 6 &ndash; Rank</b>: Sort by composite score ascending. <b>Lower score = better</b>.",
    ]:
        story.append(Paragraph(f"{step}", styles['BulletItem']))

    story.append(Paragraph("6.3 Output Per Series", styles['H2']))
    story.append(make_table(
        ['Field', 'Description'],
        [
            ['best_method', 'Name of the winning method'],
            ['best_score', 'Composite score of the winner'],
            ['runner_up_method', 'Second-best method'],
            ['runner_up_score', 'Composite score of the runner-up'],
            ['all_rankings', 'JSON array with all methods ranked'],
        ],
        col_widths=[4, 8]
    ))
    story.append(Spacer(1, 4))
    story.append(Paragraph(
        "<b>Fallback</b>: If no usable metrics exist or all are NaN, the first method in the "
        "group is returned as the default best method.", styles['Body']))

    # ══════════════════════════════════════════════════════════════════════
    # 7. PARAMETER OVERRIDE SYSTEM
    # ══════════════════════════════════════════════════════════════════════
    story.append(PageBreak())
    story.append(Paragraph("7. Parameter Override System", styles['H1']))
    story.append(Paragraph("7.1 Resolution Hierarchy", styles['H2']))
    story.append(Paragraph(
        "Parameters are resolved from highest to lowest priority:", styles['Body']))
    story.append(make_table(
        ['Priority', 'Source', 'Description'],
        [
            ['1 (highest)', 'hyperparameter_overrides table', 'Per-SKU, per-method partial overrides'],
            ['2', 'parameters table via series_parameter_assignment', 'Segment-based parameter sets'],
            ['3', 'Default parameter set (is_default=TRUE)', 'Global default parameters'],
            ['4 (lowest)', 'config.yaml', 'Base configuration'],
        ],
        col_widths=[2, 4.5, 5]
    ))
    story.append(Spacer(1, 4))
    story.append(Paragraph(
        "<b>Deep merge</b>: Overrides are applied via recursive deep merge &mdash; override keys "
        "win over base keys, non-dict values are replaced entirely, new keys are added.",
        styles['Body']))

    story.append(Paragraph("7.2 Business Types", styles['H2']))
    story.append(make_table(
        ['Business Type', 'Config Section', 'Purpose'],
        [
            ['forecasting', 'forecasting', 'Forecasting model parameters'],
            ['outlier_detection', 'outlier_detection', 'Outlier detection parameters'],
            ['characterization', 'characterization', 'Characterization parameters'],
            ['evaluation', 'forecasting', 'Backtesting parameters'],
            ['best_method', 'best_method', 'Best method selection weights'],
        ],
        col_widths=[3, 3, 5]
    ))
    story.append(Spacer(1, 4))
    story.append(Paragraph(
        "Special override method <b>'_backtesting'</b>: overrides backtesting parameters "
        "(backtest_horizon, window_size, n_tests) for individual series.", styles['Body']))

    # ══════════════════════════════════════════════════════════════════════
    # 8. EDGE CASES
    # ══════════════════════════════════════════════════════════════════════
    story.append(PageBreak())
    story.append(Paragraph("8. Edge Cases and Special Handling", styles['H1']))

    edge_cases = [
        ("<b>Series with &lt; 4 observations</b>: Characterization may be incomplete; "
         "method recommendation falls through to sparse/intermittent/standard."),
        ("<b>Seasonality gating by date span</b>: Series with less than 2 years always "
         "receive has_seasonality = False, even if periodic patterns exist."),
        ("<b>Conformal intervals for intermittent models</b>: Only available when "
         "n_obs &ge; 2*horizon + 1. Otherwise, point forecasts only."),
        ("<b>ML horizon steps with insufficient data</b>: Individual steps with &lt; 10 "
         "supervised samples are filled with NaN. If &gt; half the steps cannot be trained, "
         "the entire ML method is skipped."),
        ("<b>Quantile monotonicity enforcement</b>: After ML quantile regression, values are "
         "sorted at each step to ensure Q10 &le; Q25 &le; Q50 &le; Q75 &le; Q90."),
        ("<b>ML internal validation fallback</b>: When backtesting produces no metrics for "
         "ML methods, falls back to 80/20 train/val split metrics."),
        ("<b>Best method NaN handling</b>: Composite scores divide by sum of weights of "
         "non-NaN metrics, so methods with missing probabilistic metrics are compared fairly."),
        ("<b>Frequency alias mapping</b>: Pandas 2.x deprecations ('M' to 'ME', 'Q' to 'QE', "
         "'Y' to 'YE') are handled transparently."),
        ("<b>Parameter group isolation</b>: Series with per-SKU overrides are processed in "
         "their own batch to prevent parameter leakage."),
        ("<b>Targeted saves for subset runs</b>: Re-running individual series uses "
         "DELETE WHERE unique_id IN (...) instead of truncating, preserving other results."),
    ]
    for i, edge in enumerate(edge_cases, 1):
        story.append(Paragraph(f"{i}. {edge}", styles['BulletItem']))
        story.append(Spacer(1, 2))

    # Build
    doc.build(story)
    return output_path


if __name__ == '__main__':
    out = Path(__file__).parent / 'ForecastAI_Methodology.pdf'
    result = build_pdf(str(out))
    print(f"PDF generated: {result}")
    print(f"Size: {os.path.getsize(result) / 1024:.1f} KB")

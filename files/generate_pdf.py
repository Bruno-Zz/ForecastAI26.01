"""
Generate comprehensive PDF documentation for ForecastAI 2026.01
Includes architecture, features, data model, API reference, user guide, and screenshots.
"""

from fpdf import FPDF
import os

SCREENSHOTS_DIR = os.path.join(os.path.dirname(__file__), "screenshots")
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "ForecastAI_2026.01_Documentation.pdf")


class DocPDF(FPDF):
    """Custom PDF class with header/footer and helper methods."""

    def __init__(self):
        super().__init__('P', 'mm', 'A4')
        self.set_auto_page_break(auto=True, margin=20)

    def header(self):
        if self.page_no() > 1:
            self.set_font('Helvetica', 'I', 8)
            self.set_text_color(120, 120, 120)
            self.cell(0, 6, 'ForecastAI 2026.01 - Technical Documentation & User Guide', 0, 0, 'L')
            self.ln(3)
            self.set_draw_color(200, 200, 200)
            self.line(10, self.get_y(), 200, self.get_y())
            self.ln(5)

    def footer(self):
        self.set_y(-15)
        self.set_font('Helvetica', 'I', 8)
        self.set_text_color(150, 150, 150)
        self.cell(0, 10, f'Page {self.page_no()}/{{nb}}', 0, 0, 'C')

    def title_page(self):
        self.add_page()
        self.ln(50)
        # Title
        self.set_font('Helvetica', 'B', 36)
        self.set_text_color(30, 64, 175)  # blue
        self.cell(0, 18, 'ForecastAI', 0, 1, 'C')
        self.set_font('Helvetica', '', 20)
        self.set_text_color(100, 100, 100)
        self.cell(0, 12, 'Version 2026.01', 0, 1, 'C')
        self.ln(8)
        self.set_draw_color(30, 64, 175)
        self.set_line_width(0.8)
        self.line(60, self.get_y(), 150, self.get_y())
        self.ln(12)
        self.set_font('Helvetica', '', 14)
        self.set_text_color(60, 60, 60)
        self.cell(0, 8, 'Technical Documentation & User Guide', 0, 1, 'C')
        self.ln(5)
        self.set_font('Helvetica', '', 11)
        self.set_text_color(100, 100, 100)
        self.cell(0, 7, 'Enterprise Demand Forecasting Platform', 0, 1, 'C')
        self.cell(0, 7, 'Multi-Method Statistical & ML Forecasting with Automated Model Selection', 0, 1, 'C')
        self.ln(40)
        self.set_font('Helvetica', 'I', 10)
        self.set_text_color(140, 140, 140)
        self.cell(0, 6, 'Generated: February 2026', 0, 1, 'C')

    def chapter_title(self, num, title):
        self.add_page()
        self.set_font('Helvetica', 'B', 22)
        self.set_text_color(30, 64, 175)
        self.cell(0, 14, f'{num}. {title}', 0, 1, 'L')
        self.set_draw_color(30, 64, 175)
        self.set_line_width(0.6)
        self.line(10, self.get_y() + 1, 200, self.get_y() + 1)
        self.ln(8)

    def section_title(self, title):
        self.ln(4)
        self.set_font('Helvetica', 'B', 14)
        self.set_text_color(50, 50, 50)
        self.cell(0, 9, title, 0, 1, 'L')
        self.ln(2)

    def subsection_title(self, title):
        self.ln(2)
        self.set_font('Helvetica', 'B', 11)
        self.set_text_color(70, 70, 70)
        self.cell(0, 7, title, 0, 1, 'L')
        self.ln(1)

    def body_text(self, text):
        self.set_font('Helvetica', '', 10)
        self.set_text_color(40, 40, 40)
        self.multi_cell(0, 5.5, text)
        self.ln(2)

    def bullet(self, text, indent=10):
        x = self.get_x()
        self.set_font('Helvetica', '', 10)
        self.set_text_color(40, 40, 40)
        self.set_x(x + indent)
        self.cell(4, 5.5, '-', 0, 0)
        self.multi_cell(0, 5.5, f' {text}')
        self.ln(0.5)

    def code_block(self, text):
        self.set_font('Courier', '', 9)
        self.set_fill_color(245, 245, 245)
        self.set_text_color(50, 50, 50)
        lines = text.strip().split('\n')
        y_start = self.get_y()
        # Check if we need a page break
        needed_height = len(lines) * 4.5 + 6
        if self.get_y() + needed_height > 270:
            self.add_page()
        self.ln(1)
        for line in lines:
            safe_line = line.replace('\t', '    ')
            # Truncate long lines
            if len(safe_line) > 95:
                safe_line = safe_line[:92] + '...'
            self.set_x(12)
            self.cell(186, 4.5, safe_line, 0, 1, 'L', fill=True)
        self.ln(3)

    def table_header(self, cols, widths):
        self.set_font('Helvetica', 'B', 9)
        self.set_fill_color(30, 64, 175)
        self.set_text_color(255, 255, 255)
        for i, col in enumerate(cols):
            self.cell(widths[i], 7, col, 1, 0, 'C', fill=True)
        self.ln()

    def table_row(self, cells, widths, fill=False):
        self.set_font('Helvetica', '', 9)
        self.set_text_color(40, 40, 40)
        if fill:
            self.set_fill_color(245, 248, 255)
        else:
            self.set_fill_color(255, 255, 255)
        for i, cell_text in enumerate(cells):
            # Truncate text to fit width
            max_chars = int(widths[i] / 2.2)
            if len(str(cell_text)) > max_chars:
                cell_text = str(cell_text)[:max_chars - 2] + '..'
            self.cell(widths[i], 6, str(cell_text), 1, 0, 'L', fill=fill)
        self.ln()

    def add_screenshot(self, filename, caption, width=180):
        path = os.path.join(SCREENSHOTS_DIR, filename)
        if not os.path.exists(path):
            self.body_text(f'[Screenshot not available: {filename}]')
            return
        # Check if enough space, otherwise add page
        if self.get_y() + 100 > 270:
            self.add_page()
        self.ln(2)
        x = (210 - width) / 2
        self.image(path, x=x, w=width)
        self.ln(2)
        self.set_font('Helvetica', 'I', 9)
        self.set_text_color(100, 100, 100)
        self.cell(0, 5, caption, 0, 1, 'C')
        self.ln(4)


def build_pdf():
    pdf = DocPDF()
    pdf.alias_nb_pages()

    # =====================================================================
    # TITLE PAGE
    # =====================================================================
    pdf.title_page()

    # =====================================================================
    # TABLE OF CONTENTS
    # =====================================================================
    pdf.add_page()
    pdf.set_font('Helvetica', 'B', 22)
    pdf.set_text_color(30, 64, 175)
    pdf.cell(0, 14, 'Table of Contents', 0, 1, 'L')
    pdf.ln(6)

    toc = [
        ('1', 'Executive Summary', ''),
        ('2', 'System Architecture', ''),
        ('', '2.1  Technology Stack', ''),
        ('', '2.2  Pipeline Architecture', ''),
        ('', '2.3  Directory Structure', ''),
        ('3', 'Data Model', ''),
        ('', '3.1  Source Database', ''),
        ('', '3.2  Output Parquet Files', ''),
        ('4', 'Pipeline Stages', ''),
        ('', '4.1  ETL (Extract-Transform-Load)', ''),
        ('', '4.2  Time Series Characterization', ''),
        ('', '4.3  Forecasting', ''),
        ('', '4.4  Backtesting & Evaluation', ''),
        ('', '4.5  Best Method Selection', ''),
        ('', '4.6  Distribution Fitting (MEIO)', ''),
        ('5', 'API Reference', ''),
        ('6', 'Frontend User Guide', ''),
        ('', '6.1  Dashboard', ''),
        ('', '6.2  Time Series Detail View', ''),
        ('', '6.3  Interactive Features', ''),
        ('7', 'Configuration Reference', ''),
        ('8', 'Installation & Running', ''),
        ('9', 'Troubleshooting', ''),
    ]
    for num, title, _ in toc:
        if num:
            pdf.set_font('Helvetica', 'B', 12)
            pdf.set_text_color(30, 64, 175)
            pdf.cell(0, 7, f'{num}.  {title}', 0, 1, 'L')
        else:
            pdf.set_font('Helvetica', '', 11)
            pdf.set_text_color(80, 80, 80)
            pdf.cell(12)
            pdf.cell(0, 6.5, title, 0, 1, 'L')

    # =====================================================================
    # 1. EXECUTIVE SUMMARY
    # =====================================================================
    pdf.chapter_title('1', 'Executive Summary')
    pdf.body_text(
        'ForecastAI 2026.01 is an enterprise-grade demand forecasting platform that automates the '
        'end-to-end process of time series forecasting. It extracts demand data from a Neon PostgreSQL '
        'database, characterizes each time series for patterns such as seasonality, trend, and intermittency, '
        'generates forecasts using up to 15 methods across 4 model families, evaluates forecast accuracy '
        'via rolling-window backtesting, selects the best method per series, and fits probability '
        'distributions for Multi-Echelon Inventory Optimization (MEIO).'
    )
    pdf.body_text(
        'The system serves its results through a FastAPI REST backend and a React/Vite interactive '
        'dashboard, enabling users to explore forecasts, compare methods, zoom into specific time periods, '
        'and review accuracy metrics across all 2,348 time series in the dataset.'
    )
    pdf.section_title('Key Capabilities')
    pdf.bullet('Automated ETL from PostgreSQL via DuckDB with monthly aggregation')
    pdf.bullet('Time series pattern detection: seasonality, trend, intermittency, stationarity, complexity')
    pdf.bullet('15 forecasting methods: Statistical (10), Neural (5), Foundation (1), ML (2)')
    pdf.bullet('Rolling-window backtesting with 10+ evaluation metrics (MAE, RMSE, MAPE, CRPS, Coverage...)')
    pdf.bullet('Automated best method selection using weighted composite scoring')
    pdf.bullet('Distribution fitting for safety stock and service level calculations')
    pdf.bullet('REST API with 12 endpoints for programmatic access')
    pdf.bullet('Interactive React dashboard with Vega-Lite visualizations')
    pdf.bullet('24-month forecast horizon with prediction intervals (50% and 90%)')
    pdf.bullet('Date-range zoom slider for interactive exploration')

    pdf.section_title('Current Dataset')
    pdf.bullet('2,348 unique time series (item-site combinations)')
    pdf.bullet('42,934 historical data points')
    pdf.bullet('Date range: January 2023 to January 2026')
    pdf.bullet('Average 18.3 observations per series')
    pdf.bullet('5,058 forecasts generated across all methods and series')
    pdf.bullet('25 series with full backtest metrics (series with 36+ observations)')

    # =====================================================================
    # 2. SYSTEM ARCHITECTURE
    # =====================================================================
    pdf.chapter_title('2', 'System Architecture')

    pdf.section_title('2.1  Technology Stack')

    pdf.subsection_title('Backend (Python 3.13)')
    w = [50, 70, 70]
    pdf.table_header(['Category', 'Library', 'Purpose'], w)
    rows = [
        ('Data Processing', 'Pandas, NumPy, PyArrow', 'DataFrames, arrays, Parquet I/O'),
        ('Database', 'DuckDB, psycopg2', 'PostgreSQL extraction via DuckDB'),
        ('Statistical', 'Nixtla StatsForecast', 'AutoARIMA, AutoETS, AutoTheta...'),
        ('Neural', 'Nixtla NeuralForecast', 'NHITS, NBEATS, PatchTST, TFT...'),
        ('Foundation', 'Google TimesFM', 'Pre-trained foundation model'),
        ('ML', 'LightGBM, XGBoost', 'Gradient boosting models'),
        ('API', 'FastAPI, Uvicorn', 'REST API server'),
        ('Parallel', 'Dask', 'Distributed batch processing'),
        ('Config', 'PyYAML', 'YAML configuration loading'),
    ]
    for i, r in enumerate(rows):
        pdf.table_row(r, w, fill=(i % 2 == 0))

    pdf.ln(4)
    pdf.subsection_title('Frontend (Node.js / React)')
    w = [50, 70, 70]
    pdf.table_header(['Category', 'Library', 'Version'], w)
    rows = [
        ('Framework', 'React', '18.2'),
        ('Routing', 'React Router', 'v6.20'),
        ('Visualization', 'Vega-Lite + react-vega', 'v5.16 / v7.6'),
        ('HTTP Client', 'Axios', 'v1.6'),
        ('Styling', 'Tailwind CSS', 'v3.4'),
        ('Build Tool', 'Vite', 'v5.0'),
    ]
    for i, r in enumerate(rows):
        pdf.table_row(r, w, fill=(i % 2 == 0))

    pdf.section_title('2.2  Pipeline Architecture')
    pdf.body_text(
        'The system follows a sequential pipeline architecture with 6 main stages. Each stage reads '
        'from and writes to Parquet files, enabling any stage to be re-run independently. The pipeline '
        'is orchestrated via run_pipeline.py, which supports both full runs and selective step execution.'
    )

    pdf.body_text(
        'Pipeline flow:\n'
        '  1. ETL: PostgreSQL -> DuckDB -> Pandas -> Parquet\n'
        '  2. Characterization: Pattern detection per series\n'
        '  3. Forecasting: Generate 24-month forecasts with all eligible methods\n'
        '  4. Backtesting: Rolling-window cross-validation (3 windows)\n'
        '  5. Best Method: Weighted composite scoring to rank methods per series\n'
        '  6. Distribution Fitting: Parametric fits for MEIO service levels'
    )

    pdf.section_title('2.3  Directory Structure')
    pdf.code_block(
        'ForecastAI2026.01/files/\n'
        '  api/\n'
        '    main.py                    # FastAPI backend (12 endpoints)\n'
        '  characterization/\n'
        '    characterization.py        # Time series pattern detection\n'
        '  config/\n'
        '    config.yaml                # Central configuration\n'
        '  data/\n'
        '    time_series.parquet        # ETL output (42,934 rows)\n'
        '  distribution/\n'
        '    fitting.py                 # Distribution fitting for MEIO\n'
        '  etl/\n'
        '    etl.py                     # PostgreSQL extraction pipeline\n'
        '  evaluation/\n'
        '    metrics.py                 # Backtesting & evaluation metrics\n'
        '  forecasting/\n'
        '    statistical_models.py      # StatsForecast wrapper (10 models)\n'
        '    neural_models.py           # NeuralForecast wrapper (5 models)\n'
        '    foundation_models.py       # TimesFM wrapper\n'
        '    ml_models.py               # LightGBM, XGBoost\n'
        '  frontend/\n'
        '    src/components/\n'
        '      Dashboard.jsx            # Main dashboard view\n'
        '      TimeSeriesViewer.jsx     # Series detail view\n'
        '    vite.config.js             # Dev server + API proxy\n'
        '  output/                      # Pipeline output Parquet files\n'
        '  selection/\n'
        '    best_method.py             # Method ranking & selection\n'
        '  utils/\n'
        '    orchestrator.py            # Dask parallel orchestration\n'
        '  run_pipeline.py              # CLI entry point\n'
        '  requirements.txt             # Python dependencies'
    )

    # =====================================================================
    # 3. DATA MODEL
    # =====================================================================
    pdf.chapter_title('3', 'Data Model')

    pdf.section_title('3.1  Source Database (Neon PostgreSQL)')
    pdf.body_text(
        'The system extracts demand data from a Neon-hosted PostgreSQL database. The schema uses '
        'three tables in the "plan" schema: demand_actual (fact table), item (SKU dimension), '
        'and site (location dimension). The unique identifier for each time series is formed by '
        'concatenating item_id and site_id (e.g., "14787_1006").'
    )

    pdf.subsection_title('Source Tables')
    w = [35, 55, 100]
    pdf.table_header(['Table', 'Key Columns', 'Description'], w)
    pdf.table_row(['plan.demand_actual', 'item_id, site_id, date, qty', 'Demand fact records'], w, fill=True)
    pdf.table_row(['plan.item', 'id, name, xuid', 'Item/SKU dimension'], w)
    pdf.table_row(['plan.site', 'id, name, xuid', 'Site/location dimension'], w, fill=True)

    pdf.ln(2)
    pdf.subsection_title('ETL Join Query')
    pdf.code_block(
        'SELECT\n'
        "  CONCAT(d.item_id, '_', d.site_id) AS unique_id,\n"
        '  d.date,\n'
        '  d.qty AS y\n'
        'FROM plan.demand_actual d\n'
        'LEFT JOIN plan.item i ON d.item_id = i.id\n'
        'LEFT JOIN plan.site s ON d.site_id = s.id\n'
        "WHERE d.date >= '2020-01-01'\n"
        'ORDER BY unique_id, d.date;'
    )

    pdf.section_title('3.2  Output Parquet Files')
    pdf.body_text('The pipeline produces 6 Parquet files, each serving a specific role:')

    pdf.subsection_title('data/time_series.parquet - Historical Demand Data')
    w = [35, 35, 120]
    pdf.table_header(['Column', 'Type', 'Description'], w)
    pdf.table_row(['unique_id', 'string', 'Item-site combination identifier (e.g., "14787_1006")'], w, True)
    pdf.table_row(['date', 'datetime', 'Monthly date of observation'], w)
    pdf.table_row(['y', 'float64', 'Demand quantity value'], w, True)
    pdf.body_text('Statistics: 42,934 rows, 2,348 unique series, range 2023-01 to 2026-01.')

    pdf.subsection_title('output/time_series_characteristics.parquet - Series Profiles')
    w = [45, 30, 115]
    pdf.table_header(['Column', 'Type', 'Description'], w)
    rows = [
        ('unique_id', 'string', 'Series identifier'),
        ('n_observations', 'int', 'Number of historical data points'),
        ('has_seasonality', 'bool', 'Seasonal pattern detected via ACF test'),
        ('seasonal_strength', 'float', 'Autocorrelation strength (0-1)'),
        ('has_trend', 'bool', 'Monotonic trend via Mann-Kendall test'),
        ('trend_direction', 'string', 'up, down, or none'),
        ('is_intermittent', 'bool', 'Intermittent demand (high zero ratio)'),
        ('complexity_score', 'float', 'Composite complexity (0-1)'),
        ('complexity_level', 'string', 'low (<0.35), medium, high (>0.65)'),
        ('recommended_methods', 'list', 'Suggested forecasting methods'),
    ]
    for i, r in enumerate(rows):
        pdf.table_row(r, w, fill=(i % 2 == 0))

    pdf.subsection_title('output/forecasts_all_methods.parquet - Forecast Results')
    w = [45, 30, 115]
    pdf.table_header(['Column', 'Type', 'Description'], w)
    rows = [
        ('unique_id', 'string', 'Series identifier'),
        ('method', 'string', 'Forecasting method name'),
        ('point_forecast', 'list[float]', 'Mean forecast values (24 months)'),
        ('quantiles', 'dict', 'Prediction quantiles (5th to 99th percentile)'),
        ('hyperparameters', 'dict', 'Method-specific fitted parameters'),
        ('training_time', 'float', 'Model fitting time in seconds'),
    ]
    for i, r in enumerate(rows):
        pdf.table_row(r, w, fill=(i % 2 == 0))

    pdf.subsection_title('output/backtest_metrics.parquet - Accuracy Evaluation')
    w = [45, 30, 115]
    pdf.table_header(['Column', 'Type', 'Description'], w)
    rows = [
        ('unique_id', 'string', 'Series identifier'),
        ('method', 'string', 'Forecasting method'),
        ('mae', 'float', 'Mean Absolute Error'),
        ('rmse', 'float', 'Root Mean Squared Error'),
        ('mape', 'float', 'Mean Absolute Percentage Error'),
        ('bias', 'float', 'Forecast bias (positive = over-forecast)'),
        ('coverage_90', 'float', '90% prediction interval coverage'),
        ('n_windows', 'int', 'Number of backtest rolling windows'),
    ]
    for i, r in enumerate(rows):
        pdf.table_row(r, w, fill=(i % 2 == 0))

    pdf.subsection_title('output/best_method_per_series.parquet - Winner Selection')
    w = [45, 30, 115]
    pdf.table_header(['Column', 'Type', 'Description'], w)
    rows = [
        ('unique_id', 'string', 'Series identifier'),
        ('best_method', 'string', 'Winning forecasting method'),
        ('best_score', 'float', 'Composite score (lower = better)'),
        ('runner_up_method', 'string', 'Second-best method'),
    ]
    for i, r in enumerate(rows):
        pdf.table_row(r, w, fill=(i % 2 == 0))

    pdf.subsection_title('output/forecasts_by_origin.parquet - Backtest Forecasts')
    pdf.body_text(
        'Contains per-window forecasts for each rolling backtest origin. Used to power the '
        '"Forecast Evolution Over Time" animation in the frontend. Columns: unique_id, method, '
        'forecast_origin, horizon_step, point_forecast, actual_value.'
    )

    # =====================================================================
    # 4. PIPELINE STAGES
    # =====================================================================
    pdf.chapter_title('4', 'Pipeline Stages')

    pdf.section_title('4.1  ETL (Extract-Transform-Load)')
    pdf.body_text(
        'The ETL stage extracts demand data from a Neon PostgreSQL database using DuckDB\'s '
        'postgres_scanner extension, which provides high-performance parallel reads. The extracted '
        'data is transformed by parsing dates, casting values to numeric, creating unique identifiers, '
        'aggregating to monthly frequency, and filtering by minimum observation count (12 months). '
        'Output is saved as a Parquet file with Snappy compression.'
    )
    pdf.subsection_title('Key Parameters')
    pdf.bullet('Aggregation frequency: Monthly (M), using sum')
    pdf.bullet('Minimum observations: 12 per series')
    pdf.bullet('Date filter: >= 2020-01-01')
    pdf.bullet('Output: data/time_series.parquet')

    pdf.section_title('4.2  Time Series Characterization')
    pdf.body_text(
        'Each time series is analyzed along 5 dimensions to understand its patterns and determine '
        'which forecasting methods are most appropriate:'
    )

    w = [40, 50, 100]
    pdf.table_header(['Dimension', 'Test', 'Output'], w)
    rows = [
        ('Seasonality', 'ACF at config lags', 'has_seasonality, strength (0-1)'),
        ('Trend', 'Mann-Kendall test', 'has_trend, direction (up/down)'),
        ('Intermittency', 'Zero ratio + ADI', 'is_intermittent, zero_ratio, adi'),
        ('Stationarity', 'ADF test', 'is_stationary, adf_pvalue'),
        ('Complexity', 'Weighted composite', 'score (0-1), level (low/med/high)'),
    ]
    for i, r in enumerate(rows):
        pdf.table_row(r, w, fill=(i % 2 == 0))

    pdf.ln(3)
    pdf.subsection_title('Complexity Score Formula')
    pdf.code_block(
        'Score = 0.20 * CV + 0.25 * Entropy + 0.15 * TurningPoints\n'
        '      + 0.15 * NonStationarity + 0.10 * SeasonalStrength\n'
        '      + 0.15 * Intermittency\n'
        '\n'
        'Classification: Low (<0.35), Medium (0.35-0.65), High (>0.65)'
    )

    pdf.subsection_title('Method Recommendation Logic')
    pdf.bullet('Sparse data (<24 obs): SeasonalNaive, HistoricAverage, TimesFM')
    pdf.bullet('Intermittent demand: CrostonOptimized, ADIDA, IMAPA, TimesFM')
    pdf.bullet('Seasonal patterns: MSTL, AutoETS, NHITS, PatchTST, TimesFM, LightGBM')
    pdf.bullet('High complexity: NHITS, NBEATS, TFT, PatchTST, AutoETS, LightGBM, XGBoost')
    pdf.bullet('Standard (default): AutoETS, AutoARIMA, AutoTheta, NHITS, TimesFM, LightGBM')

    pdf.section_title('4.3  Forecasting')
    pdf.body_text(
        'The system supports up to 15 forecasting methods across 4 families. For each series, methods '
        'are selected based on the characterization results. All methods generate 24-month ahead '
        'point forecasts along with prediction quantiles at 7 confidence levels (10th through 99th '
        'percentile) for use in probabilistic inventory optimization.'
    )

    pdf.subsection_title('Statistical Models (Nixtla StatsForecast)')
    w = [45, 145]
    pdf.table_header(['Method', 'Description'], w)
    rows = [
        ('AutoARIMA', 'Auto-selected ARIMA with automatic differencing, order selection'),
        ('AutoETS', 'Exponential Smoothing with automatic error/trend/seasonal selection'),
        ('AutoTheta', 'Theta method with automatic decomposition'),
        ('AutoCES', 'Complex Exponential Smoothing'),
        ('MSTL', 'Multiple Seasonal-Trend decomposition using LOESS'),
        ('CrostonOptimized', 'Optimized Croston method for intermittent demand'),
        ('ADIDA', 'Aggregate-Disaggregate Intermittent Demand Approach'),
        ('IMAPA', 'Intermittent Multiple Aggregation Prediction Algorithm'),
        ('HistoricAverage', 'Simple historical mean (baseline)'),
        ('SeasonalNaive', 'Last seasonal cycle repeated (baseline)'),
    ]
    for i, r in enumerate(rows):
        pdf.table_row(r, w, fill=(i % 2 == 0))

    pdf.ln(3)
    pdf.subsection_title('Neural Models (Nixtla NeuralForecast)')
    w = [45, 145]
    pdf.table_header(['Method', 'Description'], w)
    rows = [
        ('NHITS', 'N-HiTS: Neural Hierarchical Interpolation for Time Series'),
        ('NBEATS', 'Neural Basis Expansion Analysis for Time Series'),
        ('PatchTST', 'Patch Time Series Transformer'),
        ('TFT', 'Temporal Fusion Transformer'),
        ('DeepAR', 'Probabilistic autoregressive RNN model'),
    ]
    for i, r in enumerate(rows):
        pdf.table_row(r, w, fill=(i % 2 == 0))

    pdf.ln(3)
    pdf.subsection_title('Other Models')
    pdf.bullet('TimesFM (Google): Pre-trained 200M parameter foundation model (timesfm-1.0-200m)')
    pdf.bullet('LightGBM: Gradient boosted decision trees with lag features')
    pdf.bullet('XGBoost: Extreme gradient boosting with lag features')

    pdf.section_title('4.4  Backtesting & Evaluation')
    pdf.body_text(
        'Forecast accuracy is evaluated using rolling-window cross-validation. The system uses 3 '
        'rolling windows with a step size of 1 month and a minimum training set of 12 observations. '
        'With a 24-month horizon, only series with 36+ observations qualify for backtesting (currently 57 '
        'series; 25 have enough methods for ranking).'
    )

    pdf.subsection_title('Evaluation Metrics')
    w = [40, 60, 90]
    pdf.table_header(['Category', 'Metric', 'Interpretation'], w)
    rows = [
        ('Point', 'MAE', 'Average absolute error (scale-dependent)'),
        ('Point', 'RMSE', 'Penalizes large errors more than MAE'),
        ('Point', 'MAPE', 'Percentage error (undefined if actual=0)'),
        ('Point', 'sMAPE', 'Symmetric MAPE (handles zeros better)'),
        ('Point', 'Bias', 'Systematic over/under-forecasting'),
        ('Point', 'MASE', 'Scaled error vs naive baseline'),
        ('Probabilistic', 'CRPS', 'Overall distributional accuracy'),
        ('Probabilistic', 'Winkler Score', 'Prediction interval quality'),
        ('Probabilistic', 'Coverage (90%)', 'Should be close to 90%'),
        ('Info Criteria', 'AIC / BIC', 'Model parsimony assessment'),
    ]
    for i, r in enumerate(rows):
        pdf.table_row(r, w, fill=(i % 2 == 0))

    pdf.section_title('4.5  Best Method Selection')
    pdf.body_text(
        'For each series with backtest metrics, methods are ranked using a weighted composite score. '
        'The method with the lowest score is selected as the winner. The scoring formula is:'
    )
    pdf.code_block(
        'Composite Score = 0.40 * MAE_normalized\n'
        '               + 0.20 * RMSE_normalized\n'
        '               + 0.15 * |Bias|_normalized\n'
        '               + 0.15 * (1 - Coverage_90)\n'
        '               + 0.10 * MASE_normalized\n'
        '\n'
        'Lower score = better method'
    )
    pdf.body_text(
        'The weights reflect a preference for point forecast accuracy (60% for MAE + RMSE), '
        'while still penalizing systematic bias and poor prediction interval calibration.'
    )

    pdf.section_title('4.6  Distribution Fitting (MEIO)')
    pdf.body_text(
        'For Multi-Echelon Inventory Optimization, parametric probability distributions are fitted '
        'to the forecast quantiles of the best method. Four distribution families are tested: '
        'Normal, Gamma, Negative Binomial, and Lognormal. The Kolmogorov-Smirnov test is used '
        'to assess goodness of fit, and service level quantiles at 90%, 95%, and 99% are calculated '
        'for safety stock computation.'
    )

    # =====================================================================
    # 5. API REFERENCE
    # =====================================================================
    pdf.chapter_title('5', 'API Reference')
    pdf.body_text(
        'The FastAPI backend runs on port 8000 and serves all forecast data via REST endpoints. '
        'All responses are JSON. The API loads Parquet files into an in-memory cache on startup '
        'for fast response times.'
    )

    endpoints = [
        ('GET', '/api/series', 'List all time series with filtering and pagination',
         'skip (int), limit (int, max 50000), search (str), complexity (str), intermittent (bool)',
         'Array of TimeSeriesInfo objects'),
        ('GET', '/api/series/{id}/data', 'Get historical data for a specific series',
         'unique_id (path)', 'Object with date[] and value[] arrays'),
        ('GET', '/api/forecasts/{id}', 'Get all forecasts for a series',
         'unique_id (path), methods (list, optional)', 'Object with forecasts[{method, point_forecast, quantiles}]'),
        ('GET', '/api/metrics/{id}', 'Get backtest metrics for a series',
         'unique_id (path)', 'Object with metrics[{method, mae, rmse, bias, mape, coverage_90}]'),
        ('GET', '/api/analytics', 'Dashboard-level summary statistics',
         'None', 'Totals, distributions, method counts'),
        ('GET', '/api/best-methods', 'Get best method for all series',
         'None', 'Array of {unique_id, best_method, best_score}'),
        ('GET', '/api/series/{id}/best-method', 'Best method for one series',
         'unique_id (path)', 'Object with best_method, best_score, runner_up'),
        ('GET', '/api/forecasts/{id}/origins', 'List backtest origin dates',
         'unique_id (path)', 'Object with origins[] date array'),
        ('GET', '/api/forecasts/{id}/origins/{date}', 'Forecasts at a specific origin',
         'unique_id, origin_date (path)', 'Object with forecasts[{method, point_forecast, actual}]'),
        ('GET', '/api/series/{id}/forecast-evolution', 'Full evolution for animation',
         'unique_id (path)', 'Object with evolution[{origin, forecasts}]'),
        ('GET', '/api/series/{id}/vega-spec', 'Vega-Lite chart specification',
         'unique_id (path)', 'Complete Vega-Lite v5 JSON spec'),
    ]

    for method, path, desc, params, response in endpoints:
        pdf.subsection_title(f'{method}  {path}')
        pdf.body_text(desc)
        pdf.set_font('Helvetica', 'B', 9)
        pdf.set_text_color(70, 70, 70)
        pdf.cell(0, 5, 'Parameters:', 0, 1)
        pdf.set_font('Helvetica', '', 9)
        pdf.set_text_color(40, 40, 40)
        pdf.set_x(20)
        pdf.multi_cell(170, 5, params)
        pdf.set_font('Helvetica', 'B', 9)
        pdf.set_text_color(70, 70, 70)
        pdf.cell(0, 5, 'Response:', 0, 1)
        pdf.set_font('Helvetica', '', 9)
        pdf.set_text_color(40, 40, 40)
        pdf.set_x(20)
        pdf.multi_cell(170, 5, response)
        pdf.ln(2)

    # =====================================================================
    # 6. FRONTEND USER GUIDE
    # =====================================================================
    pdf.chapter_title('6', 'Frontend User Guide')
    pdf.body_text(
        'The ForecastAI frontend is a single-page application built with React and Vega-Lite. '
        'It provides two main views: the Dashboard (overview of all series) and the Time Series '
        'Detail View (deep-dive into a single series). The frontend communicates with the API '
        'via a Vite development proxy on port 5173.'
    )

    pdf.section_title('6.1  Dashboard')
    pdf.body_text(
        'The Dashboard is the landing page and provides a high-level overview of all 2,348 time series. '
        'It features 6 summary cards, 2 interactive charts, search and filter controls, and a sortable '
        'paginated table of all series.'
    )

    pdf.subsection_title('Summary Cards')
    pdf.bullet('Total Series: Count of all time series in the dataset (2,348)')
    pdf.bullet('Backtested: Number of series with backtest metrics (25 with 24-month horizon)')
    pdf.bullet('Seasonal: Series with detected seasonality (143)')
    pdf.bullet('Trending: Series with detected monotonic trend (415)')
    pdf.bullet('Intermittent: Series with intermittent demand patterns (0 in current dataset)')
    pdf.bullet('Avg Observations: Average number of monthly data points per series (18)')

    pdf.subsection_title('Charts')
    pdf.bullet('Complexity Distribution (pie chart): Shows proportion of low vs. medium vs. high complexity series')
    pdf.bullet('Best Method Distribution (bar chart): Shows which forecasting method wins most often across backtested series')

    pdf.add_screenshot('01_dashboard.png', 'Figure 1: Dashboard - Summary cards, charts, filters, and series table', 170)

    pdf.subsection_title('Series Table')
    pdf.body_text(
        'The table lists all series with columns: Series ID, Observations, Complexity, Intermittent, '
        'Seasonal, Trend, Mean, and Best Method. Clicking any row navigates to the detail view. '
        'The table supports search by ID, filtering by complexity level and intermittency, and '
        'pagination (50 rows per page).'
    )

    pdf.add_screenshot('02_dashboard_table.png', 'Figure 2: Dashboard - Series table with filters and pagination', 170)

    pdf.section_title('6.2  Time Series Detail View')
    pdf.body_text(
        'The detail view is the core analytical tool. It shows everything about a single time series: '
        'its characteristics, all forecast methods with their predictions, accuracy metrics from '
        'backtesting, and the evolution of forecasts over time.'
    )

    pdf.subsection_title('Header & Characteristics')
    pdf.body_text(
        'The header shows the series ID and characteristic badges: observation count, continuous vs. '
        'intermittent, seasonal vs. non-seasonal, trending vs. stationary, complexity level, and the '
        'winning forecast method (if backtest data is available).'
    )

    pdf.subsection_title('Method Toggles')
    pdf.body_text(
        'Color-coded toggle buttons let you show/hide individual forecasting methods on all charts. '
        'Each method has a unique color. The best method is marked with a star icon.'
    )

    pdf.subsection_title('Main Forecast Chart')
    pdf.body_text(
        'The main chart overlays historical data (solid black line) with forecast lines (dashed, color-coded '
        'per method) and prediction intervals (shaded bands for 50% and 90% confidence). The chart '
        'title indicates the horizon length (24-month). A Vega-Lite tooltip shows exact values on hover.'
    )

    pdf.add_screenshot('03_series_header_chart.png',
                       'Figure 3: Series detail - Header, method toggles, and main forecast chart with 24-month horizon', 170)

    pdf.subsection_title('Backtest Metrics')
    pdf.body_text(
        'Two panels show accuracy results from backtesting: a horizontal bar chart comparing MAE '
        'across methods (green border highlights the winner), and a detailed table with MAE, RMSE, '
        'Bias, MAPE, 90% Coverage, and number of backtest windows. For series without enough history '
        'for backtesting, a note explains the minimum observation requirement.'
    )

    pdf.add_screenshot('05_series_metrics.png',
                       'Figure 4: Backtest metrics - MAE bar chart and detailed metrics table', 170)

    pdf.subsection_title('Forecast Evolution & Racing Bars')
    pdf.body_text(
        'The "Forecast Evolution Over Time" section shows how forecasts changed at different backtest '
        'origins. A Play/Pause button animates through origins. A slider allows manual origin selection. '
        'The "racing bars" chart compares forecast values across methods for a selected horizon month, '
        'with a red dashed line indicating the actual value when available.'
    )
    pdf.body_text(
        'The horizon month selector provides quick-access buttons (M1, M3, M6, M12, M18, M24) plus '
        'a fine-grained slider for any month in between.'
    )

    pdf.add_screenshot('06_series_evolution.png',
                       'Figure 5: Forecast evolution - Origin slider, racing bars, and horizon selector', 170)

    pdf.subsection_title('Forecast Values Table')
    pdf.body_text(
        'At the bottom, a horizontally scrollable table shows the exact point forecast values for '
        'all 24 months across all methods. The method column is sticky (stays visible while scrolling). '
        'The best method row is highlighted in green.'
    )

    pdf.add_screenshot('07_series_forecast_table.png',
                       'Figure 6: Forecast point values table (24 months, all methods)', 170)

    pdf.section_title('6.3  Interactive Features')

    pdf.subsection_title('Date Range Zoom Slider')
    pdf.body_text(
        'Below the main chart, a dual-handle slider allows zooming into any date range. The left '
        'handle sets the start date and the right handle sets the end date. A blue highlighted bar '
        'shows the selected range. The chart updates in real-time as you drag. A "Reset" button '
        'returns to the full date range.'
    )

    pdf.subsection_title('Method Toggles')
    pdf.body_text(
        'Click any method button to show/hide it across all charts. Active methods are shown with their '
        'color background; inactive ones are grayed out. This helps compare specific method subsets.'
    )

    pdf.subsection_title('Forecast Evolution Animation')
    pdf.body_text(
        'Click "Play" to animate through all backtest origins, watching how forecasts evolved. '
        'The animation advances every 800ms. Click "Stop" to pause at any point.'
    )

    # =====================================================================
    # 7. CONFIGURATION REFERENCE
    # =====================================================================
    pdf.chapter_title('7', 'Configuration Reference')
    pdf.body_text(
        'All system parameters are controlled via config/config.yaml. Key sections include:'
    )

    pdf.subsection_title('Data Source')
    pdf.code_block(
        'data_source:\n'
        '  type: "postgres"\n'
        '  postgres:\n'
        '    host: "ep-gentle-rain-a5ba1hxu.us-east-2.aws.neon.tech"\n'
        '    port: 5432\n'
        '    database: "scenario"\n'
        '    schema: "plan"\n'
        '    sslmode: "require"'
    )

    pdf.subsection_title('ETL Settings')
    pdf.code_block(
        'etl:\n'
        '  output_path: "./data/time_series.parquet"\n'
        '  query:\n'
        '    date_column: "date"\n'
        '    value_column: "qty"\n'
        '    min_observations: 12\n'
        '  aggregation:\n'
        '    frequency: "M"      # Monthly\n'
        '    method: "sum"'
    )

    pdf.subsection_title('Forecasting')
    pdf.code_block(
        'forecasting:\n'
        '  horizon: 24              # 24-month forecast\n'
        '  frequency: "M"\n'
        '  confidence_levels: [10, 25, 50, 75, 90, 95, 99]\n'
        '  backtesting:\n'
        '    n_windows: 3           # Rolling windows\n'
        '    step_size: 1\n'
        '    min_train_size: 12'
    )

    pdf.subsection_title('Best Method Selection Weights')
    pdf.code_block(
        'best_method:\n'
        '  weights:\n'
        '    mae: 0.40\n'
        '    rmse: 0.20\n'
        '    bias: 0.15\n'
        '    coverage_90: 0.15\n'
        '    mase: 0.10'
    )

    pdf.subsection_title('MEIO Distribution Fitting')
    pdf.code_block(
        'meio:\n'
        '  distributions: [normal, gamma, negative_binomial, lognormal]\n'
        '  fitting_method: "quantile_matching"\n'
        '  service_levels: [0.90, 0.95, 0.99]'
    )

    pdf.subsection_title('Parallel Processing')
    pdf.code_block(
        'parallel:\n'
        '  backend: "dask"\n'
        '  dask:\n'
        '    scheduler: "processes"\n'
        '    n_workers: null        # Auto-detect CPU cores\n'
        '  batch_size: 100'
    )

    # =====================================================================
    # 8. INSTALLATION & RUNNING
    # =====================================================================
    pdf.chapter_title('8', 'Installation & Running')

    pdf.section_title('Prerequisites')
    pdf.bullet('Python 3.11+ (tested with 3.13)')
    pdf.bullet('Node.js 18+ with npm')
    pdf.bullet('Network access to Neon PostgreSQL (for ETL step)')

    pdf.section_title('Backend Setup')
    pdf.code_block(
        'cd files/\n'
        'pip install -r requirements.txt'
    )

    pdf.section_title('Frontend Setup')
    pdf.code_block(
        'cd files/frontend/\n'
        'npm install'
    )

    pdf.section_title('Running the Full Pipeline')
    pdf.code_block(
        '# Run all stages (ETL through distribution fitting)\n'
        'python run_pipeline.py\n'
        '\n'
        '# Skip ETL (use existing data/time_series.parquet)\n'
        'python run_pipeline.py --skip-etl\n'
        '\n'
        '# Run only a specific stage\n'
        'python run_pipeline.py --only etl\n'
        'python run_pipeline.py --only characterize\n'
        'python run_pipeline.py --only forecast\n'
        'python run_pipeline.py --only backtest\n'
        'python run_pipeline.py --only best-method\n'
        'python run_pipeline.py --only distributions\n'
        '\n'
        '# Discover database schema\n'
        'python run_pipeline.py --discover-schema'
    )

    pdf.section_title('Starting the API Server')
    pdf.code_block(
        '# From the files/ directory\n'
        'uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload\n'
        '\n'
        '# Or directly:\n'
        'python api/main.py'
    )
    pdf.body_text('The API loads all Parquet files into memory on startup and serves at http://localhost:8000.')

    pdf.section_title('Starting the Frontend')
    pdf.code_block(
        'cd files/frontend/\n'
        'npm run dev\n'
        '\n'
        '# For production build:\n'
        'npm run build\n'
        'npm run preview'
    )
    pdf.body_text(
        'The Vite dev server starts on http://localhost:5173 and automatically proxies /api/* '
        'requests to the backend on port 8000. Open http://localhost:5173 in a browser to access '
        'the dashboard.'
    )

    pdf.section_title('Quick Start (Both Servers)')
    pdf.code_block(
        '# Terminal 1: Start API\n'
        'cd files/ && uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload\n'
        '\n'
        '# Terminal 2: Start Frontend\n'
        'cd files/frontend/ && npm run dev\n'
        '\n'
        '# Open http://localhost:5173 in your browser'
    )

    # =====================================================================
    # 9. TROUBLESHOOTING
    # =====================================================================
    pdf.chapter_title('9', 'Troubleshooting')

    pdf.subsection_title('Dashboard shows 0 series in the table')
    pdf.body_text(
        'This can happen if the API limit parameter is too restrictive. The frontend requests '
        'limit=50000. Verify the API is running and responding: curl http://localhost:8000/api/series?limit=5'
    )

    pdf.subsection_title('Charts appear empty in the detail view')
    pdf.body_text(
        'Ensure the API server was restarted after regenerating Parquet files. The API caches data '
        'in memory at startup. If the Parquet files changed, restart the API with: '
        'uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload'
    )

    pdf.subsection_title('Port already in use')
    pdf.body_text(
        'If port 8000 or 5173 is occupied:\n'
        'Windows: netstat -ano | findstr :8000  then  taskkill /F /PID <pid>\n'
        'Linux/Mac: lsof -i :8000  then  kill <pid>'
    )

    pdf.subsection_title('Few series have backtest metrics')
    pdf.body_text(
        'With a 24-month forecast horizon and min_train_size=12, a series needs at least 36 monthly '
        'observations. Only 57 of 2,348 series meet this threshold. To increase backtesting coverage, '
        'reduce the horizon in config.yaml (e.g., horizon: 6) or reduce min_train_size. Note that this '
        'is a data limitation, not a system bug.'
    )

    pdf.subsection_title('Vite dev server keeps dying')
    pdf.body_text(
        'The Vite dev server may crash if system memory is low. Try restarting with: '
        'cd files/frontend && npx vite --host. For production use, build the frontend with '
        'npm run build and serve the static files.'
    )

    pdf.subsection_title('Database connection failures')
    pdf.body_text(
        'Verify the Neon PostgreSQL credentials in config/config.yaml. Neon databases may auto-suspend '
        'after inactivity. The first connection may take a few seconds as the compute endpoint wakes up. '
        'Ensure sslmode: "require" is set for Neon connections.'
    )

    # =====================================================================
    # Save
    # =====================================================================
    pdf.output(OUTPUT_PATH)
    print(f"PDF saved to: {OUTPUT_PATH}")
    print(f"Pages: {pdf.page_no()}")


if __name__ == '__main__':
    build_pdf()

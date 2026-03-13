# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Starting the Application
```bash
# Start both API and frontend (Windows)
start.bat

# API only (port 8002)
cd files && python -m uvicorn api.main:app --host 127.0.0.1 --port 8002 --reload

# Frontend only (port 5173)
cd files/frontend && npm run dev

# Frontend production build
cd files/frontend && npm run build
```

### Install Dependencies
```bash
pip install -r files/requirements.txt
cd files/frontend && npm install
```

### API Documentation
Interactive Swagger docs available at `http://localhost:8002/docs` when API is running.

## Architecture

### Data Pipeline (End-to-End)
```
PostgreSQL (demand_db, localhost:5432)
  → ETL (files/etl/) — Weekly aggregation, data loading
  → Outlier Detection (files/outlier/) — IQR/z-score/STL
  → Characterization (files/characterization/) — seasonality, trend, intermittency
  → Forecasting (files/forecasting/) — StatsForecast, NeuralForecast, TimesFM via Dask
  → Evaluation (files/evaluation/) — Rolling window backtesting
  → Best Method Selection (files/selection/best_method.py) — Composite weighted scoring
  → Distribution Fitting (files/distribution/fitting.py) — Normal/Gamma/NegBin/Lognorm for MEIO
  → PostgreSQL storage
  → FastAPI (files/api/main.py, port 8002)
  → React frontend (files/frontend/, port 5173)
```

### Backend (`files/api/main.py`)
Single 6000+ line FastAPI file with 77+ endpoints. All routes prefixed with `/api/`. Key areas:
- Authentication: JWT via `files/api/auth.py` + `files/api/auth_middleware.py`
- Data routes: `/api/series`, `/api/series/{id}/data`
- Pipeline routes: `/api/pipeline/*`
- Forecast routes: `/api/forecast/*`
- MEIO/distribution: `/api/meio/*`
- ABC analysis, segments, adjustments, audit log

CORS allows `localhost:3000` and `localhost:5173`. JWT secret is stored in DB (falls back to env var).

### Database (`files/db/db.py`)
PostgreSQL via psycopg2. Schema in `files/DDL/schema.sql` — uses `pg_mooncake` columnar extension for `demand_actuals`. Key tables: `item`, `site`, `demand_actuals`, `demand_corrections`, `customer_segments`, `meio_results`. Config is stored in DB and loaded via `load_config_from_db()`.

### Configuration (`files/config/config.yaml`)
Master config for the entire pipeline:
- `data_source`: PostgreSQL connection (demand_db)
- `etl.frequency`: `W` (Weekly)
- `forecasting.horizon`: 24 periods
- `forecasting` models: AutoARIMA, AutoETS, MSTL, CrostonOptimized, NHITS, NBEATS, PatchTST, TFT, DeepAR, TimesFM
- `parallel`: Dask processes scheduler, `n_workers: auto`
- `meio`: Distribution fitting parameters

### Frontend (`files/frontend/`)
React 18 SPA. Vite proxies `/api/*` → `http://localhost:8002`. Key components:
- `Dashboard.jsx` — Series table with inline SVG sparklines (not Plotly, for performance)
- `TimeSeriesViewer.jsx` — Drill-down view with Plotly charts, metrics, ridge chart
- `PipelineRunner.jsx` — Run the forecasting pipeline
- Auth: Google OAuth (`@react-oauth/google`) + Azure MSAL (`@azure/msal-browser`)
- Charts: Plotly.js (chunked as `vendor-plotly` in build)
- Dark mode: Tailwind class-based, toggled via `ThemeProvider` context

### Method Selection Logic
`files/characterization/characterization.py` → `_recommend_methods()` routes series to appropriate forecasting methods based on characterization (intermittency → Croston, strong seasonality → MSTL, etc.). `files/selection/best_method.py` then picks the winner via composite weighted scoring across backtesting metrics.

### Parallelization
`files/orchestrator.py` manages Dask-based parallel execution of forecasting jobs across series.

## Key Conventions
- Best method highlighted in green (`#059669` / `bg-emerald-*`)
- `METHOD_COLORS` constant in `TimeSeriesViewer.jsx` for consistent method color mapping
- Sparklines use inline SVG (not Vega-Lite/Plotly) for performance in the series table
- All styling via Tailwind CSS — no external CSS files
- `files/api/main.py` is the single source of truth for all API logic; avoid splitting unless necessary

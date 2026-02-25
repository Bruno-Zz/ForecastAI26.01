"""
FastAPI Backend for Forecasting System
RESTful API for accessing forecasts, metrics, and visualizations
"""

from fastapi import FastAPI, HTTPException, Query, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from typing import List, Optional, Dict, Any
import pandas as pd
import numpy as np
from pathlib import Path
from pydantic import BaseModel
from scipy import stats as scipy_stats
import logging
import yaml
import json
import subprocess
import sys
import asyncio
import uuid
import threading
import os
import signal
from datetime import datetime, date as date_type

# DB helpers — adjust sys.path so we can import from the files/ tree
_files_dir = Path(__file__).resolve().parent.parent
if str(_files_dir) not in sys.path:
    sys.path.insert(0, str(_files_dir))

try:
    from db.db import get_conn, init_schema as _init_db_schema, get_schema, load_table
    _DB_AVAILABLE = True
except Exception as _db_import_err:
    _DB_AVAILABLE = False
    logging.warning(f"db.db import failed: {_db_import_err}")

try:
    import dask.dataframe as dd
    _DASK_AVAILABLE = True
except ImportError:
    _DASK_AVAILABLE = False
    logging.warning("Dask not available; data_cache will use pandas DataFrames")

# Initialize FastAPI app
app = FastAPI(
    title="Time Series Forecasting API",
    description="API for accessing forecasts, demand analytics, and MEIO data",
    version="1.0.0"
)

# Configure CORS for React frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:5173"],  # React dev servers
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Data cache
data_cache = {
    'time_series': None,
    'time_series_original': None,
    'characteristics': None,
    'forecasts': None,
    'distributions': None,
    'metrics': None,
    'best_methods': None,
    'forecasts_by_origin': None,
    'outliers': None,
    'config': None,
}


# Pydantic models for API responses
class TimeSeriesInfo(BaseModel):
    unique_id: str
    n_observations: int
    date_range_start: str
    date_range_end: str
    mean: float
    is_intermittent: bool
    has_seasonality: bool
    has_trend: bool
    complexity_level: str
    has_outlier_corrections: Optional[bool] = False
    n_outliers: Optional[int] = 0
    best_method: Optional[str] = None
    best_method_source: Optional[str] = None  # 'backtested' or 'recommended'


class ForecastData(BaseModel):
    unique_id: str
    method: str
    point_forecast: List[float]
    quantiles: Dict[str, List[float]]
    forecast_dates: Optional[List[str]] = None


class MetricsData(BaseModel):
    unique_id: str
    method: str
    mae: float
    rmse: float
    bias: float
    coverage_90: Optional[float] = None


class RunForecastRequest(BaseModel):
    series: List[str]           # e.g., ["63530_517", "63531_518"]
    all_methods: bool = True    # run ALL methods (not just recommended)


# ── Helper: wrap a pandas DataFrame into a Dask DataFrame when available ──
#
# IMPORTANT: Dask serializes Python list/dict objects in object-dtype columns
# to their string representation.  Tables with JSONB columns (forecast_results,
# best_method_per_series, fitted_distributions, time_series_characteristics)
# MUST stay as plain pandas to preserve native Python types.  Only large,
# flat-column tables should be Dask-wrapped.
#
_DASK_SAFE_TABLES = {
    "time_series", "time_series_original", "metrics",
    "forecasts_by_origin", "outliers",
}

def _to_dask(pdf: pd.DataFrame, name: str = "") -> "pd.DataFrame | dd.DataFrame":
    """Convert a pandas DataFrame to a Dask DataFrame when Dask is available.
    Only wraps tables listed in _DASK_SAFE_TABLES (no JSONB columns)."""
    if _DASK_AVAILABLE and not pdf.empty and name in _DASK_SAFE_TABLES:
        nparts = max(1, len(pdf) // 50_000)
        ddf = dd.from_pandas(pdf, npartitions=nparts)
        return ddf
    return pdf


def _compute(df_or_ddf):
    """Call .compute() on Dask DataFrames; return pandas DataFrames as-is."""
    if _DASK_AVAILABLE and hasattr(df_or_ddf, 'compute'):
        return df_or_ddf.compute()
    return df_or_ddf


# Helper functions
def load_data():
    """Load all data from PostgreSQL into cache as Dask DataFrames."""
    if not _DB_AVAILABLE:
        logger.warning("Database not available — cannot load data")
        return

    cfg_path = _api_config_path()

    try:
        schema = get_schema(cfg_path)
        conn = get_conn(cfg_path)

        # ── time_series (corrected values, used for charts) ──
        if data_cache['time_series'] is None:
            pdf = pd.read_sql(
                f"SELECT unique_id, date, COALESCE(corrected_qty, qty) AS y "
                f"FROM {schema}.demand_actuals ORDER BY unique_id, date",
                conn,
            )
            pdf['date'] = pd.to_datetime(pdf['date'])
            data_cache['time_series'] = _to_dask(pdf, "time_series")
            logger.info(f"Loaded time_series from DB: {len(pdf)} rows")

        # ── time_series_original (raw qty, for outlier comparison) ──
        if data_cache['time_series_original'] is None:
            pdf = pd.read_sql(
                f"SELECT unique_id, date, qty AS y "
                f"FROM {schema}.demand_actuals ORDER BY unique_id, date",
                conn,
            )
            pdf['date'] = pd.to_datetime(pdf['date'])
            data_cache['time_series_original'] = _to_dask(pdf, "time_series_original")
            logger.info(f"Loaded time_series_original from DB: {len(pdf)} rows")

        # ── Backfill missing series from source DB ──
        # When demand_actuals has fewer series than forecast_results (e.g. after
        # a partial ETL re-run), load the missing actuals from the source DB.
        try:
            ts_df = _compute(data_cache['time_series'])
            local_uids = set(ts_df['unique_id'].unique())
            fc_df = None
            if data_cache.get('forecasts') is not None:
                fc_df = _compute(data_cache['forecasts'])
            elif True:
                fc_df = pd.read_sql(f"SELECT DISTINCT unique_id FROM {schema}.forecast_results", conn)
            if fc_df is not None:
                forecast_uids = set(fc_df['unique_id'].unique())
                missing = forecast_uids - local_uids
                if missing:
                    logger.info(f"Backfill: {len(missing)} series in forecasts but not in demand_actuals — querying source DB…")
                    # Read config directly from file (data_cache['config'] isn't loaded yet)
                    _bf_cfg_path = Path(cfg_path) if not isinstance(cfg_path, Path) else cfg_path
                    with open(_bf_cfg_path, 'r') as _bf_fh:
                        cfg = yaml.safe_load(_bf_fh)
                    src = cfg.get('data_source', {}).get('source_db', {})
                    if src.get('host'):
                        import psycopg2
                        src_conn = psycopg2.connect(
                            host=src['host'], port=src.get('port', 5432),
                            dbname=src.get('database', 'postgres'),
                            user=src.get('user'), password=src.get('password'),
                            sslmode=src.get('sslmode', 'require'),
                        )
                        cols = src.get('columns', {})
                        item_col = cols.get('item_id', 'item_id')
                        site_col = cols.get('site_id', 'site_id')
                        date_col = cols.get('date', 'date')
                        qty_col  = cols.get('qty', 'qty')
                        table    = src.get('demand_table', 'dp_plan.calc_dp_actual')
                        agg_freq = cfg.get('etl', {}).get('aggregation', {}).get('frequency', 'W')
                        # Build a list of (item, site) pairs to query
                        pairs = [(uid.split('_', 1)[0], uid.split('_', 1)[1]) for uid in missing if '_' in uid]
                        if pairs:
                            # Query source in batches
                            batch_size = 200
                            src_frames = []
                            for i in range(0, len(pairs), batch_size):
                                batch = pairs[i:i+batch_size]
                                where_clauses = " OR ".join(
                                    [f"({item_col}='{it}' AND {site_col}='{si}')" for it, si in batch]
                                )
                                q = (f"SELECT {item_col} || '_' || {site_col} AS unique_id, "
                                     f"{date_col} AS date, {qty_col} AS y "
                                     f"FROM {table} WHERE {where_clauses} "
                                     f"ORDER BY unique_id, date")
                                batch_df = pd.read_sql(q, src_conn)
                                if not batch_df.empty:
                                    src_frames.append(batch_df)
                            src_conn.close()
                            if src_frames:
                                src_pdf = pd.concat(src_frames, ignore_index=True)
                                src_pdf['date'] = pd.to_datetime(src_pdf['date'])
                                # Aggregate to the same frequency as ETL config
                                if agg_freq:
                                    src_pdf = (src_pdf.groupby('unique_id')
                                               .apply(lambda g: g.set_index('date').resample(agg_freq)['y'].sum().reset_index(), include_groups=False)
                                               .reset_index(level=0))
                                # Merge with existing time_series
                                ts_merged = pd.concat([ts_df, src_pdf], ignore_index=True)
                                data_cache['time_series'] = _to_dask(ts_merged, "time_series")
                                # Also update time_series_original
                                orig_df = _compute(data_cache['time_series_original'])
                                orig_merged = pd.concat([orig_df, src_pdf], ignore_index=True)
                                data_cache['time_series_original'] = _to_dask(orig_merged, "time_series_original")
                                logger.info(f"Backfill: added {len(src_pdf)} rows for {src_pdf['unique_id'].nunique()} series from source DB")
                            else:
                                logger.warning("Backfill: source DB returned no data for missing series")
                    else:
                        logger.warning("Backfill: no source_db config — cannot load missing series")
        except Exception as backfill_err:
            logger.warning(f"Backfill from source DB failed (non-fatal): {backfill_err}")

        # ── characteristics ──
        if data_cache['characteristics'] is None:
            pdf = pd.read_sql(f"SELECT * FROM {schema}.time_series_characteristics", conn)
            # JSONB columns are auto-deserialized by psycopg2
            for col in ('seasonal_periods', 'recommended_methods'):
                if col in pdf.columns:
                    pdf[col] = pdf[col].apply(lambda x: x if isinstance(x, list) else [])
            data_cache['characteristics'] = _to_dask(pdf, "characteristics")
            logger.info(f"Loaded characteristics from DB: {len(pdf)} series")

        # ── forecasts ──
        if data_cache['forecasts'] is None:
            pdf = pd.read_sql(f"SELECT * FROM {schema}.forecast_results", conn)
            data_cache['forecasts'] = _to_dask(pdf, "forecasts")
            logger.info(f"Loaded forecasts from DB: {len(pdf)} rows")

        # ── distributions ──
        if data_cache['distributions'] is None:
            pdf = pd.read_sql(f"SELECT * FROM {schema}.fitted_distributions", conn)
            data_cache['distributions'] = _to_dask(pdf, "distributions") if not pdf.empty else None
            if not pdf.empty:
                logger.info(f"Loaded distributions from DB: {len(pdf)} rows")

        # ── metrics ──
        if data_cache['metrics'] is None:
            pdf = pd.read_sql(f"SELECT * FROM {schema}.backtest_metrics", conn)
            # Coerce metric columns to numeric (DB NULLs can cause object dtype)
            for mc in ['mae', 'rmse', 'mape', 'smape', 'mase', 'bias',
                       'crps', 'winkler_score',
                       'coverage_50', 'coverage_80', 'coverage_90', 'coverage_95',
                       'quantile_loss', 'aic', 'bic', 'aicc']:
                if mc in pdf.columns:
                    pdf[mc] = pd.to_numeric(pdf[mc], errors='coerce')
            data_cache['metrics'] = _to_dask(pdf, "metrics")
            logger.info(f"Loaded metrics from DB: {len(pdf)} rows")

        # ── best_methods ──
        if data_cache['best_methods'] is None:
            pdf = pd.read_sql(f"SELECT * FROM {schema}.best_method_per_series", conn)
            data_cache['best_methods'] = _to_dask(pdf, "best_methods")
            logger.info(f"Loaded best_methods from DB: {len(pdf)} rows")

        # ── forecasts_by_origin ──
        if data_cache['forecasts_by_origin'] is None:
            pdf = pd.read_sql(f"SELECT * FROM {schema}.forecasts_by_origin", conn)
            for col in ['forecast_origin', 'origin', 'origin_date']:
                if col in pdf.columns:
                    pdf[col] = pd.to_datetime(pdf[col])
            data_cache['forecasts_by_origin'] = _to_dask(pdf, "forecasts_by_origin")
            logger.info(f"Loaded forecasts_by_origin from DB: {len(pdf)} rows")

        # ── outliers ──
        if data_cache['outliers'] is None:
            pdf = pd.read_sql(f"SELECT * FROM {schema}.detected_outliers", conn)
            if not pdf.empty and 'date' in pdf.columns:
                pdf['date'] = pd.to_datetime(pdf['date'])
            data_cache['outliers'] = _to_dask(pdf, "outliers") if not pdf.empty else None
            if not pdf.empty:
                logger.info(f"Loaded outliers from DB: {len(pdf)} rows")

        conn.close()

        # ── config (YAML, not DB) ──
        if data_cache['config'] is None:
            config_path = Path('./config/config.yaml')
            if config_path.exists():
                with open(config_path, 'r') as fh:
                    data_cache['config'] = yaml.safe_load(fh)
                logger.info("Loaded config.yaml")

    except Exception as e:
        logger.error(f"Error loading data from DB: {e}", exc_info=True)


def _get_config():
    """Get config from cache with defaults."""
    return data_cache.get('config') or {}


def _api_config_path() -> str:
    """Return the path to config.yaml as a string."""
    p = Path(__file__).resolve().parent.parent / "config" / "config.yaml"
    return str(p)


@app.on_event("startup")
async def startup_event():
    """Load data on startup and ensure DB schema exists."""
    logger.info("Starting API server...")
    # Ensure local DB schema is up-to-date
    if _DB_AVAILABLE:
        try:
            _init_db_schema(_api_config_path())
            logger.info("DB schema initialised")
        except Exception as exc:
            logger.warning(f"DB schema init failed (non-fatal): {exc}")
    load_data()


@app.post("/api/reload")
async def reload_data():
    """Reload all data from PostgreSQL into cache (call after a pipeline run)."""
    # Reset cache so load_data() re-reads everything
    for key in data_cache:
        data_cache[key] = None
    load_data()
    sizes = {k: (len(_compute(v)) if v is not None and hasattr(v, '__len__') else 0)
             for k, v in data_cache.items() if k != 'config'}
    return {"status": "reloaded", "cache_sizes": sizes}


@app.get("/api/config")
async def get_config():
    """Return the global YAML config (read-only, with sensitive fields redacted)."""
    config = _get_config()
    if not config:
        raise HTTPException(status_code=404, detail="Config not loaded")

    import copy
    safe = copy.deepcopy(config)

    # Redact sensitive fields
    def _redact(d, keys=('password', 'account_key', 'secret', 'token', 'connection_string')):
        if isinstance(d, dict):
            for k, v in d.items():
                if any(s in k.lower() for s in keys) and isinstance(v, str):
                    d[k] = '********'
                else:
                    _redact(v, keys)
        elif isinstance(d, list):
            for item in d:
                _redact(item, keys)

    _redact(safe)

    # Also return as raw YAML string for display
    raw_yaml = yaml.dump(safe, default_flow_style=False, sort_keys=False)

    return {"config": safe, "config_yaml": raw_yaml}


@app.post("/api/config/update")
async def update_config(body: dict = Body(...)):
    """
    Update specific config keys and persist to config.yaml.

    Expects JSON body like:
      { "path": "forecasting.horizon", "value": 52 }
    or batch:
      { "updates": [ {"path": "forecasting.horizon", "value": 52}, ... ] }
    """
    config_path = Path(_api_config_path())
    if not config_path.exists():
        raise HTTPException(status_code=404, detail="config.yaml not found")

    # Load current config
    with open(config_path, 'r') as fh:
        config = yaml.safe_load(fh)

    # Build list of updates
    updates = body.get('updates', [])
    if 'path' in body and 'value' in body:
        updates = [{'path': body['path'], 'value': body['value']}]

    if not updates:
        raise HTTPException(status_code=400, detail="No updates provided")

    # Sensitive keys that cannot be written via API
    BLOCKED_KEYS = {'password', 'secret', 'token', 'account_key', 'connection_string', 'sslmode'}

    applied = []
    for upd in updates:
        key_path = upd.get('path', '')
        value = upd.get('value')
        parts = key_path.split('.')

        if not parts or not key_path:
            continue

        # Block sensitive keys
        if any(bk in parts[-1].lower() for bk in BLOCKED_KEYS):
            continue

        # Navigate to parent
        node = config
        for p in parts[:-1]:
            if isinstance(node, dict) and p in node:
                node = node[p]
            else:
                node = None
                break

        if node is not None and isinstance(node, dict):
            old_val = node.get(parts[-1])
            node[parts[-1]] = value
            applied.append({'path': key_path, 'old': old_val, 'new': value})

    if not applied:
        raise HTTPException(status_code=400, detail="No valid updates applied")

    # Write back
    with open(config_path, 'w') as fh:
        yaml.dump(config, fh, default_flow_style=False, sort_keys=False, allow_unicode=True)

    # Refresh in-memory cache
    data_cache['config'] = config

    logger.info(f"Config updated: {[a['path'] for a in applied]}")
    return {"status": "ok", "applied": applied}


# API Endpoints

@app.get("/")
async def root():
    """API root endpoint."""
    return {
        "message": "Time Series Forecasting API",
        "version": "1.0.0",
        "endpoints": {
            "series": "/api/series",
            "forecasts": "/api/forecasts/{unique_id}",
            "metrics": "/api/metrics/{unique_id}",
            "analytics": "/api/analytics",
            "best_methods": "/api/best-methods",
            "series_best_method": "/api/series/{unique_id}/best-method",
            "forecast_origins": "/api/forecasts/{unique_id}/origins",
            "forecast_at_origin": "/api/forecasts/{unique_id}/origins/{origin_date}",
            "forecast_evolution": "/api/series/{unique_id}/forecast-evolution"
        }
    }


@app.get("/api/series", response_model=List[TimeSeriesInfo])
async def get_series_list(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=50000),
    search: Optional[str] = None,
    complexity: Optional[str] = None,
    intermittent: Optional[bool] = None
):
    """
    Get list of time series with filtering and pagination.

    Args:
        skip: Number of records to skip
        limit: Maximum number of records to return
        search: Search term for unique_id
        complexity: Filter by complexity level (low, medium, high)
        intermittent: Filter by intermittency status
    """
    if data_cache['characteristics'] is None:
        raise HTTPException(status_code=503, detail="Data not loaded")

    df = _compute(data_cache['characteristics']).copy()

    # Apply filters
    if search:
        df = df[df['unique_id'].str.contains(search, case=False, na=False)]

    if complexity:
        df = df[df['complexity_level'] == complexity]

    if intermittent is not None:
        df = df[df['is_intermittent'] == intermittent]

    # Pagination
    total = len(df)
    df = df.iloc[skip:skip + limit]

    # Build outlier lookup
    outlier_counts = {}
    if data_cache['outliers'] is not None:
        outlier_counts = _compute(data_cache['outliers']).groupby('unique_id').size().to_dict()

    # Build best-method lookup: backtested results take priority over recommendations
    backtested_map = {}
    if data_cache['best_methods'] is not None:
        for _, bm_row in _compute(data_cache['best_methods']).iterrows():
            backtested_map[bm_row['unique_id']] = str(bm_row['best_method'])

    # Convert to response model
    series_list = []
    for _, row in df.iterrows():
        uid = row['unique_id']
        n_out = outlier_counts.get(uid, 0)

        # Determine best method + source
        if uid in backtested_map:
            best_method_val = backtested_map[uid]
            best_method_source_val = 'backtested'
        else:
            rec = row.get('recommended_methods', None)
            if rec is not None and len(rec) > 0:
                best_method_val = str(rec[0])
                best_method_source_val = 'recommended'
            else:
                best_method_val = None
                best_method_source_val = None

        series_list.append(TimeSeriesInfo(
            unique_id=uid,
            n_observations=int(row['n_observations']),
            date_range_start=str(row['date_range_start']),
            date_range_end=str(row['date_range_end']),
            mean=float(row['mean']),
            is_intermittent=bool(row['is_intermittent']),
            has_seasonality=bool(row['has_seasonality']),
            has_trend=bool(row['has_trend']),
            complexity_level=str(row['complexity_level']),
            has_outlier_corrections=n_out > 0,
            n_outliers=n_out,
            best_method=best_method_val,
            best_method_source=best_method_source_val,
        ))

    return series_list


@app.get("/api/series/{unique_id}/data")
async def get_series_data(unique_id: str):
    """
    Get historical data for a specific time series.

    Returns data in format ready for Vega visualization.
    """
    if data_cache['time_series'] is None:
        raise HTTPException(status_code=503, detail="Data not loaded")

    df = _compute(data_cache['time_series'])
    series_df = df[df['unique_id'] == unique_id].sort_values('date')

    if series_df.empty:
        raise HTTPException(status_code=404, detail=f"Series {unique_id} not found")

    # Convert to Vega-ready format (corrected values)
    data = {
        'date': series_df['date'].dt.strftime('%Y-%m-%d').tolist(),
        'value': series_df['y'].tolist()
    }

    result = {
        'unique_id': unique_id,
        'data': data,
        'n_points': len(series_df),
        'has_outlier_corrections': False,
        'n_outliers': 0,
    }

    # Include original data if outlier corrections exist
    if data_cache.get('time_series_original') is not None:
        orig_df = _compute(data_cache['time_series_original'])
        orig_series = orig_df[orig_df['unique_id'] == unique_id].sort_values('date')
        if not orig_series.empty:
            orig_data = {
                'date': orig_series['date'].dt.strftime('%Y-%m-%d').tolist(),
                'value': orig_series['y'].tolist()
            }
            result['original_data'] = orig_data

            # Check if there are actual differences
            if data_cache.get('outliers') is not None:
                outliers_pdf = _compute(data_cache['outliers'])
                uid_outliers = outliers_pdf[outliers_pdf['unique_id'] == unique_id]
                if not uid_outliers.empty:
                    result['has_outlier_corrections'] = True
                    result['n_outliers'] = len(uid_outliers)

    return result


@app.get("/api/forecasts/{unique_id}")
async def get_forecasts(
    unique_id: str,
    methods: Optional[List[str]] = Query(None)
):
    """
    Get all forecasts for a specific time series.

    Args:
        unique_id: Time series identifier
        methods: Optional list of methods to filter by
    """
    if data_cache['forecasts'] is None:
        raise HTTPException(status_code=503, detail="Forecasts not loaded")

    df = _compute(data_cache['forecasts'])
    forecasts_df = df[df['unique_id'] == unique_id]

    if forecasts_df.empty:
        raise HTTPException(status_code=404, detail=f"No forecasts for {unique_id}")

    # Filter by methods if specified
    if methods:
        forecasts_df = forecasts_df[forecasts_df['method'].isin(methods)]

    # Convert to response format
    forecasts = []
    for _, row in forecasts_df.iterrows():
        # Parse quantiles if string
        quantiles = row['quantiles']
        if isinstance(quantiles, str):
            import json
            quantiles = json.loads(quantiles.replace("'", '"'))

        # Convert quantile arrays to lists
        quantiles_dict = {}
        for q, values in quantiles.items():
            if isinstance(values, (list, np.ndarray)):
                quantiles_dict[str(q)] = [float(v) for v in values]
            else:
                quantiles_dict[str(q)] = [float(values)]

        # Parse hyperparameters if available
        hyperparams = row.get('hyperparameters')
        if hyperparams is not None:
            if isinstance(hyperparams, str):
                try:
                    hyperparams = json.loads(hyperparams)
                except (json.JSONDecodeError, ValueError):
                    hyperparams = None

        forecasts.append({
            'method': row['method'],
            'point_forecast': [float(v) for v in row['point_forecast']],
            'quantiles': quantiles_dict,
            'hyperparameters': hyperparams,
            'training_time': float(row['training_time']) if pd.notna(row.get('training_time')) else None
        })

    # Include date_range_end from characteristics so the frontend can compute
    # forecast dates even when historical demand_actuals data is unavailable.
    date_range_end = None
    frequency = None
    if data_cache.get('characteristics') is not None:
        chars_df = _compute(data_cache['characteristics'])
        char_row = chars_df[chars_df['unique_id'] == unique_id]
        if not char_row.empty:
            dre = char_row.iloc[0].get('date_range_end')
            if pd.notna(dre):
                date_range_end = pd.Timestamp(dre).strftime('%Y-%m-%d')
            freq = char_row.iloc[0].get('frequency') or char_row.iloc[0].get('obs_per_year')
            if freq is not None:
                frequency = str(freq)

    # Also include lightweight historical data inline when available so
    # the frontend doesn't need a separate /series/{uid}/data call.
    historical = None
    if data_cache.get('time_series') is not None:
        ts_df = _compute(data_cache['time_series'])
        series_ts = ts_df[ts_df['unique_id'] == unique_id].sort_values('date')
        if not series_ts.empty:
            historical = {
                'date': series_ts['date'].dt.strftime('%Y-%m-%d').tolist(),
                'value': series_ts['y'].tolist(),
            }

    return {
        'unique_id': unique_id,
        'forecasts': forecasts,
        'date_range_end': date_range_end,
        'frequency': frequency,
        'historical': historical,
    }


def _compute_metrics_from_origin(unique_id: str) -> list:
    """
    Compute backtest metrics on-the-fly.

    Strategy A: use forecasts_by_origin + actual_value column (covers 25 series).
    Strategy B (fallback for all other series): pseudo-holdout using the last
    min(horizon, n_obs//3) actual observations vs the forecast that was produced.
    This gives a meaningful accuracy estimate even for short series.
    """
    ts_ddf = data_cache.get('time_series')
    if ts_ddf is None:
        return []

    ts_df = _compute(ts_ddf)
    actuals_df = ts_df[ts_df['unique_id'] == unique_id].sort_values('date')
    if actuals_df.empty:
        return []

    actuals_vals = actuals_df['y'].values
    n_obs = len(actuals_vals)

    # ── Strategy A: forecasts_by_origin ──────────────────────────────────────
    if data_cache.get('forecasts_by_origin') is not None:
        origin_df = _compute(data_cache['forecasts_by_origin'])
        for col in ['forecast_origin', 'origin', 'origin_date']:
            if col in origin_df.columns:
                origin_col = col
                break
        else:
            origin_col = None

        if origin_col:
            series_origins = origin_df[origin_df['unique_id'] == unique_id]
            if not series_origins.empty:
                actual_col = next((c for c in ['actual_value', 'actual'] if c in series_origins.columns), None)
                method_pairs: dict = {}
                for method, grp in series_origins.groupby('method'):
                    pairs = []
                    for _, row in grp.iterrows():
                        pf = row.get('point_forecast', None)
                        if pf is None:
                            continue
                        pred = float(pf) if not isinstance(pf, (list, np.ndarray)) else float(np.array(pf).ravel()[0])
                        act = None
                        if actual_col and pd.notna(row.get(actual_col)):
                            act = float(row[actual_col])
                        if act is not None and np.isfinite(pred) and np.isfinite(act):
                            pairs.append((pred, act))
                    if pairs:
                        method_pairs[method] = pairs

                if method_pairs:
                    return _pairs_to_metrics(method_pairs)

    # ── Strategy B: pseudo-holdout from final forecast vs last actuals ────────
    fc_ddf = data_cache.get('forecasts')
    if fc_ddf is None:
        return []

    fc_df = _compute(fc_ddf)
    uid_fc = fc_df[fc_df['unique_id'] == unique_id]
    if uid_fc.empty:
        return []

    # Use last third of history (min 3, max 12 points) as holdout
    n_holdout = max(3, min(12, n_obs // 3))
    holdout_acts = actuals_vals[-n_holdout:]

    method_pairs = {}
    for _, row in uid_fc.iterrows():
        method = row['method']
        pf = row['point_forecast']
        if isinstance(pf, (list, np.ndarray)):
            pf_arr = np.array(pf, dtype=float)
        else:
            continue

        # Pair the first n_holdout forecast values against holdout actuals
        n_compare = min(n_holdout, len(pf_arr))
        if n_compare < 1:
            continue
        pairs = list(zip(pf_arr[:n_compare], holdout_acts[:n_compare]))
        pairs = [(p, a) for p, a in pairs if np.isfinite(p) and np.isfinite(a)]
        if pairs:
            method_pairs[method] = pairs

    return _pairs_to_metrics(method_pairs)


def _pairs_to_metrics(method_pairs: dict) -> list:
    """Convert (pred, actual) pairs per method into metric dicts."""
    results = []
    for method, pairs in method_pairs.items():
        preds = np.array([p for p, _ in pairs])
        acts = np.array([a for _, a in pairs])
        errors = preds - acts
        abs_errors = np.abs(errors)

        mae = float(np.mean(abs_errors))
        rmse = float(np.sqrt(np.mean(errors ** 2)))
        bias = float(np.mean(errors))
        nonzero = acts != 0
        mape = float(np.mean(abs_errors[nonzero] / np.abs(acts[nonzero])) * 100) if nonzero.any() else None
        smape_vals = 2 * abs_errors / (np.abs(preds) + np.abs(acts) + 1e-8)
        smape = float(np.mean(smape_vals) * 100)
        naive = np.abs(np.diff(acts)) if len(acts) > 1 else np.array([1.0])
        mase = float(mae / np.mean(naive)) if np.mean(naive) > 0 else None

        results.append({
            'method': method,
            'n_windows': len(pairs),
            'mae': mae,
            'rmse': rmse,
            'bias': bias,
            'mape': mape,
            'smape': smape,
            'mase': mase,
            'crps': None,
            'winkler_score': None,
            'coverage_50': None,
            'coverage_80': None,
            'coverage_90': None,
            'coverage_95': None,
            'quantile_loss': None,
        })

    return results


def _compute_composite_ranking(metrics_by_method: list, weights: dict) -> dict:
    """Compute normalized composite ranking from metrics."""
    if not metrics_by_method:
        return {}

    # Collect values per metric key
    metric_keys = ['mae', 'rmse', 'bias', 'coverage_90', 'mase']
    values = {k: [] for k in metric_keys}
    for m in metrics_by_method:
        for k in metric_keys:
            v = m.get(k)
            if k == 'bias' and v is not None:
                v = abs(v)
            if k == 'coverage_90' and v is not None:
                v = abs(v - 0.90)  # Distance from target
            values[k].append(v)

    # Normalize each metric to [0,1] per method
    ranking = {}
    for m in metrics_by_method:
        score = 0.0
        total_w = 0.0
        for k in metric_keys:
            w = weights.get(k, 0)
            v = m.get(k)
            if k == 'bias' and v is not None:
                v = abs(v)
            if k == 'coverage_90' and v is not None:
                v = abs(v - 0.90)
            col_vals = [x for x in values[k] if x is not None]
            if v is None or not col_vals:
                continue
            col_min, col_max = min(col_vals), max(col_vals)
            norm = (v - col_min) / (col_max - col_min) if col_max > col_min else 0.0
            score += w * norm
            total_w += w
        ranking[m['method']] = round(score / total_w, 6) if total_w > 0 else None

    return ranking


@app.get("/api/metrics/{unique_id}")
async def get_metrics(unique_id: str):
    """Get evaluation metrics for a specific time series with full metric set and composite ranking.
    Falls back to on-the-fly computation from forecasts_by_origin if backtest table has no data."""

    metric_cols = ['mae', 'rmse', 'bias', 'mape', 'smape', 'mase',
                   'crps', 'winkler_score',
                   'coverage_50', 'coverage_80', 'coverage_90', 'coverage_95',
                   'quantile_loss']

    metrics_by_method = []
    source = 'database'

    # Try database cache first
    if data_cache['metrics'] is not None:
        df = _compute(data_cache['metrics'])
        metrics_df = df[df['unique_id'] == unique_id]
        if not metrics_df.empty:
            for method in metrics_df['method'].unique():
                method_df = metrics_df[metrics_df['method'] == method]
                entry = {'method': method, 'n_windows': len(method_df)}
                for col in metric_cols:
                    if col in method_df.columns:
                        val = method_df[col].mean()
                        entry[col] = float(val) if pd.notna(val) else None
                    else:
                        entry[col] = None
                metrics_by_method.append(entry)

    # Fallback: compute on-the-fly from origins + actuals
    if not metrics_by_method:
        metrics_by_method = _compute_metrics_from_origin(unique_id)
        source = 'computed'

    if not metrics_by_method:
        raise HTTPException(status_code=404, detail=f"No metrics for {unique_id} — series may have insufficient history for backtesting")

    config = _get_config()
    weights = config.get('best_method', {}).get('weights', {
        'mae': 0.40, 'rmse': 0.20, 'bias': 0.15, 'coverage_90': 0.15, 'mase': 0.10
    })

    # Include composite ranking from best_methods if available
    ranking = None
    if data_cache['best_methods'] is not None:
        best_df = _compute(data_cache['best_methods'])
        row_df = best_df[best_df['unique_id'] == unique_id]
        if not row_df.empty:
            row = row_df.iloc[0]
            all_rankings = row.get('all_rankings', {})
            if isinstance(all_rankings, str):
                all_rankings = json.loads(all_rankings.replace("'", '"'))
            ranking = {str(k): float(v) if pd.notna(v) else None for k, v in all_rankings.items()}

    # If ranking not in DB, compute it on-the-fly
    if ranking is None and metrics_by_method:
        ranking = _compute_composite_ranking(metrics_by_method, weights)

    # Inject composite ranking into each method's entry for convenience
    if ranking:
        for entry in metrics_by_method:
            entry['composite_score'] = ranking.get(entry['method'])

    # Derive best method from ranking if not in DB
    best_method_name = None
    if ranking:
        best_method_name = min(ranking, key=lambda k: ranking[k] if ranking[k] is not None else float('inf'))

    return {
        'unique_id': unique_id,
        'metrics': metrics_by_method,
        'composite_ranking': ranking,
        'composite_weights': weights,
        'best_method': best_method_name,
        'source': source,
    }


@app.get("/api/analytics")
async def get_analytics():
    """
    Get top-level analytics across all time series.

    Returns summary statistics for dashboard.
    """
    if data_cache['characteristics'] is None:
        raise HTTPException(status_code=503, detail="Data not loaded")

    chars_df = _compute(data_cache['characteristics'])

    analytics = {
        'total_series': len(chars_df),
        'intermittent_count': int(chars_df['is_intermittent'].sum()),
        'seasonal_count': int(chars_df['has_seasonality'].sum()),
        'trending_count': int(chars_df['has_trend'].sum()),
        'complexity_distribution': chars_df['complexity_level'].value_counts().to_dict(),
        'avg_observations': float(chars_df['n_observations'].mean()),
        'methods_summary': {}
    }

    # Method recommendations summary
    if data_cache['forecasts'] is not None:
        forecasts_df = _compute(data_cache['forecasts'])
        method_counts = forecasts_df['method'].value_counts().to_dict()
        analytics['methods_summary'] = method_counts

    # Distribution types summary
    if data_cache['distributions'] is not None:
        dist_df = _compute(data_cache['distributions'])
        dist_types = dist_df['distribution_type'].value_counts().to_dict()
        analytics['distribution_types'] = dist_types

    # Best method distribution
    if data_cache['best_methods'] is not None:
        best_df = _compute(data_cache['best_methods'])
        best_method_counts = best_df['best_method'].value_counts().to_dict()
        analytics['best_method_distribution'] = best_method_counts
        analytics['best_method_total_series'] = len(best_df)

    # Outlier summary
    if data_cache['outliers'] is not None:
        outlier_df = _compute(data_cache['outliers'])
        analytics['outlier_adjusted_count'] = int(outlier_df['unique_id'].nunique())
        analytics['outlier_total_count'] = int(len(outlier_df))
    else:
        analytics['outlier_adjusted_count'] = 0
        analytics['outlier_total_count'] = 0

    return analytics


@app.get("/api/best-methods")
async def get_best_methods():
    """
    Get best method for all series.

    Returns list of {unique_id, best_method, best_score, runner_up_method}.
    """
    if data_cache['best_methods'] is None:
        raise HTTPException(status_code=503, detail="Best methods data not loaded")

    df = _compute(data_cache['best_methods'])

    results = []
    for _, row in df.iterrows():
        entry = {
            'unique_id': str(row['unique_id']),
            'best_method': str(row['best_method']),
            'best_score': float(row['best_score']) if 'best_score' in row.index and pd.notna(row.get('best_score')) else None,
            'runner_up_method': str(row['runner_up_method']) if 'runner_up_method' in row.index and pd.notna(row.get('runner_up_method')) else None
        }
        results.append(entry)

    return results


@app.get("/api/series/{unique_id}/best-method")
async def get_series_best_method(unique_id: str):
    """
    Get best method for a single series.
    Falls back to on-the-fly computation if not in database.
    """
    # Try database cache first
    if data_cache['best_methods'] is not None:
        df = _compute(data_cache['best_methods'])
        row_df = df[df['unique_id'] == unique_id]
        if not row_df.empty:
            row = row_df.iloc[0]
            return {
                'unique_id': str(row['unique_id']),
                'best_method': str(row['best_method']),
                'best_score': float(row['best_score']) if 'best_score' in row.index and pd.notna(row.get('best_score')) else None,
                'runner_up_method': str(row['runner_up_method']) if 'runner_up_method' in row.index and pd.notna(row.get('runner_up_method')) else None
            }

    # Fallback: compute from origin data
    metrics_list = _compute_metrics_from_origin(unique_id)
    if not metrics_list:
        raise HTTPException(status_code=404, detail=f"No best method data for {unique_id}")

    config = _get_config()
    weights = config.get('best_method', {}).get('weights', {
        'mae': 0.40, 'rmse': 0.20, 'bias': 0.15, 'coverage_90': 0.15, 'mase': 0.10
    })
    ranking = _compute_composite_ranking(metrics_list, weights)
    if not ranking:
        raise HTTPException(status_code=404, detail=f"Could not compute best method for {unique_id}")

    sorted_methods = sorted(ranking, key=lambda k: ranking[k] if ranking[k] is not None else float('inf'))
    best = sorted_methods[0]
    runner_up = sorted_methods[1] if len(sorted_methods) > 1 else None
    return {
        'unique_id': unique_id,
        'best_method': best,
        'best_score': ranking.get(best),
        'runner_up_method': runner_up,
    }


@app.get("/api/series/{unique_id}/outliers")
async def get_series_outliers(unique_id: str):
    """
    Get outlier detection results for a specific series.

    Returns detected outliers with original vs corrected values.
    """
    if data_cache['outliers'] is None:
        return {
            'unique_id': unique_id,
            'n_outliers': 0,
            'detection_method': None,
            'correction_method': None,
            'outliers': []
        }

    df = _compute(data_cache['outliers'])
    series_outliers = df[df['unique_id'] == unique_id]

    if series_outliers.empty:
        return {
            'unique_id': unique_id,
            'n_outliers': 0,
            'detection_method': None,
            'correction_method': None,
            'outliers': []
        }

    outlier_list = []
    for _, row in series_outliers.iterrows():
        outlier_list.append({
            'date': pd.Timestamp(row['date']).strftime('%Y-%m-%d') if pd.notna(row['date']) else str(row['date']),
            'original_value': float(row['original_value']),
            'corrected_value': float(row['corrected_value']),
            'z_score': float(row['z_score']),
            'lower_bound': float(row['lower_bound']),
            'upper_bound': float(row['upper_bound']),
        })

    return {
        'unique_id': unique_id,
        'n_outliers': len(outlier_list),
        'detection_method': str(series_outliers.iloc[0]['detection_method']),
        'correction_method': str(series_outliers.iloc[0]['correction_method']),
        'outliers': outlier_list
    }


@app.get("/api/forecasts/{unique_id}/origins")
async def get_forecast_origins(unique_id: str):
    """
    List all forecast origins for a series.

    Returns {unique_id, origins: [list of date strings]}.
    """
    if data_cache['forecasts_by_origin'] is None:
        raise HTTPException(status_code=503, detail="Forecasts by origin data not loaded")

    df = _compute(data_cache['forecasts_by_origin'])
    series_df = df[df['unique_id'] == unique_id]

    if series_df.empty:
        raise HTTPException(status_code=404, detail=f"No forecast origins for {unique_id}")

    # Extract unique origin dates
    if 'forecast_origin' in series_df.columns:
        origin_col = 'forecast_origin'
    elif 'origin' in series_df.columns:
        origin_col = 'origin'
    elif 'origin_date' in series_df.columns:
        origin_col = 'origin_date'
    else:
        raise HTTPException(status_code=500, detail="Origin column not found in data")

    origins = series_df[origin_col].drop_duplicates().sort_values()

    # Convert to string dates
    if pd.api.types.is_datetime64_any_dtype(origins):
        origin_strings = origins.dt.strftime('%Y-%m-%d').tolist()
    else:
        origin_strings = [str(o) for o in origins.tolist()]

    return {
        'unique_id': unique_id,
        'origins': origin_strings
    }


@app.get("/api/forecasts/{unique_id}/origins/{origin_date}")
async def get_forecasts_at_origin(unique_id: str, origin_date: str):
    """
    Get forecasts at a specific origin date.

    Returns {unique_id, origin: date, forecasts: [{method, point_forecast: [values], actual: [values]}]}.
    """
    if data_cache['forecasts_by_origin'] is None:
        raise HTTPException(status_code=503, detail="Forecasts by origin data not loaded")

    df = _compute(data_cache['forecasts_by_origin'])

    # Determine origin column name
    if 'forecast_origin' in df.columns:
        origin_col = 'forecast_origin'
    elif 'origin' in df.columns:
        origin_col = 'origin'
    elif 'origin_date' in df.columns:
        origin_col = 'origin_date'
    else:
        raise HTTPException(status_code=500, detail="Origin column not found in data")

    # Filter by unique_id
    series_df = df[df['unique_id'] == unique_id]
    if series_df.empty:
        raise HTTPException(status_code=404, detail=f"No forecast origins for {unique_id}")

    # Filter by origin date - handle both datetime and string columns
    if pd.api.types.is_datetime64_any_dtype(series_df[origin_col]):
        try:
            origin_dt = pd.Timestamp(origin_date)
        except Exception:
            raise HTTPException(status_code=400, detail=f"Invalid date format: {origin_date}")
        origin_df = series_df[series_df[origin_col] == origin_dt]
    else:
        origin_df = series_df[series_df[origin_col].astype(str) == origin_date]

    if origin_df.empty:
        raise HTTPException(status_code=404, detail=f"No forecasts at origin {origin_date} for {unique_id}")

    # Group by method, aggregate horizon steps into arrays
    forecasts = []
    has_horizon = 'horizon_step' in origin_df.columns
    actual_col = 'actual_value' if 'actual_value' in origin_df.columns else 'actual'

    for method, group in origin_df.sort_values('horizon_step' if has_horizon else 'method').groupby('method'):
        if has_horizon:
            group = group.sort_values('horizon_step')

        pf_values = []
        act_values = []
        for _, row in group.iterrows():
            if 'point_forecast' in row.index:
                pf = row['point_forecast']
                pf_values.append(float(pf) if not isinstance(pf, (list, np.ndarray)) else float(pf[0]))
            if actual_col in row.index and pd.notna(row[actual_col]):
                act_values.append(float(row[actual_col]))

        forecasts.append({
            'method': str(method),
            'point_forecast': pf_values,
            'actual': act_values
        })

    return {
        'unique_id': unique_id,
        'origin': origin_date,
        'forecasts': forecasts
    }


@app.get("/api/series/{unique_id}/forecast-evolution")
async def get_forecast_evolution(unique_id: str):
    """
    Get all origins with forecast values for animation.

    Returns {unique_id, evolution: [{origin, forecasts: [{method, point_forecast, actual}]}]}.
    """
    if data_cache['forecasts_by_origin'] is None:
        raise HTTPException(status_code=503, detail="Forecasts by origin data not loaded")

    df = _compute(data_cache['forecasts_by_origin'])

    # Determine origin column name
    if 'forecast_origin' in df.columns:
        origin_col = 'forecast_origin'
    elif 'origin' in df.columns:
        origin_col = 'origin'
    elif 'origin_date' in df.columns:
        origin_col = 'origin_date'
    else:
        raise HTTPException(status_code=500, detail="Origin column not found in data")

    series_df = df[df['unique_id'] == unique_id]

    if series_df.empty:
        raise HTTPException(status_code=404, detail=f"No forecast evolution data for {unique_id}")

    # Group by origin and build evolution list
    evolution = []
    origins_sorted = series_df[origin_col].drop_duplicates().sort_values()

    for origin_val in origins_sorted:
        if pd.api.types.is_datetime64_any_dtype(series_df[origin_col]):
            origin_rows = series_df[series_df[origin_col] == origin_val]
            origin_str = origin_val.strftime('%Y-%m-%d')
        else:
            origin_rows = series_df[series_df[origin_col] == origin_val]
            origin_str = str(origin_val)

        has_horizon = 'horizon_step' in origin_rows.columns
        actual_col = 'actual_value' if 'actual_value' in origin_rows.columns else 'actual'

        forecasts = []
        for method, group in origin_rows.sort_values('horizon_step' if has_horizon else 'method').groupby('method'):
            if has_horizon:
                group = group.sort_values('horizon_step')
            pf_values = []
            act_values = []
            for _, row in group.iterrows():
                if 'point_forecast' in row.index:
                    pf = row['point_forecast']
                    pf_values.append(float(pf) if not isinstance(pf, (list, np.ndarray)) else float(pf[0]))
                if actual_col in row.index and pd.notna(row[actual_col]):
                    act_values.append(float(row[actual_col]))
            forecasts.append({
                'method': str(method),
                'point_forecast': pf_values,
                'actual': act_values
            })

        evolution.append({
            'origin': origin_str,
            'forecasts': forecasts
        })

    return {
        'unique_id': unique_id,
        'evolution': evolution
    }


@app.get("/api/series/{unique_id}/forecast-convergence")
async def get_forecast_convergence(unique_id: str, method: Optional[str] = None):
    """
    Forecast convergence view: how predictions for each target month evolved
    across origin dates.

    For every (origin, horizon_step) pair, we compute:
        target_date = origin + horizon_step months
    Then we group by target_date and return, for each target month, the
    list of (origin, forecast_value) pairs so the frontend can draw bars
    showing how the forecast converged as the target date approached.

    Optional query param ``method`` restricts to a single forecast method
    (defaults to all methods, returning the best/first available).

    Returns::

        {
          "unique_id": "63530_517",
          "methods": ["AutoARIMA", "AutoETS", ...],
          "targets": [
            {
              "target_date": "2025-06-01",
              "actual": 1234.0,
              "origins": [
                {"origin": "2025-01-01", "months_ahead": 5, "forecasts": {"AutoARIMA": 1100, "AutoETS": 1200}},
                {"origin": "2025-02-01", "months_ahead": 4, "forecasts": {"AutoARIMA": 1150, "AutoETS": 1210}},
                ...
              ]
            },
            ...
          ]
        }
    """
    if data_cache['forecasts_by_origin'] is None:
        raise HTTPException(status_code=503, detail="Forecasts by origin data not loaded")

    df = _compute(data_cache['forecasts_by_origin'])

    # Determine column names
    if 'forecast_origin' in df.columns:
        origin_col = 'forecast_origin'
    elif 'origin' in df.columns:
        origin_col = 'origin'
    elif 'origin_date' in df.columns:
        origin_col = 'origin_date'
    else:
        raise HTTPException(status_code=500, detail="Origin column not found in data")

    actual_col = 'actual_value' if 'actual_value' in df.columns else 'actual'
    has_horizon = 'horizon_step' in df.columns

    series_df = df[df['unique_id'] == unique_id].copy()
    if series_df.empty:
        raise HTTPException(status_code=404, detail=f"No forecast convergence data for {unique_id}")

    # Filter by method if requested
    if method:
        series_df = series_df[series_df['method'] == method]
        if series_df.empty:
            raise HTTPException(status_code=404, detail=f"No data for method {method}")

    # Ensure origin is datetime
    if not pd.api.types.is_datetime64_any_dtype(series_df[origin_col]):
        series_df[origin_col] = pd.to_datetime(series_df[origin_col])

    # Compute target_date = origin + horizon_step months
    if has_horizon:
        def _add_months(origin_ts, steps):
            try:
                return (origin_ts + pd.DateOffset(months=int(steps))).strftime('%Y-%m-%d')
            except Exception:
                return None
        series_df['target_date'] = series_df.apply(
            lambda r: _add_months(r[origin_col], r['horizon_step']), axis=1
        )
        series_df = series_df.dropna(subset=['target_date'])
    else:
        # Without horizon_step, we can't compute target dates
        raise HTTPException(status_code=500, detail="horizon_step column required for convergence view")

    if series_df.empty:
        raise HTTPException(status_code=404, detail=f"No convergence data after target_date computation for {unique_id}")

    # Get unique methods present
    methods_list = sorted(series_df['method'].unique().tolist())

    # Group by target_date (string, so groupby is reliable)
    targets = []
    for target_str, tgroup in series_df.groupby('target_date'):

        # Get actual value (should be same across all rows for same target)
        actual_val = None
        if actual_col in tgroup.columns:
            act_vals = tgroup[actual_col].dropna()
            if len(act_vals) > 0:
                actual_val = float(act_vals.iloc[0])

        # Group by origin within this target_date
        origin_entries = []
        for origin_val, ogroup in tgroup.groupby(origin_col):
            origin_str = origin_val.strftime('%Y-%m-%d') if hasattr(origin_val, 'strftime') else str(origin_val)

            # Calculate months ahead
            months_ahead = int(ogroup['horizon_step'].iloc[0]) if has_horizon else None

            # Collect forecast values per method
            method_forecasts = {}
            for _, row in ogroup.iterrows():
                m = str(row['method'])
                pf = row.get('point_forecast', None)
                if pf is not None:
                    if isinstance(pf, (list, np.ndarray)):
                        method_forecasts[m] = float(pf[0])
                    else:
                        method_forecasts[m] = float(pf)

            origin_entries.append({
                'origin': origin_str,
                'months_ahead': months_ahead,
                'forecasts': method_forecasts
            })

        # Sort origins chronologically
        origin_entries.sort(key=lambda e: e['origin'])

        targets.append({
            'target_date': target_str,
            'actual': actual_val,
            'origins': origin_entries
        })

    # Sort targets chronologically
    targets.sort(key=lambda t: t['target_date'])

    return {
        'unique_id': unique_id,
        'methods': methods_list,
        'targets': targets
    }


@app.get("/api/series/{unique_id}/vega-spec")
async def get_vega_spec(unique_id: str):
    """
    Get Vega-Lite specification for visualizing time series with forecasts.

    This returns a complete Vega spec ready to render in React.
    """
    # Get historical data
    if data_cache['time_series'] is None:
        raise HTTPException(status_code=503, detail="Data not loaded")

    df = _compute(data_cache['time_series'])
    series_df = df[df['unique_id'] == unique_id].sort_values('date')

    if series_df.empty:
        raise HTTPException(status_code=404, detail=f"Series {unique_id} not found")

    # Prepare historical data
    historical_data = []
    for _, row in series_df.iterrows():
        historical_data.append({
            'date': row['date'].strftime('%Y-%m-%d'),
            'value': float(row['y']),
            'type': 'actual'
        })

    # Get forecasts
    forecast_data = []
    if data_cache['forecasts'] is not None:
        forecasts_df = _compute(data_cache['forecasts'])
        series_forecasts = forecasts_df[forecasts_df['unique_id'] == unique_id]

        if not series_forecasts.empty:
            # Add forecast horizon dates
            last_date = series_df['date'].max()
            forecast_dates = pd.date_range(start=last_date, periods=len(series_forecasts.iloc[0]['point_forecast']) + 1, freq='ME')[1:]

            for _, forecast_row in series_forecasts.iterrows():
                method = forecast_row['method']
                point_forecast = forecast_row['point_forecast']

                for i, (date, value) in enumerate(zip(forecast_dates, point_forecast)):
                    forecast_data.append({
                        'date': date.strftime('%Y-%m-%d'),
                        'value': float(value),
                        'type': 'forecast',
                        'method': method
                    })

    # Vega-Lite specification
    vega_spec = {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "title": f"Time Series: {unique_id}",
        "width": 800,
        "height": 400,
        "data": {
            "values": historical_data + forecast_data
        },
        "mark": {
            "type": "line",
            "point": True
        },
        "encoding": {
            "x": {
                "field": "date",
                "type": "temporal",
                "title": "Date"
            },
            "y": {
                "field": "value",
                "type": "quantitative",
                "title": "Value"
            },
            "color": {
                "field": "type",
                "type": "nominal",
                "scale": {
                    "domain": ["actual", "forecast"],
                    "range": ["#1f77b4", "#ff7f0e"]
                }
            },
            "strokeDash": {
                "field": "type",
                "type": "nominal",
                "scale": {
                    "domain": ["actual", "forecast"],
                    "range": [[1, 0], [5, 5]]
                }
            }
        }
    }

    return vega_spec


# ======================================================================
# NEW ENDPOINTS: Sparklines, Method Explanation, Distributions/Ridge
# ======================================================================


@app.post("/api/sparklines")
async def get_sparklines(unique_ids: List[str] = Body(...)):
    """
    Get lightweight sparkline data for a batch of series.
    Returns last 12 historical values + first 12 forecast values (best method).
    """
    if data_cache['time_series'] is None:
        raise HTTPException(status_code=503, detail="Data not loaded")

    ts_df = _compute(data_cache['time_series'])
    forecasts_df = _compute(data_cache['forecasts']) if data_cache.get('forecasts') is not None else None
    best_df = _compute(data_cache['best_methods']) if data_cache.get('best_methods') is not None else None

    # Build best-method lookup
    best_method_map = {}
    if best_df is not None:
        for _, row in best_df.iterrows():
            best_method_map[row['unique_id']] = str(row['best_method'])

    def monthly_to_weekly(dates, values, n_months=12):
        """Interpolate monthly values to weekly resolution for smoother sparklines."""
        import numpy as np
        if len(dates) == 0 or len(values) == 0:
            return []
        # Take last n_months
        dates = dates[-n_months:]
        values = values[-n_months:]
        # Convert dates to ordinal for interpolation
        ordinals = np.array([d.toordinal() if hasattr(d, 'toordinal') else pd.Timestamp(d).toordinal() for d in dates], dtype=float)
        vals = np.array(values, dtype=float)
        # Build weekly grid from first to last date
        start_ord = int(ordinals[0])
        end_ord = int(ordinals[-1])
        weekly_ords = np.arange(start_ord, end_ord + 1, 7)
        if len(weekly_ords) == 0:
            return [float(v) for v in values]
        weekly_vals = np.interp(weekly_ords, ordinals, vals)
        return [float(v) for v in weekly_vals]

    def forecast_to_weekly(last_date, fc_values, n_months=12):
        """Spread monthly forecast values to weekly resolution."""
        import numpy as np
        if not fc_values:
            return []
        fc_values = fc_values[:n_months]
        last_ts = pd.Timestamp(last_date)
        # Build monthly dates for forecast
        fc_dates = []
        for i in range(len(fc_values)):
            fc_dates.append((last_ts + pd.DateOffset(months=i + 1)).toordinal())
        start_ord = last_ts.toordinal()
        end_ord = fc_dates[-1]
        weekly_ords = np.arange(start_ord, end_ord + 1, 7)[1:]  # skip first (belongs to historical)
        if len(weekly_ords) == 0:
            return [float(v) for v in fc_values]
        # Include last historical ordinal as anchor at fc_values[0] entry
        all_ords = np.array([start_ord] + fc_dates, dtype=float)
        all_vals = np.array([fc_values[0]] + fc_values, dtype=float)
        weekly_vals = np.interp(weekly_ords, all_ords, all_vals)
        return [float(v) for v in weekly_vals]

    result = {}
    for uid in unique_ids:
        # Historical: last 12 months → interpolated to weekly
        series_rows = ts_df[ts_df['unique_id'] == uid].sort_values('date')
        if series_rows.empty:
            continue
        hist_dates = series_rows['date'].tolist()
        hist_values = series_rows['y'].tolist()
        hist_weekly = monthly_to_weekly(hist_dates, hist_values, n_months=12)

        # Forecast: first 12 months from best method → interpolated to weekly
        fc_weekly = []
        if forecasts_df is not None:
            uid_forecasts = forecasts_df[forecasts_df['unique_id'] == uid]
            if not uid_forecasts.empty:
                best = best_method_map.get(uid)
                if best:
                    method_fc = uid_forecasts[uid_forecasts['method'] == best]
                    if method_fc.empty:
                        method_fc = uid_forecasts.iloc[[0]]
                else:
                    method_fc = uid_forecasts.iloc[[0]]

                pf = method_fc.iloc[0]['point_forecast']
                if isinstance(pf, (list, np.ndarray)):
                    last_date = hist_dates[-1] if hist_dates else None
                    if last_date is not None:
                        fc_weekly = forecast_to_weekly(last_date, [float(v) for v in pf[:12]])

        result[uid] = {
            'historical': hist_weekly,
            'forecast': fc_weekly,
        }

    return result


@app.get("/api/series/{unique_id}/method-explanation")
async def get_method_explanation(unique_id: str):
    """
    Explain why each candidate method was included or excluded for this series.
    """
    if data_cache['characteristics'] is None:
        raise HTTPException(status_code=503, detail="Characteristics not loaded")

    chars_df = _compute(data_cache['characteristics'])
    char_row = chars_df[chars_df['unique_id'] == unique_id]
    if char_row.empty:
        raise HTTPException(status_code=404, detail=f"No characteristics for {unique_id}")

    char = char_row.iloc[0]
    config = _get_config()
    method_sel = config.get('forecasting', {}).get('method_selection', {})
    sufficiency = config.get('characterization', {}).get('data_sufficiency', {})

    n_obs = int(char.get('n_observations', 0))
    is_intermittent = bool(char.get('is_intermittent', False))
    has_seasonality = bool(char.get('has_seasonality', False))
    complexity_level = str(char.get('complexity_level', 'low'))
    sufficient_ml = bool(char.get('sufficient_for_ml', False))
    sufficient_dl = bool(char.get('sufficient_for_deep_learning', False))
    sparse_obs_per_year = sufficiency.get('sparse_obs_per_year', 5)
    min_for_ml = sufficiency.get('min_for_ml', 100)
    min_for_dl = sufficiency.get('min_for_deep_learning', 200)

    # Sparse: fewer than sparse_obs_per_year observations per year on average
    try:
        date_start = pd.Timestamp(str(char.get('date_range_start', '')))
        date_end   = pd.Timestamp(str(char.get('date_range_end', '')))
        span_years = max((date_end - date_start).days / 365.25, 1 / 12)
        obs_per_year = n_obs / span_years
        is_sparse = obs_per_year < sparse_obs_per_year
    except Exception:
        is_sparse = n_obs < sparse_obs_per_year

    # Determine which selection category was used
    if is_sparse:
        category = 'sparse_data'
        category_reason = (
            f"Series has only {obs_per_year:.1f} observations/year "
            f"(threshold: < {sparse_obs_per_year} obs/year)"
        )
    elif is_intermittent:
        category = 'intermittent'
        category_reason = f"Series classified as intermittent (zero_ratio={char.get('zero_ratio', 0):.2f}, ADI={char.get('adi', 0):.2f})"
    elif complexity_level == 'high':
        category = 'complex'
        category_reason = f"Series classified as high complexity (score={char.get('complexity_score', 0):.2f})"
    elif has_seasonality:
        category = 'seasonal'
        category_reason = f"Series has detected seasonality (strength={char.get('seasonal_strength', 0):.2f})"
    else:
        category = 'standard'
        category_reason = "Standard non-seasonal, non-intermittent series"

    # Get candidate methods from the category
    candidate_methods = list(method_sel.get(category, []))

    # All possible methods in the system
    ml_methods = {'LightGBM', 'XGBoost'}
    dl_methods = {'NHITS', 'NBEATS', 'PatchTST', 'TFT', 'DeepAR'}
    all_stat = set(config.get('forecasting', {}).get('statsforecast_models', []))
    all_neural = set(config.get('forecasting', {}).get('neuralforecast_models', []))
    all_ml = set(config.get('forecasting', {}).get('ml_models', []))
    all_methods = all_stat | all_neural | all_ml | {'TimesFM'}

    # Methods that were actually forecasted
    forecasted_methods = set()
    if data_cache['forecasts'] is not None:
        fc_pdf = _compute(data_cache['forecasts'])
        uid_fc = fc_pdf[fc_pdf['unique_id'] == unique_id]
        forecasted_methods = set(uid_fc['method'].unique())

    included = []
    excluded = []

    for method in sorted(all_methods):
        in_candidate = method in candidate_methods

        if not in_candidate:
            # Method not in the selected category
            reason = f"Not in '{category}' method pool — {category_reason}"
            excluded.append({'method': method, 'reason': reason})
            continue

        # Check data sufficiency filters
        if method in dl_methods and not sufficient_dl:
            reason = f"Requires >= {min_for_dl} observations for deep learning, series has {n_obs}"
            excluded.append({'method': method, 'reason': reason})
            continue

        if method in ml_methods and not sufficient_ml:
            reason = f"Requires >= {min_for_ml} observations for ML, series has {n_obs}"
            excluded.append({'method': method, 'reason': reason})
            continue

        # Method passed all filters
        was_run = method in forecasted_methods
        if was_run:
            included.append({'method': method, 'reason': f"Selected from '{category}' pool — {category_reason}", 'status': 'forecasted'})
        else:
            included.append({'method': method, 'reason': f"Eligible from '{category}' pool but no forecast produced (model may have failed)", 'status': 'eligible_no_result'})

    # ---- ACF / PACF ----
    acf_values = []
    pacf_values = []
    acf_lags = []
    acf_ci_upper = []
    acf_ci_lower = []

    try:
        from statsmodels.tsa.stattools import acf as sm_acf, pacf as sm_pacf

        ts_ddf = data_cache.get('time_series')
        if ts_ddf is not None:
            ts_pdf = _compute(ts_ddf)
            uid_ts = ts_pdf[ts_pdf['unique_id'] == unique_id].sort_values('date')
            y_vals = uid_ts['y'].values.astype(float)

            if len(y_vals) >= 6:
                n_lags = min(24, len(y_vals) // 2 - 1)
                # ACF with confidence interval
                acf_out, confint = sm_acf(y_vals, nlags=n_lags, fft=True,
                                          alpha=0.05, missing='conservative')
                acf_values = [round(float(v), 4) for v in acf_out[1:]]   # skip lag-0
                acf_lags = list(range(1, len(acf_values) + 1))
                acf_ci_upper = [round(float(confint[i+1][1] - acf_out[i+1]), 4) for i in range(len(acf_values))]
                acf_ci_lower = [round(float(acf_out[i+1] - confint[i+1][0]), 4) for i in range(len(acf_values))]

                # PACF
                n_lags_pacf = min(n_lags, (len(y_vals) - 1) // 2)
                pacf_out = sm_pacf(y_vals, nlags=n_lags_pacf, method='ywm')
                pacf_values = [round(float(v), 4) for v in pacf_out[1:]]
    except Exception as exc:
        logger.debug(f"ACF/PACF computation failed for {unique_id}: {exc}")

    return {
        'unique_id': unique_id,
        'selection_category': category,
        'selection_reason': category_reason,
        'n_observations': n_obs,
        'included': included,
        'excluded': excluded,
        'acf': {
            'lags': acf_lags,
            'values': acf_values,
            'ci_upper': acf_ci_upper,
            'ci_lower': acf_ci_lower,
        },
        'pacf': {
            'lags': acf_lags[:len(pacf_values)],
            'values': pacf_values,
        },
        'characteristics': {
            # Identity / size
            'n_observations': n_obs,
            'date_range_start': str(char.get('date_range_start', '')),
            'date_range_end': str(char.get('date_range_end', '')),
            # Basic stats
            'mean': float(char.get('mean', 0) or 0),
            'std': float(char.get('std', 0) or 0),
            # Seasonality
            'has_seasonality': has_seasonality,
            'seasonal_strength': float(char.get('seasonal_strength', 0) or 0),
            'seasonal_periods': char.get('seasonal_periods', []),
            # Trend
            'has_trend': bool(char.get('has_trend', False)),
            'trend_direction': str(char.get('trend_direction', 'none')),
            'trend_strength': float(char.get('trend_strength', 0) or 0),
            # Intermittency
            'is_intermittent': is_intermittent,
            'zero_ratio': float(char.get('zero_ratio', 0) or 0),
            'adi': float(char.get('adi', 0) or 0),
            'cov': float(char.get('cov', 0) or 0),
            # Stationarity
            'is_stationary': bool(char.get('is_stationary', False)),
            'adf_pvalue': float(char.get('adf_pvalue', 1.0) if char.get('adf_pvalue') is not None and not (isinstance(char.get('adf_pvalue'), float) and np.isnan(char.get('adf_pvalue'))) else 1.0),
            # Complexity
            'complexity_level': complexity_level,
            'complexity_score': float(char.get('complexity_score', 0) or 0),
            # Data sufficiency
            'sufficient_for_ml': sufficient_ml,
            'sufficient_for_deep_learning': sufficient_dl,
            # Sparse check
            'obs_per_year': round(obs_per_year, 2),
            'sparse_obs_per_year_threshold': sparse_obs_per_year,
            'is_sparse': is_sparse,
        },
    }


@app.get("/api/series/{unique_id}/distributions")
async def get_series_distributions(unique_id: str):
    """
    Get distribution data for the ridge chart.
    Returns per-horizon density curves from fitted distributions or bootstrap fallback.
    """
    # Get forecasts for this series to extract quantiles
    if data_cache['forecasts'] is None:
        raise HTTPException(status_code=503, detail="Forecasts not loaded")

    fc_df = _compute(data_cache['forecasts'])
    uid_fc = fc_df[fc_df['unique_id'] == unique_id]
    if uid_fc.empty:
        raise HTTPException(status_code=404, detail=f"No forecasts for {unique_id}")

    # Use best method's forecast
    best_method_name = None
    if data_cache['best_methods'] is not None:
        best_df = _compute(data_cache['best_methods'])
        best_row = best_df[best_df['unique_id'] == unique_id]
        if not best_row.empty:
            best_method_name = str(best_row.iloc[0]['best_method'])

    if best_method_name:
        method_fc = uid_fc[uid_fc['method'] == best_method_name]
        if method_fc.empty:
            method_fc = uid_fc.iloc[[0]]
    else:
        method_fc = uid_fc.iloc[[0]]

    row = method_fc.iloc[0]
    method_name = row['method']

    # Parse quantiles
    quantiles = row['quantiles']
    if isinstance(quantiles, str):
        quantiles = json.loads(quantiles.replace("'", '"'))
    quantiles_dict = {}
    for q, vals in quantiles.items():
        if isinstance(vals, (list, np.ndarray)):
            quantiles_dict[float(q)] = [float(v) for v in vals]
        else:
            quantiles_dict[float(q)] = [float(vals)]

    point_forecast = [float(v) for v in row['point_forecast']]
    n_horizon = len(point_forecast)

    # Check if we have fitted distributions
    dist_ddf = data_cache.get('distributions')
    has_fitted = False
    fitted_rows = None
    if dist_ddf is not None:
        dist_df = _compute(dist_ddf)
        fitted_rows = dist_df[(dist_df['unique_id'] == unique_id) & (dist_df['method'] == method_name)]
        if not fitted_rows.empty:
            has_fitted = True

    horizons = []
    for h in range(n_horizon):
        # Gather quantile values for this horizon step
        h_quantiles = {}
        for q, vals in quantiles_dict.items():
            if h < len(vals):
                h_quantiles[q] = vals[h]

        if len(h_quantiles) < 2:
            continue

        q_vals = sorted(h_quantiles.values())
        x_min = q_vals[0]
        x_max = q_vals[-1]
        x_range = x_max - x_min
        if x_range <= 0:
            x_range = max(abs(x_min) * 0.1, 1.0)
        x_lo = x_min - x_range * 0.15
        x_hi = x_max + x_range * 0.15

        density_points = []
        is_bootstrap = True
        dist_type = 'bootstrap'

        # Try parametric distribution first
        if has_fitted and fitted_rows is not None:
            fit_row = fitted_rows.iloc[0]
            ks_pvalue = fit_row.get('ks_pvalue', None)
            fit_ok = ks_pvalue is None or (pd.notna(ks_pvalue) and ks_pvalue >= 0.05)

            if fit_ok:
                dist_type_name = str(fit_row['distribution_type'])
                params = fit_row['params']
                if isinstance(params, str):
                    params = json.loads(params.replace("'", '"'))

                try:
                    xs = np.linspace(x_lo, x_hi, 80)
                    if dist_type_name == 'normal':
                        ys = scipy_stats.norm.pdf(xs, loc=params.get('loc', fit_row['mean']), scale=params.get('scale', fit_row['std']))
                    elif dist_type_name == 'gamma':
                        ys = scipy_stats.gamma.pdf(xs, a=params['shape'], scale=params['scale'])
                    elif dist_type_name == 'lognormal':
                        ys = scipy_stats.lognorm.pdf(xs, s=params['s'], scale=params['scale'])
                    elif dist_type_name == 'negative_binomial':
                        xs_int = np.round(xs).astype(int)
                        xs_int = np.clip(xs_int, 0, None)
                        ys = scipy_stats.nbinom.pmf(xs_int, n=params['n'], p=params['p'])
                        xs = xs_int.astype(float)
                    else:
                        raise ValueError(f"Unknown dist: {dist_type_name}")

                    # Scale density by horizon std variation
                    h_mean = point_forecast[h]
                    h_std = fit_row['std']
                    if h > 0 and h_std > 0:
                        # Widen distribution for further horizons
                        growth = 1.0 + 0.03 * h
                        xs = h_mean + (xs - fit_row['mean']) * growth
                        ys = ys / growth

                    ys = np.nan_to_num(ys, 0.0)
                    if np.max(ys) > 0:
                        ys = ys / np.max(ys)  # Normalize to [0, 1]

                    density_points = [{'x': float(xs[i]), 'y': float(ys[i])} for i in range(len(xs))]
                    is_bootstrap = False
                    dist_type = dist_type_name
                except Exception:
                    pass  # Fall through to bootstrap

        # Bootstrap fallback from quantiles
        if not density_points and len(h_quantiles) >= 3:
            q_levels = sorted(h_quantiles.keys())
            q_values = [h_quantiles[q] for q in q_levels]

            # Generate bootstrap samples from quantile interpolation
            n_samples = 500
            uniform_samples = np.random.uniform(0, 1, n_samples)
            bootstrap_samples = np.interp(uniform_samples, q_levels, q_values)

            # KDE
            try:
                kde = scipy_stats.gaussian_kde(bootstrap_samples)
                xs = np.linspace(x_lo, x_hi, 80)
                ys = kde(xs)
                ys = np.nan_to_num(ys, 0.0)
                if np.max(ys) > 0:
                    ys = ys / np.max(ys)
                density_points = [{'x': float(xs[i]), 'y': float(ys[i])} for i in range(len(xs))]
            except Exception:
                # Last resort: uniform between quantile extremes
                xs = np.linspace(x_lo, x_hi, 80)
                ys = np.ones_like(xs) / len(xs)
                density_points = [{'x': float(xs[i]), 'y': float(ys[i])} for i in range(len(xs))]

        horizons.append({
            'horizon_month': h + 1,
            'mean': point_forecast[h],
            'distribution_type': dist_type,
            'is_bootstrap': is_bootstrap,
            'density_points': density_points,
        })

    return {
        'unique_id': unique_id,
        'method': method_name,
        'n_horizons': len(horizons),
        'horizons': horizons,
    }


# ---------------------------------------------------------------------------
# Pipeline Runner
# ---------------------------------------------------------------------------

# In-memory job store: job_id -> {status, step, log_lines, started_at, ended_at}
_pipeline_jobs: Dict[str, Dict] = {}
_pipeline_lock = threading.Lock()

_STALE_JOB_TIMEOUT_SEC = 7200  # 2 hours — mark "running" jobs as stale after this


def _cleanup_stale_jobs():
    """Mark any job that has been 'running' for longer than the timeout as 'error'.

    Also checks whether the process is actually alive (via PID).
    Must be called **inside** _pipeline_lock.
    """
    now = datetime.utcnow()
    for job in _pipeline_jobs.values():
        if job.get("status") != "running":
            continue

        # Check if the process is still alive
        pid = job.get("pid")
        process_alive = False
        if pid:
            try:
                if sys.platform == "win32":
                    result = subprocess.run(
                        ["tasklist", "/FI", f"PID eq {pid}", "/NH"],
                        capture_output=True, text=True, timeout=5
                    )
                    process_alive = str(pid) in result.stdout
                else:
                    os.kill(pid, 0)
                    process_alive = True
            except (ProcessLookupError, PermissionError, OSError):
                process_alive = False
            except Exception:
                process_alive = False

        if not process_alive:
            job["status"] = "error"
            job["ended_at"] = now.isoformat()
            job["log_lines"].append("[STALE] Process no longer running — marked as failed")
            job["exit_code"] = -2
            logger.warning(f"Stale job {job['job_id']} (pid={pid}) marked as error — process not alive")
            continue

        # Check timeout
        started = job.get("started_at")
        if started:
            try:
                started_dt = datetime.fromisoformat(started)
                elapsed = (now - started_dt).total_seconds()
                if elapsed > _STALE_JOB_TIMEOUT_SEC:
                    job["status"] = "error"
                    job["ended_at"] = now.isoformat()
                    job["log_lines"].append(
                        f"[TIMEOUT] Job exceeded {_STALE_JOB_TIMEOUT_SEC}s — marked as stale"
                    )
                    job["exit_code"] = -3
                    logger.warning(f"Stale job {job['job_id']} timed out after {elapsed:.0f}s")
            except Exception:
                pass


PIPELINE_STEPS = {
    "etl":               {"label": "ETL",               "arg": "etl",               "desc": "Extract data from the source database into demand_actuals"},
    "outlier-detection": {"label": "Outlier Detection",  "arg": "outlier-detection", "desc": "Detect and correct outliers in the time series"},
    "forecast":          {"label": "Forecast",           "arg": "forecast",          "desc": "Run all forecasting models (statistical, ML, neural, foundation)"},
    "backtest":          {"label": "Backtest",           "arg": "backtest",          "desc": "Rolling-window backtesting and metric computation"},
    "best-method":       {"label": "Best Method",        "arg": "best-method",       "desc": "Select the best method per series using composite scoring"},
    "distributions":     {"label": "Distributions",      "arg": "distributions",     "desc": "Fit forecast distributions for MEIO safety-stock computation"},
}


PIPELINE_STEP_ORDER = ["etl", "outlier-detection", "forecast", "backtest", "best-method", "distributions"]


def _run_pipeline_step_thread(job_id: str, step_arg: str, extra_args: list = None):
    """Run a pipeline step in a background thread, capturing output line by line."""
    files_dir = Path(__file__).parent.parent  # files/ directory
    cmd = [sys.executable, "run_pipeline.py", "--only", step_arg, "--log-level", "INFO"]
    if extra_args:
        cmd.extend(extra_args)

    with _pipeline_lock:
        _pipeline_jobs[job_id]["status"] = "running"
        _pipeline_jobs[job_id]["started_at"] = datetime.utcnow().isoformat()

    try:
        # On Unix, start a new process group so we can kill the whole tree
        popen_kwargs = dict(
            cwd=str(files_dir),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        if sys.platform != "win32":
            popen_kwargs["start_new_session"] = True  # new process group for killpg

        proc = subprocess.Popen(cmd, **popen_kwargs)
        with _pipeline_lock:
            _pipeline_jobs[job_id]["pid"] = proc.pid

        for line in proc.stdout:
            line = line.rstrip("\n")
            with _pipeline_lock:
                # If job was killed externally, stop reading stdout
                if _pipeline_jobs[job_id].get("status") == "error" and _pipeline_jobs[job_id].get("exit_code") == -1:
                    proc.kill()
                    break
                _pipeline_jobs[job_id]["log_lines"].append(line)

        proc.wait()
        exit_code = proc.returncode

        with _pipeline_lock:
            # Don't overwrite status if already marked as killed
            if _pipeline_jobs[job_id].get("exit_code") != -1:
                _pipeline_jobs[job_id]["exit_code"] = exit_code
                _pipeline_jobs[job_id]["status"] = "success" if exit_code == 0 else "error"
                _pipeline_jobs[job_id]["ended_at"] = datetime.utcnow().isoformat()

    except Exception as exc:
        with _pipeline_lock:
            _pipeline_jobs[job_id]["log_lines"].append(f"[ERROR] {exc}")
            _pipeline_jobs[job_id]["status"] = "error"
            _pipeline_jobs[job_id]["ended_at"] = datetime.utcnow().isoformat()


def _run_full_pipeline_thread(job_id: str):
    """
    Run all pipeline steps in order, sequentially.
    Stops immediately if any step exits with a non-zero code.
    All log lines are collected into a single job entry; the 'current_step'
    field is updated as execution advances.
    """
    files_dir = Path(__file__).parent.parent

    with _pipeline_lock:
        _pipeline_jobs[job_id]["status"] = "running"
        _pipeline_jobs[job_id]["started_at"] = datetime.utcnow().isoformat()

    for step_id in PIPELINE_STEP_ORDER:
        step_arg  = PIPELINE_STEPS[step_id]["arg"]
        step_label = PIPELINE_STEPS[step_id]["label"]

        with _pipeline_lock:
            _pipeline_jobs[job_id]["current_step"] = step_id
            _pipeline_jobs[job_id]["log_lines"].append(
                f"\n{'='*60}\n▶ Starting: {step_label}\n{'='*60}"
            )

        cmd = [sys.executable, "run_pipeline.py", "--only", step_arg, "--log-level", "INFO"]
        popen_kwargs = dict(
            cwd=str(files_dir),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        if sys.platform != "win32":
            popen_kwargs["start_new_session"] = True

        try:
            proc = subprocess.Popen(cmd, **popen_kwargs)
            with _pipeline_lock:
                _pipeline_jobs[job_id]["pid"] = proc.pid

            for line in proc.stdout:
                line = line.rstrip("\n")
                with _pipeline_lock:
                    # Abort if killed externally
                    if _pipeline_jobs[job_id].get("exit_code") == -1:
                        proc.kill()
                        return
                    _pipeline_jobs[job_id]["log_lines"].append(line)

            proc.wait()
            exit_code = proc.returncode

        except Exception as exc:
            with _pipeline_lock:
                _pipeline_jobs[job_id]["log_lines"].append(f"[ERROR] {exc}")
                _pipeline_jobs[job_id]["status"] = "error"
                _pipeline_jobs[job_id]["ended_at"] = datetime.utcnow().isoformat()
            return

        if exit_code != 0:
            with _pipeline_lock:
                _pipeline_jobs[job_id]["log_lines"].append(
                    f"\n✕ Step '{step_label}' failed with exit code {exit_code}. Pipeline aborted."
                )
                _pipeline_jobs[job_id]["exit_code"] = exit_code
                _pipeline_jobs[job_id]["status"] = "error"
                _pipeline_jobs[job_id]["ended_at"] = datetime.utcnow().isoformat()
            return

        with _pipeline_lock:
            _pipeline_jobs[job_id]["log_lines"].append(f"✓ {step_label} complete")

    # All steps succeeded
    with _pipeline_lock:
        _pipeline_jobs[job_id]["log_lines"].append(
            f"\n{'='*60}\n✓ Full pipeline complete!\n{'='*60}"
        )
        _pipeline_jobs[job_id]["exit_code"] = 0
        _pipeline_jobs[job_id]["status"] = "success"
        _pipeline_jobs[job_id]["ended_at"] = datetime.utcnow().isoformat()
        _pipeline_jobs[job_id]["current_step"] = None


@app.post("/api/pipeline/run-all")
async def run_full_pipeline():
    """Launch all pipeline steps in order as a single background job."""
    # Clean up stale jobs first, then reject if any job is genuinely running
    with _pipeline_lock:
        _cleanup_stale_jobs()
        for job in _pipeline_jobs.values():
            if job.get("status") == "running":
                raise HTTPException(
                    status_code=409,
                    detail=f"A pipeline job is already running (job {job['job_id']})"
                )

    job_id = str(uuid.uuid4())[:8]
    with _pipeline_lock:
        _pipeline_jobs[job_id] = {
            "job_id": job_id,
            "step": "full-pipeline",
            "step_label": "Full Pipeline",
            "current_step": None,
            "status": "pending",
            "log_lines": [],
            "started_at": None,
            "ended_at": None,
            "pid": None,
            "exit_code": None,
        }

    t = threading.Thread(target=_run_full_pipeline_thread, args=(job_id,), daemon=True)
    t.start()

    return {"job_id": job_id, "step": "full-pipeline", "status": "pending"}


@app.get("/api/pipeline/steps")
async def get_pipeline_steps():
    """Return the list of available pipeline steps."""
    return [{"id": k, **v} for k, v in PIPELINE_STEPS.items()]


@app.post("/api/pipeline/run/{step}")
async def run_pipeline_step(step: str):
    """Launch a pipeline step as a background subprocess. Returns a job_id."""
    if step not in PIPELINE_STEPS:
        raise HTTPException(status_code=400, detail=f"Unknown step '{step}'. Valid: {list(PIPELINE_STEPS)}")

    # Clean up stale jobs first, then reject if same step is genuinely running
    with _pipeline_lock:
        _cleanup_stale_jobs()
        for job in _pipeline_jobs.values():
            if job.get("step") == step and job.get("status") == "running":
                raise HTTPException(status_code=409, detail=f"Step '{step}' is already running (job {job['job_id']})")

    job_id = str(uuid.uuid4())[:8]
    with _pipeline_lock:
        _pipeline_jobs[job_id] = {
            "job_id": job_id,
            "step": step,
            "step_label": PIPELINE_STEPS[step]["label"],
            "status": "pending",
            "log_lines": [],
            "started_at": None,
            "ended_at": None,
            "pid": None,
            "exit_code": None,
        }

    t = threading.Thread(target=_run_pipeline_step_thread, args=(job_id, PIPELINE_STEPS[step]["arg"]), daemon=True)
    t.start()

    return {"job_id": job_id, "step": step, "status": "pending"}


@app.post("/api/pipeline/run-forecast")
async def run_forecast_for_series(req: RunForecastRequest):
    """Launch a forecast for specific series (optionally with all methods).

    Used by the "Run Forecast" button in the Time Series Viewer.
    Returns a job_id that can be polled via GET /api/pipeline/jobs/{job_id}.
    """
    if not req.series:
        raise HTTPException(status_code=400, detail="No series provided")

    # Clean up stale jobs first, then reject if a series-forecast is genuinely running
    with _pipeline_lock:
        _cleanup_stale_jobs()
        for job in _pipeline_jobs.values():
            if job.get("step") == "forecast-series" and job.get("status") == "running":
                raise HTTPException(
                    status_code=409,
                    detail=f"A series forecast job is already running (job {job['job_id']})"
                )

    job_id = str(uuid.uuid4())[:8]
    with _pipeline_lock:
        _pipeline_jobs[job_id] = {
            "job_id": job_id,
            "step": "forecast-series",
            "step_label": f"Forecast ({len(req.series)} series)",
            "status": "pending",
            "log_lines": [],
            "started_at": None,
            "ended_at": None,
            "pid": None,
            "exit_code": None,
        }

    extra_args = ["--series", ",".join(req.series)]
    if req.all_methods:
        extra_args.append("--all-methods")

    # Load any hyperparameter overrides from DB for the requested series
    if _DB_AVAILABLE:
        try:
            conn = get_conn(_api_config_path())
            schema = get_schema(_api_config_path())
            placeholders = ",".join(["%s"] * len(req.series))
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT unique_id, method, overrides FROM {schema}.hyperparameter_overrides "
                    f"WHERE unique_id IN ({placeholders})",
                    tuple(req.series),
                )
                rows = cur.fetchall()
            conn.close()
            if rows:
                overrides_map = {}
                for uid, method, ovr in rows:
                    if isinstance(ovr, str):
                        ovr = json.loads(ovr)
                    overrides_map.setdefault(uid, {})[method] = ovr
                extra_args.extend(["--overrides-json", json.dumps(overrides_map)])
        except Exception as exc:
            logger.warning(f"Could not load hyperparameter overrides: {exc}")

    t = threading.Thread(
        target=_run_pipeline_step_thread,
        args=(job_id, "forecast", extra_args),
        daemon=True,
    )
    t.start()

    return {"job_id": job_id, "step": "forecast-series", "status": "pending"}


# ══════════════════════════════════════════════════════════════════════
# Hyperparameter overrides CRUD
# ══════════════════════════════════════════════════════════════════════

class HyperparamOverridesRequest(BaseModel):
    overrides: Dict[str, Dict[str, Any]]  # {method: {param: value, ...}, ...}


@app.get("/api/hyperparams/{unique_id}")
async def get_hyperparams(unique_id: str):
    """Return all saved hyperparameter overrides for a series."""
    if not _DB_AVAILABLE:
        raise HTTPException(status_code=503, detail="Database not available")
    try:
        conn = get_conn(_api_config_path())
        schema = get_schema(_api_config_path())
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT method, overrides FROM {schema}.hyperparameter_overrides WHERE unique_id = %s",
                (unique_id,),
            )
            rows = cur.fetchall()
        conn.close()
        result = {}
        for method, overrides in rows:
            if isinstance(overrides, str):
                overrides = json.loads(overrides)
            result[method] = overrides
        return {"unique_id": unique_id, "overrides": result}
    except Exception as exc:
        logger.error(f"Failed to load hyperparameter overrides: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))


@app.put("/api/hyperparams/{unique_id}")
async def put_hyperparams(unique_id: str, req: HyperparamOverridesRequest):
    """Upsert hyperparameter overrides for one or more methods."""
    if not _DB_AVAILABLE:
        raise HTTPException(status_code=503, detail="Database not available")
    try:
        conn = get_conn(_api_config_path())
        schema = get_schema(_api_config_path())
        with conn.cursor() as cur:
            for method, overrides in req.overrides.items():
                cur.execute(
                    f"""INSERT INTO {schema}.hyperparameter_overrides (unique_id, method, overrides, updated_at)
                        VALUES (%s, %s, %s, NOW())
                        ON CONFLICT (unique_id, method)
                        DO UPDATE SET overrides = EXCLUDED.overrides, updated_at = NOW()""",
                    (unique_id, method, json.dumps(overrides)),
                )
        conn.commit()
        conn.close()
        return {"status": "ok", "unique_id": unique_id, "methods_updated": list(req.overrides.keys())}
    except Exception as exc:
        logger.error(f"Failed to save hyperparameter overrides: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))


@app.delete("/api/hyperparams/{unique_id}")
async def delete_hyperparams(unique_id: str, method: Optional[str] = None):
    """Delete hyperparameter overrides. If ?method=X is provided, delete only that method."""
    if not _DB_AVAILABLE:
        raise HTTPException(status_code=503, detail="Database not available")
    try:
        conn = get_conn(_api_config_path())
        schema = get_schema(_api_config_path())
        with conn.cursor() as cur:
            if method:
                cur.execute(
                    f"DELETE FROM {schema}.hyperparameter_overrides WHERE unique_id = %s AND method = %s",
                    (unique_id, method),
                )
            else:
                cur.execute(
                    f"DELETE FROM {schema}.hyperparameter_overrides WHERE unique_id = %s",
                    (unique_id,),
                )
            deleted = cur.rowcount
        conn.commit()
        conn.close()
        return {"status": "ok", "unique_id": unique_id, "deleted": deleted}
    except Exception as exc:
        logger.error(f"Failed to delete hyperparameter overrides: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/pipeline/jobs")
async def get_pipeline_jobs():
    """Return all recent pipeline jobs (latest first)."""
    with _pipeline_lock:
        jobs = list(_pipeline_jobs.values())
    return sorted(jobs, key=lambda j: j.get("started_at") or "", reverse=True)


@app.get("/api/pipeline/jobs/{job_id}")
async def get_pipeline_job(job_id: str):
    """Return status + full log for a specific job."""
    with _pipeline_lock:
        job = _pipeline_jobs.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    return job


@app.post("/api/pipeline/jobs/{job_id}/kill")
async def kill_pipeline_job(job_id: str):
    """Interrupt (kill) a running pipeline job."""
    with _pipeline_lock:
        job = _pipeline_jobs.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    if job.get("status") != "running":
        raise HTTPException(status_code=409, detail=f"Job {job_id} is not running (status: {job.get('status')})")

    pid = job.get("pid")
    if not pid:
        raise HTTPException(status_code=409, detail="Job has no PID yet — it may still be starting")

    try:
        if sys.platform == "win32":
            # On Windows use taskkill to kill the process tree
            subprocess.call(["taskkill", "/F", "/T", "/PID", str(pid)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        else:
            os.killpg(os.getpgid(pid), signal.SIGTERM)
    except Exception as exc:
        # Process may have already ended
        pass

    with _pipeline_lock:
        _pipeline_jobs[job_id]["status"] = "error"
        _pipeline_jobs[job_id]["ended_at"] = datetime.utcnow().isoformat()
        _pipeline_jobs[job_id]["log_lines"].append("[INTERRUPTED] Process killed by user")
        _pipeline_jobs[job_id]["exit_code"] = -1

    return {"job_id": job_id, "status": "error", "message": "Job interrupted"}


@app.post("/api/pipeline/jobs/reset")
async def reset_pipeline_jobs():
    """Force-clear all stale/completed jobs from the in-memory store.

    Running jobs whose process is still alive are left untouched.
    Dead-process "running" jobs are marked as error.
    Completed/errored jobs older than 10 minutes are removed.
    """
    with _pipeline_lock:
        _cleanup_stale_jobs()
        now = datetime.utcnow()
        to_remove = []
        for job_id, job in _pipeline_jobs.items():
            if job.get("status") in ("success", "error"):
                ended = job.get("ended_at")
                if ended:
                    try:
                        ended_dt = datetime.fromisoformat(ended)
                        if (now - ended_dt).total_seconds() > 600:  # 10 min
                            to_remove.append(job_id)
                    except Exception:
                        to_remove.append(job_id)
                else:
                    to_remove.append(job_id)
            elif job.get("status") == "pending":
                to_remove.append(job_id)
        for jid in to_remove:
            del _pipeline_jobs[jid]
        remaining = {jid: j.get("status") for jid, j in _pipeline_jobs.items()}
    return {"status": "ok", "removed": len(to_remove), "remaining": remaining}


@app.get("/api/pipeline/jobs/{job_id}/stream")
async def stream_pipeline_logs(job_id: str):
    """
    Server-Sent Events stream of log lines for a running/completed job.
    The client receives lines as they are produced and a final 'done' event.
    """
    with _pipeline_lock:
        if job_id not in _pipeline_jobs:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

    async def event_generator():
        sent = 0
        while True:
            with _pipeline_lock:
                job = _pipeline_jobs.get(job_id, {})
                lines = job.get("log_lines", [])
                status = job.get("status", "pending")

            # Send any new lines
            while sent < len(lines):
                line = lines[sent]
                yield f"data: {json.dumps({'line': line})}\n\n"
                sent += 1

            if status in ("success", "error") and sent >= len(lines):
                yield f"event: done\ndata: {json.dumps({'status': status, 'exit_code': job.get('exit_code')})}\n\n"
                break

            await asyncio.sleep(0.3)

    return StreamingResponse(event_generator(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


# ===========================================================================
# PROCESS LOG endpoints
# ===========================================================================

def _require_db():
    if not _DB_AVAILABLE:
        raise HTTPException(status_code=503, detail="Database not available")


@app.get("/api/process-log")
async def get_process_log(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    step: Optional[str] = None,
    status: Optional[str] = None,
    run_id: Optional[str] = None,
):
    """
    Return process log entries, newest first.
    Optional filters: step_name, status, run_id.
    """
    _require_db()
    conn = get_conn(_api_config_path())
    try:
        where_parts = []
        params: list = []
        if step:
            where_parts.append("step_name = %s")
            params.append(step)
        if status:
            where_parts.append("status = %s")
            params.append(status)
        if run_id:
            where_parts.append("run_id = %s")
            params.append(run_id)

        where_sql = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

        query = f"""
            SELECT id, run_id, step_name, status,
                   started_at, ended_at, duration_s,
                   rows_processed, error_message
            FROM zcube.process_log
            {where_sql}
            ORDER BY started_at DESC
            LIMIT %s OFFSET %s
        """
        params += [limit, offset]

        with conn.cursor() as cur:
            cur.execute(query, params)
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()

        result = []
        for row in rows:
            entry = dict(zip(cols, row))
            # Serialise timestamps
            for k in ("started_at", "ended_at"):
                if entry.get(k) is not None:
                    entry[k] = entry[k].isoformat()
            # Coerce numeric
            if entry.get("duration_s") is not None:
                entry["duration_s"] = float(entry["duration_s"])
            result.append(entry)

        # Also get total count
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM zcube.process_log {where_sql}",
                        params[:-2] if params else [])
            total = cur.fetchone()[0]

        return {"total": total, "offset": offset, "limit": limit, "items": result}
    finally:
        conn.close()


@app.get("/api/process-log/runs")
async def get_process_log_runs():
    """
    Return one summary row per run_id: step count, total duration, status.
    """
    _require_db()
    conn = get_conn(_api_config_path())
    try:
        query = """
            SELECT
                run_id,
                MIN(started_at)  AS run_started_at,
                MAX(COALESCE(ended_at, NOW())) AS run_ended_at,
                SUM(COALESCE(duration_s, 0)) AS total_duration_s,
                COUNT(*)         AS step_count,
                SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END)   AS error_count,
                SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS success_count,
                SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) AS running_count
            FROM zcube.process_log
            GROUP BY run_id
            ORDER BY MIN(started_at) DESC
            LIMIT 100
        """
        with conn.cursor() as cur:
            cur.execute(query)
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()

        result = []
        for row in rows:
            entry = dict(zip(cols, row))
            for k in ("run_started_at", "run_ended_at"):
                if entry.get(k) is not None:
                    entry[k] = entry[k].isoformat()
            for k in ("total_duration_s",):
                if entry.get(k) is not None:
                    entry[k] = float(entry[k])
            # Overall status
            if entry["running_count"] > 0:
                entry["overall_status"] = "running"
            elif entry["error_count"] > 0:
                entry["overall_status"] = "error"
            else:
                entry["overall_status"] = "success"
            result.append(entry)
        return result
    finally:
        conn.close()


@app.get("/api/process-log/{run_id}/steps")
async def get_run_steps(run_id: str):
    """Return all steps for a specific run_id."""
    _require_db()
    conn = get_conn(_api_config_path())
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, run_id, step_name, status,
                       started_at, ended_at, duration_s,
                       rows_processed, error_message,
                       (log_tail IS NOT NULL AND log_tail <> '') AS has_log_tail
                FROM zcube.process_log
                WHERE run_id = %s
                ORDER BY started_at
                """,
                (run_id,),
            )
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()

        result = []
        for row in rows:
            entry = dict(zip(cols, row))
            for k in ("started_at", "ended_at"):
                if entry.get(k) is not None:
                    entry[k] = entry[k].isoformat()
            if entry.get("duration_s") is not None:
                entry["duration_s"] = float(entry["duration_s"])
            result.append(entry)
        return result
    finally:
        conn.close()


@app.get("/api/process-log/step/{step_id}/tail")
async def get_step_log_tail(step_id: int):
    """Return the captured log_tail for a specific step row (polling endpoint)."""
    _require_db()
    conn = get_conn(_api_config_path())
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, step_name, status, log_tail FROM zcube.process_log WHERE id = %s",
                (step_id,),
            )
            row = cur.fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail=f"Step {step_id} not found")
        return {
            "id": row[0],
            "step_name": row[1],
            "status": row[2],
            "log_tail": row[3] or "",
        }
    finally:
        conn.close()


# ===========================================================================
# ADJUSTMENTS endpoints
# ===========================================================================

class AdjustmentIn(BaseModel):
    forecast_date: str          # "YYYY-MM-DD"
    adjustment_type: str        # "adjustment" | "override"
    value: float
    note: Optional[str] = None
    created_by: Optional[str] = "planner"


@app.get("/api/adjustments/{unique_id}")
async def get_adjustments(unique_id: str):
    """Return all adjustments for a series."""
    _require_db()
    conn = get_conn(_api_config_path())
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, unique_id, forecast_date, adjustment_type,
                       value, note, created_by, created_at, updated_at
                FROM zcube.forecast_adjustments
                WHERE unique_id = %s
                ORDER BY forecast_date, adjustment_type
                """,
                (unique_id,),
            )
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()

        result = []
        for row in rows:
            entry = dict(zip(cols, row))
            for k in ("forecast_date", "created_at", "updated_at"):
                if entry.get(k) is not None:
                    entry[k] = entry[k].isoformat() if hasattr(entry[k], 'isoformat') else str(entry[k])
            entry["value"] = float(entry["value"])
            result.append(entry)
        return result
    finally:
        conn.close()


@app.post("/api/adjustments/{unique_id}", status_code=200)
async def upsert_adjustment(unique_id: str, body: AdjustmentIn):
    """
    Create or update an adjustment / override for a series + date.
    Uses INSERT ... ON CONFLICT DO UPDATE (upsert).
    """
    _require_db()
    if body.adjustment_type not in ("adjustment", "override"):
        raise HTTPException(status_code=400, detail="adjustment_type must be 'adjustment' or 'override'")

    conn = get_conn(_api_config_path())
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO zcube.forecast_adjustments
                    (unique_id, forecast_date, adjustment_type, value, note, created_by, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (unique_id, forecast_date, adjustment_type)
                DO UPDATE SET
                    value      = EXCLUDED.value,
                    note       = EXCLUDED.note,
                    created_by = EXCLUDED.created_by,
                    updated_at = NOW()
                RETURNING id, forecast_date, adjustment_type, value, note, updated_at
                """,
                (
                    unique_id,
                    body.forecast_date,
                    body.adjustment_type,
                    body.value,
                    body.note,
                    body.created_by or "planner",
                ),
            )
            row = cur.fetchone()
        conn.commit()
        return {
            "id": row[0],
            "unique_id": unique_id,
            "forecast_date": row[1].isoformat() if hasattr(row[1], 'isoformat') else str(row[1]),
            "adjustment_type": row[2],
            "value": float(row[3]),
            "note": row[4],
            "updated_at": row[5].isoformat() if hasattr(row[5], 'isoformat') else str(row[5]),
        }
    except Exception as exc:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        conn.close()


@app.delete("/api/adjustments/{unique_id}/{forecast_date}/{adjustment_type}", status_code=204)
async def delete_adjustment(unique_id: str, forecast_date: str, adjustment_type: str):
    """Delete a single adjustment row."""
    _require_db()
    conn = get_conn(_api_config_path())
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM zcube.forecast_adjustments
                WHERE unique_id = %s
                  AND forecast_date = %s
                  AND adjustment_type = %s
                """,
                (unique_id, forecast_date, adjustment_type),
            )
            deleted = cur.rowcount
        conn.commit()
        if deleted == 0:
            raise HTTPException(status_code=404, detail="Adjustment not found")
    except HTTPException:
        raise
    except Exception as exc:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        conn.close()


@app.delete("/api/adjustments/{unique_id}", status_code=204)
async def delete_all_adjustments(unique_id: str):
    """Delete ALL adjustments for a series (reset)."""
    _require_db()
    conn = get_conn(_api_config_path())
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM zcube.forecast_adjustments WHERE unique_id = %s",
                (unique_id,),
            )
        conn.commit()
    except Exception as exc:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        conn.close()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

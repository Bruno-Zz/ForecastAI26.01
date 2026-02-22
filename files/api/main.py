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
from datetime import datetime

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


# Helper functions
def load_data():
    """Load all data files into cache."""
    data_dir = Path('./output')

    try:
        # Load time series: prefer corrected version, keep original for comparison
        if data_cache['time_series'] is None:
            ts_corrected_path = Path('./data/time_series_corrected.parquet')
            ts_path = Path('./data/time_series.parquet')
            if ts_corrected_path.exists():
                data_cache['time_series'] = pd.read_parquet(ts_corrected_path)
                logger.info(f"Loaded corrected time series: {len(data_cache['time_series'])} rows")
            elif ts_path.exists():
                data_cache['time_series'] = pd.read_parquet(ts_path)
                logger.info(f"Loaded time series: {len(data_cache['time_series'])} rows")

        if data_cache['time_series_original'] is None:
            ts_path = Path('./data/time_series.parquet')
            if ts_path.exists():
                data_cache['time_series_original'] = pd.read_parquet(ts_path)
                logger.info(f"Loaded original time series: {len(data_cache['time_series_original'])} rows")

        if data_cache['characteristics'] is None:
            char_path = data_dir / 'time_series_characteristics.parquet'
            if char_path.exists():
                data_cache['characteristics'] = pd.read_parquet(char_path)
                logger.info(f"Loaded characteristics: {len(data_cache['characteristics'])} series")

        if data_cache['forecasts'] is None:
            forecast_path = data_dir / 'forecasts_all_methods.parquet'
            if forecast_path.exists():
                data_cache['forecasts'] = pd.read_parquet(forecast_path)
                logger.info(f"Loaded forecasts: {len(data_cache['forecasts'])} forecasts")

        if data_cache['distributions'] is None:
            dist_path = data_dir / 'fitted_distributions.parquet'
            if dist_path.exists():
                data_cache['distributions'] = pd.read_parquet(dist_path)
                logger.info(f"Loaded distributions: {len(data_cache['distributions'])} fits")

        if data_cache['metrics'] is None:
            metrics_path = data_dir / 'backtest_metrics.parquet'
            if metrics_path.exists():
                data_cache['metrics'] = pd.read_parquet(metrics_path)
                logger.info(f"Loaded metrics: {len(data_cache['metrics'])} evaluations")

        if data_cache['best_methods'] is None:
            best_path = data_dir / 'best_method_per_series.parquet'
            if best_path.exists():
                data_cache['best_methods'] = pd.read_parquet(best_path)
                logger.info(f"Loaded best methods: {len(data_cache['best_methods'])} series")

        if data_cache['forecasts_by_origin'] is None:
            origin_path = data_dir / 'forecasts_by_origin.parquet'
            if origin_path.exists():
                data_cache['forecasts_by_origin'] = pd.read_parquet(origin_path)
                # Ensure forecast_origin is datetime
                for col in ['forecast_origin', 'origin', 'origin_date']:
                    if col in data_cache['forecasts_by_origin'].columns:
                        data_cache['forecasts_by_origin'][col] = pd.to_datetime(data_cache['forecasts_by_origin'][col])
                logger.info(f"Loaded forecasts by origin: {len(data_cache['forecasts_by_origin'])} rows")

        if data_cache['outliers'] is None:
            outlier_path = data_dir / 'detected_outliers.parquet'
            if outlier_path.exists():
                data_cache['outliers'] = pd.read_parquet(outlier_path)
                logger.info(f"Loaded outliers: {len(data_cache['outliers'])} detections")

        if data_cache['config'] is None:
            config_path = Path('./config/config.yaml')
            if config_path.exists():
                with open(config_path, 'r') as fh:
                    data_cache['config'] = yaml.safe_load(fh)
                logger.info("Loaded config.yaml")

    except Exception as e:
        logger.error(f"Error loading data: {e}")


def _get_config():
    """Get config from cache with defaults."""
    return data_cache.get('config') or {}


@app.on_event("startup")
async def startup_event():
    """Load data on startup."""
    logger.info("Starting API server...")
    load_data()


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

    df = data_cache['characteristics'].copy()

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
        outlier_counts = data_cache['outliers'].groupby('unique_id').size().to_dict()

    # Build best-method lookup: backtested results take priority over recommendations
    backtested_map = {}
    if data_cache['best_methods'] is not None:
        for _, bm_row in data_cache['best_methods'].iterrows():
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

    df = data_cache['time_series']
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
        orig_df = data_cache['time_series_original']
        orig_series = orig_df[orig_df['unique_id'] == unique_id].sort_values('date')
        if not orig_series.empty:
            orig_data = {
                'date': orig_series['date'].dt.strftime('%Y-%m-%d').tolist(),
                'value': orig_series['y'].tolist()
            }
            result['original_data'] = orig_data

            # Check if there are actual differences
            if data_cache.get('outliers') is not None:
                uid_outliers = data_cache['outliers'][data_cache['outliers']['unique_id'] == unique_id]
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

    df = data_cache['forecasts']
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

        forecasts.append({
            'method': row['method'],
            'point_forecast': [float(v) for v in row['point_forecast']],
            'quantiles': quantiles_dict
        })

    return {
        'unique_id': unique_id,
        'forecasts': forecasts
    }


def _compute_metrics_from_origin(unique_id: str) -> list:
    """
    Compute backtest metrics on-the-fly.

    Strategy A: use forecasts_by_origin + actual_value column (covers 25 series).
    Strategy B (fallback for all other series): pseudo-holdout using the last
    min(horizon, n_obs//3) actual observations vs the forecast that was produced.
    This gives a meaningful accuracy estimate even for short series.
    """
    ts_df = data_cache.get('time_series')
    if ts_df is None:
        return []

    actuals_df = ts_df[ts_df['unique_id'] == unique_id].sort_values('date')
    if actuals_df.empty:
        return []

    actuals_vals = actuals_df['y'].values
    n_obs = len(actuals_vals)

    # ── Strategy A: forecasts_by_origin ──────────────────────────────────────
    if data_cache.get('forecasts_by_origin') is not None:
        origin_df = data_cache['forecasts_by_origin']
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
    fc_df = data_cache.get('forecasts')
    if fc_df is None:
        return []

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
    Falls back to on-the-fly computation from forecasts_by_origin if backtest parquet has no data."""

    metric_cols = ['mae', 'rmse', 'bias', 'mape', 'smape', 'mase',
                   'crps', 'winkler_score',
                   'coverage_50', 'coverage_80', 'coverage_90', 'coverage_95',
                   'quantile_loss']

    metrics_by_method = []
    source = 'parquet'

    # Try parquet first
    if data_cache['metrics'] is not None:
        df = data_cache['metrics']
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

    # Include composite ranking from best_methods parquet if available
    ranking = None
    if data_cache['best_methods'] is not None:
        best_df = data_cache['best_methods']
        row_df = best_df[best_df['unique_id'] == unique_id]
        if not row_df.empty:
            row = row_df.iloc[0]
            all_rankings = row.get('all_rankings', {})
            if isinstance(all_rankings, str):
                all_rankings = json.loads(all_rankings.replace("'", '"'))
            ranking = {str(k): float(v) if pd.notna(v) else None for k, v in all_rankings.items()}

    # If ranking not in parquet, compute it on-the-fly
    if ranking is None and metrics_by_method:
        ranking = _compute_composite_ranking(metrics_by_method, weights)

    # Inject composite ranking into each method's entry for convenience
    if ranking:
        for entry in metrics_by_method:
            entry['composite_score'] = ranking.get(entry['method'])

    # Derive best method from ranking if not in parquet
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

    chars_df = data_cache['characteristics']

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
        forecasts_df = data_cache['forecasts']
        method_counts = forecasts_df['method'].value_counts().to_dict()
        analytics['methods_summary'] = method_counts

    # Distribution types summary
    if data_cache['distributions'] is not None:
        dist_df = data_cache['distributions']
        dist_types = dist_df['distribution_type'].value_counts().to_dict()
        analytics['distribution_types'] = dist_types

    # Best method distribution
    if data_cache['best_methods'] is not None:
        best_df = data_cache['best_methods']
        best_method_counts = best_df['best_method'].value_counts().to_dict()
        analytics['best_method_distribution'] = best_method_counts
        analytics['best_method_total_series'] = len(best_df)

    # Outlier summary
    if data_cache['outliers'] is not None:
        outlier_df = data_cache['outliers']
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

    df = data_cache['best_methods']

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
    Falls back to on-the-fly computation if not in parquet.
    """
    # Try parquet first
    if data_cache['best_methods'] is not None:
        df = data_cache['best_methods']
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

    df = data_cache['outliers']
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

    df = data_cache['forecasts_by_origin']
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

    df = data_cache['forecasts_by_origin']

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

    df = data_cache['forecasts_by_origin']

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


@app.get("/api/series/{unique_id}/vega-spec")
async def get_vega_spec(unique_id: str):
    """
    Get Vega-Lite specification for visualizing time series with forecasts.

    This returns a complete Vega spec ready to render in React.
    """
    # Get historical data
    if data_cache['time_series'] is None:
        raise HTTPException(status_code=503, detail="Data not loaded")

    df = data_cache['time_series']
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
        forecasts_df = data_cache['forecasts']
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

    ts_df = data_cache['time_series']
    forecasts_df = data_cache.get('forecasts')
    best_df = data_cache.get('best_methods')

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

    chars_df = data_cache['characteristics']
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
    min_for_seasonal = sufficiency.get('min_for_seasonal', 24)
    min_for_ml = sufficiency.get('min_for_ml', 100)
    min_for_dl = sufficiency.get('min_for_deep_learning', 200)

    # Determine which selection category was used
    if n_obs < min_for_seasonal:
        category = 'sparse_data'
        category_reason = f"Series has only {n_obs} observations (< {min_for_seasonal} required for seasonal models)"
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
        uid_fc = data_cache['forecasts'][data_cache['forecasts']['unique_id'] == unique_id]
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

        ts_df = data_cache.get('time_series')
        if ts_df is not None:
            uid_ts = ts_df[ts_df['unique_id'] == unique_id].sort_values('date')
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
            'n_observations': n_obs,
            'is_intermittent': is_intermittent,
            'has_seasonality': has_seasonality,
            'complexity_level': complexity_level,
            'seasonal_strength': float(char.get('seasonal_strength', 0)),
            'seasonal_periods': char.get('seasonal_periods', []),
            'has_trend': bool(char.get('has_trend', False)),
            'trend_direction': str(char.get('trend_direction', 'none')),
            'zero_ratio': float(char.get('zero_ratio', 0)),
            'adi': float(char.get('adi', 0)),
            'date_range_start': str(char.get('date_range_start', '')),
            'date_range_end': str(char.get('date_range_end', '')),
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

    fc_df = data_cache['forecasts']
    uid_fc = fc_df[fc_df['unique_id'] == unique_id]
    if uid_fc.empty:
        raise HTTPException(status_code=404, detail=f"No forecasts for {unique_id}")

    # Use best method's forecast
    best_method_name = None
    if data_cache['best_methods'] is not None:
        best_df = data_cache['best_methods']
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
    dist_df = data_cache.get('distributions')
    has_fitted = False
    fitted_rows = None
    if dist_df is not None:
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

PIPELINE_STEPS = {
    "etl":               {"label": "ETL",               "arg": "etl",               "desc": "Extract data from the database and save to Parquet"},
    "outlier-detection": {"label": "Outlier Detection",  "arg": "outlier-detection", "desc": "Detect and correct outliers in the time series"},
    "forecast":          {"label": "Forecast",           "arg": "forecast",          "desc": "Run all forecasting models (statistical, ML, neural, foundation)"},
    "backtest":          {"label": "Backtest",           "arg": "backtest",          "desc": "Rolling-window backtesting and metric computation"},
    "best-method":       {"label": "Best Method",        "arg": "best-method",       "desc": "Select the best method per series using composite scoring"},
    "distributions":     {"label": "Distributions",      "arg": "distributions",     "desc": "Fit forecast distributions for MEIO safety-stock computation"},
}


PIPELINE_STEP_ORDER = ["etl", "outlier-detection", "forecast", "backtest", "best-method", "distributions"]


def _run_pipeline_step_thread(job_id: str, step_arg: str):
    """Run a pipeline step in a background thread, capturing output line by line."""
    files_dir = Path(__file__).parent.parent  # files/ directory
    cmd = [sys.executable, "run_pipeline.py", "--only", step_arg, "--log-level", "INFO"]

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
    # Reject if any step or full-pipeline job is already running
    with _pipeline_lock:
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

    # Reject if same step is already running
    with _pipeline_lock:
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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

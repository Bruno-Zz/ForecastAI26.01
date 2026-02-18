"""
FastAPI Backend for Forecasting System
RESTful API for accessing forecasts, metrics, and visualizations
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from typing import List, Optional, Dict, Any
import pandas as pd
import numpy as np
from pathlib import Path
from pydantic import BaseModel
import logging

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
    'characteristics': None,
    'forecasts': None,
    'distributions': None,
    'metrics': None
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
        if data_cache['time_series'] is None:
            ts_path = Path('./data/time_series.parquet')
            if ts_path.exists():
                data_cache['time_series'] = pd.read_parquet(ts_path)
                logger.info(f"Loaded time series: {len(data_cache['time_series'])} rows")
        
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
            metrics_path = data_dir / 'backtest_metrics_sample.parquet'
            if metrics_path.exists():
                data_cache['metrics'] = pd.read_parquet(metrics_path)
                logger.info(f"Loaded metrics: {len(data_cache['metrics'])} evaluations")
    
    except Exception as e:
        logger.error(f"Error loading data: {e}")


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
            "analytics": "/api/analytics"
        }
    }


@app.get("/api/series", response_model=List[TimeSeriesInfo])
async def get_series_list(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
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
    
    # Convert to response model
    series_list = []
    for _, row in df.iterrows():
        series_list.append(TimeSeriesInfo(
            unique_id=row['unique_id'],
            n_observations=int(row['n_observations']),
            date_range_start=str(row['date_range_start']),
            date_range_end=str(row['date_range_end']),
            mean=float(row['mean']),
            is_intermittent=bool(row['is_intermittent']),
            has_seasonality=bool(row['has_seasonality']),
            has_trend=bool(row['has_trend']),
            complexity_level=str(row['complexity_level'])
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
    
    # Convert to Vega-ready format
    data = {
        'date': series_df['date'].dt.strftime('%Y-%m-%d').tolist(),
        'value': series_df['y'].tolist()
    }
    
    return {
        'unique_id': unique_id,
        'data': data,
        'n_points': len(series_df)
    }


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


@app.get("/api/metrics/{unique_id}")
async def get_metrics(unique_id: str):
    """Get evaluation metrics for a specific time series."""
    if data_cache['metrics'] is None:
        raise HTTPException(status_code=404, detail="Metrics not available")
    
    df = data_cache['metrics']
    metrics_df = df[df['unique_id'] == unique_id]
    
    if metrics_df.empty:
        raise HTTPException(status_code=404, detail=f"No metrics for {unique_id}")
    
    # Aggregate metrics by method
    metrics_by_method = []
    for method in metrics_df['method'].unique():
        method_df = metrics_df[metrics_df['method'] == method]
        
        metrics_by_method.append({
            'method': method,
            'mae': float(method_df['mae'].mean()),
            'rmse': float(method_df['rmse'].mean()),
            'bias': float(method_df['bias'].mean()),
            'mape': float(method_df['mape'].mean()) if 'mape' in method_df.columns else None,
            'coverage_90': float(method_df['coverage_90'].mean()) if 'coverage_90' in method_df.columns else None,
            'n_windows': len(method_df)
        })
    
    return {
        'unique_id': unique_id,
        'metrics': metrics_by_method
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
    
    return analytics


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
            forecast_dates = pd.date_range(start=last_date, periods=len(series_forecasts.iloc[0]['point_forecast']) + 1, freq='M')[1:]
            
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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

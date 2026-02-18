/**
 * TimeSeriesViewer Component
 * 
 * Displays time series with forecasts and animated "racing bars" to show
 * forecast evolution over time. Users can drag a slider to see how forecasts
 * changed at different points in time.
 */

import React, { useState, useEffect, useMemo } from 'react';
import { VegaLite } from 'react-vega';
import axios from 'axios';

const API_BASE_URL = 'http://localhost:8000';

export const TimeSeriesViewer = ({ uniqueId }) => {
  const [historicalData, setHistoricalData] = useState([]);
  const [forecasts, setForecasts] = useState([]);
  const [characteristics, setCharacteristics] = useState(null);
  const [selectedOrigin, setSelectedOrigin] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  // Load data on mount
  useEffect(() => {
    loadData();
  }, [uniqueId]);

  const loadData = async () => {
    setLoading(true);
    setError(null);

    try {
      // Load historical data
      const dataResponse = await axios.get(`${API_BASE_URL}/api/series/${uniqueId}/data`);
      setHistoricalData(dataResponse.data.data);

      // Load forecasts
      const forecastResponse = await axios.get(`${API_BASE_URL}/api/forecasts/${uniqueId}`);
      setForecasts(forecastResponse.data.forecasts);

      // Load characteristics
      const charResponse = await axios.get(`${API_BASE_URL}/api/series`);
      const char = charResponse.data.find(s => s.unique_id === uniqueId);
      setCharacteristics(char);

      setLoading(false);
    } catch (err) {
      setError(err.message);
      setLoading(false);
    }
  };

  // Prepare Vega specification
  const vegaSpec = useMemo(() => {
    if (!historicalData || historicalData.length === 0) return null;

    // Combine historical and forecast data
    const combinedData = historicalData.date.map((date, i) => ({
      date,
      value: historicalData.value[i],
      type: 'Actual',
      method: 'Historical'
    }));

    // Add forecast data
    if (forecasts && forecasts.length > 0) {
      const lastDate = new Date(historicalData.date[historicalData.date.length - 1]);
      
      forecasts.forEach(forecast => {
        forecast.point_forecast.forEach((value, i) => {
          const forecastDate = new Date(lastDate);
          forecastDate.setMonth(forecastDate.getMonth() + i + 1);
          
          combinedData.push({
            date: forecastDate.toISOString().split('T')[0],
            value: value,
            type: 'Forecast',
            method: forecast.method
          });
        });
      });
    }

    return {
      $schema: "https://vega.github.io/schema/vega-lite/v5.json",
      width: 800,
      height: 400,
      data: { values: combinedData },
      layer: [
        {
          mark: { type: "line", point: true },
          encoding: {
            x: {
              field: "date",
              type: "temporal",
              title: "Date",
              axis: { format: "%Y-%m" }
            },
            y: {
              field: "value",
              type: "quantitative",
              title: "Value",
              scale: { zero: false }
            },
            color: {
              field: "method",
              type: "nominal",
              legend: { title: "Method" }
            },
            strokeDash: {
              field: "type",
              type: "nominal",
              scale: {
                domain: ["Actual", "Forecast"],
                range: [[1, 0], [5, 5]]
              }
            },
            tooltip: [
              { field: "date", type: "temporal", title: "Date" },
              { field: "value", type: "quantitative", title: "Value", format: ".2f" },
              { field: "method", type: "nominal", title: "Method" },
              { field: "type", type: "nominal", title: "Type" }
            ]
          }
        }
      ]
    };
  }, [historicalData, forecasts]);

  // Racing bars data (forecast evolution over time)
  const racingBarsSpec = useMemo(() => {
    if (!forecasts || forecasts.length === 0) return null;

    // Prepare data showing forecast values for each method
    const barData = forecasts.map(forecast => ({
      method: forecast.method,
      forecast_1: forecast.point_forecast[0] || 0,
      forecast_2: forecast.point_forecast[1] || 0,
      forecast_3: forecast.point_forecast[2] || 0
    }));

    return {
      $schema: "https://vega.github.io/schema/vega-lite/v5.json",
      width: 600,
      height: 300,
      data: { values: barData },
      mark: "bar",
      encoding: {
        y: {
          field: "method",
          type: "nominal",
          title: "Method",
          sort: { field: "forecast_1", order: "descending" }
        },
        x: {
          field: "forecast_1",
          type: "quantitative",
          title: "Forecast Value (Period 1)"
        },
        color: {
          field: "method",
          type: "nominal",
          legend: null
        },
        tooltip: [
          { field: "method", type: "nominal", title: "Method" },
          { field: "forecast_1", type: "quantitative", title: "Period 1", format: ".2f" },
          { field: "forecast_2", type: "quantitative", title: "Period 2", format: ".2f" },
          { field: "forecast_3", type: "quantitative", title: "Period 3", format: ".2f" }
        ]
      }
    };
  }, [forecasts]);

  if (loading) {
    return (
      <div className="flex items-center justify-center h-screen">
        <div className="text-xl">Loading time series data...</div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex items-center justify-center h-screen">
        <div className="text-xl text-red-600">Error: {error}</div>
      </div>
    );
  }

  return (
    <div className="p-6 max-w-7xl mx-auto">
      {/* Header */}
      <div className="mb-6">
        <h1 className="text-3xl font-bold mb-2">Time Series: {uniqueId}</h1>
        {characteristics && (
          <div className="flex gap-4 text-sm text-gray-600">
            <span>📊 Observations: {characteristics.n_observations}</span>
            <span>🔄 Intermittent: {characteristics.is_intermittent ? 'Yes' : 'No'}</span>
            <span>📈 Seasonal: {characteristics.has_seasonality ? 'Yes' : 'No'}</span>
            <span>📉 Trend: {characteristics.has_trend ? 'Yes' : 'No'}</span>
            <span>🎯 Complexity: {characteristics.complexity_level}</span>
          </div>
        )}
      </div>

      {/* Main Time Series Chart */}
      <div className="mb-8 bg-white rounded-lg shadow p-6">
        <h2 className="text-xl font-semibold mb-4">Historical Data & Forecasts</h2>
        {vegaSpec && <VegaLite spec={vegaSpec} actions={false} />}
      </div>

      {/* Racing Bars */}
      <div className="bg-white rounded-lg shadow p-6">
        <h2 className="text-xl font-semibold mb-4">Method Comparison (Racing Bars)</h2>
        <p className="text-sm text-gray-600 mb-4">
          Compare forecast values across different methods for the first forecast period.
        </p>
        {racingBarsSpec && <VegaLite spec={racingBarsSpec} actions={false} />}
      </div>

      {/* Forecast Details Table */}
      <div className="mt-8 bg-white rounded-lg shadow p-6">
        <h2 className="text-xl font-semibold mb-4">Forecast Details</h2>
        <table className="min-w-full divide-y divide-gray-200">
          <thead>
            <tr>
              <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase">Method</th>
              <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase">Period 1</th>
              <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase">Period 2</th>
              <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase">Period 3</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-200">
            {forecasts.map((forecast, idx) => (
              <tr key={idx}>
                <td className="px-4 py-2 text-sm font-medium text-gray-900">{forecast.method}</td>
                <td className="px-4 py-2 text-sm text-gray-500">{forecast.point_forecast[0]?.toFixed(2)}</td>
                <td className="px-4 py-2 text-sm text-gray-500">{forecast.point_forecast[1]?.toFixed(2)}</td>
                <td className="px-4 py-2 text-sm text-gray-500">{forecast.point_forecast[2]?.toFixed(2)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};

export default TimeSeriesViewer;

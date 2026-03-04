/**
 * Dashboard Component
 *
 * Top-level view to review all parts/groups of time series.
 * Shows summary cards, filterable table, and aggregate charts.
 * All sections are individually collapsible.
 * All charts use Plotly (no Vega-Lite dependency).
 */

import React, { useState, useEffect, useMemo, useCallback, useRef } from 'react';
import { useNavigate } from 'react-router-dom';
import Plot from 'react-plotly.js';
import { useLocale } from '../contexts/LocaleContext';
import { useTheme } from '../contexts/ThemeContext';
import { formatNumber } from '../utils/formatting';
import api from '../utils/api';

const TABLEAU10 = ['#4e79a7','#f28e2b','#e15759','#76b7b2','#59a14f','#edc948','#b07aa1','#ff9da7','#9c755f','#bab0ac'];
const ABC_COLORS = { A: '#22c55e', B: '#eab308', C: '#f97316', D: '#ef4444', X: '#3b82f6', Y: '#a855f7', Z: '#ec4899' };

/** Collapsible section wrapper */
const Section = ({ title, storageKey, defaultOpen = true, children, id }) => {
  const [open, setOpen] = useState(() => {
    const stored = localStorage.getItem(storageKey);
    return stored === null ? defaultOpen : stored === 'true';
  });
  const toggle = () => {
    setOpen(prev => {
      const next = !prev;
      localStorage.setItem(storageKey, String(next));
      return next;
    });
  };
  return (
    <div id={id} className="mb-6 bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900/50">
      <button
        onClick={toggle}
        className="w-full flex items-center justify-between p-4 text-left hover:bg-gray-50 dark:hover:bg-gray-700/50 transition-colors rounded-lg"
      >
        <h2 className="text-lg font-semibold text-gray-900 dark:text-white">{title}</h2>
        <span className="text-gray-400 dark:text-gray-500 text-xl">{open ? '\u25B2' : '\u25BC'}</span>
      </button>
      {open && <div className="px-4 pb-4 sm:px-6 sm:pb-6">{children}</div>}
    </div>
  );
};

/** Inline SVG sparkline */
const Sparkline = ({ historical = [], forecast = [], width = 100, height = 28 }) => {
  const all = [...historical, ...forecast];
  if (all.length === 0) return <span className="text-gray-300 dark:text-gray-600 text-xs">-</span>;

  const min = Math.min(...all);
  const max = Math.max(...all);
  const range = max - min || 1;
  const pad = 1;

  const toX = (i, total) => pad + ((width - 2 * pad) * i) / Math.max(total - 1, 1);
  const toY = (v) => height - pad - ((v - min) / range) * (height - 2 * pad);

  const hLen = historical.length;
  const totalLen = hLen + forecast.length;

  const histPoints = historical.map((v, i) => `${toX(i, totalLen)},${toY(v)}`).join(' ');
  const fcStart = hLen > 0 ? `${toX(hLen - 1, totalLen)},${toY(historical[hLen - 1])} ` : '';
  const fcPoints = fcStart + forecast.map((v, i) => `${toX(hLen + i, totalLen)},${toY(v)}`).join(' ');
  const divX = hLen > 0 ? toX(hLen - 1, totalLen) : null;

  return (
    <svg width={width} height={height} className="inline-block align-middle">
      {histPoints && <polyline points={histPoints} fill="none" stroke="#6b7280" strokeWidth="1.5" />}
      {forecast.length > 0 && <polyline points={fcPoints} fill="none" stroke="#2563eb" strokeWidth="1.5" strokeDasharray="3,2" />}
      {divX != null && forecast.length > 0 && <line x1={divX} y1={0} x2={divX} y2={height} stroke="#d1d5db" strokeWidth="0.5" strokeDasharray="2,2" />}
    </svg>
  );
};

export const Dashboard = () => {
  const { locale, numberDecimals } = useLocale();
  const { isDark } = useTheme();
  const [series, setSeries] = useState([]);
  const [analytics, setAnalytics] = useState(null);
  const [sparklineData, setSparklineData] = useState({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  // Accuracy/Precision data
  const [accuracyPrecisionData, setAccuracyPrecisionData] = useState(null);
  const [selectedAccuracyMethod, setSelectedAccuracyMethod] = useState('');

  // Aggregate demand data
  const [aggregateDemand, setAggregateDemand] = useState(null);
  const [aggLoading, setAggLoading] = useState(false);
  const [aggError, setAggError] = useState(null);

  // ABC classification configs (for dynamic columns)
  const [abcConfigs, setAbcConfigs] = useState([]);

  // Filters
  const [search, setSearch] = useState('');
  const [complexityFilter, setComplexityFilter] = useState('');
  const [intermittentFilter, setIntermittentFilter] = useState('');
  const [bestMethodFilter, setBestMethodFilter] = useState('');
  const [classificationFilters, setClassificationFilters] = useState({}); // { configName: 'A' }
  const [sortField, setSortField] = useState('unique_id');
  const [sortDir, setSortDir] = useState('asc');
  const [accuracyZoom, setAccuracyZoom] = useState(null);

  // Pagination
  const [page, setPage] = useState(0);
  const pageSize = 50;

  const navigate = useNavigate();

  useEffect(() => { loadData(); }, []);

  const loadData = async () => {
    setLoading(true);
    setError(null);
    try {
      const [seriesRes, analyticsRes, abcRes] = await Promise.allSettled([
        api.get('/series', { params: { limit: 50000 } }),
        api.get('/analytics'),
        api.get('/abc/configurations'),
      ]);
      if (seriesRes.status === 'fulfilled') {
        setSeries(seriesRes.value.data || []);
      } else {
        console.error('Failed to load series:', seriesRes.reason);
      }
      if (analyticsRes.status === 'fulfilled') {
        setAnalytics(analyticsRes.value.data);
      } else {
        console.error('Failed to load analytics:', analyticsRes.reason);
      }
      if (abcRes.status === 'fulfilled') {
        setAbcConfigs((abcRes.value.data || []).filter(c => c.is_active));
      }
    } catch (err) {
      console.error('Dashboard load error:', err);
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  // Load accuracy/precision data when method filter changes
  useEffect(() => {
    const loadAccuracyPrecision = async () => {
      try {
        const params = selectedAccuracyMethod ? { method: selectedAccuracyMethod } : {};
        const res = await api.get('/analytics/accuracy-precision', { params });
        setAccuracyPrecisionData(res.data);
      } catch (err) {
        console.error('Failed to load accuracy/precision data:', err);
        setAccuracyPrecisionData(null);
      }
    };
    loadAccuracyPrecision();
  }, [selectedAccuracyMethod]);

  const filteredSeries = useMemo(() => {
    let result = series || [];
    if (search) {
      const lower = search.toLowerCase();
      result = result.filter(s => s.unique_id.toLowerCase().includes(lower));
    }
    if (complexityFilter) result = result.filter(s => s.complexity_level === complexityFilter);
    if (intermittentFilter !== '') {
      const isInt = intermittentFilter === 'true';
      result = result.filter(s => s.is_intermittent === isInt);
    }
    if (bestMethodFilter) result = result.filter(s => s.best_method === bestMethodFilter);

    // Apply classification filters
    for (const [cfgName, classVal] of Object.entries(classificationFilters)) {
      if (classVal) {
        result = result.filter(s => s.classifications?.[cfgName] === classVal);
      }
    }

    // Apply accuracy/precision zoom filter
    if (accuracyZoom && accuracyPrecisionData?.points) {
      const zoomedIds = new Set(
        accuracyPrecisionData.points
          .filter(d =>
            d.accuracy >= accuracyZoom.x[0] && d.accuracy <= accuracyZoom.x[1] &&
            d.precision >= accuracyZoom.y[0] && d.precision <= accuracyZoom.y[1]
          )
          .map(d => d.unique_id)
      );
      result = result.filter(s => zoomedIds.has(s.unique_id));
    }

    result.sort((a, b) => {
      let va, vb;
      if (sortField.startsWith('classifications.')) {
        const cfgName = sortField.slice('classifications.'.length);
        va = a.classifications?.[cfgName] ?? '';
        vb = b.classifications?.[cfgName] ?? '';
      } else {
        va = a[sortField]; vb = b[sortField];
      }
      if (typeof va === 'string') va = va.toLowerCase();
      if (typeof vb === 'string') vb = vb.toLowerCase();
      if (va < vb) return sortDir === 'asc' ? -1 : 1;
      if (va > vb) return sortDir === 'asc' ? 1 : -1;
      return 0;
    });
    return result;
  }, [series, search, complexityFilter, intermittentFilter, bestMethodFilter, classificationFilters, sortField, sortDir, accuracyZoom, accuracyPrecisionData]);

  // Reload aggregate demand when filtered series change (debounced 400ms)
  const aggTimerRef = useRef(null);
  useEffect(() => {
    if (aggTimerRef.current) clearTimeout(aggTimerRef.current);
    setAggLoading(true);
    setAggError(null);
    aggTimerRef.current = setTimeout(async () => {
      try {
        const ids = filteredSeries.map(s => s.unique_id);
        console.log(`[Dashboard] aggregate-demand: filteredSeries=${ids.length}, series=${series?.length}, accuracyZoom=${!!accuracyZoom}, bestMethodFilter=${bestMethodFilter || 'none'}`);
        let res;
        // Use POST when sending many ids to avoid URL length limits
        if (ids.length > 0 && ids.length < (series?.length || 0)) {
          res = await api.post('/analytics/aggregate-demand', { unique_ids: ids });
        } else {
          res = await api.get('/analytics/aggregate-demand');
        }
        console.log(`[Dashboard] aggregate-demand response: hist=${res.data?.historical?.length}, fc=${res.data?.forecast?.length}`);
        setAggregateDemand(res.data);
      } catch (err) {
        console.error('Failed to load aggregate demand:', err);
        setAggError(err.response?.data?.detail || err.message || 'Unknown error');
        setAggregateDemand(null);
      } finally {
        setAggLoading(false);
      }
    }, 400);
    return () => { if (aggTimerRef.current) clearTimeout(aggTimerRef.current); };
  }, [filteredSeries, series]);

  const pagedSeries = filteredSeries.slice(page * pageSize, (page + 1) * pageSize);
  const totalPages = Math.ceil(filteredSeries.length / pageSize);

  const fetchSparklines = useCallback(async (ids) => {
    if (ids.length === 0) return;
    try {
      const res = await api.post('/sparklines', ids);
      setSparklineData(prev => ({ ...prev, ...res.data }));
    } catch { /* non-critical */ }
  }, []);

  useEffect(() => {
    const ids = pagedSeries.map(s => s.unique_id);
    const missing = ids.filter(id => !sparklineData[id]);
    if (missing.length > 0) fetchSparklines(missing);
  }, [pagedSeries.map(s => s.unique_id).join(',')]);

  const handleSort = (field) => {
    if (sortField === field) setSortDir(sortDir === 'asc' ? 'desc' : 'asc');
    else { setSortField(field); setSortDir('asc'); }
  };
  const sortIndicator = (field) => sortField === field ? (sortDir === 'asc' ? ' \u25B2' : ' \u25BC') : '';

  // ─── Plotly base layout (dark-mode aware) ─────────────────────────
  const plotlyBase = useMemo(() => ({
    paper_bgcolor: 'rgba(0,0,0,0)',
    plot_bgcolor: 'rgba(0,0,0,0)',
    font: { color: isDark ? '#d1d5db' : '#374151', size: 11 },
    margin: { t: 30, r: 10, b: 40, l: 10, pad: 4 },
    xaxis: {
      gridcolor: isDark ? '#374151' : '#e5e7eb',
      zerolinecolor: isDark ? '#4b5563' : '#d1d5db',
      color: isDark ? '#d1d5db' : '#374151',
    },
    yaxis: {
      gridcolor: isDark ? '#374151' : '#e5e7eb',
      zerolinecolor: isDark ? '#4b5563' : '#d1d5db',
      color: isDark ? '#d1d5db' : '#374151',
    },
  }), [isDark]);

  // ─── Best Method Bar (Plotly) — recomputed from filteredSeries ────
  const bestMethodChart = useMemo(() => {
    // Count best_method distribution from the currently filtered series
    const withMethod = filteredSeries.filter(s => s.best_method);
    if (withMethod.length === 0) return null;
    const dist = {};
    withMethod.forEach(s => { dist[s.best_method] = (dist[s.best_method] || 0) + 1; });
    const entries = Object.entries(dist).sort((a, b) => a[1] - b[1]); // ascending for horizontal
    const methods = entries.map(([m]) => m);
    const counts = entries.map(([, c]) => c);
    const colors = methods.map((m, i) => {
      const base = TABLEAU10[i % TABLEAU10.length];
      if (bestMethodFilter && m !== bestMethodFilter) {
        const r = parseInt(base.slice(1,3),16), g = parseInt(base.slice(3,5),16), b = parseInt(base.slice(5,7),16);
        return `rgba(${r},${g},${b},0.3)`;
      }
      return base;
    });
    return {
      data: [{ type: 'bar', y: methods, x: counts, orientation: 'h', marker: { color: colors }, hovertemplate: '%{y}: %{x} series<extra></extra>' }],
      layout: {
        ...plotlyBase,
        height: Math.max(200, methods.length * 32),
        margin: { t: 30, r: 10, b: 30, l: 120 },
        title: { text: `Best method across ${formatNumber(withMethod.length, locale, 0)} backtested series`, font: { size: 12 } },
        yaxis: { ...plotlyBase.yaxis, automargin: true },
        xaxis: { ...plotlyBase.xaxis, title: 'Series Won' },
      },
    };
  }, [filteredSeries, plotlyBase, locale, bestMethodFilter]);

  // ─── Accuracy vs Precision Scatter (Plotly) ───────────────────────
  const accuracyChart = useMemo(() => {
    try {
      if (!accuracyPrecisionData?.points || accuracyPrecisionData.points.length === 0) return null;
      let data = accuracyPrecisionData.points;
      if (accuracyZoom) {
        data = data.filter(d =>
          d.accuracy >= accuracyZoom.x[0] && d.accuracy <= accuracyZoom.x[1] &&
          d.precision >= accuracyZoom.y[0] && d.precision <= accuracyZoom.y[1]
        );
      }
      if (data.length === 0) return null;

      const maxAcc = Math.max(...data.map(d => d.accuracy || 0), 1);
      const maxPrec = Math.max(...data.map(d => d.precision || 0), 1);
      const avgAcc = accuracyPrecisionData.summary?.avg_accuracy || 0;
      const avgPrec = accuracyPrecisionData.summary?.avg_precision || 0;

      // Group points by method
      const byMethod = {};
      data.forEach(d => {
        if (!byMethod[d.method]) byMethod[d.method] = { x: [], y: [], ids: [], text: [] };
        byMethod[d.method].x.push(d.accuracy);
        byMethod[d.method].y.push(d.precision);
        byMethod[d.method].ids.push(d.unique_id);
        byMethod[d.method].text.push(d.unique_id);
      });
      // Sort methods by count (desc) for legend order
      const methodOrder = Object.entries(byMethod).sort((a, b) => b[1].x.length - a[1].x.length).map(([m]) => m);

      const traces = methodOrder.map((m, i) => ({
        type: 'scatter', mode: 'markers', name: m,
        x: byMethod[m].x, y: byMethod[m].y,
        customdata: byMethod[m].ids,
        text: byMethod[m].text,
        marker: { size: 8, color: TABLEAU10[i % TABLEAU10.length], opacity: 0.8 },
        hovertemplate: '<b>%{text}</b><br>|Bias|: %{x:.2f}<br>RMSE: %{y:.2f}<extra>%{fullData.name}</extra>',
      }));

      const xMax = maxAcc * 1.1;
      const yMax = maxPrec * 1.1;

      return {
        data: traces,
        layout: {
          ...plotlyBase,
          height: 350,
          margin: { t: 35, r: 10, b: 50, l: 60 },
          title: { text: `Accuracy vs Precision (${formatNumber(data.length, locale, 0)} series${accuracyZoom ? ' \u2014 filtered' : ''})`, font: { size: 13 } },
          dragmode: 'select',
          selectdirection: 'any',
          xaxis: { ...plotlyBase.xaxis, title: '|Bias| (Accuracy)', range: [0, xMax], zeroline: false },
          yaxis: { ...plotlyBase.yaxis, title: 'RMSE (Precision)', range: [0, yMax], zeroline: false },
          legend: { orientation: 'v', x: 1.02, y: 1, font: { size: 10 } },
          shapes: [
            // Green "best" quadrant
            { type: 'rect', x0: 0, y0: 0, x1: avgAcc, y1: avgPrec, fillcolor: isDark ? 'rgba(34,197,94,0.1)' : 'rgba(22,163,106,0.06)', line: { width: 0 }, layer: 'below' },
            // Average lines
            { type: 'line', x0: avgAcc, x1: avgAcc, y0: 0, y1: yMax, line: { dash: 'dash', color: isDark ? '#60a5fa' : '#3b82f6', width: 1.5 } },
            { type: 'line', x0: 0, x1: xMax, y0: avgPrec, y1: avgPrec, line: { dash: 'dash', color: isDark ? '#60a5fa' : '#3b82f6', width: 1.5 } },
          ],
          annotations: [
            { x: xMax * 0.02, y: yMax * 0.02, text: 'Best', showarrow: false, font: { size: 11, color: isDark ? 'rgba(255,255,255,0.35)' : 'rgba(0,0,0,0.2)', weight: 'bold' } },
            { x: xMax * 0.98, y: yMax * 0.02, text: 'Biased', showarrow: false, xanchor: 'right', font: { size: 11, color: isDark ? 'rgba(255,255,255,0.35)' : 'rgba(0,0,0,0.2)', weight: 'bold' } },
            { x: xMax * 0.02, y: yMax * 0.98, text: 'Noisy', showarrow: false, yanchor: 'top', font: { size: 11, color: isDark ? 'rgba(255,255,255,0.35)' : 'rgba(0,0,0,0.2)', weight: 'bold' } },
            { x: xMax * 0.98, y: yMax * 0.98, text: 'Worst', showarrow: false, xanchor: 'right', yanchor: 'top', font: { size: 11, color: isDark ? 'rgba(255,255,255,0.35)' : 'rgba(0,0,0,0.2)', weight: 'bold' } },
          ],
        },
      };
    } catch (err) {
      console.error('Error building accuracy chart:', err);
      return null;
    }
  }, [accuracyPrecisionData, plotlyBase, locale, isDark, accuracyZoom]);

  // ─── Aggregate Demand Chart (Plotly) ──────────────────────────────
  const demandChart = useMemo(() => {
    if (!aggregateDemand) return null;
    const hist = aggregateDemand.historical || [];
    const fc = aggregateDemand.forecast || [];
    if (hist.length === 0 && fc.length === 0) return null;

    const traces = [];
    const shapes = [];
    const annotations = [];

    // ── Historical demand bars ──
    if (hist.length > 0) {
      traces.push({
        type: 'bar', name: 'Historical Demand',
        x: hist.map(d => d.date), y: hist.map(d => d.value),
        marker: { color: isDark ? '#9ca3af' : '#374151', opacity: 0.55 },
        hovertemplate: '%{x|%b %Y}<br>Demand: %{y:,.0f}<extra>Historical</extra>',
      });
    }

    // ── Forecast line + markers ──
    if (fc.length > 0) {
      // Bridge: connect last historical point to first forecast for continuity
      const bridgeX = [], bridgeY = [];
      if (hist.length > 0) {
        bridgeX.push(hist[hist.length - 1].date);
        bridgeY.push(hist[hist.length - 1].value);
      }
      fc.forEach(d => { bridgeX.push(d.date); bridgeY.push(d.value); });

      traces.push({
        type: 'scatter', mode: 'lines+markers', name: 'Forecast (best method / series)',
        x: bridgeX, y: bridgeY,
        line: { color: '#2563eb', width: 2.5, dash: 'dash' },
        marker: { color: '#2563eb', size: 5, symbol: 'circle' },
        hovertemplate: '%{x|%b %Y}<br>Forecast: %{y:,.0f}<extra>Forecast</extra>',
      });

      // Vertical boundary line between historical and forecast
      if (hist.length > 0) {
        const boundary = hist[hist.length - 1].date;
        shapes.push({
          type: 'line', xref: 'x', yref: 'paper',
          x0: boundary, x1: boundary, y0: 0, y1: 1,
          line: { color: isDark ? '#6b7280' : '#9ca3af', width: 1.5, dash: 'dot' },
        });
        annotations.push({
          x: boundary, y: 1, xref: 'x', yref: 'paper',
          text: 'Forecast \u2192', showarrow: false,
          xanchor: 'left', yanchor: 'bottom',
          xshift: 6,
          font: { size: 10, color: '#2563eb' },
        });
      }
    }

    return {
      data: traces,
      layout: {
        ...plotlyBase,
        height: 320,
        margin: { t: 20, r: 20, b: 45, l: 70 },
        xaxis: { ...plotlyBase.xaxis, type: 'date', tickformat: '%b %Y', tickangle: -30 },
        yaxis: { ...plotlyBase.yaxis, title: { text: 'Total Demand', standoff: 10 }, rangemode: 'tozero' },
        legend: { orientation: 'h', y: -0.18, font: { size: 10 } },
        barmode: 'overlay',
        shapes,
        annotations,
        hovermode: 'x unified',
      },
    };
  }, [aggregateDemand, plotlyBase, isDark]);

  if (loading) return <div className="flex items-center justify-center h-64"><div className="text-xl text-gray-500 dark:text-gray-400 animate-pulse">Loading dashboard...</div></div>;
  if (error) return <div className="flex items-center justify-center h-64"><div className="text-xl text-red-600 dark:text-red-400">Error: {error}</div></div>;

  // Ensure we have data
  if (!analytics && !series.length) {
    return (
      <div className="p-4 sm:p-6">
        <h1 className="text-2xl sm:text-3xl font-bold mb-6 text-gray-900 dark:text-white">Forecasting Dashboard</h1>
        <div className="text-gray-500 dark:text-gray-400">No data available. Please run the pipeline first.</div>
      </div>
    );
  }

  // Table column definitions with responsive visibility
  const abcColumns = abcConfigs.map(cfg => ({
    field: `classifications.${cfg.name}`,
    label: cfg.name,
    hideClass: 'hidden md:table-cell',
    isClassification: true,
    classLabels: cfg.class_labels || [],
  }));

  const columns = [
    { field: 'unique_id', label: 'Series ID', hideClass: '' },
    { field: 'n_observations', label: 'Obs', hideClass: '' },
    { field: 'complexity_level', label: 'Complexity', hideClass: 'hidden sm:table-cell' },
    ...abcColumns,
    { field: 'is_intermittent', label: 'Interm.', hideClass: 'hidden md:table-cell' },
    { field: 'has_seasonality', label: 'Seasonal', hideClass: 'hidden md:table-cell' },
    { field: 'has_trend', label: 'Trend', hideClass: 'hidden lg:table-cell' },
    { field: 'mean', label: 'Mean', hideClass: 'hidden sm:table-cell' },
    { field: null, label: 'Demand', hideClass: '' },
    { field: 'n_outliers', label: 'Adj.', hideClass: 'hidden lg:table-cell' },
    { field: 'best_method', label: 'Best Method', hideClass: '' },
  ];

  const plotlyConfig = {
    responsive: true,
    displayModeBar: 'hover',
    displaylogo: false,
    modeBarButtonsToRemove: ['toImage', 'lasso2d', 'select2d'],
  };
  // AP chart keeps modebar hidden — it has its own Reset Zoom button + drag-select
  const apPlotlyConfig = { responsive: true, displayModeBar: false };

  return (
    <div className="p-4 sm:p-6">
      <h1 className="text-2xl sm:text-3xl font-bold mb-6 text-gray-900 dark:text-white">Forecasting Dashboard</h1>

      {/* Summary Cards */}
      {analytics && (
        <Section title="Summary" storageKey="dash_summary_open" id="dash-summary">
          <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 xl:grid-cols-7 gap-3">
            {[
              { label: 'Total Series', value: formatNumber(analytics.total_series, locale, 0), color: 'text-blue-600 dark:text-blue-400' },
              { label: 'Backtested', value: formatNumber(analytics.best_method_total_series || 0, locale, 0), color: 'text-emerald-600 dark:text-emerald-400' },
              { label: 'Seasonal', value: formatNumber(analytics.seasonal_count, locale, 0), color: '' },
              { label: 'Trending', value: formatNumber(analytics.trending_count, locale, 0), color: '' },
              { label: 'Intermittent', value: formatNumber(analytics.intermittent_count, locale, 0), color: '' },
              { label: 'Avg Obs', value: formatNumber(analytics.avg_observations, locale, 0), color: '' },
              { label: 'Outlier Adj.', value: formatNumber(analytics.outlier_adjusted_count || 0, locale, 0), color: 'text-orange-600 dark:text-orange-400' },
            ].map(({ label, value, color }) => (
              <div key={label} className="bg-gray-50 dark:bg-gray-700/50 rounded-lg p-3 border border-gray-100 dark:border-gray-600">
                <div className="text-xs text-gray-500 dark:text-gray-400 mb-1">{label}</div>
                <div className={`text-xl sm:text-2xl font-bold ${color || 'text-gray-900 dark:text-white'}`}>{value}</div>
              </div>
            ))}
          </div>
        </Section>
      )}

      {/* Charts */}
      {(accuracyChart || bestMethodChart) && (
        <Section title="Charts" storageKey="dash_charts_open" id="dash-charts">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            {accuracyChart && (
              <div>
                <div className="flex items-center justify-between mb-2">
                  <h3 className="text-sm font-semibold text-gray-600 dark:text-gray-400">Accuracy vs Precision</h3>
                  <div className="flex items-center gap-2">
                    <select
                      value={selectedAccuracyMethod}
                      onChange={(e) => { setSelectedAccuracyMethod(e.target.value); setPage(0); }}
                      className="text-xs border border-gray-300 dark:border-gray-600 rounded px-2 py-1 bg-white dark:bg-gray-700 dark:text-gray-200"
                    >
                      <option value="">All Methods</option>
                      {analytics?.best_method_distribution && Object.keys(analytics.best_method_distribution).map(m => (
                        <option key={m} value={m}>{m}</option>
                      ))}
                    </select>
                    {accuracyZoom && (
                      <button
                        onClick={() => setAccuracyZoom(null)}
                        className="text-xs px-2 py-1 bg-gray-200 dark:bg-gray-600 hover:bg-gray-300 dark:hover:bg-gray-500 rounded text-gray-700 dark:text-gray-200"
                      >
                        Reset Zoom
                      </button>
                    )}
                  </div>
                </div>
                <Plot
                  data={accuracyChart.data}
                  layout={accuracyChart.layout}
                  config={apPlotlyConfig}
                  useResizeHandler
                  style={{ width: '100%' }}
                  onSelected={(event) => {
                    console.log('[Dashboard] onSelected event:', JSON.stringify(event?.range), 'points:', event?.points?.length);
                    if (event?.range?.x && event?.range?.y) {
                      setAccuracyZoom({ x: event.range.x, y: event.range.y });
                      setPage(0);
                    }
                  }}
                  onDeselect={() => { console.log('[Dashboard] onDeselect fired'); }}
                />
                <p className="text-xs text-gray-400 dark:text-gray-500 mt-1">Drag to select a region and filter the table below.</p>
              </div>
            )}
            {bestMethodChart && (
              <div>
                <h3 className="text-sm font-semibold text-gray-600 dark:text-gray-400 mb-2">Best Method Distribution</h3>
                <Plot
                  data={bestMethodChart.data}
                  layout={bestMethodChart.layout}
                  config={plotlyConfig}
                  useResizeHandler
                  style={{ width: '100%' }}
                  onClick={(event) => {
                    const pt = event?.points?.[0];
                    if (pt?.y) {
                      const method = pt.y;
                      setBestMethodFilter(prev => { setPage(0); return prev === method ? '' : method; });
                    }
                  }}
                />
              </div>
            )}
          </div>
        </Section>
      )}

      {/* Aggregate Demand & Forecast */}
      <Section
        title={`Demand & Forecast${filteredSeries.length < (series?.length || 0) ? ` (${formatNumber(filteredSeries.length, locale, 0)} of ${formatNumber(series.length, locale, 0)} series)` : ` (${formatNumber(series.length, locale, 0)} series)`}`}
        storageKey="dash_demand_open"
        id="dash-demand"
      >
        {aggLoading ? (
          <div className="flex items-center justify-center h-40 gap-3">
            <div className="animate-spin rounded-full h-5 w-5 border-b-2 border-blue-600" />
            <span className="text-sm text-gray-400 dark:text-gray-500">Loading demand data…</span>
          </div>
        ) : aggError ? (
          <div className="flex items-center justify-center h-32 text-red-500 dark:text-red-400 text-sm">
            Error loading demand: {aggError}
          </div>
        ) : demandChart ? (
          <Plot data={demandChart.data} layout={demandChart.layout} config={plotlyConfig} useResizeHandler style={{ width: '100%' }} />
        ) : (
          <div className="flex items-center justify-center h-32 text-gray-400 dark:text-gray-500 text-sm">
            No demand data available for the current filter.
          </div>
        )}
        {(accuracyZoom || bestMethodFilter) && (
          <p className="text-xs text-blue-500 dark:text-blue-400 mt-1">
            Filtered by{accuracyZoom ? ' accuracy/precision selection' : ''}{accuracyZoom && bestMethodFilter ? ' + ' : ''}{bestMethodFilter ? ` method: ${bestMethodFilter}` : ''}
          </p>
        )}
      </Section>

      {/* Series Table */}
      <Section title={`Series Table (${formatNumber(filteredSeries.length, locale, 0)} series)`} storageKey="dash_table_open" id="dash-table">
        {/* Filters */}
        <div id="dash-filters" className="flex flex-wrap gap-3 mb-4">
          <input
            type="text"
            placeholder="Search by ID..."
            value={search}
            onChange={e => { setSearch(e.target.value); setPage(0); }}
            className="border border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-white dark:placeholder-gray-400 rounded-lg px-3 py-2 text-sm w-full sm:w-56 focus:outline-none focus:ring-2 focus:ring-blue-400"
          />
          <select
            value={complexityFilter}
            onChange={e => { setComplexityFilter(e.target.value); setPage(0); }}
            className="border border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-white rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-400"
          >
            <option value="">All Complexity</option>
            <option value="low">Low</option>
            <option value="medium">Medium</option>
            <option value="high">High</option>
          </select>
          <select
            value={intermittentFilter}
            onChange={e => { setIntermittentFilter(e.target.value); setPage(0); }}
            className="border border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-white rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-400"
          >
            <option value="">All Types</option>
            <option value="true">Intermittent</option>
            <option value="false">Non-Intermittent</option>
          </select>
          {abcConfigs.map(cfg => (
            <select
              key={cfg.id}
              value={classificationFilters[cfg.name] || ''}
              onChange={e => { setClassificationFilters(prev => ({ ...prev, [cfg.name]: e.target.value })); setPage(0); }}
              className="border border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-white rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-400"
            >
              <option value="">All {cfg.name}</option>
              {(cfg.class_labels || []).map(lbl => <option key={lbl} value={lbl}>{lbl}</option>)}
            </select>
          ))}
          {bestMethodFilter && (
            <button
              onClick={() => { setBestMethodFilter(''); setPage(0); }}
              className="inline-flex items-center gap-1.5 px-3 py-2 bg-blue-100 text-blue-700 dark:bg-blue-900/40 dark:text-blue-400 rounded-lg text-sm font-medium hover:bg-blue-200 dark:hover:bg-blue-900/60 transition-colors"
            >
              Method: {bestMethodFilter}
              <span className="text-xs">{'\u2715'}</span>
            </button>
          )}
          {(complexityFilter || intermittentFilter || bestMethodFilter || search || Object.values(classificationFilters).some(v => v)) && (
            <button
              onClick={() => { setSearch(''); setComplexityFilter(''); setIntermittentFilter(''); setBestMethodFilter(''); setClassificationFilters({}); setPage(0); }}
              className="px-3 py-2 text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-200 text-sm transition-colors"
            >
              Clear all
            </button>
          )}
        </div>

        {/* Table */}
        <div className="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
          <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-700 text-sm">
            <thead className="bg-gray-50 dark:bg-gray-900">
              <tr>
                {columns.map(({ field, label, hideClass }) => (
                  <th
                    key={label}
                    onClick={field ? () => handleSort(field) : undefined}
                    className={`px-3 py-2 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase whitespace-nowrap ${hideClass} ${field ? 'cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-800 select-none' : ''}`}
                  >
                    {label}{field ? sortIndicator(field) : ''}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-200 dark:divide-gray-700 bg-white dark:bg-gray-800">
              {pagedSeries.map(s => (
                <tr
                  key={s.unique_id}
                  onClick={() => navigate(`/series/${encodeURIComponent(s.unique_id)}`)}
                  className="hover:bg-blue-50 dark:hover:bg-blue-900/20 cursor-pointer transition-colors"
                >
                  <td className="px-3 py-2 font-medium text-blue-600 dark:text-blue-400 whitespace-nowrap">{s.unique_id}</td>
                  <td className="px-3 py-2 text-right text-gray-700 dark:text-gray-300">{s.n_observations}</td>
                  <td className="px-3 py-2 hidden sm:table-cell">
                    <span className={`px-1.5 py-0.5 rounded text-xs font-medium ${
                      s.complexity_level === 'high' ? 'bg-red-100 text-red-700 dark:bg-red-900/40 dark:text-red-400' :
                      s.complexity_level === 'medium' ? 'bg-yellow-100 text-yellow-700 dark:bg-yellow-900/40 dark:text-yellow-400' :
                      'bg-green-100 text-green-700 dark:bg-green-900/40 dark:text-green-400'
                    }`}>
                      {s.complexity_level}
                    </span>
                  </td>
                  {abcConfigs.map(cfg => {
                    const cls = s.classifications?.[cfg.name];
                    return (
                      <td key={cfg.id} className="px-3 py-2 text-center hidden md:table-cell">
                        {cls ? (
                          <span
                            className="inline-flex items-center justify-center w-6 h-6 rounded text-xs font-bold text-white"
                            style={{ backgroundColor: ABC_COLORS[cls] || '#6b7280' }}
                          >
                            {cls}
                          </span>
                        ) : <span className="text-gray-300 dark:text-gray-600">-</span>}
                      </td>
                    );
                  })}
                  <td className="px-3 py-2 text-center hidden md:table-cell text-gray-600 dark:text-gray-400">{s.is_intermittent ? '\u2713' : '-'}</td>
                  <td className="px-3 py-2 text-center hidden md:table-cell text-gray-600 dark:text-gray-400">{s.has_seasonality ? '\u2713' : '-'}</td>
                  <td className="px-3 py-2 text-center hidden lg:table-cell text-gray-600 dark:text-gray-400">{s.has_trend ? '\u2713' : '-'}</td>
                  <td className="px-3 py-2 text-right font-mono hidden sm:table-cell text-gray-700 dark:text-gray-300">{formatNumber(s.mean, locale, numberDecimals)}</td>
                  <td className="px-3 py-2">
                    <Sparkline historical={sparklineData[s.unique_id]?.historical || []} forecast={sparklineData[s.unique_id]?.forecast || []} />
                  </td>
                  <td className="px-3 py-2 text-center hidden lg:table-cell">
                    {s.has_outlier_corrections ? (
                      <span className="bg-orange-100 text-orange-700 dark:bg-orange-900/40 dark:text-orange-400 px-1.5 py-0.5 rounded text-xs font-medium">{s.n_outliers}</span>
                    ) : <span className="text-gray-300 dark:text-gray-600">-</span>}
                  </td>
                  <td className="px-3 py-2 whitespace-nowrap">
                    {s.best_method ? (
                      <span className={`inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-xs font-medium ${
                        s.best_method_source === 'backtested'
                          ? 'bg-emerald-100 text-emerald-700 dark:bg-emerald-900/40 dark:text-emerald-400'
                          : 'bg-blue-50 text-blue-600 dark:bg-blue-900/30 dark:text-blue-400'
                      }`}>
                        {s.best_method_source === 'backtested' && <span title="Backtested">{'\u2713'}</span>}
                        {s.best_method}
                      </span>
                    ) : <span className="text-gray-300 dark:text-gray-600">-</span>}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>

          {/* Pagination */}
          {totalPages > 1 && (
            <div className="flex items-center justify-between px-4 py-3 bg-gray-50 dark:bg-gray-900 border-t border-gray-200 dark:border-gray-700">
              <button onClick={() => setPage(Math.max(0, page - 1))} disabled={page === 0}
                className="px-3 py-1.5 border border-gray-300 dark:border-gray-600 rounded-lg text-sm disabled:opacity-40 hover:bg-gray-100 dark:hover:bg-gray-800 dark:text-gray-300 transition-colors">
                {'\u2190'} Previous
              </button>
              <span className="text-sm text-gray-500 dark:text-gray-400">Page {page + 1} of {totalPages}</span>
              <button onClick={() => setPage(Math.min(totalPages - 1, page + 1))} disabled={page >= totalPages - 1}
                className="px-3 py-1.5 border border-gray-300 dark:border-gray-600 rounded-lg text-sm disabled:opacity-40 hover:bg-gray-100 dark:hover:bg-gray-800 dark:text-gray-300 transition-colors">
                Next {'\u2192'}
              </button>
            </div>
          )}
        </div>
      </Section>
    </div>
  );
};

export default Dashboard;

/**
 * Dashboard Component
 *
 * Top-level view to review all parts/groups of time series.
 * Shows summary cards, filterable table, and aggregate charts.
 * All sections are individually collapsible.
 */

import React, { useState, useEffect, useMemo, useCallback, useRef } from 'react';
import { useNavigate } from 'react-router-dom';
import { VegaLite } from 'react-vega';
import { useLocale } from '../contexts/LocaleContext';
import { useTheme } from '../contexts/ThemeContext';
import { formatNumber } from '../utils/formatting';
import api from '../utils/api';

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

  // Filters
  const [search, setSearch] = useState('');
  const [complexityFilter, setComplexityFilter] = useState('');
  const [intermittentFilter, setIntermittentFilter] = useState('');
  const [bestMethodFilter, setBestMethodFilter] = useState('');
  const [sortField, setSortField] = useState('unique_id');
  const [sortDir, setSortDir] = useState('asc');
  const [accuracyZoom, setAccuracyZoom] = useState(null);

  // Brush overlay refs for accuracy/precision chart (direct DOM — no re-renders during drag)
  const apChartRef = useRef(null);
  const apViewRef = useRef(null);
  const apBrushRef = useRef(null);    // the overlay div
  const apDragRef = useRef({ active: false, x0: 0, y0: 0 });
  const apDomainsRef = useRef({ x: [0, 1], y: [0, 1] }); // data domains for pixel↔data conversion

  // Pagination
  const [page, setPage] = useState(0);
  const pageSize = 50;

  const navigate = useNavigate();

  useEffect(() => { loadData(); }, []);

  const loadData = async () => {
    setLoading(true);
    setError(null);
    try {
      const [seriesRes, analyticsRes] = await Promise.allSettled([
        api.get('/series', { params: { limit: 50000 } }),
        api.get('/analytics'),
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
      let va = a[sortField], vb = b[sortField];
      if (typeof va === 'string') va = va.toLowerCase();
      if (typeof vb === 'string') vb = vb.toLowerCase();
      if (va < vb) return sortDir === 'asc' ? -1 : 1;
      if (va > vb) return sortDir === 'asc' ? 1 : -1;
      return 0;
    });
    return result;
  }, [series, search, complexityFilter, intermittentFilter, bestMethodFilter, sortField, sortDir, accuracyZoom, accuracyPrecisionData]);

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

  // Vega theme for dark mode
  const vegaConfig = useMemo(() => ({
    background: isDark ? '#1f2937' : '#ffffff',
    axis: {
      labelColor: isDark ? '#d1d5db' : '#374151',
      titleColor: isDark ? '#e5e7eb' : '#111827',
      gridColor: isDark ? '#374151' : '#e5e7eb',
      tickColor: isDark ? '#4b5563' : '#d1d5db',
      domainColor: isDark ? '#4b5563' : '#d1d5db',
    },
    legend: {
      labelColor: isDark ? '#d1d5db' : '#374151',
      titleColor: isDark ? '#e5e7eb' : '#111827',
    },
    title: { color: isDark ? '#e5e7eb' : '#111827' },
  }), [isDark]);

  const complexitySpec = useMemo(() => {
    if (!analytics?.complexity_distribution) return null;
    const data = Object.entries(analytics.complexity_distribution).map(([k, v]) => ({ level: k, count: v }));
    const encoding = {
      theta: { field: 'count', type: 'quantitative' },
      color: {
        field: 'level', type: 'nominal',
        scale: { domain: ['low', 'medium', 'high'], range: ['#4ade80', '#facc15', '#f87171'] },
        legend: { title: 'Complexity' }
      },
      tooltip: [
        { field: 'level', type: 'nominal', title: 'Complexity' },
        { field: 'count', type: 'quantitative', title: 'Count' }
      ]
    };
    if (complexityFilter) {
      encoding.opacity = {
        condition: { test: `datum.level === '${complexityFilter}'`, value: 1 },
        value: 0.3
      };
      encoding.stroke = {
        condition: { test: `datum.level === '${complexityFilter}'`, value: isDark ? '#e5e7eb' : '#1f2937' },
        value: null
      };
      encoding.strokeWidth = {
        condition: { test: `datum.level === '${complexityFilter}'`, value: 2 },
        value: 0
      };
    }
    return {
      $schema: 'https://vega.github.io/schema/vega-lite/v5.json',
      width: 280, height: 200,
      config: vegaConfig,
      data: { values: data },
      mark: { type: 'arc', cursor: 'pointer' },
      encoding
    };
  }, [analytics, vegaConfig, complexityFilter, isDark]);

  const bestMethodSpec = useMemo(() => {
    if (!analytics?.best_method_distribution) return null;
    const data = Object.entries(analytics.best_method_distribution)
      .map(([k, v]) => ({ method: k, count: v }))
      .sort((a, b) => b.count - a.count);
    const encoding = {
      y: { field: 'method', type: 'nominal', sort: '-x', title: 'Method' },
      x: { field: 'count', type: 'quantitative', title: 'Series Won' },
      color: { field: 'method', type: 'nominal', legend: null, scale: { scheme: 'tableau10' } },
      tooltip: [
        { field: 'method', type: 'nominal', title: 'Method' },
        { field: 'count', type: 'quantitative', title: 'Series Won' }
      ]
    };
    if (bestMethodFilter) {
      encoding.opacity = {
        condition: { test: `datum.method === '${bestMethodFilter}'`, value: 1 },
        value: 0.3
      };
      encoding.stroke = {
        condition: { test: `datum.method === '${bestMethodFilter}'`, value: isDark ? '#e5e7eb' : '#1f2937' },
        value: null
      };
      encoding.strokeWidth = {
        condition: { test: `datum.method === '${bestMethodFilter}'`, value: 2 },
        value: 0
      };
    }
    return {
      $schema: 'https://vega.github.io/schema/vega-lite/v5.json',
      width: 'container', height: 250,
      config: vegaConfig,
      title: { text: `Best method across ${formatNumber(analytics.best_method_total_series || 0, locale, 0)} backtested series`, fontSize: 12 },
      data: { values: data },
      mark: { type: 'bar', cornerRadiusEnd: 4, cursor: 'pointer' },
      encoding
    };
  }, [analytics, vegaConfig, locale, bestMethodFilter, isDark]);

  const accuracyPrecisionSpec = useMemo(() => {
    try {
      if (!accuracyPrecisionData?.points || accuracyPrecisionData.points.length === 0) return null;
      let data = accuracyPrecisionData.points;
      
      // Apply zoom filter if active
      if (accuracyZoom) {
        data = data.filter(d => 
          d.accuracy >= accuracyZoom.x[0] && d.accuracy <= accuracyZoom.x[1] &&
          d.precision >= accuracyZoom.y[0] && d.precision <= accuracyZoom.y[1]
        );
      }
      
      if (data.length === 0) return null;
      
      const maxAccuracy = Math.max(...data.map(d => d.accuracy || 0), 1);
      const maxPrecision = Math.max(...data.map(d => d.precision || 0), 1);
      const avgAccuracy = accuracyPrecisionData.summary?.avg_accuracy || 0;
      const avgPrecision = accuracyPrecisionData.summary?.avg_precision || 0;
      
      // Get unique methods for color scale, sorted by count (same order as best method chart)
      const methodCounts = {};
      data.forEach(d => { if (d.method) methodCounts[d.method] = (methodCounts[d.method] || 0) + 1; });
      const methods = Object.entries(methodCounts).sort((a, b) => b[1] - a[1]).map(([m]) => m);
      
      const xDom = [0, maxAccuracy * 1.1];
      const yDom = [0, maxPrecision * 1.1];
      apDomainsRef.current = { x: xDom, y: yDom };
      const xScale = { domain: xDom };
      const yScale = { domain: yDom };

      return {
        $schema: 'https://vega.github.io/schema/vega-lite/v5.json',
        width: 'container', height: 350,
        config: { ...vegaConfig, legend: { labelColor: isDark ? '#e5e7eb' : '#374151', titleColor: isDark ? '#e5e7eb' : '#374151' } },
        title: {
          text: `Accuracy vs Precision (${formatNumber(data.length, locale, 0)} series${accuracyZoom ? ' (filtered)' : ''})`,
          fontSize: 14,
          color: isDark ? '#e5e7eb' : '#111827'
        },
        data: { values: data },
        layer: [
          // Green quadrant (best)
          { data: { values: [{ x: 0, y: 0, x2: avgAccuracy, y2: avgPrecision }] },
            mark: { type: 'rect', opacity: isDark ? 0.1 : 0.06, color: isDark ? '#22c55e' : '#16a34a' },
            encoding: {
              x: { field: 'x', type: 'quantitative', scale: xScale, title: '|Bias| (Accuracy)' },
              x2: { field: 'x2' },
              y: { field: 'y', type: 'quantitative', scale: yScale, title: 'RMSE (Precision)' },
              y2: { field: 'y2' }
            }
          },
          // Average lines
          { data: { values: [{ x: avgAccuracy }] },
            mark: { type: 'rule', strokeDash: [4, 4], color: isDark ? '#60a5fa' : '#3b82f6', strokeWidth: 1.5 },
            encoding: { x: { field: 'x', type: 'quantitative', scale: xScale } }
          },
          { data: { values: [{ y: avgPrecision }] },
            mark: { type: 'rule', strokeDash: [4, 4], color: isDark ? '#60a5fa' : '#3b82f6', strokeWidth: 1.5 },
            encoding: { y: { field: 'y', type: 'quantitative', scale: yScale } }
          },
          // Labels
          { data: { values: [
            { x: maxAccuracy * 0.02, y: maxPrecision * 0.02, label: 'Best' },
            { x: maxAccuracy * 0.98, y: maxPrecision * 0.02, label: 'Biased' },
            { x: maxAccuracy * 0.02, y: maxPrecision * 0.98, label: 'Noisy' },
            { x: maxAccuracy * 0.98, y: maxPrecision * 0.98, label: 'Worst' }
          ]},
            mark: { type: 'text', fontSize: 11, fontWeight: 'bold', opacity: isDark ? 0.4 : 0.25 },
            encoding: { x: { field: 'x', type: 'quantitative', scale: xScale }, y: { field: 'y', type: 'quantitative', scale: yScale }, text: { field: 'label', type: 'nominal' } }
          },
          // Points colored by method
          { data: { values: data },
            mark: { type: 'point', filled: true, size: 80, opacity: 0.8 },
            encoding: {
              x: { field: 'accuracy', type: 'quantitative', scale: xScale, title: '|Bias| (Accuracy)' },
              y: { field: 'precision', type: 'quantitative', scale: yScale, title: 'RMSE (Precision)' },
              color: {
                field: 'method',
                type: 'nominal',
                scale: { scheme: 'tableau10', domain: methods },
                legend: { title: 'Method', orient: 'right' }
              },
              tooltip: [
                { field: 'unique_id', type: 'nominal', title: 'Series' },
                { field: 'method', type: 'nominal', title: 'Method' },
                { field: 'accuracy', type: 'quantitative', title: '|Bias|', format: ',.2f' },
                { field: 'precision', type: 'quantitative', title: 'RMSE', format: ',.2f' }
              ]
            }
          }
        ]
      };
    } catch (err) {
      console.error('Error building accuracyPrecisionSpec:', err);
      return null;
    }
  }, [accuracyPrecisionData, vegaConfig, locale, isDark, accuracyZoom]);

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
  const columns = [
    { field: 'unique_id', label: 'Series ID', hideClass: '' },
    { field: 'n_observations', label: 'Obs', hideClass: '' },
    { field: 'complexity_level', label: 'Complexity', hideClass: 'hidden sm:table-cell' },
    { field: 'is_intermittent', label: 'Interm.', hideClass: 'hidden md:table-cell' },
    { field: 'has_seasonality', label: 'Seasonal', hideClass: 'hidden md:table-cell' },
    { field: 'has_trend', label: 'Trend', hideClass: 'hidden lg:table-cell' },
    { field: 'mean', label: 'Mean', hideClass: 'hidden sm:table-cell' },
    { field: null, label: 'Demand', hideClass: '' },
    { field: 'n_outliers', label: 'Adj.', hideClass: 'hidden lg:table-cell' },
    { field: 'best_method', label: 'Best Method', hideClass: '' },
  ];

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
      {(accuracyPrecisionSpec || bestMethodSpec) && (
        <Section title="Charts" storageKey="dash_charts_open" id="dash-charts">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            {accuracyPrecisionSpec && (
              <div>
                <div className="flex items-center justify-between mb-2">
                  <h3 className="text-sm font-semibold text-gray-600 dark:text-gray-400">Accuracy vs Precision</h3>
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
                </div>
                <div className="flex items-center gap-2 mb-2">
                  <div
                    ref={apChartRef}
                    className="w-full overflow-x-auto relative select-none"
                    style={{ cursor: 'crosshair' }}
                    onMouseDown={(e) => {
                      if (e.button !== 0) return;
                      const rect = apChartRef.current.getBoundingClientRect();
                      apDragRef.current = { active: true, x0: e.clientX - rect.left, y0: e.clientY - rect.top };
                      if (apBrushRef.current) {
                        const b = apBrushRef.current;
                        b.style.display = 'block';
                        b.style.left = `${apDragRef.current.x0}px`;
                        b.style.top = `${apDragRef.current.y0}px`;
                        b.style.width = '0px';
                        b.style.height = '0px';
                      }
                    }}
                    onMouseMove={(e) => {
                      if (!apDragRef.current.active) return;
                      const rect = apChartRef.current.getBoundingClientRect();
                      const { x0, y0 } = apDragRef.current;
                      const cx = e.clientX - rect.left;
                      const cy = e.clientY - rect.top;
                      if (apBrushRef.current) {
                        const b = apBrushRef.current;
                        b.style.left = `${Math.min(x0, cx)}px`;
                        b.style.top = `${Math.min(y0, cy)}px`;
                        b.style.width = `${Math.abs(cx - x0)}px`;
                        b.style.height = `${Math.abs(cy - y0)}px`;
                      }
                    }}
                    onMouseUp={(e) => {
                      if (!apDragRef.current.active) return;
                      apDragRef.current.active = false;
                      if (apBrushRef.current) apBrushRef.current.style.display = 'none';
                      const divRect = apChartRef.current.getBoundingClientRect();
                      const { x0, y0 } = apDragRef.current;
                      const cx = e.clientX - divRect.left;
                      const cy = e.clientY - divRect.top;
                      // Ignore clicks (require at least 10px drag)
                      if (Math.abs(cx - x0) < 10 || Math.abs(cy - y0) < 10) return;
                      const view = apViewRef.current;
                      if (!view) return;
                      try {
                        // Get the SVG element inside the wrapper to compute the offset
                        const svgEl = apChartRef.current.querySelector('svg');
                        if (!svgEl) return;
                        const svgRect = svgEl.getBoundingClientRect();
                        const svgOffX = svgRect.left - divRect.left;
                        const svgOffY = svgRect.top - divRect.top;
                        // Vega origin = pixel offset of the plot area within the SVG
                        const origin = view.origin();
                        const plotW = view.width();
                        const plotH = view.height();
                        if (plotW <= 0 || plotH <= 0) return;
                        // Convert div-relative px → plot-area-relative px
                        const pxLeft  = Math.min(x0, cx) - svgOffX - origin[0];
                        const pxRight = Math.max(x0, cx) - svgOffX - origin[0];
                        const pxTop   = Math.min(y0, cy) - svgOffY - origin[1];
                        const pxBot   = Math.max(y0, cy) - svgOffY - origin[1];
                        // Clamp to plot area
                        const cL = Math.max(0, Math.min(plotW, pxLeft));
                        const cR = Math.max(0, Math.min(plotW, pxRight));
                        const cT = Math.max(0, Math.min(plotH, pxTop));
                        const cB = Math.max(0, Math.min(plotH, pxBot));
                        // Linear interpolation using the known data domains
                        const { x: xDom, y: yDom } = apDomainsRef.current;
                        const dataX1 = xDom[0] + (cL / plotW) * (xDom[1] - xDom[0]);
                        const dataX2 = xDom[0] + (cR / plotW) * (xDom[1] - xDom[0]);
                        // Y axis is inverted: top pixel → high data value
                        const dataY2 = yDom[1] - (cT / plotH) * (yDom[1] - yDom[0]);
                        const dataY1 = yDom[1] - (cB / plotH) * (yDom[1] - yDom[0]);
                        if (dataX2 > dataX1 && dataY2 > dataY1) {
                          setAccuracyZoom({ x: [dataX1, dataX2], y: [dataY1, dataY2] });
                          setPage(0);
                        }
                      } catch (err) { console.warn('brush zoom failed:', err); }
                    }}
                    onMouseLeave={() => {
                      if (apDragRef.current.active) {
                        apDragRef.current.active = false;
                        if (apBrushRef.current) apBrushRef.current.style.display = 'none';
                      }
                    }}
                  >
                    <VegaLite
                      spec={accuracyPrecisionSpec}
                      actions={false}
                      renderer="svg"
                      style={{width: '100%'}}
                      onNewView={(v) => { apViewRef.current = v; }}
                    />
                    {/* Brush selection rectangle (CSS overlay — no React re-renders) */}
                    <div
                      ref={apBrushRef}
                      style={{ display: 'none', position: 'absolute', background: 'rgba(59,130,246,0.15)',
                               border: '1px solid rgba(59,130,246,0.5)', borderRadius: 2,
                               pointerEvents: 'none', zIndex: 10 }}
                    />
                  </div>
                  {accuracyZoom && (
                    <button
                      onClick={() => setAccuracyZoom(null)}
                      className="flex-shrink-0 text-xs px-2 py-1 bg-gray-200 dark:bg-gray-600 hover:bg-gray-300 dark:hover:bg-gray-500 rounded text-gray-700 dark:text-gray-200"
                    >
                      Reset Zoom
                    </button>
                  )}
                </div>
              </div>
            )}
            {bestMethodSpec && (
              <div>
                <h3 className="text-sm font-semibold text-gray-600 dark:text-gray-400 mb-2">Best Method Distribution</h3>
                <div className="w-full overflow-x-auto">
                  <VegaLite
                    spec={bestMethodSpec}
                    actions={false}
                    renderer="svg"
                    style={{width:'100%'}}
                    onNewView={(view) => {
                      view.addEventListener('click', (event, item) => {
                        if (item?.datum?.method) {
                          const method = item.datum.method;
                          setBestMethodFilter(prev => { setPage(0); return prev === method ? '' : method; });
                        }
                      });
                    }}
                  />
                </div>
              </div>
            )}
          </div>
        </Section>
      )}

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
          {bestMethodFilter && (
            <button
              onClick={() => { setBestMethodFilter(''); setPage(0); }}
              className="inline-flex items-center gap-1.5 px-3 py-2 bg-blue-100 text-blue-700 dark:bg-blue-900/40 dark:text-blue-400 rounded-lg text-sm font-medium hover:bg-blue-200 dark:hover:bg-blue-900/60 transition-colors"
            >
              Method: {bestMethodFilter}
              <span className="text-xs">{'\u2715'}</span>
            </button>
          )}
          {(complexityFilter || intermittentFilter || bestMethodFilter || search) && (
            <button
              onClick={() => { setSearch(''); setComplexityFilter(''); setIntermittentFilter(''); setBestMethodFilter(''); setPage(0); }}
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

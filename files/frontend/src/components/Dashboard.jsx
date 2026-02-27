/**
 * Dashboard Component
 *
 * Top-level view to review all parts/groups of time series.
 * Shows summary cards, filterable table, and aggregate charts.
 * All sections are individually collapsible.
 */

import React, { useState, useEffect, useMemo, useCallback } from 'react';
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

  // Filters
  const [search, setSearch] = useState('');
  const [complexityFilter, setComplexityFilter] = useState('');
  const [intermittentFilter, setIntermittentFilter] = useState('');
  const [bestMethodFilter, setBestMethodFilter] = useState('');
  const [sortField, setSortField] = useState('unique_id');
  const [sortDir, setSortDir] = useState('asc');

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
      if (seriesRes.status === 'fulfilled') setSeries(seriesRes.value.data);
      if (analyticsRes.status === 'fulfilled') setAnalytics(analyticsRes.value.data);
      setLoading(false);
    } catch (err) {
      setError(err.message);
      setLoading(false);
    }
  };

  const filteredSeries = useMemo(() => {
    let result = series;
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
    result.sort((a, b) => {
      let va = a[sortField], vb = b[sortField];
      if (typeof va === 'string') va = va.toLowerCase();
      if (typeof vb === 'string') vb = vb.toLowerCase();
      if (va < vb) return sortDir === 'asc' ? -1 : 1;
      if (va > vb) return sortDir === 'asc' ? 1 : -1;
      return 0;
    });
    return result;
  }, [series, search, complexityFilter, intermittentFilter, bestMethodFilter, sortField, sortDir]);

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

  if (loading) return <div className="flex items-center justify-center h-64"><div className="text-xl text-gray-500 dark:text-gray-400 animate-pulse">Loading dashboard...</div></div>;
  if (error) return <div className="flex items-center justify-center h-64"><div className="text-xl text-red-600 dark:text-red-400">Error: {error}</div></div>;

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
      {(complexitySpec || bestMethodSpec) && (
        <Section title="Charts" storageKey="dash_charts_open" id="dash-charts">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            {complexitySpec && (
              <div>
                <h3 className="text-sm font-semibold text-gray-600 dark:text-gray-400 mb-2">Complexity Distribution</h3>
                <div className="w-full flex justify-center">
                  <VegaLite
                    spec={complexitySpec}
                    actions={false}
                    renderer="svg"
                    style={{display:'block'}}
                    onNewView={(view) => {
                      view.addEventListener('click', (event, item) => {
                        if (item?.datum?.level) {
                          const level = item.datum.level;
                          setComplexityFilter(prev => { setPage(0); return prev === level ? '' : level; });
                        }
                      });
                    }}
                  />
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

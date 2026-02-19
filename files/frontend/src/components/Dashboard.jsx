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
import axios from 'axios';

const API_BASE_URL = '/api';

/** Collapsible section wrapper — matches Method Selection Rationale pattern */
const Section = ({ title, storageKey, defaultOpen = true, children }) => {
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
    <div className="mb-6 bg-white rounded-lg shadow">
      <button
        onClick={toggle}
        className="w-full flex items-center justify-between p-4 text-left hover:bg-gray-50 transition-colors rounded-lg"
      >
        <h2 className="text-lg font-semibold">{title}</h2>
        <span className="text-gray-400 text-xl">{open ? '▲' : '▼'}</span>
      </button>
      {open && <div className="px-4 pb-4 sm:px-6 sm:pb-6">{children}</div>}
    </div>
  );
};

/** Inline SVG sparkline — historical (gray) + forecast (dashed blue) */
const Sparkline = ({ historical = [], forecast = [], width = 100, height = 28 }) => {
  const all = [...historical, ...forecast];
  if (all.length === 0) return <span className="text-gray-300 text-xs">-</span>;

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
  const [series, setSeries] = useState([]);
  const [analytics, setAnalytics] = useState(null);
  const [bestMethods, setBestMethods] = useState([]);
  const [sparklineData, setSparklineData] = useState({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  // Filters
  const [search, setSearch] = useState('');
  const [complexityFilter, setComplexityFilter] = useState('');
  const [intermittentFilter, setIntermittentFilter] = useState('');
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
      const [seriesRes, analyticsRes, bestMethodsRes] = await Promise.allSettled([
        axios.get(`${API_BASE_URL}/series`, { params: { limit: 50000 } }),
        axios.get(`${API_BASE_URL}/analytics`),
        axios.get(`${API_BASE_URL}/best-methods`)
      ]);
      if (seriesRes.status === 'fulfilled') setSeries(seriesRes.value.data);
      if (analyticsRes.status === 'fulfilled') setAnalytics(analyticsRes.value.data);
      if (bestMethodsRes.status === 'fulfilled') setBestMethods(bestMethodsRes.value.data);
      setLoading(false);
    } catch (err) {
      setError(err.message);
      setLoading(false);
    }
  };

  const enrichedSeries = useMemo(() => {
    const bestMethodMap = {};
    bestMethods.forEach(bm => { bestMethodMap[bm.unique_id] = bm; });
    return series.map(s => ({
      ...s,
      best_method: bestMethodMap[s.unique_id]?.best_method || '-',
      best_score: bestMethodMap[s.unique_id]?.best_score || null
    }));
  }, [series, bestMethods]);

  const filteredSeries = useMemo(() => {
    let result = enrichedSeries;
    if (search) {
      const lower = search.toLowerCase();
      result = result.filter(s => s.unique_id.toLowerCase().includes(lower));
    }
    if (complexityFilter) result = result.filter(s => s.complexity_level === complexityFilter);
    if (intermittentFilter !== '') {
      const isInt = intermittentFilter === 'true';
      result = result.filter(s => s.is_intermittent === isInt);
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
  }, [enrichedSeries, search, complexityFilter, intermittentFilter, sortField, sortDir]);

  const pagedSeries = filteredSeries.slice(page * pageSize, (page + 1) * pageSize);
  const totalPages = Math.ceil(filteredSeries.length / pageSize);

  const fetchSparklines = useCallback(async (ids) => {
    if (ids.length === 0) return;
    try {
      const res = await axios.post(`${API_BASE_URL}/sparklines`, ids);
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
  const sortIndicator = (field) => sortField === field ? (sortDir === 'asc' ? ' ▲' : ' ▼') : '';

  const complexitySpec = useMemo(() => {
    if (!analytics?.complexity_distribution) return null;
    const data = Object.entries(analytics.complexity_distribution).map(([k, v]) => ({ level: k, count: v }));
    return {
      $schema: 'https://vega.github.io/schema/vega-lite/v5.json',
      width: 280, height: 200,
      data: { values: data },
      mark: 'arc',
      encoding: {
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
      }
    };
  }, [analytics]);

  const bestMethodSpec = useMemo(() => {
    if (!analytics?.best_method_distribution) return null;
    const data = Object.entries(analytics.best_method_distribution)
      .map(([k, v]) => ({ method: k, count: v }))
      .sort((a, b) => b.count - a.count);
    return {
      $schema: 'https://vega.github.io/schema/vega-lite/v5.json',
      width: 'container', height: 250,
      title: { text: `Best method across ${analytics.best_method_total_series || 0} backtested series`, fontSize: 12 },
      data: { values: data },
      mark: { type: 'bar', cornerRadiusEnd: 4 },
      encoding: {
        y: { field: 'method', type: 'nominal', sort: '-x', title: 'Method' },
        x: { field: 'count', type: 'quantitative', title: 'Series Won' },
        color: { field: 'method', type: 'nominal', legend: null, scale: { scheme: 'tableau10' } },
        tooltip: [
          { field: 'method', type: 'nominal', title: 'Method' },
          { field: 'count', type: 'quantitative', title: 'Series Won' }
        ]
      }
    };
  }, [analytics]);

  if (loading) return <div className="flex items-center justify-center h-64"><div className="text-xl text-gray-500 animate-pulse">Loading dashboard...</div></div>;
  if (error) return <div className="flex items-center justify-center h-64"><div className="text-xl text-red-600">Error: {error}</div></div>;

  return (
    <div className="p-4 sm:p-6">
      <h1 className="text-2xl sm:text-3xl font-bold mb-6">Forecasting Dashboard</h1>

      {/* Summary Cards */}
      {analytics && (
        <Section title="Summary" storageKey="dash_summary_open">
          <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 xl:grid-cols-7 gap-3">
            {[
              { label: 'Total Series', value: analytics.total_series?.toLocaleString(), color: 'text-blue-600' },
              { label: 'Backtested', value: (analytics.best_method_total_series || 0).toLocaleString(), color: 'text-emerald-600' },
              { label: 'Seasonal', value: analytics.seasonal_count, color: '' },
              { label: 'Trending', value: analytics.trending_count, color: '' },
              { label: 'Intermittent', value: analytics.intermittent_count, color: '' },
              { label: 'Avg Obs', value: analytics.avg_observations?.toFixed(0), color: '' },
              { label: 'Outlier Adj.', value: (analytics.outlier_adjusted_count || 0).toLocaleString(), color: 'text-orange-600' },
            ].map(({ label, value, color }) => (
              <div key={label} className="bg-gray-50 rounded-lg p-3 border border-gray-100">
                <div className="text-xs text-gray-500 mb-1">{label}</div>
                <div className={`text-xl sm:text-2xl font-bold ${color}`}>{value}</div>
              </div>
            ))}
          </div>
        </Section>
      )}

      {/* Charts */}
      {(complexitySpec || bestMethodSpec) && (
        <Section title="Charts" storageKey="dash_charts_open">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            {complexitySpec && (
              <div>
                <h3 className="text-sm font-semibold text-gray-600 mb-2">Complexity Distribution</h3>
                <div className="w-full flex justify-center"><VegaLite spec={complexitySpec} actions={false} renderer="svg" style={{display:'block'}} /></div>
              </div>
            )}
            {bestMethodSpec && (
              <div>
                <h3 className="text-sm font-semibold text-gray-600 mb-2">Best Method Distribution</h3>
                <div className="w-full overflow-x-auto"><VegaLite spec={bestMethodSpec} actions={false} renderer="svg" style={{width:'100%'}} /></div>
              </div>
            )}
          </div>
        </Section>
      )}

      {/* Series Table */}
      <Section title={`Series Table (${filteredSeries.length} series)`} storageKey="dash_table_open">
        {/* Filters */}
        <div className="flex flex-wrap gap-3 mb-4">
          <input
            type="text"
            placeholder="Search by ID..."
            value={search}
            onChange={e => { setSearch(e.target.value); setPage(0); }}
            className="border border-gray-300 rounded-lg px-3 py-2 text-sm w-full sm:w-56 focus:outline-none focus:ring-2 focus:ring-blue-400"
          />
          <select
            value={complexityFilter}
            onChange={e => { setComplexityFilter(e.target.value); setPage(0); }}
            className="border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-400"
          >
            <option value="">All Complexity</option>
            <option value="low">Low</option>
            <option value="medium">Medium</option>
            <option value="high">High</option>
          </select>
          <select
            value={intermittentFilter}
            onChange={e => { setIntermittentFilter(e.target.value); setPage(0); }}
            className="border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-400"
          >
            <option value="">All Types</option>
            <option value="true">Intermittent</option>
            <option value="false">Non-Intermittent</option>
          </select>
        </div>

        {/* Table */}
        <div className="overflow-x-auto rounded-lg border border-gray-200">
          <table className="min-w-full divide-y divide-gray-200 text-sm">
            <thead className="bg-gray-50">
              <tr>
                {[
                  ['unique_id', 'Series ID'],
                  ['n_observations', 'Obs'],
                  ['complexity_level', 'Complexity'],
                  ['is_intermittent', 'Interm.'],
                  ['has_seasonality', 'Seasonal'],
                  ['has_trend', 'Trend'],
                  ['mean', 'Mean'],
                  [null, 'Demand'],
                  ['n_outliers', 'Adj.'],
                  ['best_method', 'Best Method']
                ].map(([field, label]) => (
                  <th
                    key={label}
                    onClick={field ? () => handleSort(field) : undefined}
                    className={`px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase whitespace-nowrap ${field ? 'cursor-pointer hover:bg-gray-100 select-none' : ''}`}
                  >
                    {label}{field ? sortIndicator(field) : ''}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-200 bg-white">
              {pagedSeries.map(s => (
                <tr
                  key={s.unique_id}
                  onClick={() => navigate(`/series/${encodeURIComponent(s.unique_id)}`)}
                  className="hover:bg-blue-50 cursor-pointer transition-colors"
                >
                  <td className="px-3 py-2 font-medium text-blue-600 whitespace-nowrap">{s.unique_id}</td>
                  <td className="px-3 py-2 text-right">{s.n_observations}</td>
                  <td className="px-3 py-2">
                    <span className={`px-1.5 py-0.5 rounded text-xs font-medium ${
                      s.complexity_level === 'high' ? 'bg-red-100 text-red-700' :
                      s.complexity_level === 'medium' ? 'bg-yellow-100 text-yellow-700' :
                      'bg-green-100 text-green-700'
                    }`}>
                      {s.complexity_level}
                    </span>
                  </td>
                  <td className="px-3 py-2 text-center">{s.is_intermittent ? '✓' : '-'}</td>
                  <td className="px-3 py-2 text-center">{s.has_seasonality ? '✓' : '-'}</td>
                  <td className="px-3 py-2 text-center">{s.has_trend ? '✓' : '-'}</td>
                  <td className="px-3 py-2 text-right font-mono">{s.mean?.toFixed(1)}</td>
                  <td className="px-3 py-2">
                    <Sparkline historical={sparklineData[s.unique_id]?.historical || []} forecast={sparklineData[s.unique_id]?.forecast || []} />
                  </td>
                  <td className="px-3 py-2 text-center">
                    {s.has_outlier_corrections ? (
                      <span className="bg-orange-100 text-orange-700 px-1.5 py-0.5 rounded text-xs font-medium">{s.n_outliers}</span>
                    ) : <span className="text-gray-300">-</span>}
                  </td>
                  <td className="px-3 py-2 font-medium whitespace-nowrap">{s.best_method}</td>
                </tr>
              ))}
            </tbody>
          </table>

          {/* Pagination */}
          {totalPages > 1 && (
            <div className="flex items-center justify-between px-4 py-3 bg-gray-50 border-t border-gray-200">
              <button onClick={() => setPage(Math.max(0, page - 1))} disabled={page === 0}
                className="px-3 py-1.5 border rounded-lg text-sm disabled:opacity-40 hover:bg-gray-100 transition-colors">
                ← Previous
              </button>
              <span className="text-sm text-gray-500">Page {page + 1} of {totalPages}</span>
              <button onClick={() => setPage(Math.min(totalPages - 1, page + 1))} disabled={page >= totalPages - 1}
                className="px-3 py-1.5 border rounded-lg text-sm disabled:opacity-40 hover:bg-gray-100 transition-colors">
                Next →
              </button>
            </div>
          )}
        </div>
      </Section>
    </div>
  );
};

export default Dashboard;

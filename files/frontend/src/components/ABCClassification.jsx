/**
 * ABCClassification.jsx
 * Manage configurable ABC/XYZ classifications.
 * - Left panel: list of configurations
 * - Right panel: selected config detail, editor, results + Pareto chart
 */

import { useState, useEffect, useMemo, useCallback } from 'react';
import Plot from 'react-plotly.js';
import { useTheme } from '../contexts/ThemeContext';
import { useLocale } from '../contexts/LocaleContext';
import { formatNumber } from '../utils/formatting';
import api from '../utils/api';

const METRIC_LABELS = { hits: 'Order Hits', demand: 'Demand Volume', value: 'Demand Value' };
const METHOD_LABELS = { cumulative_pct: 'Cumulative %', rank_pct: 'Rank %', rank_absolute: 'Rank Absolute' };
const GRANULARITY_LABELS = { item_site: 'Item / Site', item: 'Item (all sites)' };
const CLASS_COLORS = { A: '#22c55e', B: '#eab308', C: '#f97316', D: '#ef4444', X: '#3b82f6', Y: '#a855f7', Z: '#ec4899' };

const defaultConfig = {
  name: '',
  metric: 'demand',
  lookback_months: 12,
  granularity: 'item_site',
  method: 'cumulative_pct',
  class_labels: ['A', 'B', 'C'],
  thresholds: [80, 95],
  segment_id: null,
  is_active: true,
};

const Badge = ({ label, color }) => (
  <span
    className="inline-flex items-center px-2 py-0.5 rounded text-xs font-semibold text-white"
    style={{ backgroundColor: color || '#6b7280' }}
  >
    {label}
  </span>
);

export const ABCClassification = () => {
  const { isDark } = useTheme();
  const { locale } = useLocale();

  const [configs, setConfigs] = useState([]);
  const [selected, setSelected] = useState(null);
  const [editing, setEditing] = useState(null); // null = not editing, object = form state
  const [results, setResults] = useState(null);
  const [summary, setSummary] = useState(null);
  const [segments, setSegments] = useState([]);
  const [priceAvailable, setPriceAvailable] = useState(null);
  const [loading, setLoading] = useState(true);
  const [running, setRunning] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState(null);

  // ── Load data ───────────────────────────────────────────────────────
  const loadConfigs = useCallback(async () => {
    try {
      const res = await api.get('/abc/configurations');
      setConfigs(res.data || []);
    } catch (err) {
      console.error('Failed to load ABC configs:', err);
      setError(err.response?.data?.detail || err.message);
    }
  }, []);

  useEffect(() => {
    const init = async () => {
      setLoading(true);
      try {
        const [cfgRes, segRes, priceRes] = await Promise.allSettled([
          api.get('/abc/configurations'),
          api.get('/segments'),
          api.get('/abc/price-available'),
        ]);
        if (cfgRes.status === 'fulfilled') setConfigs(cfgRes.value.data || []);
        if (segRes.status === 'fulfilled') setSegments(segRes.value.data || []);
        if (priceRes.status === 'fulfilled') setPriceAvailable(priceRes.value.data);
      } catch (err) {
        setError(err.message);
      } finally {
        setLoading(false);
      }
    };
    init();
  }, []);

  // Load results when a config is selected
  useEffect(() => {
    if (!selected) { setResults(null); setSummary(null); return; }
    const load = async () => {
      try {
        const [resRes, sumRes] = await Promise.allSettled([
          api.get(`/abc/results/${selected.id}`, { params: { limit: 10000 } }),
          api.get(`/abc/summary/${selected.id}`),
        ]);
        if (resRes.status === 'fulfilled') setResults(resRes.value.data);
        if (sumRes.status === 'fulfilled') setSummary(sumRes.value.data);
      } catch { /* non-critical */ }
    };
    load();
  }, [selected?.id]);

  // ── Actions ─────────────────────────────────────────────────────────
  const handleRun = async (configId) => {
    setRunning(true);
    try {
      await api.post(`/abc/run/${configId}`);
      await loadConfigs();
      // Reload results if this is the selected config
      if (selected?.id === configId) {
        const [resRes, sumRes] = await Promise.allSettled([
          api.get(`/abc/results/${configId}`, { params: { limit: 10000 } }),
          api.get(`/abc/summary/${configId}`),
        ]);
        if (resRes.status === 'fulfilled') setResults(resRes.value.data);
        if (sumRes.status === 'fulfilled') setSummary(sumRes.value.data);
      }
    } catch (err) {
      setError(err.response?.data?.detail || err.message);
    } finally {
      setRunning(false);
    }
  };

  const handleRunAll = async () => {
    setRunning(true);
    try {
      await api.post('/abc/run-all');
      await loadConfigs();
      if (selected) {
        const [resRes, sumRes] = await Promise.allSettled([
          api.get(`/abc/results/${selected.id}`, { params: { limit: 10000 } }),
          api.get(`/abc/summary/${selected.id}`),
        ]);
        if (resRes.status === 'fulfilled') setResults(resRes.value.data);
        if (sumRes.status === 'fulfilled') setSummary(sumRes.value.data);
      }
    } catch (err) {
      setError(err.response?.data?.detail || err.message);
    } finally {
      setRunning(false);
    }
  };

  const handleSave = async () => {
    if (!editing) return;
    setSaving(true);
    setError(null);
    try {
      const payload = {
        ...editing,
        thresholds: editing.thresholds.map(Number),
        lookback_months: Number(editing.lookback_months),
        segment_id: editing.segment_id || null,
      };
      if (editing.id) {
        await api.put(`/abc/configurations/${editing.id}`, payload);
      } else {
        await api.post('/abc/configurations', payload);
      }
      await loadConfigs();
      setEditing(null);
    } catch (err) {
      setError(err.response?.data?.detail || err.message);
    } finally {
      setSaving(false);
    }
  };

  const handleDelete = async (id) => {
    if (!confirm('Delete this classification configuration and all its results?')) return;
    try {
      await api.delete(`/abc/configurations/${id}`);
      if (selected?.id === id) { setSelected(null); setResults(null); setSummary(null); }
      await loadConfigs();
    } catch (err) {
      setError(err.response?.data?.detail || err.message);
    }
  };

  // ── Editor helpers ──────────────────────────────────────────────────
  const updateEditing = (key, val) => setEditing(prev => ({ ...prev, [key]: val }));

  const addLabel = () => {
    if (!editing) return;
    const labels = [...editing.class_labels, String.fromCharCode(65 + editing.class_labels.length)];
    const thresholds = [...editing.thresholds, editing.thresholds.length > 0 ? editing.thresholds[editing.thresholds.length - 1] + 5 : 80];
    updateEditing('class_labels', labels);
    updateEditing('thresholds', thresholds);
  };

  const removeLabel = (idx) => {
    if (!editing || editing.class_labels.length <= 2) return;
    const labels = editing.class_labels.filter((_, i) => i !== idx);
    // Remove the corresponding threshold (thresholds has N-1 entries; remove the one matching idx, but cap)
    const thresholds = editing.thresholds.filter((_, i) => i !== Math.min(idx, editing.thresholds.length - 1));
    updateEditing('class_labels', labels);
    updateEditing('thresholds', thresholds);
  };

  // ── Pareto chart ────────────────────────────────────────────────────
  const paretoChart = useMemo(() => {
    if (!results?.results || results.results.length === 0) return null;
    const data = [...results.results].sort((a, b) => b.metric_value - a.metric_value);
    const total = data.reduce((s, d) => s + (d.metric_value || 0), 0);
    let cumSum = 0;
    const cumPcts = data.map(d => {
      cumSum += d.metric_value || 0;
      return total > 0 ? (cumSum / total) * 100 : 0;
    });

    const barColors = data.map(d => CLASS_COLORS[d.class_label] || '#6b7280');

    return {
      data: [
        {
          type: 'bar',
          x: data.map((_, i) => i),
          y: data.map(d => d.metric_value),
          marker: { color: barColors },
          name: 'Metric Value',
          hovertemplate: '%{customdata}<br>Value: %{y:,.0f}<extra></extra>',
          customdata: data.map(d => d.unique_id),
        },
        {
          type: 'scatter',
          mode: 'lines',
          x: data.map((_, i) => i),
          y: cumPcts,
          yaxis: 'y2',
          name: 'Cumulative %',
          line: { color: '#3b82f6', width: 2 },
          hovertemplate: 'Cumulative: %{y:.1f}%<extra></extra>',
        },
      ],
      layout: {
        paper_bgcolor: 'rgba(0,0,0,0)',
        plot_bgcolor: 'rgba(0,0,0,0)',
        font: { color: isDark ? '#d1d5db' : '#374151', size: 11 },
        height: 320,
        margin: { t: 30, r: 50, b: 30, l: 60 },
        xaxis: {
          showticklabels: false,
          gridcolor: isDark ? '#374151' : '#e5e7eb',
        },
        yaxis: {
          title: 'Metric Value',
          gridcolor: isDark ? '#374151' : '#e5e7eb',
          color: isDark ? '#d1d5db' : '#374151',
        },
        yaxis2: {
          title: 'Cumulative %',
          overlaying: 'y',
          side: 'right',
          range: [0, 105],
          color: '#3b82f6',
          gridcolor: 'transparent',
        },
        showlegend: false,
        bargap: 0,
      },
    };
  }, [results, isDark]);

  // ── Render ──────────────────────────────────────────────────────────
  const inputCls = 'w-full border border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-white rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-400';
  const selectCls = inputCls;
  const btnPrimary = 'px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-lg text-sm font-medium transition-colors disabled:opacity-50';
  const btnSecondary = 'px-4 py-2 border border-gray-300 dark:border-gray-600 text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-700 rounded-lg text-sm font-medium transition-colors';
  const btnDanger = 'px-3 py-1.5 text-red-600 dark:text-red-400 hover:bg-red-50 dark:hover:bg-red-900/20 rounded text-xs font-medium transition-colors';

  if (loading) return (
    <div className="flex items-center justify-center h-64">
      <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600" />
    </div>
  );

  return (
    <div className="p-4 sm:p-6 max-w-[1600px] mx-auto">
      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-gray-900 dark:text-white">Classifications</h1>
          <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
            Configure and run ABC / XYZ classifications on demand data
          </p>
        </div>
        <div className="flex items-center gap-3">
          <button
            onClick={() => setEditing({ ...defaultConfig })}
            className={btnPrimary}
            disabled={!!editing}
          >
            + New Classification
          </button>
          <button
            onClick={handleRunAll}
            className={btnSecondary}
            disabled={running || configs.length === 0}
          >
            {running ? 'Running...' : 'Run All'}
          </button>
        </div>
      </div>

      {error && (
        <div className="mb-4 p-3 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg text-sm text-red-700 dark:text-red-400 flex items-center justify-between">
          <span>{error}</span>
          <button onClick={() => setError(null)} className="text-red-500 hover:text-red-700 ml-4">{'\u2715'}</button>
        </div>
      )}

      <div className="flex flex-col lg:flex-row gap-6">
        {/* ── Left Panel: Config List ──────────────────────────────────── */}
        <div className="w-full lg:w-80 flex-shrink-0 space-y-3">
          {configs.length === 0 && !editing && (
            <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6 text-center text-gray-400 dark:text-gray-500">
              <p className="text-sm">No classifications configured yet.</p>
              <p className="text-xs mt-1">Click "New Classification" to get started.</p>
            </div>
          )}
          {configs.map(cfg => (
            <div
              key={cfg.id}
              onClick={() => { setSelected(cfg); setEditing(null); }}
              className={`bg-white dark:bg-gray-800 rounded-lg shadow p-4 cursor-pointer transition-all border-2 ${
                selected?.id === cfg.id
                  ? 'border-blue-500 dark:border-blue-400'
                  : 'border-transparent hover:border-gray-200 dark:hover:border-gray-600'
              }`}
            >
              <div className="flex items-center justify-between mb-2">
                <h3 className="font-semibold text-gray-900 dark:text-white text-sm truncate">{cfg.name}</h3>
                {!cfg.is_active && (
                  <span className="text-xs px-1.5 py-0.5 bg-gray-100 dark:bg-gray-700 text-gray-500 dark:text-gray-400 rounded">Inactive</span>
                )}
              </div>
              <div className="flex items-center gap-2 flex-wrap">
                <Badge label={METRIC_LABELS[cfg.metric] || cfg.metric} color="#6366f1" />
                <Badge label={METHOD_LABELS[cfg.method] || cfg.method} color="#0891b2" />
                <span className="text-xs text-gray-500 dark:text-gray-400">{cfg.lookback_months}mo</span>
              </div>
              <div className="flex items-center gap-1.5 mt-2">
                {(cfg.class_labels || []).map(lbl => (
                  <span
                    key={lbl}
                    className="inline-flex items-center justify-center w-6 h-6 rounded text-xs font-bold text-white"
                    style={{ backgroundColor: CLASS_COLORS[lbl] || '#6b7280' }}
                  >
                    {lbl}
                  </span>
                ))}
              </div>
              {cfg.result_count > 0 && (
                <p className="text-xs text-gray-400 dark:text-gray-500 mt-2">
                  {formatNumber(cfg.result_count, locale, 0)} series classified
                </p>
              )}
            </div>
          ))}
        </div>

        {/* ── Right Panel ──────────────────────────────────────────────── */}
        <div className="flex-1 min-w-0">
          {/* Editor Mode */}
          {editing && (
            <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6">
              <h2 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
                {editing.id ? 'Edit Classification' : 'New Classification'}
              </h2>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                {/* Name */}
                <div className="md:col-span-2">
                  <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Name</label>
                  <input
                    type="text"
                    value={editing.name}
                    onChange={e => updateEditing('name', e.target.value)}
                    placeholder="e.g. Demand Volume, Order Hits"
                    className={inputCls}
                  />
                </div>

                {/* Metric */}
                <div>
                  <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Metric</label>
                  <select value={editing.metric} onChange={e => updateEditing('metric', e.target.value)} className={selectCls}>
                    <option value="hits">Hits (order count)</option>
                    <option value="demand">Demand (quantity sum)</option>
                    <option value="value" disabled={priceAvailable && !priceAvailable.available}>
                      Value (qty x price){priceAvailable && !priceAvailable.available ? ' - no price data' : ''}
                    </option>
                  </select>
                </div>

                {/* Lookback */}
                <div>
                  <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Lookback (months)</label>
                  <input
                    type="number"
                    min={1}
                    max={120}
                    value={editing.lookback_months}
                    onChange={e => updateEditing('lookback_months', e.target.value)}
                    className={inputCls}
                  />
                </div>

                {/* Granularity */}
                <div>
                  <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Granularity</label>
                  <select value={editing.granularity} onChange={e => updateEditing('granularity', e.target.value)} className={selectCls}>
                    <option value="item_site">Per Item / Site</option>
                    <option value="item">Per Item (propagate to all sites)</option>
                  </select>
                </div>

                {/* Method */}
                <div>
                  <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Classification Method</label>
                  <select value={editing.method} onChange={e => updateEditing('method', e.target.value)} className={selectCls}>
                    <option value="cumulative_pct">Cumulative Percentage</option>
                    <option value="rank_pct">Rank Percentage</option>
                    <option value="rank_absolute">Rank Absolute</option>
                  </select>
                </div>

                {/* Segment scope */}
                <div>
                  <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Segment Scope (optional)</label>
                  <select value={editing.segment_id || ''} onChange={e => updateEditing('segment_id', e.target.value ? Number(e.target.value) : null)} className={selectCls}>
                    <option value="">All Series</option>
                    {segments.map(s => <option key={s.id} value={s.id}>{s.name}</option>)}
                  </select>
                </div>

                {/* Active */}
                <div className="flex items-center gap-2">
                  <input
                    type="checkbox"
                    checked={editing.is_active}
                    onChange={e => updateEditing('is_active', e.target.checked)}
                    className="w-4 h-4 rounded border-gray-300 dark:border-gray-600 text-blue-600 focus:ring-blue-500"
                  />
                  <label className="text-sm text-gray-700 dark:text-gray-300">Active</label>
                </div>

                {/* Class Labels + Thresholds */}
                <div className="md:col-span-2">
                  <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-2">Class Labels & Thresholds</label>
                  <div className="space-y-2">
                    {editing.class_labels.map((lbl, idx) => (
                      <div key={idx} className="flex items-center gap-3">
                        <input
                          type="text"
                          value={lbl}
                          onChange={e => {
                            const labels = [...editing.class_labels];
                            labels[idx] = e.target.value;
                            updateEditing('class_labels', labels);
                          }}
                          className="w-16 border border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-white rounded px-2 py-1.5 text-sm text-center font-bold"
                          maxLength={5}
                        />
                        {idx < editing.class_labels.length - 1 ? (
                          <div className="flex items-center gap-2">
                            <span className="text-xs text-gray-500 dark:text-gray-400">
                              {editing.method === 'cumulative_pct' ? '\u2264' : editing.method === 'rank_pct' ? 'Top' : 'Rank \u2264'}
                            </span>
                            <input
                              type="number"
                              value={editing.thresholds[idx] ?? ''}
                              onChange={e => {
                                const thresholds = [...editing.thresholds];
                                thresholds[idx] = e.target.value;
                                updateEditing('thresholds', thresholds);
                              }}
                              className="w-20 border border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-white rounded px-2 py-1.5 text-sm"
                            />
                            <span className="text-xs text-gray-500 dark:text-gray-400">
                              {editing.method === 'cumulative_pct' ? '%' : editing.method === 'rank_pct' ? '%' : ''}
                            </span>
                          </div>
                        ) : (
                          <span className="text-xs text-gray-400 dark:text-gray-500 italic">remainder</span>
                        )}
                        {editing.class_labels.length > 2 && (
                          <button onClick={() => removeLabel(idx)} className="text-red-400 hover:text-red-600 text-xs">{'\u2715'}</button>
                        )}
                      </div>
                    ))}
                    <button onClick={addLabel} className="text-xs text-blue-600 dark:text-blue-400 hover:underline">+ Add class</button>
                  </div>
                </div>
              </div>

              {/* Save / Cancel */}
              <div className="flex items-center gap-3 mt-6 pt-4 border-t border-gray-200 dark:border-gray-700">
                <button onClick={handleSave} className={btnPrimary} disabled={saving || !editing.name}>
                  {saving ? 'Saving...' : editing.id ? 'Update' : 'Create'}
                </button>
                <button onClick={() => setEditing(null)} className={btnSecondary}>Cancel</button>
              </div>
            </div>
          )}

          {/* Selected Config Detail + Results */}
          {selected && !editing && (
            <div className="space-y-6">
              {/* Config detail card */}
              <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6">
                <div className="flex items-center justify-between mb-4">
                  <h2 className="text-lg font-semibold text-gray-900 dark:text-white">{selected.name}</h2>
                  <div className="flex items-center gap-2">
                    <button
                      onClick={() => handleRun(selected.id)}
                      className={btnPrimary}
                      disabled={running}
                    >
                      {running ? 'Running...' : 'Run'}
                    </button>
                    <button
                      onClick={() => setEditing({ ...selected })}
                      className={btnSecondary}
                    >
                      Edit
                    </button>
                    <button
                      onClick={() => handleDelete(selected.id)}
                      className={btnDanger}
                    >
                      Delete
                    </button>
                  </div>
                </div>
                <div className="grid grid-cols-2 sm:grid-cols-4 gap-4 text-sm">
                  <div>
                    <span className="text-xs text-gray-500 dark:text-gray-400">Metric</span>
                    <p className="font-medium text-gray-900 dark:text-white">{METRIC_LABELS[selected.metric] || selected.metric}</p>
                  </div>
                  <div>
                    <span className="text-xs text-gray-500 dark:text-gray-400">Method</span>
                    <p className="font-medium text-gray-900 dark:text-white">{METHOD_LABELS[selected.method] || selected.method}</p>
                  </div>
                  <div>
                    <span className="text-xs text-gray-500 dark:text-gray-400">Lookback</span>
                    <p className="font-medium text-gray-900 dark:text-white">{selected.lookback_months} months</p>
                  </div>
                  <div>
                    <span className="text-xs text-gray-500 dark:text-gray-400">Granularity</span>
                    <p className="font-medium text-gray-900 dark:text-white">{GRANULARITY_LABELS[selected.granularity] || selected.granularity}</p>
                  </div>
                </div>
                {/* Class distribution badges */}
                {summary?.distribution && (
                  <div className="mt-4 flex items-center gap-4 flex-wrap">
                    {summary.distribution.map(d => (
                      <div key={d.class_label} className="flex items-center gap-2">
                        <span
                          className="inline-flex items-center justify-center w-7 h-7 rounded text-xs font-bold text-white"
                          style={{ backgroundColor: CLASS_COLORS[d.class_label] || '#6b7280' }}
                        >
                          {d.class_label}
                        </span>
                        <div className="text-xs">
                          <span className="font-semibold text-gray-900 dark:text-white">{formatNumber(d.count, locale, 0)}</span>
                          <span className="text-gray-400 dark:text-gray-500 ml-1">({formatNumber(d.pct, locale, 1)}%)</span>
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </div>

              {/* Pareto Chart */}
              {paretoChart && (
                <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-4">
                  <h3 className="text-sm font-semibold text-gray-600 dark:text-gray-400 mb-2">Pareto Chart</h3>
                  <Plot
                    data={paretoChart.data}
                    layout={paretoChart.layout}
                    config={{ responsive: true, displayModeBar: 'hover', displaylogo: false }}
                    useResizeHandler
                    style={{ width: '100%' }}
                  />
                </div>
              )}

              {/* Results Table */}
              {results?.results && results.results.length > 0 && (
                <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-4">
                  <h3 className="text-sm font-semibold text-gray-600 dark:text-gray-400 mb-3">
                    Results ({formatNumber(results.results.length, locale, 0)} series)
                  </h3>
                  <div className="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700 max-h-96 overflow-y-auto">
                    <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-700 text-sm">
                      <thead className="bg-gray-50 dark:bg-gray-900 sticky top-0">
                        <tr>
                          <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase">Series</th>
                          <th className="px-3 py-2 text-center text-xs font-medium text-gray-500 dark:text-gray-400 uppercase">Class</th>
                          <th className="px-3 py-2 text-right text-xs font-medium text-gray-500 dark:text-gray-400 uppercase">Metric</th>
                          <th className="px-3 py-2 text-right text-xs font-medium text-gray-500 dark:text-gray-400 uppercase">Rank</th>
                          <th className="px-3 py-2 text-right text-xs font-medium text-gray-500 dark:text-gray-400 uppercase">Cum %</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-gray-200 dark:divide-gray-700 bg-white dark:bg-gray-800">
                        {results.results.slice(0, 500).map((r, i) => (
                          <tr key={i} className="hover:bg-gray-50 dark:hover:bg-gray-700/50">
                            <td className="px-3 py-1.5 font-mono text-xs text-gray-700 dark:text-gray-300">{r.unique_id}</td>
                            <td className="px-3 py-1.5 text-center">
                              <span
                                className="inline-flex items-center justify-center w-6 h-6 rounded text-xs font-bold text-white"
                                style={{ backgroundColor: CLASS_COLORS[r.class_label] || '#6b7280' }}
                              >
                                {r.class_label}
                              </span>
                            </td>
                            <td className="px-3 py-1.5 text-right font-mono text-gray-700 dark:text-gray-300">{formatNumber(r.metric_value, locale, 0)}</td>
                            <td className="px-3 py-1.5 text-right text-gray-600 dark:text-gray-400">{r.rank}</td>
                            <td className="px-3 py-1.5 text-right text-gray-600 dark:text-gray-400">{r.cumulative_pct != null ? `${r.cumulative_pct.toFixed(1)}%` : '-'}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                  {results.results.length > 500 && (
                    <p className="text-xs text-gray-400 dark:text-gray-500 mt-2">Showing first 500 of {formatNumber(results.results.length, locale, 0)} results</p>
                  )}
                </div>
              )}

              {(!results?.results || results.results.length === 0) && (
                <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-8 text-center text-gray-400 dark:text-gray-500">
                  <p className="text-sm">No results yet. Click "Run" to execute this classification.</p>
                </div>
              )}
            </div>
          )}

          {/* Empty state */}
          {!selected && !editing && (
            <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-8 flex items-center justify-center min-h-[300px]">
              <div className="text-center text-gray-400 dark:text-gray-500">
                <p className="text-lg mb-2">Select a classification to view details</p>
                <p className="text-sm">or create a new one to get started</p>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default ABCClassification;

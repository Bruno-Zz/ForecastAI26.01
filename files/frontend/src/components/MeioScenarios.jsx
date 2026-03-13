/**
 * MeioScenarios — MEIO Inventory Optimization Scenario Manager
 *
 * Three-panel layout:
 *   Panel A — Scenario Manager: list, create, edit, delete, clone
 *   Panel B — Run Panel: multi-select + live log stream
 *   Panel C — Comparison Table: side-by-side metrics vs base scenario
 */

import React, { useState, useEffect, useRef, useCallback } from 'react';
import api from '../utils/api';

// ── Utility helpers ────────────────────────────────────────────────────────

const fmt = (v, decimals = 2) =>
  v == null ? '—' : Number(v).toLocaleString(undefined, { minimumFractionDigits: decimals, maximumFractionDigits: decimals });

const fmtPct = (v) => (v == null ? '—' : `${(v * 100).toFixed(1)}%`);

const fmtDate = (s) => {
  if (!s) return '—';
  try { return new Date(s).toLocaleString(); } catch { return s; }
};

// ── Sub-components ─────────────────────────────────────────────────────────

const Spinner = ({ cls = 'w-4 h-4' }) => (
  <svg className={`animate-spin ${cls}`} viewBox="0 0 24 24" fill="none">
    <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
    <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8z" />
  </svg>
);

const StatusBadge = ({ status }) => {
  const map = {
    pending:  { cls: 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400',               label: 'Pending' },
    running:  { cls: 'bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300',             label: 'Running…' },
    success:  { cls: 'bg-emerald-100 dark:bg-emerald-900/30 text-emerald-700 dark:text-emerald-300', label: 'Done' },
    error:    { cls: 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-300',                 label: 'Error' },
    interrupted: { cls: 'bg-amber-100 dark:bg-amber-900/30 text-amber-700 dark:text-amber-300',      label: 'Stopped' },
  };
  const { cls, label } = map[status] || { cls: 'bg-gray-100 dark:bg-gray-700 text-gray-500', label: status };
  return (
    <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium ${cls}`}>
      {status === 'running' && <Spinner cls="w-3 h-3" />}
      {label}
    </span>
  );
};

/** Log viewer — contained scroll, colour-coded lines */
const LogViewer = ({ lines, visible }) => {
  const containerRef = useRef(null);
  const userScrolledUp = useRef(false);
  const handleScroll = () => {
    const el = containerRef.current;
    if (!el) return;
    userScrolledUp.current = el.scrollHeight - el.scrollTop - el.clientHeight > 40;
  };
  useEffect(() => {
    const el = containerRef.current;
    if (visible && el && !userScrolledUp.current) el.scrollTop = el.scrollHeight;
  }, [lines, visible]);
  if (!visible) return null;
  const colorLine = (l) => {
    if (/error|exception|failed|traceback/i.test(l)) return 'text-red-400';
    if (/warning/i.test(l)) return 'text-yellow-300';
    if (/✓|success|complete|done|finished/i.test(l)) return 'text-emerald-400';
    if (/▶|={3,}/i.test(l)) return 'text-purple-300 font-semibold';
    return 'text-gray-300';
  };
  return (
    <div
      ref={containerRef}
      onScroll={handleScroll}
      className="mt-3 bg-gray-900 rounded-lg p-3 max-h-64 overflow-y-auto font-mono text-xs leading-5 border border-gray-700"
    >
      {lines.length === 0
        ? <span className="text-gray-500 italic">Waiting for output…</span>
        : lines.map((l, i) => <div key={i} className={colorLine(l)}>{l || '\u00A0'}</div>)}
    </div>
  );
};

/** Delta cell: show Δ vs base, colour green/red */
const Delta = ({ value, inverse = false }) => {
  if (value == null) return <span className="text-gray-400">—</span>;
  const pos = inverse ? value < 0 : value > 0;
  const neg = inverse ? value > 0 : value < 0;
  const cls = pos ? 'text-emerald-600 dark:text-emerald-400'
    : neg ? 'text-red-600 dark:text-red-400'
    : 'text-gray-500 dark:text-gray-400';
  const sign = value > 0 ? '+' : '';
  return <span className={`font-medium ${cls}`}>{sign}{fmt(value)}</span>;
};

// ── Default param_overrides skeleton shown in the form ─────────────────────

const DEFAULT_OVERRIDES = {
  sku_overrides: {
    demand_multiplier: 1.0,
    lead_time_multiplier: 1.0,
    lt_stddev_multiplier: 1.0,
    fill_rate_target: null,
  },
  config_overrides: {},
  group_target_overrides: [],
};

// ── Scenario modal (create / edit) ─────────────────────────────────────────

const ScenarioModal = ({ scenario, onClose, onSave }) => {
  const isEdit = !!scenario?.scenario_id;
  const [name, setName]         = useState(scenario?.name || '');
  const [desc, setDesc]         = useState(scenario?.description || '');
  const [isBase, setIsBase]     = useState(scenario?.is_base || false);
  const [rawJson, setRawJson]   = useState(
    JSON.stringify(scenario?.param_overrides ?? DEFAULT_OVERRIDES, null, 2)
  );
  const [jsonMode, setJsonMode] = useState(false);
  const [jsonError, setJsonError] = useState(null);

  // Structured override state
  const [demandMult,   setDemandMult]   = useState(() => {
    try { return scenario?.param_overrides?.sku_overrides?.demand_multiplier ?? 1.0; } catch { return 1.0; }
  });
  const [ltMult,       setLtMult]       = useState(() => {
    try { return scenario?.param_overrides?.sku_overrides?.lead_time_multiplier ?? 1.0; } catch { return 1.0; }
  });
  const [ltCvMult,     setLtCvMult]     = useState(() => {
    try { return scenario?.param_overrides?.sku_overrides?.lt_stddev_multiplier ?? 1.0; } catch { return 1.0; }
  });
  const [fillRateTgt,  setFillRateTgt]  = useState(() => {
    try { return scenario?.param_overrides?.sku_overrides?.fill_rate_target ?? ''; } catch { return ''; }
  });

  const buildOverrides = () => ({
    sku_overrides: {
      demand_multiplier:    parseFloat(demandMult) || 1.0,
      lead_time_multiplier: parseFloat(ltMult)     || 1.0,
      lt_stddev_multiplier: parseFloat(ltCvMult)   || 1.0,
      ...(fillRateTgt !== '' ? { fill_rate_target: parseFloat(fillRateTgt) } : {}),
    },
    config_overrides: {},
    group_target_overrides: [],
  });

  const handleSave = () => {
    let overrides;
    if (jsonMode) {
      try { overrides = JSON.parse(rawJson); }
      catch (e) { setJsonError(`Invalid JSON: ${e.message}`); return; }
    } else {
      overrides = buildOverrides();
    }
    if (!name.trim()) { setJsonError('Name is required.'); return; }
    onSave({ name: name.trim(), description: desc.trim(), is_base: isBase, param_overrides: overrides });
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
      <div className="bg-white dark:bg-gray-800 rounded-2xl shadow-2xl w-full max-w-lg border border-gray-200 dark:border-gray-700 max-h-[90vh] overflow-y-auto">
        <div className="flex items-center justify-between px-6 py-4 border-b border-gray-200 dark:border-gray-700">
          <h2 className="text-base font-bold text-gray-900 dark:text-white">
            {isEdit ? 'Edit Scenario' : 'New Scenario'}
          </h2>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-600 dark:hover:text-gray-300 text-xl leading-none">✕</button>
        </div>

        <div className="px-6 py-4 space-y-4">
          {/* Name */}
          <div>
            <label className="block text-xs font-semibold text-gray-600 dark:text-gray-400 mb-1">Name *</label>
            <input
              value={name} onChange={e => setName(e.target.value)}
              placeholder="e.g. High Demand +20%"
              className="w-full px-3 py-2 text-sm rounded-lg border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-700 text-gray-900 dark:text-white focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            />
          </div>
          {/* Description */}
          <div>
            <label className="block text-xs font-semibold text-gray-600 dark:text-gray-400 mb-1">Description</label>
            <textarea
              value={desc} onChange={e => setDesc(e.target.value)} rows={2}
              placeholder="Optional description…"
              className="w-full px-3 py-2 text-sm rounded-lg border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-700 text-gray-900 dark:text-white focus:ring-2 focus:ring-blue-500 focus:border-transparent resize-none"
            />
          </div>
          {/* Base flag */}
          <label className="flex items-center gap-2 cursor-pointer">
            <input type="checkbox" checked={isBase} onChange={e => setIsBase(e.target.checked)}
              className="rounded border-gray-300 dark:border-gray-600 text-blue-600" />
            <span className="text-sm text-gray-700 dark:text-gray-300">Mark as base scenario</span>
          </label>

          {/* Overrides — structured or raw JSON */}
          <div>
            <div className="flex items-center justify-between mb-2">
              <span className="text-xs font-semibold text-gray-600 dark:text-gray-400">Parameter Overrides</span>
              <button
                onClick={() => {
                  if (!jsonMode) setRawJson(JSON.stringify(buildOverrides(), null, 2));
                  setJsonMode(v => !v);
                  setJsonError(null);
                }}
                className="text-xs text-blue-500 dark:text-blue-400 hover:underline"
              >
                {jsonMode ? 'Use form' : 'Edit raw JSON'}
              </button>
            </div>

            {jsonMode ? (
              <textarea
                value={rawJson}
                onChange={e => { setRawJson(e.target.value); setJsonError(null); }}
                rows={8}
                className="w-full px-3 py-2 text-xs font-mono rounded-lg border border-gray-300 dark:border-gray-600 bg-gray-50 dark:bg-gray-900 text-gray-900 dark:text-green-300 focus:ring-2 focus:ring-blue-500 focus:border-transparent resize-y"
              />
            ) : (
              <div className="space-y-3 bg-gray-50 dark:bg-gray-900/50 rounded-lg p-3 border border-gray-200 dark:border-gray-700">
                <div className="grid grid-cols-2 gap-3">
                  <div>
                    <label className="block text-xs text-gray-500 dark:text-gray-400 mb-1">Demand multiplier</label>
                    <input type="number" step="0.01" min="0.1" max="10"
                      value={demandMult} onChange={e => setDemandMult(e.target.value)}
                      className="w-full px-2 py-1.5 text-sm rounded border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-700 text-gray-900 dark:text-white"
                    />
                    <p className="text-[10px] text-gray-400 mt-0.5">1.0 = no change; 1.2 = +20% demand</p>
                  </div>
                  <div>
                    <label className="block text-xs text-gray-500 dark:text-gray-400 mb-1">Lead time multiplier</label>
                    <input type="number" step="0.01" min="0.1" max="10"
                      value={ltMult} onChange={e => setLtMult(e.target.value)}
                      className="w-full px-2 py-1.5 text-sm rounded border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-700 text-gray-900 dark:text-white"
                    />
                    <p className="text-[10px] text-gray-400 mt-0.5">1.0 = no change; 1.5 = +50% lead time</p>
                  </div>
                  <div>
                    <label className="block text-xs text-gray-500 dark:text-gray-400 mb-1">LT std-dev multiplier</label>
                    <input type="number" step="0.01" min="0" max="10"
                      value={ltCvMult} onChange={e => setLtCvMult(e.target.value)}
                      className="w-full px-2 py-1.5 text-sm rounded border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-700 text-gray-900 dark:text-white"
                    />
                    <p className="text-[10px] text-gray-400 mt-0.5">Scales LT variability (safety stock impact)</p>
                  </div>
                  <div>
                    <label className="block text-xs text-gray-500 dark:text-gray-400 mb-1">Fill rate target override</label>
                    <input type="number" step="0.01" min="0" max="1" placeholder="e.g. 0.95"
                      value={fillRateTgt} onChange={e => setFillRateTgt(e.target.value)}
                      className="w-full px-2 py-1.5 text-sm rounded border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-700 text-gray-900 dark:text-white"
                    />
                    <p className="text-[10px] text-gray-400 mt-0.5">Leave blank to use SKU defaults</p>
                  </div>
                </div>
              </div>
            )}

            {jsonError && (
              <p className="mt-1 text-xs text-red-600 dark:text-red-400">{jsonError}</p>
            )}
          </div>
        </div>

        <div className="flex justify-end gap-3 px-6 py-4 border-t border-gray-200 dark:border-gray-700">
          <button onClick={onClose}
            className="px-4 py-2 text-sm text-gray-600 dark:text-gray-400 hover:text-gray-900 dark:hover:text-white">
            Cancel
          </button>
          <button onClick={handleSave}
            className="px-5 py-2 text-sm font-semibold bg-blue-600 hover:bg-blue-700 text-white rounded-lg active:scale-95 transition-colors">
            {isEdit ? 'Save changes' : 'Create scenario'}
          </button>
        </div>
      </div>
    </div>
  );
};

// ── Main component ─────────────────────────────────────────────────────────

export const MeioScenarios = () => {
  // ── State ────────────────────────────────────────────────────────────
  const [scenarios,    setScenarios]    = useState([]);
  const [loading,      setLoading]      = useState(true);
  const [error,        setError]        = useState(null);

  // Panel A — modal
  const [modal,        setModal]        = useState(null);   // null | 'create' | scenarioObj

  // Panel B — run
  const [selected,     setSelected]     = useState(new Set());
  const [runJob,       setRunJob]       = useState(null);
  const [showLogs,     setShowLogs]     = useState(false);
  const eventSourceRef = useRef(null);

  // Panel C — compare
  const [compareData,  setCompareData]  = useState(null);
  const [compareIds,   setCompareIds]   = useState([]);
  const [compareLoading, setCompareLoading] = useState(false);

  // ── Fetch scenarios ──────────────────────────────────────────────────
  const fetchScenarios = useCallback(async () => {
    try {
      const r = await api.get('/meio/scenarios');
      setScenarios(r.data);
    } catch (e) {
      setError(e.response?.data?.detail || e.message);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { fetchScenarios(); }, [fetchScenarios]);

  // ── SSE helper ────────────────────────────────────────────────────────
  const openSSE = useCallback((jobId) => {
    if (eventSourceRef.current) {
      eventSourceRef.current.close();
      eventSourceRef.current = null;
    }
    const token = localStorage.getItem('forecastai_token') || '';
    const es = new EventSource(`/api/pipeline/jobs/${jobId}/stream${token ? `?token=${token}` : ''}`);
    es.onmessage = (e) => {
      try {
        const { line } = JSON.parse(e.data);
        setRunJob(prev => prev ? { ...prev, log_lines: [...(prev.log_lines || []), line] } : prev);
      } catch { /* ignore */ }
    };
    es.addEventListener('done', (e) => {
      try {
        const { status } = JSON.parse(e.data);
        setRunJob(prev => prev ? { ...prev, status, ended_at: new Date().toISOString() } : prev);
        fetchScenarios(); // refresh last_run_at
      } catch { /* ignore */ }
      es.close();
      eventSourceRef.current = null;
    });
    es.onerror = () => { es.close(); eventSourceRef.current = null; };
    eventSourceRef.current = es;
  }, [fetchScenarios]);

  useEffect(() => () => { eventSourceRef.current?.close(); }, []);

  // ── Handlers: scenario CRUD ──────────────────────────────────────────
  const handleCreate = () => setModal('create');
  const handleEdit   = (s) => setModal(s);
  const handleClone  = (s) => setModal({
    ...s,
    scenario_id: undefined,
    name: `${s.name} (copy)`,
    is_base: false,
  });

  const handleDelete = async (id) => {
    if (!window.confirm('Delete this scenario and all its results?')) return;
    try {
      await api.delete(`/meio/scenarios/${id}`);
      setScenarios(prev => prev.filter(s => s.scenario_id !== id));
      setSelected(prev => { const n = new Set(prev); n.delete(id); return n; });
    } catch (e) {
      setError(e.response?.data?.detail || e.message);
    }
  };

  const handleSave = async (data) => {
    try {
      if (modal?.scenario_id) {
        // Edit
        const r = await api.put(`/meio/scenarios/${modal.scenario_id}`, data);
        setScenarios(prev => prev.map(s => s.scenario_id === modal.scenario_id ? r.data : s));
      } else {
        // Create / clone
        const r = await api.post('/meio/scenarios', data);
        setScenarios(prev => [...prev, r.data]);
      }
      setModal(null);
    } catch (e) {
      setError(e.response?.data?.detail || e.message);
    }
  };

  // ── Handlers: run ────────────────────────────────────────────────────
  const handleRun = async () => {
    if (selected.size === 0) { setError('Select at least one scenario to run.'); return; }
    try {
      setError(null);
      const r = await api.post('/meio/scenarios/run', { scenario_ids: [...selected] });
      const job = { ...r.data, log_lines: [], ended_at: null };
      setRunJob(job);
      setShowLogs(true);
      openSSE(r.data.job_id);
    } catch (e) {
      setError(e.response?.data?.detail || e.message);
    }
  };

  const handleToggle = (id) => {
    setSelected(prev => {
      const n = new Set(prev);
      if (n.has(id)) n.delete(id); else n.add(id);
      return n;
    });
  };

  const handleSelectAll = () => {
    if (selected.size === scenarios.length) setSelected(new Set());
    else setSelected(new Set(scenarios.map(s => s.scenario_id)));
  };

  // ── Handlers: compare ────────────────────────────────────────────────
  const handleCompare = async (ids) => {
    if (ids.length < 1) return;
    setCompareIds(ids);
    setCompareLoading(true);
    setCompareData(null);
    try {
      const r = await api.get('/meio/results/compare', { params: { scenario_ids: ids.join(',') } });
      setCompareData(r.data);
    } catch (e) {
      setError(e.response?.data?.detail || e.message);
    } finally {
      setCompareLoading(false);
    }
  };

  // ── CSV export ────────────────────────────────────────────────────────
  const handleExportCSV = () => {
    if (!compareData?.scenarios) return;
    const cols = ['metric', ...compareData.scenarios.map(s => s.name)];
    const metrics = [
      ['total_inventory_value', 'Total Inventory Value'],
      ['weighted_fill_rate',    'Weighted Fill Rate'],
      ['budget_utilization',    'Budget Utilization'],
      ['sku_count',             'SKU Count'],
    ];
    const rows = metrics.map(([k, label]) => [
      label,
      ...compareData.scenarios.map(s => s[k] ?? ''),
    ]);
    const csv = [cols, ...rows].map(r => r.join(',')).join('\n');
    const blob = new Blob([csv], { type: 'text/csv' });
    const a = document.createElement('a'); a.href = URL.createObjectURL(blob); a.download = 'meio_comparison.csv'; a.click();
  };

  // ── Render ────────────────────────────────────────────────────────────
  const base = scenarios.find(s => s.is_base);
  const isRunning = runJob?.status === 'running' || runJob?.status === 'pending';

  return (
    <div className="p-4 sm:p-6 max-w-6xl mx-auto space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-2xl font-bold text-gray-900 dark:text-white">MEIO Scenarios</h1>
        <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
          Manage inventory optimization scenarios, run the Rust optimizer in parallel, and compare results.
        </p>
      </div>

      {/* Error banner */}
      {error && (
        <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg px-4 py-3 text-sm text-red-700 dark:text-red-300 flex items-start gap-2">
          <span className="flex-shrink-0 mt-0.5">⚠️</span>
          <span className="flex-1">{error}</span>
          <button onClick={() => setError(null)} className="text-red-400 hover:text-red-300">✕</button>
        </div>
      )}

      {/* ── Panel A: Scenario Manager ─────────────────────────────────── */}
      <section className="bg-white dark:bg-gray-800 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
        <div className="flex items-center justify-between px-5 py-4 border-b border-gray-100 dark:border-gray-700">
          <div className="flex items-center gap-2">
            <span className="text-xl">📋</span>
            <h2 className="font-bold text-gray-900 dark:text-white text-sm">Scenario Manager</h2>
            <span className="px-2 py-0.5 text-xs bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400 rounded-full">
              {scenarios.length}
            </span>
          </div>
          <button
            onClick={handleCreate}
            className="flex items-center gap-1.5 px-3 py-1.5 bg-blue-600 hover:bg-blue-700 text-white text-xs font-semibold rounded-lg active:scale-95 transition-colors"
          >
            <span>＋</span> New Scenario
          </button>
        </div>

        {loading ? (
          <div className="flex items-center justify-center h-32">
            <Spinner cls="w-6 h-6 text-blue-600" />
          </div>
        ) : scenarios.length === 0 ? (
          <div className="flex flex-col items-center justify-center h-32 text-gray-400 dark:text-gray-500 text-sm gap-2">
            <span className="text-3xl">🗃️</span>
            <p>No scenarios yet. Create one to get started.</p>
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="bg-gray-50 dark:bg-gray-900/40 border-b border-gray-100 dark:border-gray-700">
                  <th className="px-4 py-3 text-left">
                    <input type="checkbox"
                      checked={selected.size === scenarios.length && scenarios.length > 0}
                      onChange={handleSelectAll}
                      className="rounded border-gray-300 dark:border-gray-600 text-blue-600"
                    />
                  </th>
                  <th className="px-4 py-3 text-left text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wide">Name</th>
                  <th className="px-4 py-3 text-left text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wide">Description</th>
                  <th className="px-4 py-3 text-left text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wide">Last Run</th>
                  <th className="px-4 py-3 text-right text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wide">Actions</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100 dark:divide-gray-700">
                {scenarios.map(s => (
                  <tr key={s.scenario_id} className={`hover:bg-gray-50 dark:hover:bg-gray-700/30 transition-colors ${selected.has(s.scenario_id) ? 'bg-blue-50/50 dark:bg-blue-900/10' : ''}`}>
                    <td className="px-4 py-3">
                      <input type="checkbox"
                        checked={selected.has(s.scenario_id)}
                        onChange={() => handleToggle(s.scenario_id)}
                        className="rounded border-gray-300 dark:border-gray-600 text-blue-600"
                      />
                    </td>
                    <td className="px-4 py-3">
                      <div className="flex items-center gap-2">
                        <span className="font-medium text-gray-900 dark:text-white">{s.name}</span>
                        {s.is_base && (
                          <span className="px-1.5 py-0.5 text-[10px] font-bold bg-indigo-100 dark:bg-indigo-900/40 text-indigo-700 dark:text-indigo-300 rounded">BASE</span>
                        )}
                      </div>
                    </td>
                    <td className="px-4 py-3 text-gray-500 dark:text-gray-400 max-w-xs truncate">{s.description || '—'}</td>
                    <td className="px-4 py-3 text-gray-500 dark:text-gray-400 text-xs">{fmtDate(s.last_run_at)}</td>
                    <td className="px-4 py-3">
                      <div className="flex items-center justify-end gap-2">
                        <button onClick={() => handleEdit(s)}
                          className="px-2 py-1 text-xs text-blue-600 dark:text-blue-400 hover:bg-blue-50 dark:hover:bg-blue-900/20 rounded transition-colors">
                          Edit
                        </button>
                        <button onClick={() => handleClone(s)}
                          className="px-2 py-1 text-xs text-gray-600 dark:text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-700 rounded transition-colors">
                          Clone
                        </button>
                        <button onClick={() => handleDelete(s.scenario_id)}
                          disabled={s.is_base}
                          className={`px-2 py-1 text-xs rounded transition-colors ${s.is_base
                            ? 'text-gray-300 dark:text-gray-600 cursor-not-allowed'
                            : 'text-red-600 dark:text-red-400 hover:bg-red-50 dark:hover:bg-red-900/20'}`}>
                          Delete
                        </button>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      {/* ── Panel B: Run Panel ────────────────────────────────────────── */}
      <section className={`bg-white dark:bg-gray-800 rounded-xl border-2 transition-colors ${
        isRunning                  ? 'border-blue-300 dark:border-blue-600 shadow-lg' :
        runJob?.status === 'success'     ? 'border-emerald-200 dark:border-emerald-700' :
        runJob?.status === 'error'       ? 'border-red-200 dark:border-red-700' :
        'border-gray-200 dark:border-gray-700'
      }`}>
        <div className="px-5 py-4 border-b border-gray-100 dark:border-gray-700 flex items-center gap-2">
          <span className="text-xl">⚡</span>
          <h2 className="font-bold text-gray-900 dark:text-white text-sm">Run Optimization</h2>
        </div>

        <div className="p-5">
          {selected.size === 0 ? (
            <p className="text-sm text-gray-500 dark:text-gray-400 italic">
              Select one or more scenarios above, then click Run.
            </p>
          ) : (
            <p className="text-sm text-gray-700 dark:text-gray-300">
              <span className="font-semibold">{selected.size}</span> scenario{selected.size !== 1 ? 's' : ''} selected:&nbsp;
              <span className="text-gray-500 dark:text-gray-400">
                {scenarios.filter(s => selected.has(s.scenario_id)).map(s => s.name).join(', ')}
              </span>
            </p>
          )}

          <div className="mt-4 flex items-center gap-3">
            <button
              onClick={handleRun}
              disabled={isRunning || selected.size === 0}
              className={`px-5 py-2 rounded-lg text-sm font-semibold transition-colors ${
                isRunning || selected.size === 0
                  ? 'bg-gray-100 dark:bg-gray-700 text-gray-400 cursor-not-allowed'
                  : 'bg-emerald-600 hover:bg-emerald-700 text-white active:scale-95'
              }`}
            >
              {isRunning ? (
                <span className="flex items-center gap-2"><Spinner />Running…</span>
              ) : runJob ? '▶ Re-run' : '▶ Run Selected'}
            </button>

            {runJob && <StatusBadge status={runJob.status} />}
            {runJob && (
              <button
                onClick={() => setShowLogs(v => !v)}
                className="text-xs text-blue-500 dark:text-blue-400 hover:underline"
              >
                {showLogs ? '▲ Hide' : '▼ Show'} logs ({runJob.log_lines?.length ?? 0} lines)
              </button>
            )}
          </div>

          {/* Indeterminate progress bar while running */}
          {isRunning && (
            <div className="mt-3">
              <style>{`@keyframes meio-slide{0%{transform:translateX(-100%)}100%{transform:translateX(500%)}}.meio-slide{animation:meio-slide 1.4s ease-in-out infinite}`}</style>
              <div className="relative w-full bg-gray-200 dark:bg-gray-700 rounded-full h-1.5 overflow-hidden">
                <div className="absolute inset-y-0 left-0 w-1/4 bg-emerald-500 dark:bg-emerald-400 rounded-full meio-slide" />
              </div>
            </div>
          )}

          <LogViewer lines={runJob?.log_lines ?? []} visible={showLogs} />

          {runJob?.status === 'success' && !isRunning && (
            <div className="mt-4 flex items-center gap-3">
              <button
                onClick={() => handleCompare([...selected])}
                className="px-4 py-2 bg-indigo-600 hover:bg-indigo-700 text-white text-sm font-semibold rounded-lg active:scale-95 transition-colors"
              >
                📊 Compare results
              </button>
              <span className="text-xs text-gray-400 dark:text-gray-500">Opens comparison panel below</span>
            </div>
          )}
        </div>
      </section>

      {/* ── Panel C: Comparison Table ─────────────────────────────────── */}
      <section className="bg-white dark:bg-gray-800 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
        <div className="flex items-center justify-between px-5 py-4 border-b border-gray-100 dark:border-gray-700">
          <div className="flex items-center gap-2">
            <span className="text-xl">📊</span>
            <h2 className="font-bold text-gray-900 dark:text-white text-sm">Scenario Comparison</h2>
          </div>
          <div className="flex items-center gap-3">
            {/* Quick compare: select all with results */}
            <button
              onClick={() => {
                const ids = scenarios.filter(s => s.last_run_at).map(s => s.scenario_id);
                if (ids.length) handleCompare(ids);
                else setError('No scenarios have been run yet.');
              }}
              className="text-xs text-blue-500 dark:text-blue-400 hover:underline"
            >
              Compare all run
            </button>
            {compareData && (
              <button onClick={handleExportCSV}
                className="flex items-center gap-1 px-3 py-1.5 text-xs font-medium bg-gray-100 dark:bg-gray-700 hover:bg-gray-200 dark:hover:bg-gray-600 text-gray-700 dark:text-gray-300 rounded-lg transition-colors">
                ⬇ CSV
              </button>
            )}
          </div>
        </div>

        <div className="p-5">
          {compareLoading && (
            <div className="flex items-center gap-2 text-sm text-gray-500 dark:text-gray-400">
              <Spinner />Fetching comparison data…
            </div>
          )}

          {!compareLoading && !compareData && (
            <p className="text-sm text-gray-400 dark:text-gray-500 italic">
              Run scenarios then click "Compare results" — or use "Compare all run" above.
            </p>
          )}

          {compareData && !compareLoading && (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-gray-200 dark:border-gray-700">
                    <th className="pb-3 text-left text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wide pr-6">Metric</th>
                    {compareData.scenarios?.map(s => (
                      <th key={s.scenario_id} className="pb-3 text-right text-xs font-semibold text-gray-700 dark:text-gray-300 px-3">
                        <div className="flex flex-col items-end gap-0.5">
                          <span>{s.name}</span>
                          {s.is_base && (
                            <span className="px-1.5 py-0.5 text-[10px] font-bold bg-indigo-100 dark:bg-indigo-900/40 text-indigo-700 dark:text-indigo-300 rounded">BASE</span>
                          )}
                        </div>
                      </th>
                    ))}
                    {base && compareData.scenarios?.some(s => !s.is_base) && (
                      <th className="pb-3 text-right text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wide px-3">
                        Δ vs Base
                      </th>
                    )}
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-100 dark:divide-gray-700">
                  {[
                    { key: 'total_inventory_value',  label: 'Total Inventory Value ($)', format: fmt, inverse: true },
                    { key: 'weighted_fill_rate',      label: 'Weighted Fill Rate',        format: fmtPct, inverse: false },
                    { key: 'budget_utilization',      label: 'Budget Utilization',        format: fmtPct, inverse: true },
                    { key: 'sku_count',               label: 'SKU Count',                 format: (v) => fmt(v, 0), inverse: false },
                  ].map(({ key, label, format, inverse }) => {
                    const baseVal = compareData.scenarios?.find(s => s.is_base)?.[key];
                    return (
                      <tr key={key} className="hover:bg-gray-50 dark:hover:bg-gray-700/20">
                        <td className="py-3 pr-6 text-gray-700 dark:text-gray-300 font-medium text-xs">{label}</td>
                        {compareData.scenarios?.map(s => (
                          <td key={s.scenario_id} className="py-3 px-3 text-right tabular-nums text-gray-900 dark:text-gray-100">
                            {format(s[key])}
                          </td>
                        ))}
                        {base && compareData.scenarios?.some(s => !s.is_base) && (
                          <td className="py-3 px-3 text-right tabular-nums">
                            {/* Show delta for the last non-base selected scenario */}
                            {(() => {
                              const nonBase = compareData.scenarios?.filter(s => !s.is_base);
                              if (!nonBase?.length || baseVal == null) return <span className="text-gray-400">—</span>;
                              const delta = nonBase[nonBase.length - 1][key] - baseVal;
                              return <Delta value={delta} inverse={inverse} />;
                            })()}
                          </td>
                        )}
                      </tr>
                    );
                  })}
                </tbody>
              </table>

              {/* Per-scenario breakdown note */}
              <p className="mt-4 text-xs text-gray-400 dark:text-gray-500">
                Δ shows the last non-base scenario vs the base. Values aggregated across all SKUs.
                Run individual scenario results via <code className="bg-gray-100 dark:bg-gray-800 px-1 rounded">GET /api/meio/results?scenario_id=…</code>
              </p>
            </div>
          )}
        </div>
      </section>

      {/* ── Modal ────────────────────────────────────────────────────── */}
      {modal && (
        <ScenarioModal
          scenario={modal === 'create' ? null : modal}
          onClose={() => setModal(null)}
          onSave={handleSave}
        />
      )}
    </div>
  );
};

export default MeioScenarios;

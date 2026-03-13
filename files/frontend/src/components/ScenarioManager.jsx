/**
 * ScenarioManager — Forecast Scenario Manager
 *
 * Lists all forecast scenarios, allows creating, editing, cloning,
 * deleting, and running them against the pipeline.
 */

import React, { useState, useEffect, useCallback } from 'react';
import api from '../utils/api';

// ── Utility helpers ─────────────────────────────────────────────────────────

const fmtDate = (s) => {
  if (!s) return 'Never';
  try { return new Date(s).toLocaleString(); } catch { return s; }
};

// ── Sub-components ──────────────────────────────────────────────────────────

const Spinner = ({ cls = 'w-4 h-4' }) => (
  <svg className={`animate-spin ${cls}`} viewBox="0 0 24 24" fill="none">
    <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
    <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8z" />
  </svg>
);

const TypeBadge = ({ isBase }) => isBase ? (
  <span className="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-semibold bg-emerald-100 dark:bg-emerald-900/40 text-emerald-700 dark:text-emerald-300">
    Base
  </span>
) : (
  <span className="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-semibold bg-blue-100 dark:bg-blue-900/40 text-blue-700 dark:text-blue-300">
    Scenario
  </span>
);

const StatusBadge = ({ status }) => {
  const map = {
    pending:  { cls: 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400',                 label: 'Pending',  pulse: false },
    running:  { cls: 'bg-yellow-100 dark:bg-yellow-900/30 text-yellow-700 dark:text-yellow-300',      label: 'Running',  pulse: true  },
    complete: { cls: 'bg-emerald-100 dark:bg-emerald-900/30 text-emerald-700 dark:text-emerald-300',  label: 'Complete', pulse: false },
    failed:   { cls: 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-300',                  label: 'Failed',   pulse: false },
  };
  const { cls, label, pulse } = map[status] || { cls: 'bg-gray-100 dark:bg-gray-700 text-gray-500', label: status || '—', pulse: false };
  return (
    <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium ${cls} ${pulse ? 'animate-pulse' : ''}`}>
      {label}
    </span>
  );
};

const DemandOverrideSummary = ({ overrides }) => {
  if (!overrides) return <span className="text-gray-400 dark:text-gray-500 text-xs">None</span>;
  const parts = [];
  if (overrides.demand_multiplier != null && overrides.demand_multiplier !== 1) {
    parts.push(`×${overrides.demand_multiplier}`);
  }
  if (overrides.date_from || overrides.date_to) {
    const from = overrides.date_from || '…';
    const to = overrides.date_to || '…';
    parts.push(`${from} → ${to}`);
  }
  if (parts.length === 0) return <span className="text-gray-400 dark:text-gray-500 text-xs">None</span>;
  return <span className="text-xs text-gray-700 dark:text-gray-300 font-mono">{parts.join(', ')}</span>;
};

// ── Modal tabs ──────────────────────────────────────────────────────────────

const TABS = ['Demand Override', 'Parameters', 'Raw JSON'];

const ScenarioModal = ({ scenario, mode, onClose, onSave }) => {
  const isEdit  = mode === 'edit';
  const isClone = mode === 'clone';
  const title   = isEdit ? 'Edit Scenario' : isClone ? 'Clone Scenario' : 'New Scenario';

  const [name, setName]   = useState(isEdit ? (scenario?.name || '') : isClone ? '' : '');
  const [desc, setDesc]   = useState(scenario?.description || '');
  const [activeTab, setActiveTab] = useState(0);
  const [jsonMode, setJsonMode]   = useState(false);
  const [formError, setFormError] = useState(null);

  // Demand override fields
  const [demandMult,   setDemandMult]   = useState(() => scenario?.demand_overrides?.demand_multiplier ?? 1.0);
  const [dateFrom,     setDateFrom]     = useState(scenario?.demand_overrides?.date_from || '');
  const [dateTo,       setDateTo]       = useState(scenario?.demand_overrides?.date_to   || '');
  const [seriesFilter, setSeriesFilter] = useState(() => {
    const sf = scenario?.demand_overrides?.series_filter;
    return Array.isArray(sf) ? sf.join(', ') : (sf || '');
  });

  // Param override fields
  const [horizon,           setHorizon]          = useState(scenario?.param_overrides?.horizon           ?? '');
  const [backtestWindows,   setBacktestWindows]  = useState(scenario?.param_overrides?.backtest_windows  ?? '');
  const [outlierMethod,     setOutlierMethod]    = useState(scenario?.param_overrides?.outlier_method    ?? '');
  const [outlierSensitivity, setOutlierSens]     = useState(scenario?.param_overrides?.outlier_sensitivity ?? '');

  // Raw JSON
  const buildPayload = () => {
    const demand_overrides = {};
    const dm = parseFloat(demandMult);
    if (!isNaN(dm) && dm !== 1.0) demand_overrides.demand_multiplier = dm;
    if (dateFrom) demand_overrides.date_from = dateFrom;
    if (dateTo)   demand_overrides.date_to   = dateTo;
    if (seriesFilter.trim()) {
      demand_overrides.series_filter = seriesFilter.split(',').map(s => s.trim()).filter(Boolean);
    }

    const param_overrides = {};
    if (horizon !== '')           param_overrides.horizon           = parseInt(horizon,  10);
    if (backtestWindows !== '')   param_overrides.backtest_windows  = parseInt(backtestWindows, 10);
    if (outlierMethod)            param_overrides.outlier_method    = outlierMethod;
    if (outlierSensitivity !== '') param_overrides.outlier_sensitivity = parseFloat(outlierSensitivity);

    return { demand_overrides, param_overrides };
  };

  const [rawJson, setRawJson] = useState(() => JSON.stringify(buildPayload(), null, 2));

  const syncToJson = () => setRawJson(JSON.stringify(buildPayload(), null, 2));

  const handleSave = () => {
    setFormError(null);
    if (!name.trim()) { setFormError('Name is required.'); return; }

    let demand_overrides, param_overrides;
    if (jsonMode) {
      try {
        const parsed = JSON.parse(rawJson);
        demand_overrides = parsed.demand_overrides || {};
        param_overrides  = parsed.param_overrides  || {};
      } catch (e) {
        setFormError(`Invalid JSON: ${e.message}`);
        return;
      }
    } else {
      ({ demand_overrides, param_overrides } = buildPayload());
    }

    onSave({ name: name.trim(), description: desc.trim(), param_overrides, demand_overrides });
  };

  const inputCls = 'w-full px-3 py-2 text-sm rounded-lg border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-700 text-gray-900 dark:text-white focus:ring-2 focus:ring-blue-500 focus:border-transparent';
  const smallInputCls = 'w-full px-2 py-1.5 text-sm rounded border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-700 text-gray-900 dark:text-white focus:ring-1 focus:ring-blue-400 focus:border-transparent';
  const labelCls = 'block text-xs font-semibold text-gray-600 dark:text-gray-400 mb-1';

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
      <div className="bg-white dark:bg-gray-800 rounded-2xl shadow-2xl w-full max-w-lg border border-gray-200 dark:border-gray-700 max-h-[90vh] flex flex-col">
        {/* Header */}
        <div className="flex items-center justify-between px-6 py-4 border-b border-gray-200 dark:border-gray-700 flex-shrink-0">
          <h2 className="text-base font-bold text-gray-900 dark:text-white">{title}</h2>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-600 dark:hover:text-gray-300 text-xl leading-none">✕</button>
        </div>

        {/* Body */}
        <div className="px-6 py-4 space-y-4 overflow-y-auto flex-1">
          {/* Name */}
          <div>
            <label className={labelCls}>Name *</label>
            <input value={name} onChange={e => setName(e.target.value)}
              placeholder="e.g. High Demand +20%"
              className={inputCls} />
          </div>
          {/* Description */}
          <div>
            <label className={labelCls}>Description</label>
            <textarea value={desc} onChange={e => setDesc(e.target.value)} rows={2}
              placeholder="Optional description…"
              className={`${inputCls} resize-none`} />
          </div>

          {/* Tabs */}
          <div>
            <div className="flex items-center justify-between mb-3">
              <div className="flex gap-1 bg-gray-100 dark:bg-gray-900/40 rounded-lg p-0.5">
                {TABS.map((tab, i) => (
                  <button key={tab}
                    onClick={() => {
                      if (tab === 'Raw JSON' && !jsonMode) { syncToJson(); setJsonMode(true); }
                      else if (tab !== 'Raw JSON') setJsonMode(false);
                      setActiveTab(i);
                    }}
                    className={`px-3 py-1 text-xs font-medium rounded-md transition-colors ${
                      activeTab === i
                        ? 'bg-white dark:bg-gray-700 text-gray-900 dark:text-white shadow-sm'
                        : 'text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300'
                    }`}
                  >
                    {tab}
                  </button>
                ))}
              </div>
            </div>

            {/* Tab: Demand Override */}
            {activeTab === 0 && !jsonMode && (
              <div className="space-y-3 bg-gray-50 dark:bg-gray-900/30 rounded-lg p-3 border border-gray-200 dark:border-gray-700">
                <div className="grid grid-cols-2 gap-3">
                  <div>
                    <label className={labelCls}>Demand Multiplier</label>
                    <input type="number" step="0.01" min="0.1" max="10"
                      value={demandMult} onChange={e => setDemandMult(e.target.value)}
                      className={smallInputCls} />
                    <p className="text-[10px] text-gray-400 mt-0.5">1.0 = no change</p>
                  </div>
                  <div>
                    <label className={labelCls}>Training Data From</label>
                    <input type="date" value={dateFrom} onChange={e => setDateFrom(e.target.value)}
                      className={smallInputCls} />
                  </div>
                  <div>
                    <label className={labelCls}>Training Data To</label>
                    <input type="date" value={dateTo} onChange={e => setDateTo(e.target.value)}
                      className={smallInputCls} />
                  </div>
                </div>
                <div>
                  <label className={labelCls}>Series Filter (comma-separated unique_ids)</label>
                  <textarea value={seriesFilter} onChange={e => setSeriesFilter(e.target.value)}
                    rows={2} placeholder="e.g. ITEM1_SITE1, ITEM2_SITE2"
                    className={`${smallInputCls} resize-none`} />
                </div>
              </div>
            )}

            {/* Tab: Parameters */}
            {activeTab === 1 && !jsonMode && (
              <div className="space-y-3 bg-gray-50 dark:bg-gray-900/30 rounded-lg p-3 border border-gray-200 dark:border-gray-700">
                <div className="grid grid-cols-2 gap-3">
                  <div>
                    <label className={labelCls}>Forecast Horizon (periods)</label>
                    <input type="number" min="1" value={horizon} onChange={e => setHorizon(e.target.value)}
                      placeholder="e.g. 24" className={smallInputCls} />
                  </div>
                  <div>
                    <label className={labelCls}>Backtest Windows</label>
                    <input type="number" min="1" value={backtestWindows} onChange={e => setBacktestWindows(e.target.value)}
                      placeholder="e.g. 3" className={smallInputCls} />
                  </div>
                  <div>
                    <label className={labelCls}>Outlier Method</label>
                    <select value={outlierMethod} onChange={e => setOutlierMethod(e.target.value)}
                      className={smallInputCls}>
                      <option value="">— default —</option>
                      <option value="iqr">IQR</option>
                      <option value="zscore">Z-Score</option>
                      <option value="stl">STL</option>
                    </select>
                  </div>
                  <div>
                    <label className={labelCls}>Outlier Sensitivity</label>
                    <input type="number" step="0.1" min="0" value={outlierSensitivity} onChange={e => setOutlierSens(e.target.value)}
                      placeholder="e.g. 1.5" className={smallInputCls} />
                  </div>
                </div>
              </div>
            )}

            {/* Tab: Raw JSON */}
            {activeTab === 2 && (
              <textarea
                value={rawJson}
                onChange={e => { setRawJson(e.target.value); setFormError(null); }}
                rows={10}
                className="w-full px-3 py-2 text-xs font-mono rounded-lg border border-gray-300 dark:border-gray-600 bg-gray-50 dark:bg-gray-900 text-gray-900 dark:text-green-300 focus:ring-2 focus:ring-blue-500 focus:border-transparent resize-y"
              />
            )}
          </div>

          {formError && (
            <p className="text-xs text-red-600 dark:text-red-400">{formError}</p>
          )}
        </div>

        {/* Footer */}
        <div className="flex justify-end gap-3 px-6 py-4 border-t border-gray-200 dark:border-gray-700 flex-shrink-0">
          <button onClick={onClose}
            className="px-4 py-2 text-sm text-gray-600 dark:text-gray-400 hover:text-gray-900 dark:hover:text-white">
            Cancel
          </button>
          <button onClick={handleSave}
            className="px-5 py-2 text-sm font-semibold bg-blue-600 hover:bg-blue-700 text-white rounded-lg active:scale-95 transition-colors">
            {isEdit ? 'Save changes' : isClone ? 'Clone scenario' : 'Create scenario'}
          </button>
        </div>
      </div>
    </div>
  );
};

// ── Run confirmation inline row ─────────────────────────────────────────────

const RunConfirm = ({ scenario, onConfirm, onCancel, running }) => (
  <div className="flex items-center gap-3 px-4 py-3 bg-indigo-50 dark:bg-indigo-900/20 border-t border-indigo-200 dark:border-indigo-700 text-sm">
    <span className="text-indigo-700 dark:text-indigo-300 flex-1">
      Run full pipeline for <strong>{scenario.name}</strong>?
    </span>
    <button onClick={onCancel}
      className="px-3 py-1 text-xs rounded-lg border border-gray-300 dark:border-gray-600 text-gray-600 dark:text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-700">
      Cancel
    </button>
    <button onClick={onConfirm} disabled={running}
      className="px-3 py-1 text-xs rounded-lg bg-indigo-600 hover:bg-indigo-700 text-white font-semibold disabled:opacity-50 flex items-center gap-1">
      {running && <Spinner cls="w-3 h-3" />}
      {running ? 'Running…' : 'Confirm'}
    </button>
  </div>
);

// ── Delete confirmation inline row ──────────────────────────────────────────

const DeleteConfirm = ({ scenario, onConfirm, onCancel }) => (
  <div className="flex items-center gap-3 px-4 py-3 bg-red-50 dark:bg-red-900/20 border-t border-red-200 dark:border-red-700 text-sm">
    <span className="text-red-700 dark:text-red-300 flex-1">
      Delete <strong>{scenario.name}</strong>? This will delete all results for this scenario.
    </span>
    <button onClick={onCancel}
      className="px-3 py-1 text-xs rounded-lg border border-gray-300 dark:border-gray-600 text-gray-600 dark:text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-700">
      Cancel
    </button>
    <button onClick={onConfirm}
      className="px-3 py-1 text-xs rounded-lg bg-red-600 hover:bg-red-700 text-white font-semibold">
      Delete
    </button>
  </div>
);

// ── Toast ───────────────────────────────────────────────────────────────────

const Toast = ({ message, type = 'success', onDismiss }) => {
  useEffect(() => {
    const t = setTimeout(onDismiss, 5000);
    return () => clearTimeout(t);
  }, [onDismiss]);
  const cls = type === 'error'
    ? 'bg-red-600 dark:bg-red-700'
    : 'bg-emerald-600 dark:bg-emerald-700';
  return (
    <div className={`fixed bottom-6 right-6 z-50 flex items-center gap-3 px-4 py-3 rounded-xl shadow-xl text-white text-sm max-w-sm ${cls}`}>
      <span className="flex-1">{message}</span>
      <button onClick={onDismiss} className="text-white/70 hover:text-white text-lg leading-none">✕</button>
    </div>
  );
};

// ── Main component ──────────────────────────────────────────────────────────

export const ScenarioManager = () => {
  const [scenarios,    setScenarios]    = useState([]);
  const [loading,      setLoading]      = useState(true);
  const [error,        setError]        = useState(null);
  const [toast,        setToast]        = useState(null); // { message, type }

  // Modal state: null | { mode: 'create'|'edit'|'clone', scenario: obj|null }
  const [modal,        setModal]        = useState(null);

  // Inline confirm state (per-row): { id, type: 'run'|'delete' }
  const [confirm,      setConfirm]      = useState(null);
  const [runningId,    setRunningId]    = useState(null); // scenario being run

  const showToast = useCallback((message, type = 'success') => {
    setToast({ message, type });
  }, []);

  // ── Fetch scenarios ────────────────────────────────────────────────────

  const fetchScenarios = useCallback(async () => {
    try {
      const res = await api.get('/forecast/scenarios');
      const data = res.data;
      const list = data.scenarios || data || [];
      // Base scenario first
      list.sort((a, b) => (b.is_base ? 1 : 0) - (a.is_base ? 1 : 0));
      setScenarios(list);
    } catch (e) {
      setError(e.response?.data?.detail || e.message);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { fetchScenarios(); }, [fetchScenarios]);

  // ── CRUD handlers ──────────────────────────────────────────────────────

  const handleSave = async (data) => {
    const { mode, scenario } = modal;
    try {
      if (mode === 'edit') {
        await api.put(`/forecast/scenarios/${scenario.scenario_id}`, data);
      } else if (mode === 'clone') {
        await api.post(`/forecast/scenarios/${scenario.scenario_id}/clone`, data);
      } else {
        await api.post('/forecast/scenarios', data);
      }
      setModal(null);
      await fetchScenarios();
      showToast(mode === 'edit' ? 'Scenario updated.' : mode === 'clone' ? 'Scenario cloned.' : 'Scenario created.');
    } catch (e) {
      setError(e.response?.data?.detail || e.message);
    }
  };

  const handleDelete = async (scenario) => {
    try {
      await api.delete(`/forecast/scenarios/${scenario.scenario_id}`);
      setConfirm(null);
      setScenarios(prev => prev.filter(s => s.scenario_id !== scenario.scenario_id));
      showToast(`Scenario "${scenario.name}" deleted.`);
    } catch (e) {
      setError(e.response?.data?.detail || e.message);
      setConfirm(null);
    }
  };

  const handleRun = async (scenario) => {
    setRunningId(scenario.scenario_id);
    try {
      const res = await api.post('/pipeline/run-all', { scenario_id: scenario.scenario_id });
      const data = res.data;
      setConfirm(null);
      showToast(`Pipeline started. Job ID: ${data.job_id}`);
    } catch (e) {
      setError(e.message);
    } finally {
      setRunningId(null);
    }
  };

  // ── Render ─────────────────────────────────────────────────────────────

  return (
    <div className="p-4 sm:p-6 max-w-5xl mx-auto space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-2xl font-bold text-gray-900 dark:text-white">Forecast Scenarios</h1>
        <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
          Manage forecast scenarios, override demand or parameters, and run the pipeline per scenario.
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

      {/* Scenario list */}
      <section className="bg-white dark:bg-gray-800 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
        <div className="flex items-center justify-between px-5 py-4 border-b border-gray-100 dark:border-gray-700">
          <div className="flex items-center gap-2">
            <span className="text-xl">🔀</span>
            <h2 className="font-bold text-gray-900 dark:text-white text-sm">Scenarios</h2>
            {!loading && (
              <span className="px-2 py-0.5 text-xs bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400 rounded-full">
                {scenarios.length}
              </span>
            )}
          </div>
          <button
            onClick={() => setModal({ mode: 'create', scenario: null })}
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
                  <th className="px-4 py-3 text-left text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wide">Name</th>
                  <th className="px-4 py-3 text-left text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wide">Type</th>
                  <th className="px-4 py-3 text-left text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wide">Status</th>
                  <th className="px-4 py-3 text-left text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wide">Last Run</th>
                  <th className="px-4 py-3 text-left text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wide">Demand Override</th>
                  <th className="px-4 py-3 text-right text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wide">Actions</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100 dark:divide-gray-700">
                {scenarios.map(s => (
                  <React.Fragment key={s.scenario_id}>
                    <tr className="hover:bg-gray-50 dark:hover:bg-gray-700/30 transition-colors">
                      <td className="px-4 py-3">
                        <span className={`font-medium text-gray-900 dark:text-white ${s.is_base ? 'font-bold' : ''}`}>
                          {s.name}
                        </span>
                        {s.description && (
                          <p className="text-xs text-gray-400 dark:text-gray-500 mt-0.5 truncate max-w-xs">{s.description}</p>
                        )}
                      </td>
                      <td className="px-4 py-3"><TypeBadge isBase={s.is_base} /></td>
                      <td className="px-4 py-3"><StatusBadge status={s.status || 'pending'} /></td>
                      <td className="px-4 py-3 text-xs text-gray-500 dark:text-gray-400">{fmtDate(s.run_at)}</td>
                      <td className="px-4 py-3"><DemandOverrideSummary overrides={s.demand_overrides} /></td>
                      <td className="px-4 py-3">
                        <div className="flex items-center justify-end gap-1.5">
                          <button
                            onClick={() => setModal({ mode: 'edit', scenario: s })}
                            className="px-2 py-1 text-xs text-blue-600 dark:text-blue-400 hover:bg-blue-50 dark:hover:bg-blue-900/20 rounded transition-colors"
                          >
                            Edit
                          </button>
                          <button
                            onClick={() => setModal({ mode: 'clone', scenario: s })}
                            className="px-2 py-1 text-xs text-gray-600 dark:text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-700 rounded transition-colors"
                          >
                            Clone
                          </button>
                          <button
                            onClick={() => setConfirm({ id: s.scenario_id, type: 'delete' })}
                            disabled={s.is_base}
                            className={`px-2 py-1 text-xs rounded transition-colors ${
                              s.is_base
                                ? 'text-gray-300 dark:text-gray-600 cursor-not-allowed'
                                : 'text-red-600 dark:text-red-400 hover:bg-red-50 dark:hover:bg-red-900/20'
                            }`}
                          >
                            Delete
                          </button>
                          <button
                            onClick={() => setConfirm({ id: s.scenario_id, type: 'run' })}
                            className="px-2 py-1 text-xs text-emerald-600 dark:text-emerald-400 hover:bg-emerald-50 dark:hover:bg-emerald-900/20 rounded transition-colors font-semibold"
                          >
                            ▶ Run
                          </button>
                        </div>
                      </td>
                    </tr>

                    {/* Inline confirmation rows */}
                    {confirm?.id === s.scenario_id && confirm.type === 'run' && (
                      <tr>
                        <td colSpan={6} className="p-0">
                          <RunConfirm
                            scenario={s}
                            running={runningId === s.scenario_id}
                            onConfirm={() => handleRun(s)}
                            onCancel={() => setConfirm(null)}
                          />
                        </td>
                      </tr>
                    )}
                    {confirm?.id === s.scenario_id && confirm.type === 'delete' && (
                      <tr>
                        <td colSpan={6} className="p-0">
                          <DeleteConfirm
                            scenario={s}
                            onConfirm={() => handleDelete(s)}
                            onCancel={() => setConfirm(null)}
                          />
                        </td>
                      </tr>
                    )}
                  </React.Fragment>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      {/* Modal */}
      {modal && (
        <ScenarioModal
          scenario={modal.scenario}
          mode={modal.mode}
          onClose={() => setModal(null)}
          onSave={handleSave}
        />
      )}

      {/* Toast */}
      {toast && (
        <Toast
          message={toast.message}
          type={toast.type}
          onDismiss={() => setToast(null)}
        />
      )}
    </div>
  );
};

export default ScenarioManager;

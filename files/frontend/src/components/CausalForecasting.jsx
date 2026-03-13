import React, { useState, useEffect, useMemo, useCallback, useRef } from 'react';
import Plot from 'react-plotly.js';
import api from '../utils/api';
import { useTheme } from '../contexts/ThemeContext';
import { useLocale } from '../contexts/LocaleContext';
import { formatNumber } from '../utils/formatting';
import BomExplorer from './BomExplorer';

// ── Section ───────────────────────────────────────────────────────────────────
const Section = ({ title, storageKey, defaultOpen = true, children, id }) => {
  const [open, setOpen] = useState(() => {
    const stored = localStorage.getItem(storageKey);
    return stored === null ? defaultOpen : stored === 'true';
  });
  const toggle = () => setOpen(prev => {
    const next = !prev;
    localStorage.setItem(storageKey, String(next));
    return next;
  });
  return (
    <div id={id} className="mb-6 bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900/50">
      <button onClick={toggle}
        className="w-full flex items-center justify-between p-4 text-left hover:bg-gray-50 dark:hover:bg-gray-700/50 transition-colors rounded-lg">
        <h2 className="text-lg font-semibold text-gray-900 dark:text-white">{title}</h2>
        <span className="text-gray-400 dark:text-gray-500 text-xl">{open ? '▲' : '▼'}</span>
      </button>
      {open && <div className="px-4 pb-4 sm:px-6 sm:pb-6">{children}</div>}
    </div>
  );
};

// ── Spinner ───────────────────────────────────────────────────────────────────
const Spinner = () => (
  <div className="flex items-center justify-center py-8">
    <div className="animate-spin rounded-full h-6 w-6 border-b-2 border-blue-600" />
  </div>
);

// ── ErrorBox ──────────────────────────────────────────────────────────────────
const ErrorBox = ({ msg }) => (
  <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-700 rounded p-3 text-sm text-red-700 dark:text-red-400">
    {msg}
  </div>
);

// ── StatusBadge ───────────────────────────────────────────────────────────────
const StatusBadge = ({ status }) => {
  const colors = {
    running: 'bg-yellow-100 dark:bg-yellow-900/30 text-yellow-700 dark:text-yellow-400',
    completed: 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400',
    error: 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400',
  };
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${colors[status] || 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400'}`}>
      {status}
    </span>
  );
};

// ── Main Component ────────────────────────────────────────────────────────────
export default function CausalForecasting() {
  const { theme } = useTheme();
  const { locale } = useLocale();
  const isDark = theme === 'dark';
  const plotBg = isDark ? '#1f2937' : '#ffffff';
  const plotPaper = isDark ? '#111827' : '#f9fafb';
  const plotFont = isDark ? '#d1d5db' : '#374151';

  // ── Shared state ────────────────────────────────────────────────────────────
  const [assetTypes, setAssetTypes] = useState([]);
  const [scenarios, setScenarios] = useState([]);
  const [loadingAssetTypes, setLoadingAssetTypes] = useState(false);
  const [loadingScenarios, setLoadingScenarios] = useState(false);
  const [errorAssetTypes, setErrorAssetTypes] = useState(null);
  const [errorScenarios, setErrorScenarios] = useState(null);

  // ── Panel 1: Fleet & Assets ─────────────────────────────────────────────────
  const [fleetPlan, setFleetPlan] = useState([]);
  const [loadingFleet, setLoadingFleet] = useState(false);
  const [errorFleet, setErrorFleet] = useState(null);
  const [mdfhJobId, setMdfhJobId] = useState(null);
  const [mdfhJobStatus, setMdfhJobStatus] = useState(null);
  const [showNewAssetTypeModal, setShowNewAssetTypeModal] = useState(false);
  const [newAssetType, setNewAssetType] = useState({
    code: '', name: '',
    removal_drivers: { hours: true, cycles: true, landings: false, calendar_days: false }
  });
  const [savingAssetType, setSavingAssetType] = useState(false);
  const [fleetPage, setFleetPage] = useState(1);

  // ── Panel 2: BOM & Effectivity ──────────────────────────────────────────────
  const [selectedAssetTypeId, setSelectedAssetTypeId] = useState(null);
  const [bomExplosion, setBomExplosion] = useState([]);
  const [loadingBomExplosion, setLoadingBomExplosion] = useState(false);
  const [errorBomExplosion, setErrorBomExplosion] = useState(null);
  const [selectedPartId, setSelectedPartId] = useState(null);

  // ── Panel 3: Scenarios ──────────────────────────────────────────────────────
  const [showNewScenarioModal, setShowNewScenarioModal] = useState(false);
  const [newScenario, setNewScenario] = useState({
    name: '', description: '', is_base: false,
    fleet_overrides: { utilization_multiplier: 1.0 },
    mdfh_overrides: {},
    linked_meio_scenario_id: null,
  });
  const [savingScenario, setSavingScenario] = useState(false);
  const [scenarioJsonMode, setScenarioJsonMode] = useState(false);
  const [scenarioJsonText, setScenarioJsonText] = useState('{}');
  const [selectedScenarioIds, setSelectedScenarioIds] = useState([]);
  const [runJobId, setRunJobId] = useState(null);
  const [runJobStatus, setRunJobStatus] = useState(null);
  const [runJobResult, setRunJobResult] = useState(null);

  // ── Panel 4: Results ────────────────────────────────────────────────────────
  const [resultItemFilter, setResultItemFilter] = useState('');
  const [resultSiteFilter, setResultSiteFilter] = useState('');
  const [resultScenarioIds, setResultScenarioIds] = useState([]);
  const [resultData, setResultData] = useState([]);
  const [compareData, setCompareData] = useState([]);
  const [loadingResults, setLoadingResults] = useState(false);
  const [errorResults, setErrorResults] = useState(null);

  // ── Load initial data ───────────────────────────────────────────────────────
  const loadAssetTypes = useCallback(async () => {
    setLoadingAssetTypes(true);
    setErrorAssetTypes(null);
    try {
      const res = await api.get('/causal/asset-types');
      setAssetTypes(res.data || []);
    } catch (e) {
      setErrorAssetTypes(e?.response?.data?.detail || e.message);
    } finally {
      setLoadingAssetTypes(false);
    }
  }, []);

  const loadScenarios = useCallback(async () => {
    setLoadingScenarios(true);
    setErrorScenarios(null);
    try {
      const res = await api.get('/causal/scenarios');
      setScenarios(res.data || []);
    } catch (e) {
      setErrorScenarios(e?.response?.data?.detail || e.message);
    } finally {
      setLoadingScenarios(false);
    }
  }, []);

  const loadFleetPlan = useCallback(async (page = 1) => {
    setLoadingFleet(true);
    setErrorFleet(null);
    try {
      const res = await api.get('/causal/fleet-plan', { params: { scenario_id: 0, page, page_size: 50 } });
      setFleetPlan(res.data || []);
      setFleetPage(page);
    } catch (e) {
      setErrorFleet(e?.response?.data?.detail || e.message);
    } finally {
      setLoadingFleet(false);
    }
  }, []);

  useEffect(() => {
    loadAssetTypes();
    loadScenarios();
    loadFleetPlan(1);
  }, [loadAssetTypes, loadScenarios, loadFleetPlan]);

  // ── BOM Explosion ───────────────────────────────────────────────────────────
  const loadBomExplosion = useCallback(async (atId) => {
    if (!atId) return;
    setLoadingBomExplosion(true);
    setErrorBomExplosion(null);
    try {
      const res = await api.get('/causal/bom/explosion', { params: { asset_type_id: atId, scenario_id: 0 } });
      setBomExplosion(res.data || []);
    } catch (e) {
      setErrorBomExplosion(e?.response?.data?.detail || e.message);
    } finally {
      setLoadingBomExplosion(false);
    }
  }, []);

  useEffect(() => {
    if (selectedAssetTypeId) loadBomExplosion(selectedAssetTypeId);
  }, [selectedAssetTypeId, loadBomExplosion]);

  // ── MDFH Fit job polling ────────────────────────────────────────────────────
  const pollJob = useCallback(async (jobId, setStatus, setResult, interval = 1500) => {
    const poll = async () => {
      try {
        const res = await api.get(`/pipeline/jobs/${jobId}`);
        const job = res.data;
        setStatus(job.status);
        if (job.result) setResult && setResult(job.result);
        if (job.status === 'running') {
          setTimeout(poll, interval);
        }
      } catch (e) {
        setStatus('error');
      }
    };
    setTimeout(poll, interval);
  }, []);

  const handleFitMdfh = useCallback(async () => {
    try {
      const res = await api.post('/causal/mdfh/fit');
      setMdfhJobId(res.data.job_id);
      setMdfhJobStatus('running');
      pollJob(res.data.job_id, setMdfhJobStatus, null);
    } catch (e) {
      setMdfhJobStatus('error');
    }
  }, [pollJob]);

  // ── Save new asset type ─────────────────────────────────────────────────────
  const handleSaveAssetType = useCallback(async () => {
    setSavingAssetType(true);
    try {
      const drivers = Object.entries(newAssetType.removal_drivers)
        .filter(([, v]) => v).map(([k]) => k);
      await api.post('/causal/asset-types', {
        code: newAssetType.code,
        name: newAssetType.name,
        removal_drivers: drivers,
      });
      setShowNewAssetTypeModal(false);
      setNewAssetType({ code: '', name: '', removal_drivers: { hours: true, cycles: true, landings: false, calendar_days: false } });
      await loadAssetTypes();
    } catch (e) {
      alert(e?.response?.data?.detail || e.message);
    } finally {
      setSavingAssetType(false);
    }
  }, [newAssetType, loadAssetTypes]);

  // ── Save new scenario ───────────────────────────────────────────────────────
  const handleSaveScenario = useCallback(async () => {
    setSavingScenario(true);
    try {
      let fleetOverrides = newScenario.fleet_overrides;
      if (scenarioJsonMode) {
        try { fleetOverrides = JSON.parse(scenarioJsonText); }
        catch { alert('Invalid JSON in overrides'); setSavingScenario(false); return; }
      }
      await api.post('/causal/scenarios', {
        ...newScenario,
        fleet_overrides: fleetOverrides,
      });
      setShowNewScenarioModal(false);
      setNewScenario({ name: '', description: '', is_base: false, fleet_overrides: { utilization_multiplier: 1.0 }, mdfh_overrides: {}, linked_meio_scenario_id: null });
      await loadScenarios();
    } catch (e) {
      alert(e?.response?.data?.detail || e.message);
    } finally {
      setSavingScenario(false);
    }
  }, [newScenario, scenarioJsonMode, scenarioJsonText, loadScenarios]);

  // ── Run selected scenarios ──────────────────────────────────────────────────
  const handleRunScenarios = useCallback(async () => {
    if (!selectedScenarioIds.length) return;
    setRunJobStatus('running');
    setRunJobResult(null);
    try {
      const res = await api.post('/causal/scenarios/run', {
        scenario_ids: selectedScenarioIds,
        feed_meio: true,
        horizon_periods: 24,
      });
      setRunJobId(res.data.job_id);
      pollJob(res.data.job_id, setRunJobStatus, setRunJobResult);
    } catch (e) {
      setRunJobStatus('error');
    }
  }, [selectedScenarioIds, pollJob]);

  // ── Load results ────────────────────────────────────────────────────────────
  const handleLoadResults = useCallback(async () => {
    setLoadingResults(true);
    setErrorResults(null);
    try {
      const params = { page: 1, page_size: 200 };
      if (resultScenarioIds.length) params.scenario_id = resultScenarioIds[0];
      const [resData, resCompare] = await Promise.all([
        api.get('/causal/results', { params }),
        resultScenarioIds.length
          ? api.get('/causal/results/compare', { params: { scenario_ids: resultScenarioIds.join(',') } })
          : Promise.resolve({ data: [] }),
      ]);
      setResultData(resData.data || []);
      setCompareData(resCompare.data || []);
    } catch (e) {
      setErrorResults(e?.response?.data?.detail || e.message);
    } finally {
      setLoadingResults(false);
    }
  }, [resultScenarioIds]);

  // ── Utilisation chart data ──────────────────────────────────────────────────
  const utilisationTraces = useMemo(() => {
    if (!fleetPlan.length) return [];
    const byType = {};
    for (const row of fleetPlan) {
      const key = String(row.asset_type_id || 'unknown');
      if (!byType[key]) byType[key] = { x: [], y: [], name: `Type ${key}` };
      byType[key].x.push(row.period_start);
      byType[key].y.push(row.util_hours || 0);
    }
    return Object.values(byType).map(t => ({
      ...t, type: 'scatter', mode: 'lines', stackgroup: 'one', fill: 'tonexty'
    }));
  }, [fleetPlan]);

  // ── Results timeline traces ─────────────────────────────────────────────────
  const resultTraces = useMemo(() => {
    if (!resultData.length) return [];
    const byScenario = {};
    for (const row of resultData) {
      const key = `Scenario ${row.scenario_id}`;
      if (!byScenario[key]) byScenario[key] = { x: [], y_mean: [], y_sched: [], y_unsched: [] };
      byScenario[key].x.push(row.period_start);
      byScenario[key].y_mean.push(row.demand_mean || 0);
      byScenario[key].y_sched.push(row.scheduled_demand || 0);
      byScenario[key].y_unsched.push(row.unscheduled_demand || 0);
    }
    const colors = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6'];
    return Object.entries(byScenario).map(([name, d], i) => ({
      x: d.x, y: d.y_mean, name, type: 'scatter', mode: 'lines+markers',
      line: { color: colors[i % colors.length] }
    }));
  }, [resultData]);

  // ── Stacked bar traces (scheduled vs unscheduled) ───────────────────────────
  const stackedBarTraces = useMemo(() => {
    if (!resultData.length) return [];
    const x = resultData.map(r => r.period_start);
    return [
      { x, y: resultData.map(r => r.scheduled_demand || 0), name: 'Scheduled', type: 'bar', marker: { color: '#3b82f6' } },
      { x, y: resultData.map(r => r.unscheduled_demand || 0), name: 'Unscheduled', type: 'bar', marker: { color: '#f59e0b' } },
    ];
  }, [resultData]);

  // ── Export CSV helper ───────────────────────────────────────────────────────
  const exportCsv = useCallback((data, filename) => {
    if (!data.length) return;
    const keys = Object.keys(data[0]);
    const csv = [keys.join(','), ...data.map(r => keys.map(k => JSON.stringify(r[k] ?? '')).join(','))].join('\n');
    const blob = new Blob([csv], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a'); a.href = url; a.download = filename; a.click();
    URL.revokeObjectURL(url);
  }, []);

  // ── Plotly layout helper ────────────────────────────────────────────────────
  const baseLayout = (title) => ({
    title: { text: title, font: { color: plotFont, size: 13 } },
    paper_bgcolor: plotPaper, plot_bgcolor: plotBg,
    font: { color: plotFont, size: 11 },
    margin: { l: 50, r: 20, t: 40, b: 40 },
    legend: { bgcolor: 'rgba(0,0,0,0)', font: { color: plotFont } },
    xaxis: { gridcolor: isDark ? '#374151' : '#e5e7eb' },
    yaxis: { gridcolor: isDark ? '#374151' : '#e5e7eb' },
  });

  const tdCls = 'px-3 py-2 text-sm text-gray-700 dark:text-gray-300 border-b border-gray-100 dark:border-gray-700';
  const thCls = 'px-3 py-2 text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wider text-left border-b border-gray-200 dark:border-gray-600';
  const btnPrimary = 'px-3 py-1.5 bg-blue-600 hover:bg-blue-700 text-white text-sm font-medium rounded-lg transition-colors disabled:opacity-50';
  const btnSecondary = 'px-3 py-1.5 bg-gray-100 dark:bg-gray-700 hover:bg-gray-200 dark:hover:bg-gray-600 text-gray-700 dark:text-gray-300 text-sm font-medium rounded-lg transition-colors';
  const inputCls = 'px-3 py-1.5 rounded-lg border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-700 text-gray-900 dark:text-white text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none w-full';

  return (
    <div className="p-4 sm:p-6 max-w-7xl mx-auto">
      <div className="mb-6">
        <h1 className="text-2xl font-bold text-gray-900 dark:text-white">Causal Forecasting</h1>
        <p className="mt-1 text-sm text-gray-500 dark:text-gray-400">
          Asset-driven demand generation from fleet utilisation, BOM, and MDFH
        </p>
      </div>

      {/* ─── Section 1: Fleet & Assets ──────────────────────────────────────── */}
      <Section title="Fleet & Assets" storageKey="causal_fleet_open" id="causal-fleet">
        {/* Asset type cards */}
        <div className="mb-4">
          <div className="flex items-center justify-between mb-3">
            <h3 className="text-sm font-semibold text-gray-700 dark:text-gray-300">Asset Types</h3>
            <button className={btnPrimary} onClick={() => setShowNewAssetTypeModal(true)}>
              + New Asset Type
            </button>
          </div>
          {loadingAssetTypes && <Spinner />}
          {errorAssetTypes && <ErrorBox msg={errorAssetTypes} />}
          {!loadingAssetTypes && !errorAssetTypes && (
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
              {assetTypes.length === 0 && (
                <p className="text-sm text-gray-400 dark:text-gray-500 col-span-3">No asset types defined yet.</p>
              )}
              {assetTypes.map(at => (
                <div key={at.asset_type_id}
                  className="bg-gray-50 dark:bg-gray-700/50 rounded-lg p-3 border border-gray-200 dark:border-gray-600">
                  <div className="font-semibold text-gray-900 dark:text-white text-sm">{at.code}</div>
                  {at.name && <div className="text-xs text-gray-500 dark:text-gray-400 mt-0.5">{at.name}</div>}
                  <div className="flex flex-wrap gap-1 mt-2">
                    {(at.removal_drivers || []).map(d => (
                      <span key={d} className="inline-block px-1.5 py-0.5 text-[10px] font-medium bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-400 rounded">
                        {d}
                      </span>
                    ))}
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>

        {/* New Asset Type Modal */}
        {showNewAssetTypeModal && (
          <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
            <div className="bg-white dark:bg-gray-800 rounded-xl shadow-xl p-6 w-full max-w-md mx-4">
              <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">New Asset Type</h3>
              <div className="space-y-3">
                <div>
                  <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Code *</label>
                  <input className={inputCls} value={newAssetType.code}
                    onChange={e => setNewAssetType(p => ({ ...p, code: e.target.value }))} placeholder="e.g. B737-800" />
                </div>
                <div>
                  <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Name</label>
                  <input className={inputCls} value={newAssetType.name}
                    onChange={e => setNewAssetType(p => ({ ...p, name: e.target.value }))} placeholder="e.g. Boeing 737-800" />
                </div>
                <div>
                  <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Removal Drivers</label>
                  <div className="flex flex-wrap gap-3">
                    {['hours', 'cycles', 'landings', 'calendar_days'].map(d => (
                      <label key={d} className="flex items-center gap-1.5 text-sm text-gray-700 dark:text-gray-300">
                        <input type="checkbox" checked={newAssetType.removal_drivers[d] || false}
                          onChange={e => setNewAssetType(p => ({ ...p, removal_drivers: { ...p.removal_drivers, [d]: e.target.checked } }))} />
                        {d}
                      </label>
                    ))}
                  </div>
                </div>
              </div>
              <div className="flex gap-2 mt-5 justify-end">
                <button className={btnSecondary} onClick={() => setShowNewAssetTypeModal(false)}>Cancel</button>
                <button className={btnPrimary} disabled={!newAssetType.code || savingAssetType} onClick={handleSaveAssetType}>
                  {savingAssetType ? 'Saving...' : 'Save'}
                </button>
              </div>
            </div>
          </div>
        )}

        {/* Fleet plan table */}
        <div className="mb-4">
          <div className="flex items-center justify-between mb-2">
            <h3 className="text-sm font-semibold text-gray-700 dark:text-gray-300">Fleet Plan (Base Scenario)</h3>
            <div className="flex gap-2">
              <button className={btnSecondary} onClick={() => loadFleetPlan(Math.max(1, fleetPage - 1))} disabled={fleetPage === 1}>
                &laquo; Prev
              </button>
              <span className="text-xs text-gray-500 dark:text-gray-400 self-center">Page {fleetPage}</span>
              <button className={btnSecondary} onClick={() => loadFleetPlan(fleetPage + 1)} disabled={fleetPlan.length < 50}>
                Next &raquo;
              </button>
            </div>
          </div>
          {loadingFleet && <Spinner />}
          {errorFleet && <ErrorBox msg={errorFleet} />}
          {!loadingFleet && !errorFleet && (
            <div className="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
              <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-700">
                <thead className="bg-gray-50 dark:bg-gray-800">
                  <tr>
                    {['Asset ID', 'Type', 'Site', 'Period Start', 'Hours', 'Cycles', 'Landings'].map(h => (
                      <th key={h} className={thCls}>{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody className="bg-white dark:bg-gray-800 divide-y divide-gray-100 dark:divide-gray-700">
                  {fleetPlan.length === 0 && (
                    <tr><td colSpan={7} className="text-center py-6 text-sm text-gray-400 dark:text-gray-500">No fleet plan data. Import data via API or CSV.</td></tr>
                  )}
                  {fleetPlan.map((row, i) => (
                    <tr key={i} className="hover:bg-gray-50 dark:hover:bg-gray-700/50">
                      <td className={tdCls}>{row.asset_id}</td>
                      <td className={tdCls}>{row.asset_type_id}</td>
                      <td className={tdCls}>{row.site_id}</td>
                      <td className={tdCls}>{row.period_start}</td>
                      <td className={tdCls}>{formatNumber(row.util_hours, locale, 1)}</td>
                      <td className={tdCls}>{formatNumber(row.util_cycles, locale, 0)}</td>
                      <td className={tdCls}>{formatNumber(row.util_landings, locale, 0)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>

        {/* Utilisation chart */}
        {utilisationTraces.length > 0 && (
          <div className="mb-4">
            <Plot
              data={utilisationTraces}
              layout={{
                ...baseLayout('Fleet Utilisation (Hours) by Asset Type'),
                barmode: 'stack',
                height: 260,
              }}
              config={{ displayModeBar: false, responsive: true }}
              style={{ width: '100%' }}
            />
          </div>
        )}

        {/* Fit MDFH */}
        <div className="flex items-center gap-3">
          <button className={btnSecondary} onClick={handleFitMdfh} disabled={mdfhJobStatus === 'running'}>
            {mdfhJobStatus === 'running' ? 'Fitting...' : 'Fit MDFH from History'}
          </button>
          {mdfhJobStatus && <StatusBadge status={mdfhJobStatus} />}
        </div>
      </Section>

      {/* ─── Section 2: BOM & Effectivity ───────────────────────────────────── */}
      <Section title="BOM & Effectivity" storageKey="causal_bom_open" id="causal-bom">
        <div className="mb-4">
          <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Asset Type</label>
          <select
            className="px-3 py-1.5 rounded-lg border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-700 text-gray-900 dark:text-white text-sm focus:ring-2 focus:ring-blue-500 outline-none"
            value={selectedAssetTypeId || ''}
            onChange={e => setSelectedAssetTypeId(e.target.value ? Number(e.target.value) : null)}
          >
            <option value="">-- Select Asset Type --</option>
            {assetTypes.map(at => (
              <option key={at.asset_type_id} value={at.asset_type_id}>{at.code} {at.name ? `(${at.name})` : ''}</option>
            ))}
          </select>
        </div>

        {selectedAssetTypeId && (
          <>
            {/* BOM Sunburst Explorer */}
            <div className="mb-4">
              <BomExplorer
                assetTypeId={selectedAssetTypeId}
                assetId={null}
                onSelectPart={(itemId) => setSelectedPartId(itemId)}
              />
            </div>

            {/* BOM Explosion Preview Table */}
            <div className="mb-4">
              <div className="flex items-center justify-between mb-2">
                <h3 className="text-sm font-semibold text-gray-700 dark:text-gray-300">BOM Explosion — Fleet Demand Rate</h3>
                <button className={btnSecondary} onClick={() => exportCsv(bomExplosion, 'bom_explosion.csv')}>
                  Export CSV
                </button>
              </div>
              {loadingBomExplosion && <Spinner />}
              {errorBomExplosion && <ErrorBox msg={errorBomExplosion} />}
              {!loadingBomExplosion && !errorBomExplosion && (
                <div className="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
                  <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-700">
                    <thead className="bg-gray-50 dark:bg-gray-800">
                      <tr>
                        {['Item ID', 'Item Name', 'Driver', 'Fleet Qty', 'MDFH Mean', 'Demand Rate/Period'].map(h => (
                          <th key={h} className={thCls}>{h}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody className="bg-white dark:bg-gray-800 divide-y divide-gray-100 dark:divide-gray-700">
                      {bomExplosion.length === 0 && (
                        <tr><td colSpan={6} className="text-center py-6 text-sm text-gray-400 dark:text-gray-500">No BOM data for this asset type.</td></tr>
                      )}
                      {bomExplosion.map((row, i) => (
                        <tr key={i}
                          className={`hover:bg-gray-50 dark:hover:bg-gray-700/50 cursor-pointer ${selectedPartId === row.item_id ? 'bg-blue-50 dark:bg-blue-900/20' : ''}`}
                          onClick={() => setSelectedPartId(row.item_id)}
                        >
                          <td className={tdCls}>{row.item_id}</td>
                          <td className={tdCls}>{row.item_name || '—'}</td>
                          <td className={tdCls}>{row.removal_driver}</td>
                          <td className={tdCls}>{formatNumber(row.effective_qty_fleet_total, locale, 2)}</td>
                          <td className={tdCls}>{formatNumber(row.mdfh_mean, locale, 6)}</td>
                          <td className={`${tdCls} font-semibold`}>{formatNumber(row.demand_rate_per_period, locale, 3)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          </>
        )}
        {!selectedAssetTypeId && (
          <p className="text-sm text-gray-400 dark:text-gray-500">Select an asset type to view its BOM.</p>
        )}
      </Section>

      {/* ─── Section 3: Scenarios ───────────────────────────────────────────── */}
      <Section title="Scenarios" storageKey="causal_scenarios_open" id="causal-scenarios">
        <div className="flex items-center justify-between mb-4">
          <div className="flex items-center gap-3">
            <button
              className={btnPrimary}
              disabled={!selectedScenarioIds.length || runJobStatus === 'running'}
              onClick={handleRunScenarios}
            >
              {runJobStatus === 'running' ? 'Running...' : `Run Selected (${selectedScenarioIds.length})`}
            </button>
            {runJobStatus && <StatusBadge status={runJobStatus} />}
            {runJobResult && (
              <span className="text-xs text-gray-500 dark:text-gray-400">
                {runJobResult.scenarios_run} scenario(s), {runJobResult.demand_rows} rows
              </span>
            )}
          </div>
          <button className={btnPrimary} onClick={() => setShowNewScenarioModal(true)}>
            + New Scenario
          </button>
        </div>

        {loadingScenarios && <Spinner />}
        {errorScenarios && <ErrorBox msg={errorScenarios} />}
        {!loadingScenarios && !errorScenarios && (
          <div className="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
            <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-700">
              <thead className="bg-gray-50 dark:bg-gray-800">
                <tr>
                  <th className={thCls}>
                    <input type="checkbox"
                      checked={selectedScenarioIds.length === scenarios.length && scenarios.length > 0}
                      onChange={e => setSelectedScenarioIds(e.target.checked ? scenarios.map(s => s.scenario_id) : [])} />
                  </th>
                  {['ID', 'Name', 'Base', 'Linked MEIO', 'Created', 'Actions'].map(h => (
                    <th key={h} className={thCls}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody className="bg-white dark:bg-gray-800 divide-y divide-gray-100 dark:divide-gray-700">
                {scenarios.length === 0 && (
                  <tr><td colSpan={7} className="text-center py-6 text-sm text-gray-400 dark:text-gray-500">No scenarios yet. Create one to get started.</td></tr>
                )}
                {scenarios.map(sc => (
                  <tr key={sc.scenario_id} className="hover:bg-gray-50 dark:hover:bg-gray-700/50">
                    <td className={tdCls}>
                      <input type="checkbox"
                        checked={selectedScenarioIds.includes(sc.scenario_id)}
                        onChange={e => setSelectedScenarioIds(prev =>
                          e.target.checked ? [...prev, sc.scenario_id] : prev.filter(id => id !== sc.scenario_id)
                        )} />
                    </td>
                    <td className={tdCls}>{sc.scenario_id}</td>
                    <td className={`${tdCls} font-medium`}>{sc.name}</td>
                    <td className={tdCls}>
                      {sc.is_base && (
                        <span className="inline-block px-1.5 py-0.5 text-[10px] font-medium bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400 rounded">
                          Base
                        </span>
                      )}
                    </td>
                    <td className={tdCls}>{sc.linked_meio_scenario_id || '—'}</td>
                    <td className={tdCls}>{sc.created_at ? new Date(sc.created_at).toLocaleDateString() : '—'}</td>
                    <td className={tdCls}>
                      <button
                        className="text-xs text-red-500 hover:text-red-700 dark:hover:text-red-400"
                        onClick={async () => {
                          if (!window.confirm(`Delete scenario "${sc.name}"?`)) return;
                          try {
                            await api.delete(`/causal/scenarios/${sc.scenario_id}`);
                            await loadScenarios();
                          } catch (e) {
                            alert(e?.response?.data?.detail || e.message);
                          }
                        }}
                      >
                        Delete
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}

        {/* New Scenario Modal */}
        {showNewScenarioModal && (
          <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
            <div className="bg-white dark:bg-gray-800 rounded-xl shadow-xl p-6 w-full max-w-lg mx-4">
              <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">New Scenario</h3>
              <div className="space-y-3">
                <div>
                  <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Name *</label>
                  <input className={inputCls} value={newScenario.name}
                    onChange={e => setNewScenario(p => ({ ...p, name: e.target.value }))} placeholder="e.g. High Utilisation Q3" />
                </div>
                <div>
                  <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Description</label>
                  <textarea className={inputCls} rows={2} value={newScenario.description || ''}
                    onChange={e => setNewScenario(p => ({ ...p, description: e.target.value }))} />
                </div>
                <div className="flex items-center gap-2">
                  <input type="checkbox" id="is_base" checked={newScenario.is_base}
                    onChange={e => setNewScenario(p => ({ ...p, is_base: e.target.checked }))} />
                  <label htmlFor="is_base" className="text-sm text-gray-700 dark:text-gray-300">Base scenario</label>
                </div>
                <div>
                  <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">
                    Utilisation Multiplier: {newScenario.fleet_overrides.utilization_multiplier}x
                  </label>
                  <input type="range" min="0.5" max="2.0" step="0.05"
                    value={newScenario.fleet_overrides.utilization_multiplier}
                    onChange={e => setNewScenario(p => ({ ...p, fleet_overrides: { ...p.fleet_overrides, utilization_multiplier: parseFloat(e.target.value) } }))}
                    className="w-full" />
                  <div className="flex justify-between text-xs text-gray-400 dark:text-gray-500">
                    <span>0.5x</span><span>1.0x</span><span>2.0x</span>
                  </div>
                </div>
                <div>
                  <div className="flex items-center justify-between mb-1">
                    <label className="block text-xs font-medium text-gray-600 dark:text-gray-400">
                      Fleet Overrides JSON
                    </label>
                    <button className="text-xs text-blue-500 hover:underline" onClick={() => {
                      setScenarioJsonMode(!scenarioJsonMode);
                      setScenarioJsonText(JSON.stringify(newScenario.fleet_overrides, null, 2));
                    }}>
                      {scenarioJsonMode ? 'Use form' : 'Edit JSON'}
                    </button>
                  </div>
                  {scenarioJsonMode && (
                    <textarea className={`${inputCls} font-mono text-xs`} rows={5}
                      value={scenarioJsonText}
                      onChange={e => setScenarioJsonText(e.target.value)} />
                  )}
                </div>
              </div>
              <div className="flex gap-2 mt-5 justify-end">
                <button className={btnSecondary} onClick={() => setShowNewScenarioModal(false)}>Cancel</button>
                <button className={btnPrimary} disabled={!newScenario.name || savingScenario} onClick={handleSaveScenario}>
                  {savingScenario ? 'Saving...' : 'Save'}
                </button>
              </div>
            </div>
          </div>
        )}
      </Section>

      {/* ─── Section 4: Results ─────────────────────────────────────────────── */}
      <Section title="Results" storageKey="causal_results_open" id="causal-results">
        {/* Filters */}
        <div className="flex flex-wrap gap-3 mb-4">
          <div className="flex-1 min-w-32">
            <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Scenarios</label>
            <div className="flex flex-wrap gap-2">
              {scenarios.map(sc => (
                <label key={sc.scenario_id} className="flex items-center gap-1 text-sm text-gray-700 dark:text-gray-300">
                  <input type="checkbox"
                    checked={resultScenarioIds.includes(sc.scenario_id)}
                    onChange={e => setResultScenarioIds(prev =>
                      e.target.checked ? [...prev, sc.scenario_id] : prev.filter(id => id !== sc.scenario_id)
                    )} />
                  {sc.name}
                </label>
              ))}
            </div>
          </div>
          <div className="self-end">
            <button className={btnPrimary} onClick={handleLoadResults} disabled={loadingResults}>
              {loadingResults ? 'Loading...' : 'Load Results'}
            </button>
          </div>
          {resultData.length > 0 && (
            <div className="self-end">
              <button className={btnSecondary} onClick={() => exportCsv(resultData, 'causal_results.csv')}>
                Export CSV
              </button>
            </div>
          )}
        </div>
        {errorResults && <ErrorBox msg={errorResults} />}

        {/* Demand Timeline */}
        {resultTraces.length > 0 && (
          <div className="mb-6">
            <Plot
              data={resultTraces}
              layout={{
                ...baseLayout('Demand Timeline by Scenario'),
                height: 300,
              }}
              config={{ displayModeBar: false, responsive: true }}
              style={{ width: '100%' }}
            />
          </div>
        )}

        {/* Stacked bar: scheduled vs unscheduled */}
        {stackedBarTraces.length > 0 && stackedBarTraces[0].x.length > 0 && (
          <div className="mb-6">
            <Plot
              data={stackedBarTraces}
              layout={{
                ...baseLayout('Scheduled vs Unscheduled Demand'),
                barmode: 'stack',
                height: 260,
              }}
              config={{ displayModeBar: false, responsive: true }}
              style={{ width: '100%' }}
            />
          </div>
        )}

        {/* Comparison table */}
        {compareData.length > 0 && (
          <div className="mb-4">
            <h3 className="text-sm font-semibold text-gray-700 dark:text-gray-300 mb-2">Scenario Comparison</h3>
            <div className="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
              <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-700">
                <thead className="bg-gray-50 dark:bg-gray-800">
                  <tr>
                    {['Item', 'Site', 'Scenario', 'Total Demand', 'Scheduled', 'Unscheduled'].map(h => (
                      <th key={h} className={thCls}>{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody className="bg-white dark:bg-gray-800 divide-y divide-gray-100 dark:divide-gray-700">
                  {compareData.map((row, i) => (
                    <tr key={i} className="hover:bg-gray-50 dark:hover:bg-gray-700/50">
                      <td className={tdCls}>{row.item_name || row.item_id}</td>
                      <td className={tdCls}>{row.site_id}</td>
                      <td className={tdCls}>{row.scenario_name || row.scenario_id}</td>
                      <td className={`${tdCls} font-semibold`}>{formatNumber(row.total_demand, locale, 2)}</td>
                      <td className={tdCls}>{formatNumber(row.total_scheduled, locale, 2)}</td>
                      <td className={tdCls}>{formatNumber(row.total_unscheduled, locale, 2)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {!loadingResults && resultData.length === 0 && (
          <p className="text-sm text-gray-400 dark:text-gray-500 text-center py-8">
            Select scenarios and click "Load Results" to view demand data.
          </p>
        )}
      </Section>
    </div>
  );
}

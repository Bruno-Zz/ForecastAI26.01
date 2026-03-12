/**
 * ProcessRunner Component
 *
 * Two tabs:
 *  1. Processes — individual runnable steps with optional segment scoping
 *  2. Pipelines — define and run custom ordered multi-step pipelines scoped to segments
 */

import React, { useState, useEffect, useRef, useCallback } from 'react';
import { useLocale } from '../contexts/LocaleContext';
import { formatTime, formatNumber } from '../utils/formatting';
import api from '../utils/api';

// ─── Constants ────────────────────────────────────────────────────────────────

/** Steps shown in the Processes tab (best-method & distributions are internal/auto) */
const PROCESS_STEP_IDS = [
  'etl', 'outlier-detection', 'segmentation', 'characterization', 'forecast', 'backtest',
];

const ICONS = {
  'etl':               '🗄️',
  'outlier-detection': '🔍',
  'segmentation':      '🏷️',
  'characterization':  '🔬',
  'forecast':          '📊',
  'backtest':          '🔁',
  'best-method':       '🏆',
  'distributions':     '📈',
};

const STEP_LABELS = {
  'etl':               'ETL',
  'outlier-detection': 'Outlier Detection',
  'segmentation':      'Segmentation',
  'characterization':  'Characterization',
  'forecast':          'Forecast',
  'backtest':          'Backtest',
  'best-method':       'Best Method',
  'distributions':     'Distributions',
};

const PIPELINES_STORAGE_KEY = 'process_runner_pipelines';

const parseUTC = (s) => new Date(s.endsWith('Z') ? s : s + 'Z');

// ─── Shared CSS animation ─────────────────────────────────────────────────────
const INDETERMINATE_STYLE = `
  @keyframes pr2-slide {
    0%   { transform: translateX(-100%); }
    100% { transform: translateX(500%); }
  }
  .pr2-slide { animation: pr2-slide 1.4s ease-in-out infinite; }
`;

// ─── Shared helper components ─────────────────────────────────────────────────

const Spinner = ({ cls = 'w-4 h-4' }) => (
  <svg className={`animate-spin ${cls}`} viewBox="0 0 24 24" fill="none">
    <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
    <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8z" />
  </svg>
);

const ElapsedTimer = ({ startedAt }) => {
  const [sec, setSec] = React.useState(0);
  React.useEffect(() => {
    if (!startedAt) return;
    const base = parseUTC(startedAt).getTime();
    const tick = () => setSec(Math.max(0, Math.floor((Date.now() - base) / 1000)));
    tick();
    const id = setInterval(tick, 1000);
    return () => clearInterval(id);
  }, [startedAt]);
  const m = Math.floor(sec / 60);
  const s = sec % 60;
  return <span className="tabular-nums font-mono">{m > 0 ? `${m}m ` : ''}{String(s).padStart(2, '0')}s</span>;
};

const StatusBadge = ({ status }) => {
  const map = {
    pending:     { cls: 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400',               label: 'Pending' },
    running:     { cls: 'bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300',             label: 'Running\u2026' },
    success:     { cls: 'bg-emerald-100 dark:bg-emerald-900/30 text-emerald-700 dark:text-emerald-300', label: 'Success' },
    error:       { cls: 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-300',                 label: 'Error' },
    interrupted: { cls: 'bg-amber-100 dark:bg-amber-900/30 text-amber-700 dark:text-amber-300',         label: 'Interrupted' },
    idle:        { cls: 'bg-gray-100 dark:bg-gray-700 text-gray-500 dark:text-gray-400',                label: 'Idle' },
  };
  const { cls, label } = map[status] || { cls: 'bg-gray-100 dark:bg-gray-700 text-gray-500', label: status };
  return (
    <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium ${cls}`}>
      {status === 'running' && <Spinner cls="w-3 h-3" />}
      {status === 'success' && '\u2713'}
      {status === 'error'   && '\u2715'}
      {status === 'interrupted' && '\u26A0'}
      {label}
    </span>
  );
};

const ForecastProgressBar = ({ progress, label = 'series' }) => {
  if (!progress || !progress.total) return null;
  const { completed = 0, total, batches_done, batches_total } = progress;
  const pct = total > 0 ? Math.min(100, Math.round((completed / total) * 100)) : 0;
  return (
    <div className="mt-2 space-y-1">
      <div className="flex items-center justify-between text-xs">
        <span className="font-medium text-blue-600 dark:text-blue-400">
          {completed.toLocaleString()} / {total.toLocaleString()} {label}
        </span>
        {batches_total != null && batches_total > 1 && (
          <span className="text-gray-400 dark:text-gray-500">batch {batches_done}/{batches_total}</span>
        )}
        <span className="font-semibold text-blue-600 dark:text-blue-400">{pct}%</span>
      </div>
      <div className="w-full bg-gray-200 dark:bg-gray-700 rounded-full h-1.5 overflow-hidden">
        <div className="bg-blue-500 dark:bg-blue-400 h-1.5 rounded-full transition-all duration-500" style={{ width: `${pct}%` }} />
      </div>
    </div>
  );
};

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
  const colorLine = (line) => {
    if (/error|exception|failed|traceback/i.test(line)) return 'text-red-400';
    if (/warning/i.test(line))                           return 'text-yellow-300';
    if (/✓|success|complete|done|finished/i.test(line)) return 'text-emerald-400';
    if (/▶|={3,}/i.test(line))                          return 'text-purple-300 font-semibold';
    if (/info/i.test(line))                             return 'text-blue-300';
    return 'text-gray-300';
  };
  return (
    <div ref={containerRef} onScroll={handleScroll}
      className="mt-3 bg-gray-900 rounded-lg p-3 max-h-72 overflow-y-auto font-mono text-xs leading-5 border border-gray-700">
      {lines.length === 0
        ? <span className="text-gray-500 italic">Waiting for output\u2026</span>
        : lines.map((l, i) => <div key={i} className={colorLine(l)}>{l || '\u00A0'}</div>)
      }
    </div>
  );
};

// ─── Segment Selector ─────────────────────────────────────────────────────────

/**
 * Multi-select dropdown for segments.
 * `selected` is an array of segment IDs. Empty array means "All segments".
 */
const SegmentSelector = ({ segments, selected, onChange }) => {
  const [open, setOpen] = useState(false);
  const ref = useRef(null);

  useEffect(() => {
    if (!open) return;
    const handler = (e) => { if (ref.current && !ref.current.contains(e.target)) setOpen(false); };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, [open]);

  if (!segments || segments.length === 0) return null;

  const label =
    selected.length === 0 || selected.length === segments.length
      ? 'All segments'
      : selected.length === 1
        ? (segments.find(s => s.id === selected[0])?.name || selected[0])
        : `${selected.length} segments`;

  const toggle = (id) => {
    onChange(selected.includes(id) ? selected.filter(s => s !== id) : [...selected, id]);
  };

  return (
    <div className="relative" ref={ref}>
      <button
        type="button"
        onClick={() => setOpen(v => !v)}
        className="flex items-center gap-1.5 text-xs rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-600 px-2.5 py-1.5 transition-colors"
      >
        <span>🏷️</span>
        <span className="max-w-28 truncate">{label}</span>
        <svg className={`w-3 h-3 flex-shrink-0 transition-transform ${open ? 'rotate-180' : ''}`}
          fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
        </svg>
      </button>
      {open && (
        <div className="absolute top-full mt-1 left-0 z-50 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg shadow-xl min-w-48 max-h-60 overflow-y-auto">
          <div className="p-1">
            <button
              type="button"
              onClick={() => onChange([])}
              className={`w-full text-left px-3 py-1.5 rounded text-xs hover:bg-gray-50 dark:hover:bg-gray-700 transition-colors ${
                selected.length === 0 ? 'font-semibold text-blue-600 dark:text-blue-400' : 'text-gray-700 dark:text-gray-300'
              }`}
            >
              All segments
            </button>
            <div className="my-1 border-t border-gray-100 dark:border-gray-700" />
            {segments.map(seg => (
              <label key={seg.id} className="flex items-center gap-2 px-3 py-1.5 rounded cursor-pointer hover:bg-gray-50 dark:hover:bg-gray-700 text-xs text-gray-700 dark:text-gray-300">
                <input
                  type="checkbox"
                  checked={selected.includes(seg.id)}
                  onChange={() => toggle(seg.id)}
                  className="rounded border-gray-300 dark:border-gray-600 text-blue-600"
                />
                <span className="truncate">{seg.name}</span>
              </label>
            ))}
          </div>
        </div>
      )}
    </div>
  );
};

// ─── SSE helper (shared across tabs) ─────────────────────────────────────────

/**
 * Open an SSE stream for a job. Calls `onLine(line)`, `onProgress(key, data)`,
 * and `onDone(status)` as events arrive.
 */
function openJobSSE(jobId, { onLine, onProgress, onDone }) {
  const token = localStorage.getItem('forecastai_token') || '';
  const es = new EventSource(`/api/pipeline/jobs/${jobId}/stream${token ? `?token=${token}` : ''}`);

  es.onmessage = (e) => {
    try {
      const { line } = JSON.parse(e.data);
      const progMatch = line.match(
        /\[(FORECAST_PROGRESS|BACKTEST_PROGRESS|OUTLIER_PROGRESS|CHAR_PROGRESS|BESTMETHOD_PROGRESS|SEGMENTATION_PROGRESS|DISTRIBUTIONS_PROGRESS)\]\s+(.*)/
      );
      if (progMatch) {
        const progData = {};
        progMatch[2].trim().split(/\s+/).forEach(pair => {
          const eq = pair.indexOf('=');
          if (eq > 0) {
            const k = pair.slice(0, eq);
            const v = pair.slice(eq + 1);
            progData[k] = isNaN(v) ? v : Number(v);
          }
        });
        onProgress?.(progMatch[1], progData);
      } else {
        onLine?.(line);
      }
    } catch { /* ignore */ }
  };

  es.addEventListener('done', (e) => {
    try {
      const { status } = JSON.parse(e.data);
      onDone?.(status);
    } catch {
      onDone?.('error');
    }
    es.close();
  });

  es.onerror = () => {
    es.close();
    onDone?.('error');
  };

  return es;
}

// ─── Step Card ────────────────────────────────────────────────────────────────

const StepCard = ({ step, onRun, onKill, activeJob, onToggleLogs, showLogs, disabled, locale, segments }) => {
  const [selectedSegments, setSelectedSegments] = useState([]);
  const isRunning = activeJob?.status === 'running';
  const isDone    = activeJob?.status === 'success' || activeJob?.status === 'error' || activeJob?.status === 'interrupted';
  const hasJob    = !!activeJob;

  return (
    <div className={`bg-white dark:bg-gray-800 rounded-xl border-2 transition-colors ${
      isRunning                       ? 'border-blue-300 dark:border-blue-600 shadow-md dark:shadow-blue-900/30' :
      activeJob?.status === 'success' ? 'border-emerald-200 dark:border-emerald-700' :
      activeJob?.status === 'error'   ? 'border-red-200 dark:border-red-700' :
      activeJob?.status === 'interrupted' ? 'border-amber-200 dark:border-amber-700' :
      'border-gray-200 dark:border-gray-600'
    }`}>
      <div className="p-4">
        <div className="flex flex-col sm:flex-row sm:items-start sm:justify-between gap-3">
          <div className="flex items-start gap-3 min-w-0">
            <span className="text-2xl flex-shrink-0">{ICONS[step.id] || '⚙️'}</span>
            <div className="min-w-0">
              <h3 className="font-semibold text-gray-900 dark:text-gray-100 text-sm">{step.label}</h3>
              <p className="text-xs text-gray-500 dark:text-gray-400 mt-0.5 leading-relaxed">{step.desc}</p>
            </div>
          </div>
          <div className="flex flex-wrap items-center gap-2 flex-shrink-0 self-end sm:self-start">
            <SegmentSelector segments={segments} selected={selectedSegments} onChange={setSelectedSegments} />
            {hasJob && <StatusBadge status={activeJob.status} />}
            {isRunning && (
              <button
                onClick={() => onKill(activeJob.job_id)}
                className="px-3 py-1.5 rounded-lg text-sm font-medium bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-300 hover:bg-red-200 dark:hover:bg-red-900/50 active:scale-95 transition-colors flex items-center gap-1"
              >
                <span>■</span> Stop
              </button>
            )}
            <button
              onClick={() => onRun(step.id, selectedSegments)}
              disabled={disabled}
              className={`px-3 py-1.5 rounded-lg text-sm font-medium transition-colors ${
                disabled
                  ? 'bg-gray-100 dark:bg-gray-700 text-gray-400 dark:text-gray-500 cursor-not-allowed'
                  : 'bg-blue-600 text-white hover:bg-blue-700 active:scale-95'
              }`}
            >
              {isRunning ? 'Running\u2026' : isDone ? 'Re-run' : 'Run'}
            </button>
          </div>
        </div>

        {/* Timing */}
        {activeJob?.started_at && (
          <div className="mt-2 flex items-center gap-3 text-xs text-gray-400 dark:text-gray-500">
            <span>Started: {formatTime(activeJob.started_at, locale)}</span>
            {activeJob.ended_at && (
              <span>· Duration: {formatNumber(
                (parseUTC(activeJob.ended_at) - parseUTC(activeJob.started_at)) / 1000, locale, 1
              )}s</span>
            )}
            {activeJob.exit_code != null && activeJob.exit_code !== 0 && (
              <span className="text-red-400 dark:text-red-300">· Exit: {activeJob.exit_code}</span>
            )}
          </div>
        )}

        {/* Progress */}
        {(isRunning || isDone) && (() => {
          const op  = step.id === 'outlier-detection' && activeJob?.progress?.OUTLIER_PROGRESS;
          const sgp = step.id === 'segmentation'      && activeJob?.progress?.SEGMENTATION_PROGRESS;
          const cp  = step.id === 'characterization'  && activeJob?.progress?.CHAR_PROGRESS;
          const mp  = step.id === 'best-method'       && activeJob?.progress?.BESTMETHOD_PROGRESS;
          const fp  = step.id === 'forecast'          && activeJob?.progress?.FORECAST_PROGRESS;
          const bp  = step.id === 'backtest'          && activeJob?.progress?.BACKTEST_PROGRESS;
          if (op)  return <ForecastProgressBar progress={op}  label="series" />;
          if (sgp) return <ForecastProgressBar progress={sgp} label="segments" />;
          if (cp)  return <ForecastProgressBar progress={cp}  label="series" />;
          if (mp)  return <ForecastProgressBar progress={mp}  label="series" />;
          if (fp)  return <ForecastProgressBar progress={fp}  label="series" />;
          if (bp)  return <ForecastProgressBar progress={bp}  label="series" />;
          if (isRunning) return (
            <div className="mt-2">
              <div className="flex justify-between items-center text-xs text-gray-400 dark:text-gray-500 mb-1">
                <span>Running\u2026</span>
                <ElapsedTimer startedAt={activeJob?.started_at} />
              </div>
              <div className="relative w-full bg-gray-200 dark:bg-gray-700 rounded-full h-1.5 overflow-hidden">
                <div className="absolute inset-y-0 left-0 w-1/4 bg-blue-500 dark:bg-blue-400 rounded-full pr2-slide" />
              </div>
            </div>
          );
          return null;
        })()}

        {/* Log toggle */}
        {hasJob && (
          <button
            onClick={() => onToggleLogs(step.id)}
            className="mt-2 text-xs text-blue-500 dark:text-blue-400 hover:text-blue-700 dark:hover:text-blue-300 flex items-center gap-1"
          >
            <span>{showLogs ? '▲ Hide' : '▼ Show'} logs</span>
            <span className="text-gray-400 dark:text-gray-500">({activeJob.log_lines?.length ?? 0} lines)</span>
          </button>
        )}
      </div>
      <LogViewer lines={activeJob?.log_lines ?? []} visible={showLogs} />
      {showLogs && <div className="h-2" />}
    </div>
  );
};

// ─── Processes Tab ────────────────────────────────────────────────────────────

const ProcessesTab = ({ allSteps, segments }) => {
  const { locale } = useLocale();
  const [jobs, setJobs]         = useState({});
  const [showLogs, setShowLogs] = useState({});
  const [error, setError]       = useState(null);
  const eventSources = useRef({});
  const jobsRef      = useRef({});
  useEffect(() => { jobsRef.current = jobs; }, [jobs]);

  // Open SSE for a single step job
  const attachSSE = useCallback((stepId, jobId) => {
    if (eventSources.current[stepId]) {
      eventSources.current[stepId].close();
    }
    const es = openJobSSE(jobId, {
      onLine: (line) => setJobs(prev => {
        const job = prev[stepId];
        if (!job || job.job_id !== jobId) return prev;
        return { ...prev, [stepId]: { ...job, log_lines: [...(job.log_lines || []), line] } };
      }),
      onProgress: (key, data) => setJobs(prev => {
        const job = prev[stepId];
        if (!job || job.job_id !== jobId) return prev;
        return { ...prev, [stepId]: { ...job, progress: { ...(job.progress || {}), [key]: data } } };
      }),
      onDone: (status) => {
        setJobs(prev => {
          const job = prev[stepId];
          if (!job || job.job_id !== jobId) return prev;
          return { ...prev, [stepId]: { ...job, status, ended_at: new Date().toISOString() } };
        });
        delete eventSources.current[stepId];
        // Confirm final state from server
        setTimeout(async () => {
          try {
            const r = await api.get(`/pipeline/jobs/${jobId}`);
            setJobs(prev => ({
              ...prev,
              [stepId]: { ...r.data, progress: { ...(prev[stepId]?.progress || {}), ...(r.data.progress || {}) } },
            }));
          } catch { /* ignore */ }
        }, 2000);
      },
    });
    eventSources.current[stepId] = es;
  }, []);

  // Restore running jobs on mount
  const restoredRef = useRef(false);
  useEffect(() => {
    if (restoredRef.current) return;
    restoredRef.current = true;
    (async () => {
      try {
        const r = await api.get(`/pipeline/jobs`);
        const restored = {};
        const seen = new Set();
        for (const job of r.data) {
          if (job.step && job.step !== 'full-pipeline' && !seen.has(job.step)) {
            seen.add(job.step);
            restored[job.step] = job;
          }
        }
        if (Object.keys(restored).length > 0) {
          setJobs(restored);
          const toShow = {};
          for (const [id, job] of Object.entries(restored)) {
            if (job.status === 'running' || job.status === 'pending') toShow[id] = true;
          }
          if (Object.keys(toShow).length > 0) setShowLogs(toShow);
        }
      } catch { /* ignore */ }
    })();
  }, []);

  // Reconnect SSE for restored running jobs
  useEffect(() => {
    for (const [stepId, job] of Object.entries(jobs)) {
      if ((job.status === 'running' || job.status === 'pending') && !eventSources.current[stepId]) {
        attachSSE(stepId, job.job_id);
      }
    }
  }, [jobs, attachSSE]);

  // Poll for status syncing
  useEffect(() => {
    const interval = setInterval(async () => {
      const nowMs = Date.now();
      const isLive = (j) =>
        j.status === 'running' || j.status === 'pending' ||
        (j.ended_at && nowMs - new Date(j.ended_at).getTime() < 8000);
      for (const [stepId, job] of Object.entries(jobsRef.current).filter(([, j]) => isLive(j))) {
        try {
          const r = await api.get(`/pipeline/jobs/${job.job_id}`);
          setJobs(prev => ({
            ...prev,
            [stepId]: { ...r.data, progress: { ...(prev[stepId]?.progress || {}), ...(r.data.progress || {}) } },
          }));
        } catch { /* ignore */ }
      }
    }, 1000);
    return () => clearInterval(interval);
  }, []);

  // Cleanup on unmount
  useEffect(() => () => Object.values(eventSources.current).forEach(es => es.close()), []);

  const handleRun = async (stepId, segmentIds = []) => {
    try {
      setError(null);
      const body = segmentIds.length > 0
        ? (segmentIds.length === 1 ? { segment_id: segmentIds[0] } : { segment_ids: segmentIds })
        : {};
      const r = await api.post(`/pipeline/run/${stepId}`, body);
      const job = { ...r.data, log_lines: [], started_at: null, ended_at: null };
      setJobs(prev => ({ ...prev, [stepId]: job }));
      setShowLogs(prev => ({ ...prev, [stepId]: true }));
      attachSSE(stepId, r.data.job_id);
    } catch (e) {
      setError(e.response?.data?.detail || e.message);
    }
  };

  const handleKill = async (jobId) => {
    try { await api.post(`/pipeline/jobs/${jobId}/kill`); }
    catch (e) { setError(e.response?.data?.detail || e.message); }
  };

  const handleResetJobs = async () => {
    try {
      setError(null);
      await api.post(`/pipeline/jobs/reset`);
      setJobs({});
      setShowLogs({});
    } catch (e) { setError(e.response?.data?.detail || e.message); }
  };

  const toggleLogs = (stepId) => setShowLogs(prev => ({ ...prev, [stepId]: !prev[stepId] }));
  const anyRunning = Object.values(jobs).some(j => j.status === 'running');
  const processSteps = PROCESS_STEP_IDS.map(id => allSteps.find(s => s.id === id)).filter(Boolean);

  return (
    <div className="space-y-3">
      {error && (
        <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg px-4 py-3 text-sm text-red-700 dark:text-red-300 flex items-start gap-2">
          <span className="flex-shrink-0 mt-0.5">⚠️</span>
          <span className="flex-1">
            {error}
            {error.toLowerCase().includes('already running') && (
              <button onClick={handleResetJobs}
                className="ml-2 underline text-blue-600 dark:text-blue-400 hover:text-blue-800 text-xs font-medium">
                Reset stale jobs
              </button>
            )}
          </span>
          <button onClick={() => setError(null)} className="text-red-400 hover:text-red-300 flex-shrink-0">✕</button>
        </div>
      )}

      {anyRunning && (
        <div className="bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-lg px-4 py-2.5 text-sm text-blue-700 dark:text-blue-300 flex items-center gap-2">
          <Spinner /> A process is currently running\u2026
        </div>
      )}

      {processSteps.map((step, i) => (
        <div key={step.id} className="flex gap-3 items-stretch">
          {/* Step number + connector */}
          <div className="flex flex-col items-center flex-shrink-0 w-8">
            <div className={`w-7 h-7 rounded-full flex items-center justify-center text-xs font-bold border-2 flex-shrink-0 ${
              jobs[step.id]?.status === 'success'     ? 'bg-emerald-100 dark:bg-emerald-900/30 border-emerald-400 text-emerald-700 dark:text-emerald-300' :
              jobs[step.id]?.status === 'error'       ? 'bg-red-100 dark:bg-red-900/30 border-red-400 text-red-700 dark:text-red-300' :
              jobs[step.id]?.status === 'interrupted' ? 'bg-amber-100 dark:bg-amber-900/30 border-amber-400 text-amber-700 dark:text-amber-300' :
              jobs[step.id]?.status === 'running'     ? 'bg-blue-100 dark:bg-blue-900/30 border-blue-400 text-blue-700 dark:text-blue-300' :
              'bg-gray-100 dark:bg-gray-700 border-gray-300 dark:border-gray-600 text-gray-500 dark:text-gray-400'
            }`}>{i + 1}</div>
            {i < processSteps.length - 1 && (
              <div className="w-0.5 flex-1 mt-1 bg-gray-200 dark:bg-gray-700 min-h-4" />
            )}
          </div>
          <div className="flex-1 min-w-0">
            <StepCard
              step={step}
              onRun={handleRun}
              onKill={handleKill}
              activeJob={jobs[step.id] || null}
              onToggleLogs={toggleLogs}
              showLogs={!!showLogs[step.id]}
              disabled={anyRunning && jobs[step.id]?.status !== 'running'}
              locale={locale}
              segments={segments}
            />
          </div>
        </div>
      ))}

      <div className="mt-4 p-4 bg-gray-50 dark:bg-gray-800/50 rounded-lg border border-gray-200 dark:border-gray-700">
        <p className="text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wide mb-2">Notes</p>
        <ul className="text-xs text-gray-500 dark:text-gray-400 space-y-1 list-disc list-inside">
          <li>Individual processes run independently — re-run any step without re-running earlier ones.</li>
          <li>Select segments to scope the run to a specific data subset (requires backend support).</li>
          <li><strong>ETL</strong> requires a live database connection (see <code>config/config.yaml</code>).</li>
          <li>Restart the API after pipeline runs to reload cached Parquet data.</li>
        </ul>
      </div>
    </div>
  );
};

// ─── Pipeline Editor ──────────────────────────────────────────────────────────

const PipelineEditor = ({ pipeline, onSave, onCancel, segments }) => {
  const [name, setName]           = useState(pipeline?.name || '');
  const [stepOrder, setStepOrder] = useState(pipeline?.steps || [...PROCESS_STEP_IDS]);
  const [segmentIds, setSegmentIds] = useState(pipeline?.segment_ids || []);
  const [nameError, setNameError] = useState('');

  const moveStep = (index, dir) => {
    const arr = [...stepOrder];
    const t = index + dir;
    if (t < 0 || t >= arr.length) return;
    [arr[index], arr[t]] = [arr[t], arr[index]];
    setStepOrder(arr);
  };

  const toggleStep = (stepId) => {
    setStepOrder(prev =>
      prev.includes(stepId) ? prev.filter(s => s !== stepId) : [...prev, stepId]
    );
  };

  const handleSave = () => {
    if (!name.trim()) { setNameError('Pipeline name is required'); return; }
    if (stepOrder.length === 0) { setNameError('Add at least one step'); return; }
    onSave({
      id: pipeline?.id || crypto.randomUUID(),
      name: name.trim(),
      steps: stepOrder,
      segment_ids: segmentIds,
      created_at: pipeline?.created_at || new Date().toISOString(),
    });
  };

  const removedSteps = PROCESS_STEP_IDS.filter(s => !stepOrder.includes(s));

  return (
    <div className="bg-white dark:bg-gray-800 rounded-xl border-2 border-indigo-200 dark:border-indigo-700 p-5">
      <h3 className="font-semibold text-gray-900 dark:text-gray-100 text-sm mb-4">
        {pipeline ? 'Edit Pipeline' : 'New Pipeline'}
      </h3>

      {/* Name */}
      <div className="mb-4">
        <label className="text-xs font-medium text-gray-500 dark:text-gray-400 mb-1 block">Pipeline Name</label>
        <input
          type="text"
          value={name}
          onChange={e => { setName(e.target.value); setNameError(''); }}
          placeholder="e.g. Weekly Forecast Run"
          className={`w-full px-3 py-2 text-sm rounded-lg border bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-indigo-500 ${
            nameError ? 'border-red-400 dark:border-red-600' : 'border-gray-300 dark:border-gray-600'
          }`}
        />
        {nameError && <p className="mt-1 text-xs text-red-500 dark:text-red-400">{nameError}</p>}
      </div>

      {/* Steps */}
      <div className="mb-4">
        <label className="text-xs font-medium text-gray-500 dark:text-gray-400 mb-2 block">
          Steps — in order
        </label>
        <div className="space-y-1.5">
          {stepOrder.map((stepId, i) => (
            <div key={stepId} className="flex items-center gap-2 bg-gray-50 dark:bg-gray-700/50 rounded-lg px-3 py-2">
              <span className="text-sm">{ICONS[stepId] || '⚙️'}</span>
              <span className="flex-1 text-sm text-gray-800 dark:text-gray-200 font-medium">
                {STEP_LABELS[stepId] || stepId}
              </span>
              <button onClick={() => moveStep(i, -1)} disabled={i === 0}
                className="p-0.5 text-gray-400 hover:text-gray-700 dark:hover:text-gray-200 disabled:opacity-30 disabled:cursor-not-allowed" title="Move up">▲</button>
              <button onClick={() => moveStep(i, 1)} disabled={i === stepOrder.length - 1}
                className="p-0.5 text-gray-400 hover:text-gray-700 dark:hover:text-gray-200 disabled:opacity-30 disabled:cursor-not-allowed" title="Move down">▼</button>
              <button onClick={() => toggleStep(stepId)}
                className="p-0.5 text-red-400 hover:text-red-600 dark:hover:text-red-300" title="Remove step">✕</button>
            </div>
          ))}

          {/* Re-add removed steps */}
          {removedSteps.length > 0 && (
            <div className="flex flex-wrap gap-1.5 pt-1">
              {removedSteps.map(stepId => (
                <button key={stepId} onClick={() => toggleStep(stepId)}
                  className="flex items-center gap-1 text-xs px-2 py-1 rounded border border-dashed border-gray-300 dark:border-gray-600 text-gray-500 dark:text-gray-400 hover:border-blue-400 hover:text-blue-600 dark:hover:text-blue-400 transition-colors">
                  <span>+</span>
                  <span>{STEP_LABELS[stepId] || stepId}</span>
                </button>
              ))}
            </div>
          )}
        </div>
      </div>

      {/* Segments */}
      <div className="mb-5">
        <label className="text-xs font-medium text-gray-500 dark:text-gray-400 mb-2 block">
          Scope — segments
        </label>
        {segments.length > 0 ? (
          <>
            <SegmentSelector segments={segments} selected={segmentIds} onChange={setSegmentIds} />
            <p className="mt-1 text-xs text-gray-400 dark:text-gray-500">
              {segmentIds.length === 0
                ? 'No segment selected — pipeline will run on all data.'
                : `Scoped to ${segmentIds.length} segment${segmentIds.length > 1 ? 's' : ''}.`}
            </p>
          </>
        ) : (
          <p className="text-xs text-gray-400 dark:text-gray-500 italic">No segments defined yet.</p>
        )}
      </div>

      {/* Actions */}
      <div className="flex items-center gap-2 justify-end">
        <button onClick={onCancel}
          className="px-3 py-1.5 rounded-lg text-sm text-gray-600 dark:text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-700 transition-colors">
          Cancel
        </button>
        <button onClick={handleSave}
          className="px-4 py-1.5 rounded-lg text-sm font-semibold bg-indigo-600 text-white hover:bg-indigo-700 active:scale-95 transition-colors">
          {pipeline ? 'Save Changes' : 'Create Pipeline'}
        </button>
      </div>
    </div>
  );
};

// ─── Pipeline Run Card ────────────────────────────────────────────────────────

const PipelineRunCard = ({ pipeline, onEdit, onDelete, segments }) => {
  const { locale } = useLocale();
  const [runState, setRunState] = useState(null);
  // runState: null (idle) | { status, currentIdx, stepResults, jobs, showLogs }
  const [error, setError]     = useState(null);
  const [showLogs, setShowLogs] = useState({});
  const activeESRef = useRef(null);
  const stopRequestedRef = useRef(false);

  const isRunning = runState?.status === 'running';
  const isDone    = runState?.status === 'success' || runState?.status === 'error' || runState?.status === 'interrupted';

  const scopeLabel =
    !pipeline.segment_ids || pipeline.segment_ids.length === 0
      ? 'All segments'
      : pipeline.segment_ids.length === 1
        ? (segments.find(s => s.id === pipeline.segment_ids[0])?.name || 'segment')
        : `${pipeline.segment_ids.length} segments`;

  const runPipeline = async () => {
    const steps = pipeline.steps || [];
    if (!steps.length) return;
    stopRequestedRef.current = false;

    const initialStepResults = {};
    steps.forEach(s => { initialStepResults[s] = 'pending'; });
    setRunState({ status: 'running', currentIdx: 0, stepResults: initialStepResults, jobs: {} });
    setError(null);
    setShowLogs({});

    for (let i = 0; i < steps.length; i++) {
      if (stopRequestedRef.current) {
        setRunState(prev => prev ? { ...prev, status: 'interrupted' } : prev);
        return;
      }
      const stepId = steps[i];
      setRunState(prev => prev ? {
        ...prev, currentIdx: i,
        stepResults: { ...prev.stepResults, [stepId]: 'running' },
      } : prev);

      try {
        const body = pipeline.segment_ids?.length > 0
          ? (pipeline.segment_ids.length === 1
            ? { segment_id: pipeline.segment_ids[0] }
            : { segment_ids: pipeline.segment_ids })
          : {};
        const r = await api.post(`/pipeline/run/${stepId}`, body);
        const jobId = r.data.job_id;
        const job = { ...r.data, log_lines: [], started_at: r.data.started_at || new Date().toISOString(), ended_at: null };

        setRunState(prev => prev ? { ...prev, jobs: { ...prev.jobs, [stepId]: job } } : prev);
        setShowLogs(prev => ({ ...prev, [stepId]: true }));

        // Wait for step to complete via SSE
        const stepStatus = await new Promise((resolve) => {
          const es = openJobSSE(jobId, {
            onLine: (line) => setRunState(prev => {
              if (!prev) return prev;
              const j = prev.jobs?.[stepId];
              if (!j) return prev;
              return { ...prev, jobs: { ...prev.jobs, [stepId]: { ...j, log_lines: [...(j.log_lines || []), line] } } };
            }),
            onProgress: (key, data) => setRunState(prev => {
              if (!prev) return prev;
              const j = prev.jobs?.[stepId];
              if (!j) return prev;
              return { ...prev, jobs: { ...prev.jobs, [stepId]: { ...j, progress: { ...(j.progress || {}), [key]: data } } } };
            }),
            onDone: (status) => {
              setRunState(prev => {
                if (!prev) return prev;
                const j = prev.jobs?.[stepId];
                if (!j) return prev;
                return {
                  ...prev,
                  stepResults: { ...prev.stepResults, [stepId]: status },
                  jobs: { ...prev.jobs, [stepId]: { ...j, status, ended_at: new Date().toISOString() } },
                };
              });
              activeESRef.current = null;
              resolve(status);
            },
          });
          activeESRef.current = es;
        });

        if (stopRequestedRef.current) {
          setRunState(prev => prev ? { ...prev, status: 'interrupted' } : prev);
          setError('Pipeline manually stopped.');
          return;
        }

        if (stepStatus !== 'success') {
          setRunState(prev => prev ? {
            ...prev,
            status: stepStatus === 'interrupted' ? 'interrupted' : 'error',
          } : prev);
          setError(`Pipeline stopped at "${STEP_LABELS[stepId] || stepId}": step ${stepStatus}.`);
          return;
        }
      } catch (e) {
        const detail = e.response?.data?.detail || e.message;
        setRunState(prev => prev ? {
          ...prev, status: 'error',
          stepResults: { ...prev.stepResults, [stepId]: 'error' },
        } : prev);
        setError(`Pipeline stopped at "${STEP_LABELS[stepId] || stepId}": ${detail}`);
        return;
      }
    }

    setRunState(prev => prev ? { ...prev, status: 'success' } : prev);
  };

  const handleStop = () => {
    stopRequestedRef.current = true;
    if (activeESRef.current) {
      activeESRef.current.close();
      activeESRef.current = null;
    }
    setRunState(prev => prev ? { ...prev, status: 'interrupted' } : prev);
    setError('Pipeline manually stopped.');
  };

  useEffect(() => () => {
    if (activeESRef.current) activeESRef.current.close();
  }, []);

  const overallStatus = !runState ? 'idle' : runState.status;

  return (
    <div className={`bg-white dark:bg-gray-800 rounded-xl border-2 transition-colors ${
      overallStatus === 'running'     ? 'border-indigo-300 dark:border-indigo-600 shadow-lg dark:shadow-indigo-900/30' :
      overallStatus === 'success'     ? 'border-emerald-200 dark:border-emerald-700' :
      overallStatus === 'error'       ? 'border-red-200 dark:border-red-700' :
      overallStatus === 'interrupted' ? 'border-amber-200 dark:border-amber-700' :
      'border-gray-200 dark:border-gray-700'
    }`}>
      <div className="p-4">
        {/* Header */}
        <div className="flex items-start justify-between gap-3">
          <div className="flex items-start gap-3 min-w-0">
            <span className="text-2xl flex-shrink-0">🚀</span>
            <div className="min-w-0">
              <h3 className="font-semibold text-gray-900 dark:text-gray-100 text-sm">{pipeline.name}</h3>
              <div className="flex flex-wrap items-center gap-2 mt-0.5">
                <span className="text-xs text-gray-500 dark:text-gray-400">
                  {pipeline.steps.length} step{pipeline.steps.length !== 1 ? 's' : ''}:
                  {' '}{pipeline.steps.map(s => STEP_LABELS[s] || s).join(' → ')}
                </span>
                <span className="text-xs text-gray-400 dark:text-gray-500">·</span>
                <span className="text-xs text-gray-500 dark:text-gray-400">🏷️ {scopeLabel}</span>
              </div>
            </div>
          </div>
          <div className="flex items-center gap-2 flex-shrink-0">
            {runState && <StatusBadge status={overallStatus} />}
            {isRunning && (
              <button onClick={handleStop}
                className="px-3 py-1.5 rounded-lg text-sm font-medium bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-300 hover:bg-red-200 dark:hover:bg-red-900/50 active:scale-95 transition-colors flex items-center gap-1">
                <span>■</span> Stop
              </button>
            )}
            <button
              onClick={runPipeline}
              disabled={isRunning}
              className={`px-3 py-1.5 rounded-lg text-sm font-medium transition-colors ${
                isRunning
                  ? 'bg-gray-100 dark:bg-gray-700 text-gray-400 cursor-not-allowed'
                  : 'bg-indigo-600 text-white hover:bg-indigo-700 active:scale-95'
              }`}
            >
              {isRunning ? 'Running\u2026' : isDone ? 'Re-run' : 'Run'}
            </button>
            {!isRunning && (
              <>
                <button onClick={onEdit}
                  className="p-1.5 rounded-lg text-gray-400 dark:text-gray-500 hover:text-gray-700 dark:hover:text-gray-200 hover:bg-gray-100 dark:hover:bg-gray-700 transition-colors"
                  title="Edit pipeline">✏️</button>
                <button onClick={onDelete}
                  className="p-1.5 rounded-lg text-gray-400 dark:text-gray-500 hover:text-red-600 dark:hover:text-red-400 hover:bg-red-50 dark:hover:bg-red-900/20 transition-colors"
                  title="Delete pipeline">🗑️</button>
              </>
            )}
          </div>
        </div>

        {/* Step progress track */}
        {runState && (
          <div className="flex items-center gap-1 flex-wrap mt-3">
            {pipeline.steps.map((stepId, i) => {
              const result = runState.stepResults?.[stepId] || 'pending';
              const isCurrent = i === runState.currentIdx && runState.status === 'running';
              return (
                <React.Fragment key={stepId}>
                  <div className="flex flex-col items-center gap-0.5">
                    <div className={`w-6 h-6 rounded-full flex items-center justify-center text-[10px] font-bold border-2 ${
                      result === 'success'     ? 'bg-emerald-100 dark:bg-emerald-900/30 border-emerald-400 text-emerald-700 dark:text-emerald-300' :
                      result === 'error'       ? 'bg-red-100 dark:bg-red-900/30 border-red-400 text-red-700 dark:text-red-300' :
                      result === 'interrupted' ? 'bg-amber-100 dark:bg-amber-900/30 border-amber-400 text-amber-700 dark:text-amber-300' :
                      isCurrent               ? 'bg-blue-100 dark:bg-blue-900/30 border-blue-400 text-blue-700 dark:text-blue-300' :
                      'bg-gray-100 dark:bg-gray-700 border-gray-300 dark:border-gray-600 text-gray-400'
                    }`}>
                      {result === 'success' ? '✓' : result === 'error' ? '✕' : isCurrent ? <Spinner cls="w-3 h-3" /> : i + 1}
                    </div>
                    <span className="text-[9px] font-medium leading-none text-gray-400 dark:text-gray-500">
                      {(STEP_LABELS[stepId] || stepId).replace(' ', '\u00A0')}
                    </span>
                  </div>
                  {i < pipeline.steps.length - 1 && (
                    <div className={`h-0.5 w-4 mb-3 flex-shrink-0 rounded ${
                      result === 'success' ? 'bg-emerald-300 dark:bg-emerald-600' : 'bg-gray-200 dark:bg-gray-600'
                    }`} />
                  )}
                </React.Fragment>
              );
            })}
          </div>
        )}

        {/* Error */}
        {error && (
          <div className="mt-2 text-xs text-red-600 dark:text-red-400 bg-red-50 dark:bg-red-900/20 rounded px-3 py-2">
            {error}
          </div>
        )}

        {/* Per-step log toggles */}
        {runState && pipeline.steps.map(stepId => {
          const job = runState.jobs?.[stepId];
          if (!job) return null;
          const result = runState.stepResults?.[stepId];
          return (
            <div key={stepId} className="mt-2">
              <button
                onClick={() => setShowLogs(prev => ({ ...prev, [stepId]: !prev[stepId] }))}
                className="text-xs text-blue-500 dark:text-blue-400 hover:text-blue-700 dark:hover:text-blue-300 flex items-center gap-1"
              >
                <span>{ICONS[stepId]}</span>
                <span>{STEP_LABELS[stepId] || stepId}</span>
                {result && result !== 'pending' && <StatusBadge status={result} />}
                <span className="text-gray-400 dark:text-gray-500">({job.log_lines?.length ?? 0} lines)</span>
                <span>{showLogs[stepId] ? '▲' : '▼'}</span>
              </button>
              <LogViewer lines={job.log_lines ?? []} visible={!!showLogs[stepId]} />
            </div>
          );
        })}
      </div>
    </div>
  );
};

// ─── Pipelines Tab ────────────────────────────────────────────────────────────

const PipelinesTab = ({ segments }) => {
  const [pipelines, setPipelines] = useState(() => {
    try { return JSON.parse(localStorage.getItem(PIPELINES_STORAGE_KEY) || '[]'); }
    catch { return []; }
  });
  const [editing, setEditing] = useState(null); // null | {} (new) | pipeline obj (edit)

  const savePipelines = (list) => {
    setPipelines(list);
    localStorage.setItem(PIPELINES_STORAGE_KEY, JSON.stringify(list));
  };

  const handleSave = (pipeline) => {
    const idx = pipelines.findIndex(p => p.id === pipeline.id);
    const updated = idx >= 0
      ? pipelines.map(p => p.id === pipeline.id ? pipeline : p)
      : [...pipelines, pipeline];
    savePipelines(updated);
    setEditing(null);
  };

  const handleDelete = (id) => savePipelines(pipelines.filter(p => p.id !== id));

  return (
    <div className="space-y-4">
      {/* New / Edit form */}
      {editing !== null ? (
        <PipelineEditor
          pipeline={editing?.id ? editing : null}
          onSave={handleSave}
          onCancel={() => setEditing(null)}
          segments={segments}
        />
      ) : (
        <div className="flex items-center justify-between">
          <p className="text-sm text-gray-500 dark:text-gray-400">
            {pipelines.length === 0
              ? 'No pipelines defined yet.'
              : `${pipelines.length} pipeline${pipelines.length !== 1 ? 's' : ''}`}
          </p>
          <button
            onClick={() => setEditing({})}
            className="flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-semibold bg-indigo-600 text-white hover:bg-indigo-700 active:scale-95 transition-colors"
          >
            <span>+</span> New Pipeline
          </button>
        </div>
      )}

      {/* Empty state */}
      {pipelines.length === 0 && editing === null && (
        <div className="bg-white dark:bg-gray-800 rounded-xl border-2 border-dashed border-gray-200 dark:border-gray-700 p-10 text-center">
          <div className="text-4xl mb-3">🚀</div>
          <p className="text-sm font-medium text-gray-700 dark:text-gray-300">No pipelines yet</p>
          <p className="text-xs text-gray-500 dark:text-gray-400 mt-1 mb-4">
            Create a pipeline to define a custom sequence of processes scoped to specific segments.
          </p>
          <button onClick={() => setEditing({})}
            className="px-4 py-2 rounded-lg text-sm font-semibold bg-indigo-600 text-white hover:bg-indigo-700 active:scale-95 transition-colors">
            Create your first pipeline
          </button>
        </div>
      )}

      {/* Pipeline cards */}
      {pipelines.map(pipeline => (
        <PipelineRunCard
          key={pipeline.id}
          pipeline={pipeline}
          segments={segments}
          onEdit={() => setEditing(pipeline)}
          onDelete={() => handleDelete(pipeline.id)}
        />
      ))}
    </div>
  );
};

// ─── Main Component ───────────────────────────────────────────────────────────

export const ProcessRunner = () => {
  const [activeTab, setActiveTab] = useState('processes');
  const [allSteps, setAllSteps]   = useState([]);
  const [segments, setSegments]   = useState([]);
  const [loadError, setLoadError] = useState(null);

  useEffect(() => {
    api.get(`/pipeline/steps`)
      .then(r => setAllSteps(r.data))
      .catch(e => setLoadError(e.message));
    api.get(`/segments`)
      .then(r => setSegments(Array.isArray(r.data) ? r.data : []))
      .catch(() => {}); // segments are optional; if unavailable the selector just hides
  }, []);

  const tabs = [
    { id: 'processes', label: '⚙️ Processes' },
    { id: 'pipelines', label: '🚀 Pipelines' },
  ];

  return (
    <>
      <style>{INDETERMINATE_STYLE}</style>
      <div className="p-4 sm:p-6 max-w-3xl mx-auto">
        <div className="mb-6">
          <h1 className="text-2xl font-bold text-gray-900 dark:text-white">Process Runner</h1>
          <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
            Run individual processes with optional segment scoping, or define and execute custom pipelines.
          </p>
        </div>

        {loadError && (
          <div className="mb-4 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg px-4 py-3 text-sm text-red-700 dark:text-red-300">
            {loadError}
          </div>
        )}

        {/* Tab switcher */}
        <div className="flex gap-1 mb-6 bg-gray-100 dark:bg-gray-800 p-1 rounded-lg w-fit">
          {tabs.map(tab => (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id)}
              className={`px-4 py-2 rounded-md text-sm font-medium transition-colors ${
                activeTab === tab.id
                  ? 'bg-white dark:bg-gray-700 text-gray-900 dark:text-white shadow-sm'
                  : 'text-gray-600 dark:text-gray-400 hover:text-gray-900 dark:hover:text-white'
              }`}
            >
              {tab.label}
            </button>
          ))}
        </div>

        {activeTab === 'processes' && <ProcessesTab allSteps={allSteps} segments={segments} />}
        {activeTab === 'pipelines' && <PipelinesTab segments={segments} />}
      </div>
    </>
  );
};

export default ProcessRunner;

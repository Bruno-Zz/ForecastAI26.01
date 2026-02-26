/**
 * PipelineRunner Component
 *
 * Lets the user trigger individual pipeline steps (ETL, outlier detection,
 * forecast, backtest, best-method, distributions) OR the full pipeline in
 * order, and see live log output via Server-Sent Events.
 */

import React, { useState, useEffect, useRef, useCallback } from 'react';
import { useLocale } from '../contexts/LocaleContext';
import { useTheme } from '../contexts/ThemeContext';
import { formatTime, formatNumber } from '../utils/formatting';
import api from '../utils/api';

const STEP_ORDER = ['etl', 'outlier-detection', 'forecast', 'backtest', 'best-method', 'distributions'];

// Parse a UTC ISO timestamp that may or may not already end with 'Z'
const parseUTC = (s) => new Date(s.endsWith('Z') ? s : s + 'Z');

const ICONS = {
  'etl':               '🗄️',
  'outlier-detection': '🔍',
  'forecast':          '📊',
  'backtest':          '🔁',
  'best-method':       '🏆',
  'distributions':     '📈',
};

/** Compact progress bar used for Forecast and Backtest steps */
const ForecastProgressBar = ({ progress, label = 'series' }) => {
  if (!progress || !progress.total) return null;
  const { completed, total, batches_done, batches_total } = progress;
  const pct = total > 0 ? Math.min(100, Math.round((completed / total) * 100)) : 0;
  return (
    <div className="mt-2 space-y-1">
      <div className="flex items-center justify-between text-xs">
        <span className="font-medium text-blue-600 dark:text-blue-400">
          {completed.toLocaleString()} / {total.toLocaleString()} {label}
        </span>
        {batches_total != null && batches_total > 1 && (
          <span className="text-gray-400 dark:text-gray-500">
            batch {batches_done}/{batches_total}
          </span>
        )}
        <span className="font-semibold text-blue-600 dark:text-blue-400">{pct}%</span>
      </div>
      <div className="w-full bg-gray-200 dark:bg-gray-700 rounded-full h-1.5 overflow-hidden">
        <div
          className="bg-blue-500 dark:bg-blue-400 h-1.5 rounded-full transition-all duration-500"
          style={{ width: `${pct}%` }}
        />
      </div>
    </div>
  );
};

/** Spinner SVG */
const Spinner = ({ cls = 'w-4 h-4' }) => (
  <svg className={`animate-spin ${cls}`} viewBox="0 0 24 24" fill="none">
    <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"/>
    <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8z"/>
  </svg>
);

/** Status badge */
const StatusBadge = ({ status }) => {
  const map = {
    pending:  { cls: 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400',         label: 'Pending' },
    running:  { cls: 'bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300',         label: 'Running…' },
    success:  { cls: 'bg-emerald-100 dark:bg-emerald-900/30 text-emerald-700 dark:text-emerald-300',   label: 'Success' },
    error:    { cls: 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-300',           label: 'Error' },
  };
  const { cls, label } = map[status] || { cls: 'bg-gray-100 dark:bg-gray-700 text-gray-500 dark:text-gray-400', label: status };
  return (
    <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium ${cls}`}>
      {status === 'running' && <Spinner cls="w-3 h-3" />}
      {status === 'success' && '✓'}
      {status === 'error'   && '✕'}
      {label}
    </span>
  );
};

/** Log viewer with auto-scroll (contained — does NOT hijack the page scroll) */
const LogViewer = ({ lines, visible }) => {
  const containerRef = useRef(null);
  const userScrolledUp = useRef(false);

  // Track whether the user manually scrolled away from the bottom
  const handleScroll = () => {
    const el = containerRef.current;
    if (!el) return;
    const atBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 40;
    userScrolledUp.current = !atBottom;
  };

  // Auto-scroll ONLY within the container, and only if user hasn't scrolled up
  useEffect(() => {
    const el = containerRef.current;
    if (visible && el && !userScrolledUp.current) {
      el.scrollTop = el.scrollHeight;
    }
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
    <div
      ref={containerRef}
      onScroll={handleScroll}
      className="mt-3 bg-gray-900 rounded-lg p-3 max-h-72 overflow-y-auto font-mono text-xs leading-5 border border-gray-700"
    >
      {lines.length === 0
        ? <span className="text-gray-500 italic">Waiting for output…</span>
        : lines.map((l, i) => <div key={i} className={colorLine(l)}>{l || '\u00A0'}</div>)
      }
    </div>
  );
};

/** Full-pipeline progress bar showing which step is active */
const PipelineProgress = ({ steps, currentStepId, jobStatus }) => {
  const stepDone   = (id) => {
    if (jobStatus === 'success') return true;
    if (!currentStepId)          return false;
    return STEP_ORDER.indexOf(id) < STEP_ORDER.indexOf(currentStepId);
  };
  const stepActive = (id) => currentStepId === id && jobStatus === 'running';
  const stepError  = (id) => currentStepId === id && jobStatus === 'error';

  return (
    <div className="flex items-center gap-1 flex-wrap mt-3">
      {steps.map((step, i) => (
        <React.Fragment key={step.id}>
          <div className="flex flex-col items-center gap-0.5">
            <div className={`w-6 h-6 rounded-full flex items-center justify-center text-[10px] font-bold border-2 transition-colors ${
              stepDone(step.id)   ? 'bg-emerald-100 dark:bg-emerald-900/30 border-emerald-400 text-emerald-700 dark:text-emerald-300' :
              stepActive(step.id) ? 'bg-blue-100 dark:bg-blue-900/30 border-blue-400 text-blue-700 dark:text-blue-300' :
              stepError(step.id)  ? 'bg-red-100 dark:bg-red-900/30 border-red-400 text-red-700 dark:text-red-300' :
              'bg-gray-100 dark:bg-gray-700 border-gray-300 dark:border-gray-600 text-gray-400'
            }`}>
              {stepDone(step.id) ? '✓' : stepError(step.id) ? '✕' : i + 1}
            </div>
            <span className={`text-[9px] font-medium leading-none ${
              stepActive(step.id) ? 'text-blue-600 dark:text-blue-400' :
              stepDone(step.id)   ? 'text-emerald-600 dark:text-emerald-400' :
              stepError(step.id)  ? 'text-red-600 dark:text-red-400' :
              'text-gray-400 dark:text-gray-500'
            }`}>{step.label.replace(' ', '\u00A0')}</span>
          </div>
          {i < steps.length - 1 && (
            <div className={`h-0.5 w-4 mb-3 flex-shrink-0 rounded ${
              stepDone(step.id) ? 'bg-emerald-300 dark:bg-emerald-600' : 'bg-gray-200 dark:bg-gray-600'
            }`} />
          )}
        </React.Fragment>
      ))}
    </div>
  );
};

/** Full-pipeline card at the top */
const FullPipelineCard = ({ steps, job, onRun, onKill, showLogs, onToggleLogs, locale }) => {
  const isRunning = job?.status === 'running';
  const isDone    = job?.status === 'success' || job?.status === 'error';

  return (
    <div className={`bg-white dark:bg-gray-800 rounded-xl border-2 transition-colors mb-6 ${
      isRunning        ? 'border-blue-300 dark:border-blue-600 shadow-lg dark:shadow-blue-900/30' :
      job?.status === 'success' ? 'border-emerald-300 dark:border-emerald-600' :
      job?.status === 'error'   ? 'border-red-300 dark:border-red-600' :
      'border-indigo-200 dark:border-indigo-700'
    }`}>
      <div className="p-4">
        <div className="flex flex-col sm:flex-row sm:items-start sm:justify-between gap-3">
          <div className="flex items-start gap-3 min-w-0">
            <span className="text-2xl flex-shrink-0">🚀</span>
            <div className="min-w-0">
              <h3 className="font-bold text-gray-900 dark:text-gray-100 text-sm">Run Full Pipeline</h3>
              <p className="text-xs text-gray-500 dark:text-gray-400 mt-0.5">
                Run all 6 steps in order: ETL → Outlier Detection → Forecast → Backtest → Best Method → Distributions.
                Stops automatically if any step fails.
              </p>
            </div>
          </div>
          <div className="flex items-center gap-2 flex-shrink-0 self-end sm:self-auto">
            {job && <StatusBadge status={job.status} />}
            {isRunning && (
              <button
                onClick={() => onKill(job.job_id)}
                className="px-3 py-1.5 rounded-lg text-sm font-medium bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-300 hover:bg-red-200 dark:hover:bg-red-900/50 active:scale-95 transition-colors flex items-center gap-1"
                title="Interrupt the pipeline"
              >
                <span>■</span> Stop
              </button>
            )}
            <button
              onClick={onRun}
              disabled={isRunning}
              className={`px-4 py-1.5 rounded-lg text-sm font-semibold transition-colors ${
                isRunning
                  ? 'bg-gray-100 dark:bg-gray-700 text-gray-400 dark:text-gray-500 cursor-not-allowed'
                  : 'bg-indigo-600 text-white hover:bg-indigo-700 active:scale-95'
              }`}
            >
              {isRunning ? 'Running…' : isDone ? 'Re-run All' : 'Run All'}
            </button>
          </div>
        </div>

        {/* Step progress */}
        {steps.length > 0 && (
          <PipelineProgress
            steps={steps}
            currentStepId={job?.current_step ?? null}
            jobStatus={job?.status ?? null}
          />
        )}

        {/* Live series progress while forecast or backtest is active */}
        {isRunning && (job?.progress?.FORECAST_PROGRESS || job?.progress?.BACKTEST_PROGRESS) && (
          <div className="mt-2 space-y-1.5">
            {job.progress?.FORECAST_PROGRESS && (
              <div className="flex items-center gap-2">
                <span className="text-xs text-gray-500 dark:text-gray-400 font-medium w-16 flex-shrink-0">📊 Forecast</span>
                <div className="flex-1"><ForecastProgressBar progress={job.progress.FORECAST_PROGRESS} label="series" /></div>
              </div>
            )}
            {job.progress?.BACKTEST_PROGRESS && (
              <div className="flex items-center gap-2">
                <span className="text-xs text-gray-500 dark:text-gray-400 font-medium w-16 flex-shrink-0">🔁 Backtest</span>
                <div className="flex-1"><ForecastProgressBar progress={job.progress.BACKTEST_PROGRESS} label="series" /></div>
              </div>
            )}
          </div>
        )}

        {/* Timing */}
        {job?.started_at && (
          <div className="mt-2 flex items-center gap-3 text-xs text-gray-400 dark:text-gray-500">
            <span>Started: {formatTime(job.started_at, locale)}</span>
            {job.ended_at && (
              <span>· Duration: {formatNumber(
                (parseUTC(job.ended_at) - parseUTC(job.started_at)) / 1000,
                locale, 1
              )}s</span>
            )}
          </div>
        )}

        {/* Log toggle */}
        {job && (
          <button
            onClick={onToggleLogs}
            className="mt-2 text-xs text-blue-500 dark:text-blue-400 hover:text-blue-700 dark:hover:text-blue-300 flex items-center gap-1"
          >
            <span>{showLogs ? '▲ Hide' : '▼ Show'} combined logs</span>
            <span className="text-gray-400 dark:text-gray-500">({job.log_lines?.length ?? 0} lines)</span>
          </button>
        )}
      </div>

      <LogViewer lines={job?.log_lines ?? []} visible={showLogs} />
      {showLogs && <div className="h-2" />}
    </div>
  );
};

/** Single step card */
const StepCard = ({ step, onRun, onKill, activeJob, onToggleLogs, showLogs, isFullPipelineRunning, locale }) => {
  const isRunning = activeJob?.status === 'running';
  const isDone    = activeJob?.status === 'success' || activeJob?.status === 'error';
  const hasJob    = !!activeJob;
  const disabled  = isRunning || isFullPipelineRunning;

  return (
    <div className={`bg-white dark:bg-gray-800 rounded-xl border-2 transition-colors ${
      isRunning           ? 'border-blue-300 dark:border-blue-600 shadow-md dark:shadow-blue-900/30' :
      activeJob?.status === 'success' ? 'border-emerald-200 dark:border-emerald-700' :
      activeJob?.status === 'error'   ? 'border-red-200 dark:border-red-700' :
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
          <div className="flex items-center gap-2 flex-shrink-0 self-end sm:self-auto">
            {hasJob && <StatusBadge status={activeJob.status} />}
            {isRunning && (
              <button
                onClick={() => onKill(activeJob.job_id)}
                className="px-3 py-1.5 rounded-lg text-sm font-medium bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-300 hover:bg-red-200 dark:hover:bg-red-900/50 active:scale-95 transition-colors flex items-center gap-1"
                title="Interrupt running process"
              >
                <span>■</span> Stop
              </button>
            )}
            <button
              onClick={() => onRun(step.id)}
              disabled={disabled}
              className={`px-3 py-1.5 rounded-lg text-sm font-medium transition-colors ${
                disabled
                  ? 'bg-gray-100 dark:bg-gray-700 text-gray-400 dark:text-gray-500 cursor-not-allowed'
                  : 'bg-blue-600 text-white hover:bg-blue-700 active:scale-95'
              }`}
            >
              {isRunning ? 'Running…' : isDone ? 'Re-run' : 'Run'}
            </button>
          </div>
        </div>

        {/* Timing */}
        {activeJob?.started_at && (
          <div className="mt-2 flex items-center gap-3 text-xs text-gray-400 dark:text-gray-500">
            <span>Started: {formatTime(activeJob.started_at, locale)}</span>
            {activeJob.ended_at && (
              <span>· Duration: {formatNumber(
                (parseUTC(activeJob.ended_at) - parseUTC(activeJob.started_at)) / 1000,
                locale, 1
              )}s</span>
            )}
            {activeJob.exit_code != null && activeJob.exit_code !== 0 && (
              <span className="text-red-400 dark:text-red-300">· Exit code: {activeJob.exit_code}</span>
            )}
          </div>
        )}

        {/* Live series progress for forecast and backtest steps */}
        {step.id === 'forecast' && activeJob?.progress?.FORECAST_PROGRESS && (
          <ForecastProgressBar progress={activeJob.progress.FORECAST_PROGRESS} label="series" />
        )}
        {step.id === 'backtest' && activeJob?.progress?.BACKTEST_PROGRESS && (
          <ForecastProgressBar progress={activeJob.progress.BACKTEST_PROGRESS} label="series" />
        )}

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

export const PipelineRunner = () => {
  const { locale } = useLocale();
  const { isDark } = useTheme();
  const [steps, setSteps]           = useState([]);
  const [jobs, setJobs]             = useState({});        // stepId -> latest job object
  const [fullJob, setFullJob]       = useState(null);      // full-pipeline job object
  const [showLogs, setShowLogs]     = useState({});        // stepId -> bool
  const [showFullLogs, setShowFullLogs] = useState(false);
  const [error, setError]           = useState(null);
  const eventSources = useRef({});                         // key -> EventSource

  // ── SSE helper (defined early so restore effects can use it) ────────
  const openSSE = useCallback((key, jobId, setter) => {
    if (eventSources.current[key]) {
      eventSources.current[key].close();
      delete eventSources.current[key];
    }

    const token = localStorage.getItem('forecastai_token') || '';
    const es = new EventSource(`/api/pipeline/jobs/${jobId}/stream${token ? `?token=${token}` : ''}`);

    es.onmessage = (e) => {
      try {
        const { line } = JSON.parse(e.data);

        // Parse structured progress markers — update job.progress, don't add to log
        const progMatch = line.match(/\[(FORECAST_PROGRESS|BACKTEST_PROGRESS)\]\s+(.*)/);
        if (progMatch) {
          const progKey = progMatch[1]; // 'FORECAST_PROGRESS' or 'BACKTEST_PROGRESS'
          const progData = {};
          progMatch[2].trim().split(/\s+/).forEach(pair => {
            const eq = pair.indexOf('=');
            if (eq > 0) {
              const k = pair.slice(0, eq);
              const v = pair.slice(eq + 1);
              progData[k] = isNaN(v) ? v : Number(v);
            }
          });
          setter(prev => {
            if (prev && typeof prev === 'object' && !prev.job_id) {
              const job = prev[key];
              if (!job || job.job_id !== jobId) return prev;
              const updatedProg = { ...(job.progress || {}), [progKey]: progData };
              return { ...prev, [key]: { ...job, progress: updatedProg } };
            } else {
              if (!prev || prev.job_id !== jobId) return prev;
              const updatedProg = { ...(prev.progress || {}), [progKey]: progData };
              return { ...prev, progress: updatedProg };
            }
          });
          return; // Don't add progress markers to log_lines
        }

        setter(prev => {
          // prev may be an object (jobs map) or a single job object
          if (prev && typeof prev === 'object' && !prev.job_id) {
            // jobs map
            const job = prev[key];
            if (!job || job.job_id !== jobId) return prev;
            return { ...prev, [key]: { ...job, log_lines: [...(job.log_lines || []), line] } };
          } else {
            // single job object (fullJob)
            if (!prev || prev.job_id !== jobId) return prev;
            return { ...prev, log_lines: [...(prev.log_lines || []), line] };
          }
        });
      } catch { /* ignore */ }
    };

    es.addEventListener('done', (e) => {
      try {
        const { status, exit_code } = JSON.parse(e.data);
        setter(prev => {
          if (prev && typeof prev === 'object' && !prev.job_id) {
            const job = prev[key];
            if (!job || job.job_id !== jobId) return prev;
            return { ...prev, [key]: { ...job, status, exit_code, ended_at: new Date().toISOString() } };
          } else {
            if (!prev || prev.job_id !== jobId) return prev;
            return { ...prev, status, exit_code, ended_at: new Date().toISOString() };
          }
        });
      } catch { /* ignore */ }
      es.close();
      delete eventSources.current[key];
    });

    es.onerror = () => { es.close(); delete eventSources.current[key]; };
    eventSources.current[key] = es;
  }, []);

  // Load step definitions once
  useEffect(() => {
    api.get(`/pipeline/steps`)
      .then(r => setSteps(r.data))
      .catch(e => setError(e.message));
  }, []);

  // ── Restore jobs from server on mount ──────────────────────────────
  // This makes sure that if the user navigates away and comes back,
  // they still see running / completed jobs, and SSE streams reconnect.
  const restoredRef = useRef(false);
  useEffect(() => {
    if (restoredRef.current) return;
    restoredRef.current = true;

    (async () => {
      try {
        const r = await api.get(`/pipeline/jobs`);
        const allJobs = r.data; // sorted newest-first

        const restoredStepJobs = {};
        let restoredFullJob = null;

        // For each step, keep only the most recent job (list is newest-first)
        const seenSteps = new Set();
        for (const job of allJobs) {
          if (job.step === 'full-pipeline') {
            if (!restoredFullJob) restoredFullJob = job;
          } else if (job.step && !seenSteps.has(job.step)) {
            seenSteps.add(job.step);
            restoredStepJobs[job.step] = job;
          }
        }

        if (restoredFullJob) {
          setFullJob(restoredFullJob);
          if (restoredFullJob.status === 'running' || restoredFullJob.status === 'pending') {
            setShowFullLogs(true);
          }
        }
        if (Object.keys(restoredStepJobs).length > 0) {
          setJobs(restoredStepJobs);
          // Auto-show logs for any running step
          const logsToShow = {};
          for (const [stepId, job] of Object.entries(restoredStepJobs)) {
            if (job.status === 'running' || job.status === 'pending') {
              logsToShow[stepId] = true;
            }
          }
          if (Object.keys(logsToShow).length > 0) setShowLogs(logsToShow);
        }
      } catch { /* ignore – server might not be up yet */ }
    })();
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  // ── Reconnect SSE streams for any running jobs after restore ──────
  useEffect(() => {
    // Reconnect full-pipeline SSE
    if (fullJob && (fullJob.status === 'running' || fullJob.status === 'pending')
        && !eventSources.current['full-pipeline']) {
      openSSE('full-pipeline', fullJob.job_id, setFullJob);
    }
    // Reconnect individual step SSEs
    for (const [stepId, job] of Object.entries(jobs)) {
      if ((job.status === 'running' || job.status === 'pending')
          && !eventSources.current[stepId]) {
        openSSE(stepId, job.job_id, setJobs);
      }
    }
  }, [fullJob, jobs, openSSE]);

  // Poll all running jobs
  useEffect(() => {
    const interval = setInterval(async () => {
      // Individual step jobs
      const runningEntries = Object.entries(jobs).filter(([, j]) => j.status === 'running' || j.status === 'pending');
      for (const [stepId, job] of runningEntries) {
        try {
          const r = await api.get(`/pipeline/jobs/${job.job_id}`);
          setJobs(prev => ({ ...prev, [stepId]: r.data }));
        } catch { /* ignore */ }
      }
      // Full-pipeline job
      if (fullJob && (fullJob.status === 'running' || fullJob.status === 'pending')) {
        try {
          const r = await api.get(`/pipeline/jobs/${fullJob.job_id}`);
          setFullJob(r.data);
        } catch { /* ignore */ }
      }
    }, 1000);
    return () => clearInterval(interval);
  }, [jobs, fullJob]);

  // Cleanup SSE on unmount
  useEffect(() => () => {
    Object.values(eventSources.current).forEach(es => es.close());
  }, []);

  const handleRun = async (stepId) => {
    try {
      setError(null);
      const r = await api.post(`/pipeline/run/${stepId}`);
      const job = { ...r.data, log_lines: [], started_at: null, ended_at: null };
      setJobs(prev => ({ ...prev, [stepId]: job }));
      setShowLogs(prev => ({ ...prev, [stepId]: true }));
      openSSE(stepId, r.data.job_id, setJobs);
    } catch (e) {
      setError(e.response?.data?.detail || e.message);
    }
  };

  const handleRunAll = async () => {
    try {
      setError(null);
      const r = await api.post(`/pipeline/run-all`);
      const job = { ...r.data, log_lines: [], started_at: null, ended_at: null, current_step: null };
      setFullJob(job);
      setShowFullLogs(true);
      openSSE('full-pipeline', r.data.job_id, setFullJob);
    } catch (e) {
      setError(e.response?.data?.detail || e.message);
    }
  };

  const handleKill = async (jobId) => {
    try {
      await api.post(`/pipeline/jobs/${jobId}/kill`);
    } catch (e) {
      setError(e.response?.data?.detail || e.message);
    }
  };

  const handleResetJobs = async () => {
    try {
      setError(null);
      const r = await api.post(`/pipeline/jobs/reset`);
      setFullJob(null);
      setJobs({});
      setShowFullLogs(false);
    } catch (e) {
      setError(e.response?.data?.detail || e.message);
    }
  };

  const toggleLogs = (stepId) => setShowLogs(prev => ({ ...prev, [stepId]: !prev[stepId] }));

  const orderedSteps = STEP_ORDER.map(id => steps.find(s => s.id === id)).filter(Boolean);
  const isFullPipelineRunning = fullJob?.status === 'running';
  const anyRunning = isFullPipelineRunning || Object.values(jobs).some(j => j.status === 'running');

  return (
    <div className="p-4 sm:p-6 max-w-3xl mx-auto">
      <div className="mb-6">
        <h1 className="text-2xl font-bold text-gray-900 dark:text-white">Pipeline Runner</h1>
        <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
          Run the full pipeline in one click, or trigger individual steps independently.
        </p>
      </div>

      {error && (
        <div className="mb-4 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg px-4 py-3 text-sm text-red-700 dark:text-red-300 flex items-start gap-2">
          <span className="flex-shrink-0 mt-0.5">⚠️</span>
          <span className="flex-1">
            {error}
            {error.toLowerCase().includes('already running') && (
              <button
                onClick={handleResetJobs}
                className="ml-2 underline text-blue-600 dark:text-blue-400 hover:text-blue-800 dark:hover:text-blue-300 text-xs font-medium"
              >
                Reset stale jobs
              </button>
            )}
          </span>
          <button onClick={() => setError(null)} className="ml-auto text-red-400 hover:text-red-300 flex-shrink-0">✕</button>
        </div>
      )}

      {/* Full pipeline card */}
      <div id="pipeline-full">
      <FullPipelineCard
        steps={orderedSteps}
        job={fullJob}
        onRun={handleRunAll}
        onKill={handleKill}
        showLogs={showFullLogs}
        onToggleLogs={() => setShowFullLogs(v => !v)}
        locale={locale}
      />
      </div>

      {/* Divider */}
      <div className="flex items-center gap-3 mb-4">
        <div className="flex-1 h-px bg-gray-200 dark:bg-gray-700" />
        <span className="text-xs font-semibold text-gray-400 dark:text-gray-500 uppercase tracking-wide">Or run individual steps</span>
        <div className="flex-1 h-px bg-gray-200 dark:bg-gray-700" />
      </div>

      {anyRunning && !isFullPipelineRunning && (
        <div className="mb-4 bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-lg px-4 py-2.5 text-sm text-blue-700 dark:text-blue-300 flex items-center gap-2">
          <Spinner />
          A pipeline step is currently running… Use the <strong className="mx-1">■ Stop</strong> button to interrupt it.
        </div>
      )}

      {/* Individual step cards */}
      <div id="pipeline-steps" className="space-y-3">
        {orderedSteps.map((step, i) => (
          <div key={step.id} className="flex gap-3 items-stretch">
            {/* Step number + connector line */}
            <div className="flex flex-col items-center flex-shrink-0 w-8">
              <div className={`w-7 h-7 rounded-full flex items-center justify-center text-xs font-bold border-2 flex-shrink-0 ${
                jobs[step.id]?.status === 'success' ? 'bg-emerald-100 dark:bg-emerald-900/30 border-emerald-400 text-emerald-700 dark:text-emerald-300' :
                jobs[step.id]?.status === 'error'   ? 'bg-red-100 dark:bg-red-900/30 border-red-400 text-red-700 dark:text-red-300' :
                jobs[step.id]?.status === 'running' ? 'bg-blue-100 dark:bg-blue-900/30 border-blue-400 text-blue-700 dark:text-blue-300' :
                'bg-gray-100 dark:bg-gray-700 border-gray-300 dark:border-gray-600 text-gray-500 dark:text-gray-400'
              }`}>
                {i + 1}
              </div>
              {i < orderedSteps.length - 1 && (
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
                isFullPipelineRunning={isFullPipelineRunning}
                locale={locale}
              />
            </div>
          </div>
        ))}
      </div>

      {/* Notes */}
      <div id="pipeline-notes" className="mt-6 p-4 bg-gray-50 dark:bg-gray-800/50 rounded-lg border border-gray-200 dark:border-gray-700">
        <p className="text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wide mb-2">Notes</p>
        <ul className="text-xs text-gray-500 dark:text-gray-400 space-y-1 list-disc list-inside">
          <li>Individual steps run independently — you can re-run any step without re-running earlier ones.</li>
          <li><strong>Run All</strong> stops at the first step that fails.</li>
          <li><strong>ETL</strong> requires a live database connection (see <code>config/config.yaml</code>).</li>
          <li><strong>Forecast</strong> can take several minutes depending on number of series and models enabled.</li>
          <li>Restart the API after pipeline runs to reload cached Parquet data.</li>
        </ul>
      </div>
    </div>
  );
};

export default PipelineRunner;

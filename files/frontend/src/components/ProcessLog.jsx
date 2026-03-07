import { useState, useEffect, useRef, useCallback } from 'react';
import { useLocale } from '../contexts/LocaleContext';
import { formatDateTime } from '../utils/formatting';
import api from '../utils/api';

const INDETERMINATE_STYLE = `
  @keyframes pl-slide {
    0%   { transform: translateX(-100%); }
    100% { transform: translateX(500%); }
  }
  .pl-slide { animation: pl-slide 1.4s ease-in-out infinite; }
`;

function ElapsedTimer({ startedAt }) {
  const [sec, setSec] = useState(0);
  useEffect(() => {
    if (!startedAt) return;
    const base = new Date(startedAt.endsWith('Z') ? startedAt : startedAt + 'Z').getTime();
    const tick = () => setSec(Math.max(0, Math.floor((Date.now() - base) / 1000)));
    tick();
    const id = setInterval(tick, 1000);
    return () => clearInterval(id);
  }, [startedAt]);
  const m = Math.floor(sec / 60);
  const s = sec % 60;
  return (
    <span className="tabular-nums font-mono">
      {m > 0 ? `${m}m ` : ''}{String(s).padStart(2, '0')}s
    </span>
  );
}

/* ─── Helpers ─── */
const STATUS_CFG = {
  success:     { bg: 'bg-emerald-100 dark:bg-emerald-900/30', text: 'text-emerald-700 dark:text-emerald-400', icon: '\u2713', dot: 'bg-emerald-500' },
  error:       { bg: 'bg-red-100 dark:bg-red-900/30',         text: 'text-red-700 dark:text-red-400',         icon: '\u2717', dot: 'bg-red-500' },
  interrupted: { bg: 'bg-amber-100 dark:bg-amber-900/30',     text: 'text-amber-700 dark:text-amber-400',     icon: '\u26A0', dot: 'bg-amber-500' },
  running:     { bg: 'bg-blue-100 dark:bg-blue-900/30',       text: 'text-blue-700 dark:text-blue-400',       icon: null,     dot: 'bg-blue-500' },
  pending:     { bg: 'bg-gray-100 dark:bg-gray-700',          text: 'text-gray-500 dark:text-gray-400',       icon: '\u2022', dot: 'bg-gray-400' },
};

function StatusBadge({ status }) {
  const cfg = STATUS_CFG[status] || STATUS_CFG.pending;
  return (
    <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[10px] font-semibold ${cfg.bg} ${cfg.text}`}>
      {status === 'running' ? <Spinner cls="w-3 h-3" /> : cfg.icon}
      {status}
    </span>
  );
}

function Spinner({ cls = 'w-4 h-4' }) {
  return (
    <svg className={`animate-spin ${cls}`} viewBox="0 0 24 24" fill="none">
      <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
      <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
    </svg>
  );
}

function fmtDuration(sec) {
  if (sec == null) return '\u2014';
  if (sec < 60) return `${Math.round(sec)}s`;
  const m = Math.floor(sec / 60);
  const s = Math.round(sec % 60);
  if (m < 60) return `${m}m ${s}s`;
  const h = Math.floor(m / 60);
  return `${h}h ${m % 60}m`;
}

function fmtTime(iso, locale) {
  if (!iso) return '\u2014';
  return formatDateTime(iso, locale);
}


/* ─── Forecast / Backtest progress bar ─── */
function ProgressBar({ progress }) {
  if (!progress || !progress.total) return null;
  const { completed = 0, total, batches_done, batches_total, pct = 0, current_step } = progress;
  const label = current_step === 'forecast' ? '📊 Forecast' : current_step === 'backtest' ? '🔁 Backtest' : '⚙️ Processing';
  return (
    <div className="space-y-1">
      <div className="flex items-center justify-between text-[11px]">
        <span className="font-medium text-blue-600 dark:text-blue-400">{label}</span>
        <span className="text-gray-500 dark:text-gray-400 tabular-nums">
          {completed.toLocaleString()} / {total.toLocaleString()} series
        </span>
        {batches_total != null && batches_total > 1 && (
          <span className="text-gray-400 dark:text-gray-500 tabular-nums">
            batch {batches_done}/{batches_total}
          </span>
        )}
        <span className="font-semibold text-blue-600 dark:text-blue-400 tabular-nums">{pct}%</span>
      </div>
      <div className="w-full bg-gray-200 dark:bg-gray-700 rounded-full h-1.5 overflow-hidden">
        <div
          className="bg-blue-500 dark:bg-blue-400 h-1.5 rounded-full transition-all duration-500"
          style={{ width: `${Math.min(100, pct)}%` }}
        />
      </div>
    </div>
  );
}


/* ─── Log Viewer (contained scroll, no page hijack) ─── */
function LogViewer({ lines }) {
  const containerRef = useRef(null);
  const userScrolledUp = useRef(false);

  const handleScroll = () => {
    const el = containerRef.current;
    if (!el) return;
    userScrolledUp.current = el.scrollHeight - el.scrollTop - el.clientHeight > 40;
  };

  useEffect(() => {
    const el = containerRef.current;
    if (el && !userScrolledUp.current) el.scrollTop = el.scrollHeight;
  }, [lines]);

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
      className="bg-gray-900 rounded-lg p-3 max-h-64 overflow-y-auto font-mono text-[11px] leading-5 border border-gray-700"
    >
      {(!lines || lines.length === 0)
        ? <span className="text-gray-500 italic">No log output captured.</span>
        : lines.map((l, i) => <div key={i} className={colorLine(l)}>{l || '\u00A0'}</div>)
      }
    </div>
  );
}


/* ─── Steps detail for a historical DB run ─── */
function RunSteps({ runId, locale }) {
  const [steps, setSteps] = useState(null);
  const [loading, setLoading] = useState(true);
  const [expandedLog, setExpandedLog] = useState(null);   // step_id whose log is open
  const [logTail, setLogTail] = useState(null);
  const [logLoading, setLogLoading] = useState(false);

  useEffect(() => {
    api.get(`/process-log/${runId}/steps`)
      .then(r => { setSteps(r.data); setLoading(false); })
      .catch(() => setLoading(false));
  }, [runId]);

  const toggleLog = async (stepId) => {
    if (expandedLog === stepId) { setExpandedLog(null); setLogTail(null); return; }
    setExpandedLog(stepId);
    setLogLoading(true);
    try {
      const r = await api.get(`/process-log/step/${stepId}/tail`);
      setLogTail(r.data.log_tail || '');
    } catch { setLogTail('Failed to load log.'); }
    setLogLoading(false);
  };

  if (loading) return <div className="py-3 text-center"><Spinner cls="w-4 h-4 mx-auto text-gray-400" /></div>;
  if (!steps || steps.length === 0) return <div className="py-3 text-center text-gray-400 dark:text-gray-500 text-xs italic">No steps recorded.</div>;

  return (
    <div className="divide-y divide-gray-100 dark:divide-gray-700/50">
      {steps.map(st => (
        <div key={st.id} className="py-2">
          <div className="flex items-center gap-3 text-xs">
            <StatusBadge status={st.status} />
            <span className="font-medium text-gray-800 dark:text-gray-200">{st.step_name}</span>
            <span className="text-gray-400 dark:text-gray-500 ml-auto tabular-nums">{fmtDuration(st.duration_s)}</span>
            {st.rows_processed != null && (
              <span className="text-gray-400 dark:text-gray-500 tabular-nums">{st.rows_processed.toLocaleString()} rows</span>
            )}
            {st.has_log_tail && (
              <button
                onClick={() => toggleLog(st.id)}
                className="text-blue-500 dark:text-blue-400 hover:underline text-[10px] font-medium"
              >
                {expandedLog === st.id ? 'Hide log' : 'View log'}
              </button>
            )}
          </div>
          {st.error_message && (
            <div className="mt-1 text-[11px] text-red-500 dark:text-red-400 bg-red-50 dark:bg-red-900/20 rounded px-2 py-1 font-mono break-all">
              {st.error_message}
            </div>
          )}
          {expandedLog === st.id && (
            <div className="mt-2">
              {logLoading
                ? <div className="py-2 text-center"><Spinner cls="w-4 h-4 mx-auto text-gray-400" /></div>
                : <LogViewer lines={logTail ? logTail.split('\n') : []} />
              }
            </div>
          )}
        </div>
      ))}
    </div>
  );
}


/* ─── Live job detail (from in-memory pipeline jobs) ─── */
function LiveJobDetail({ job }) {
  if (!job) return null;
  const lines = job.log_lines || [];
  const progress = job.progress;
  const hasProgress = progress && progress.total > 0;
  const isRunning = job.status === 'running' || job.status === 'pending';
  return (
    <div className="mt-2 space-y-2">
      {job.current_step && (
        <div className="flex items-center gap-2 text-xs text-blue-600 dark:text-blue-400">
          <Spinner cls="w-3 h-3" />
          <span>Currently running: <strong>{job.current_step}</strong></span>
        </div>
      )}
      {hasProgress ? (
        <ProgressBar progress={progress} />
      ) : isRunning ? (
        <div>
          <div className="flex justify-between items-center text-xs text-gray-400 dark:text-gray-500 mb-1">
            <span>Running…</span>
            <ElapsedTimer startedAt={job.started_at} />
          </div>
          <div className="relative w-full bg-gray-200 dark:bg-gray-700 rounded-full h-1.5 overflow-hidden">
            <div className="absolute inset-y-0 left-0 w-1/4 bg-blue-500 dark:bg-blue-400 rounded-full pl-slide" />
          </div>
        </div>
      ) : null}
      <LogViewer lines={lines} />
    </div>
  );
}


/* ─── Main Component ─── */
const ProcessLog = () => {
  const { locale } = useLocale();
  const [dbRuns, setDbRuns] = useState([]);         // from /api/process-log/runs
  const [liveJobs, setLiveJobs] = useState([]);     // from /api/pipeline/jobs
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [expandedRun, setExpandedRun] = useState(null);   // run_id or job_id
  const [expandedLive, setExpandedLive] = useState(null);  // job_id for live detail
  const pollRef = useRef(null);

  const fetchAll = useCallback(async () => {
    try {
      const [runsRes, jobsRes] = await Promise.all([
        api.get(`/process-log/runs`).catch(() => ({ data: [] })),
        api.get(`/pipeline/jobs`).catch(() => ({ data: [] })),
      ]);
      setDbRuns(runsRes.data || []);
      setLiveJobs(Array.isArray(jobsRes.data) ? jobsRes.data : []);
      setError(null);
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  }, []);

  // Initial fetch + polling
  useEffect(() => {
    fetchAll();
    pollRef.current = setInterval(fetchAll, 4000);
    return () => clearInterval(pollRef.current);
  }, [fetchAll]);

  // Speed up polling when something is running
  const hasRunning = liveJobs.some(j => j.status === 'running');
  useEffect(() => {
    clearInterval(pollRef.current);
    pollRef.current = setInterval(fetchAll, hasRunning ? 2000 : 8000);
    return () => clearInterval(pollRef.current);
  }, [hasRunning, fetchAll]);

  // Active live jobs (running or just completed within last 60s)
  const activeLiveJobs = liveJobs.filter(j =>
    j.status === 'running' || j.status === 'pending' ||
    (j.ended_at && (Date.now() - new Date(j.ended_at).getTime()) < 60000)
  );

  return (
    <>
    <style>{INDETERMINATE_STYLE}</style>
    <div id="logs-page" className="p-4 sm:p-6 max-w-4xl mx-auto">
      <div id="logs-header" className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl sm:text-3xl font-bold text-gray-900 dark:text-white">Process Log</h1>
          <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
            Pipeline execution history and live running processes.
          </p>
        </div>
        <button
          onClick={() => { setLoading(true); fetchAll(); }}
          className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium rounded-lg border border-gray-300 dark:border-gray-600 text-gray-600 dark:text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-700 transition-colors"
        >
          <svg className={`w-3.5 h-3.5 ${loading ? 'animate-spin' : ''}`} fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
          </svg>
          Refresh
        </button>
      </div>

      {error && (
        <div className="mb-4 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg px-4 py-3 text-sm text-red-700 dark:text-red-300">
          {error}
        </div>
      )}

      {/* ─── Live Running Processes ─── */}
      {activeLiveJobs.length > 0 && (
        <div className="mb-6">
          <h2 className="text-xs font-semibold text-gray-400 dark:text-gray-500 uppercase tracking-wider mb-3 flex items-center gap-2">
            <span className="relative flex h-2 w-2">
              <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-blue-400 opacity-75"></span>
              <span className="relative inline-flex rounded-full h-2 w-2 bg-blue-500"></span>
            </span>
            Live Processes
          </h2>
          <div className="space-y-3">
            {activeLiveJobs.map(job => {
              const prog = job.progress;
              const hasProg = prog && prog.total > 0;
              const progPct = hasProg ? Math.min(100, prog.pct || 0) : 0;
              return (
                <div key={job.job_id} className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900/30 border border-blue-200 dark:border-blue-800/50 overflow-hidden">
                  <button
                    onClick={() => setExpandedLive(expandedLive === job.job_id ? null : job.job_id)}
                    className="w-full flex items-center gap-3 px-4 py-3 text-left hover:bg-gray-50 dark:hover:bg-gray-700/50 transition-colors"
                  >
                    <StatusBadge status={job.status} />
                    <div className="flex-1 min-w-0">
                      <span className="text-sm font-medium text-gray-900 dark:text-white">
                        {job.step_label || job.step || 'Pipeline'}
                      </span>
                      {job.current_step && (
                        <span className="ml-2 text-xs text-blue-500 dark:text-blue-400">
                          {'\u25B6'} {job.current_step}
                        </span>
                      )}
                    </div>
                    {hasProg && (
                      <span className="text-xs font-semibold text-blue-600 dark:text-blue-400 tabular-nums flex-shrink-0">
                        {progPct}%
                      </span>
                    )}
                    <span className="text-[10px] font-mono text-gray-400 dark:text-gray-500">{job.job_id}</span>
                    {job.started_at && (
                      <span className="text-xs text-gray-400 dark:text-gray-500 tabular-nums hidden sm:block">
                        {fmtTime(job.started_at, locale)}
                      </span>
                    )}
                    <span className="text-xs text-gray-400 dark:text-gray-500 tabular-nums">
                      {job.log_lines ? `${job.log_lines.length} lines` : ''}
                    </span>
                    <svg className={`w-4 h-4 text-gray-400 flex-shrink-0 transition-transform ${expandedLive === job.job_id ? 'rotate-180' : ''}`}
                      fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                    </svg>
                  </button>

                  {/* Progress bar — always visible while running */}
                  {(job.status === 'running' || job.status === 'pending' || hasProg) && (
                    <div className="px-4 pb-2 -mt-1">
                      {hasProg ? (
                        /* Series-count bar for forecast / backtest */
                        <>
                          <div className="flex items-center justify-between text-[10px] text-gray-400 dark:text-gray-500 mb-0.5">
                            <span>
                              {prog.current_step === 'forecast' ? '📊 Forecast' : prog.current_step === 'backtest' ? '🔁 Backtest' : '⚙️ Processing'}
                              {prog.batches_total > 1 && ` · batch ${prog.batches_done}/${prog.batches_total}`}
                            </span>
                            <span className="tabular-nums">{(prog.completed || 0).toLocaleString()} / {prog.total.toLocaleString()} series</span>
                          </div>
                          <div className="w-full bg-gray-100 dark:bg-gray-700 rounded-full h-1 overflow-hidden">
                            <div
                              className="bg-blue-500 dark:bg-blue-400 h-1 rounded-full transition-all duration-500"
                              style={{ width: `${progPct}%` }}
                            />
                          </div>
                        </>
                      ) : (
                        /* Indeterminate bar for all other running steps */
                        <>
                          <div className="flex items-center justify-between text-[10px] text-gray-400 dark:text-gray-500 mb-0.5">
                            <span>Running…</span>
                            <ElapsedTimer startedAt={job.started_at} />
                          </div>
                          <div className="relative w-full bg-gray-100 dark:bg-gray-700 rounded-full h-1 overflow-hidden">
                            <div className="absolute inset-y-0 left-0 w-1/4 bg-blue-500 dark:bg-blue-400 rounded-full pl-slide" />
                          </div>
                        </>
                      )}
                    </div>
                  )}

                  {expandedLive === job.job_id && (
                    <div className="px-4 pb-4 border-t border-gray-100 dark:border-gray-700">
                      <LiveJobDetail job={job} />
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* ─── Historical Runs ─── */}
      <div id="logs-history">
        <h2 className="text-xs font-semibold text-gray-400 dark:text-gray-500 uppercase tracking-wider mb-3">
          Run History
          {dbRuns.length > 0 && <span className="ml-1.5 text-gray-300 dark:text-gray-600">({dbRuns.length})</span>}
        </h2>

        {loading && dbRuns.length === 0 && (
          <div className="flex items-center gap-2 text-gray-400 dark:text-gray-500 py-8 justify-center">
            <Spinner cls="w-5 h-5" />
            Loading process log...
          </div>
        )}

        {!loading && dbRuns.length === 0 && activeLiveJobs.length === 0 && (
          <div className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900/30 p-8 text-center">
            <svg className="w-12 h-12 mx-auto text-gray-300 dark:text-gray-600 mb-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5}
                d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2" />
            </svg>
            <p className="text-gray-500 dark:text-gray-400 text-sm">No pipeline runs recorded yet.</p>
            <p className="text-gray-400 dark:text-gray-500 text-xs mt-1">
              Run the pipeline from the Pipeline Runner page to see execution history here.
            </p>
          </div>
        )}

        {dbRuns.length > 0 && (
          <div className="space-y-2">
            {dbRuns.map(run => {
              const isExpanded = expandedRun === run.run_id;
              return (
                <div key={run.run_id} className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900/30 overflow-hidden">
                  <button
                    onClick={() => setExpandedRun(isExpanded ? null : run.run_id)}
                    className="w-full flex items-center gap-3 px-4 py-3 text-left hover:bg-gray-50 dark:hover:bg-gray-700/50 transition-colors"
                  >
                    <StatusBadge status={run.overall_status} />
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center gap-2">
                        <span className="text-sm font-medium text-gray-900 dark:text-white">Pipeline Run</span>
                        <span className="text-[10px] font-mono text-gray-400 dark:text-gray-500 hidden sm:inline">{run.run_id.slice(0, 8)}</span>
                      </div>
                      <div className="flex items-center gap-3 mt-0.5 text-[10px] text-gray-400 dark:text-gray-500">
                        <span>{fmtTime(run.run_started_at, locale)}</span>
                      </div>
                    </div>

                    {/* Step summary badges */}
                    <div className="hidden sm:flex items-center gap-1.5">
                      {run.success_count > 0 && (
                        <span className="text-[10px] bg-emerald-100 dark:bg-emerald-900/30 text-emerald-600 dark:text-emerald-400 px-1.5 py-0.5 rounded-full font-medium">
                          {'\u2713'} {run.success_count}
                        </span>
                      )}
                      {run.error_count > 0 && (
                        <span className="text-[10px] bg-red-100 dark:bg-red-900/30 text-red-600 dark:text-red-400 px-1.5 py-0.5 rounded-full font-medium">
                          {'\u2717'} {run.error_count}
                        </span>
                      )}
                      {run.interrupted_count > 0 && (
                        <span className="text-[10px] bg-amber-100 dark:bg-amber-900/30 text-amber-600 dark:text-amber-400 px-1.5 py-0.5 rounded-full font-medium">
                          {'\u26A0'} {run.interrupted_count}
                        </span>
                      )}
                      {run.running_count > 0 && (
                        <span className="text-[10px] bg-blue-100 dark:bg-blue-900/30 text-blue-600 dark:text-blue-400 px-1.5 py-0.5 rounded-full font-medium flex items-center gap-0.5">
                          <Spinner cls="w-2.5 h-2.5" /> {run.running_count}
                        </span>
                      )}
                    </div>

                    <span className="text-xs text-gray-400 dark:text-gray-500 tabular-nums flex-shrink-0">
                      {fmtDuration(run.total_duration_s)}
                    </span>
                    <span className="text-[10px] text-gray-300 dark:text-gray-600 flex-shrink-0">
                      {run.step_count} step{run.step_count !== 1 ? 's' : ''}
                    </span>
                    <svg className={`w-4 h-4 text-gray-400 flex-shrink-0 transition-transform ${isExpanded ? 'rotate-180' : ''}`}
                      fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                    </svg>
                  </button>
                  {isExpanded && (
                    <div className="px-4 pb-4 border-t border-gray-100 dark:border-gray-700">
                      <RunSteps runId={run.run_id} locale={locale} />
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        )}
      </div>
    </div>
    </>
  );
};

export default ProcessLog;

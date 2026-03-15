import React, { useState, useEffect, useRef, useCallback } from 'react';
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

/* ─── Tiny helpers ─── */
function fmtDuration(sec) {
  if (sec == null) return '—';
  if (sec < 60) return `${Math.round(sec)}s`;
  const m = Math.floor(sec / 60);
  const s = Math.round(sec % 60);
  if (m < 60) return `${m}m ${s}s`;
  const h = Math.floor(m / 60);
  return `${h}h ${m % 60}m`;
}
function fmtTime(iso, locale) {
  if (!iso) return '—';
  return formatDateTime(iso, locale);
}

function Spinner({ cls = 'w-4 h-4' }) {
  return (
    <svg className={`animate-spin ${cls}`} viewBox="0 0 24 24" fill="none">
      <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
      <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
    </svg>
  );
}

const STATUS_CFG = {
  success:     { bg: 'bg-emerald-100 dark:bg-emerald-900/30', text: 'text-emerald-700 dark:text-emerald-400', icon: '✓',  dot: 'bg-emerald-500' },
  error:       { bg: 'bg-red-100 dark:bg-red-900/30',         text: 'text-red-700 dark:text-red-400',         icon: '✗',  dot: 'bg-red-500' },
  interrupted: { bg: 'bg-amber-100 dark:bg-amber-900/30',     text: 'text-amber-700 dark:text-amber-400',     icon: '⚠', dot: 'bg-amber-500' },
  running:     { bg: 'bg-blue-100 dark:bg-blue-900/30',       text: 'text-blue-700 dark:text-blue-400',       icon: null, dot: 'bg-blue-500' },
  pending:     { bg: 'bg-gray-100 dark:bg-gray-700',          text: 'text-gray-500 dark:text-gray-400',       icon: '•',  dot: 'bg-gray-400' },
};

function StatusBadge({ status }) {
  const cfg = STATUS_CFG[status] || STATUS_CFG.pending;
  return (
    <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[10px] font-semibold whitespace-nowrap ${cfg.bg} ${cfg.text}`}>
      {status === 'running' ? <Spinner cls="w-2.5 h-2.5" /> : cfg.icon}
      {status}
    </span>
  );
}

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
  return <span className="tabular-nums font-mono">{m > 0 ? `${m}m ` : ''}{String(s).padStart(2, '0')}s</span>;
}

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
    <div ref={containerRef} onScroll={handleScroll}
      className="bg-gray-900 rounded-lg p-3 max-h-48 overflow-y-auto font-mono text-[11px] leading-5 border border-gray-700">
      {(!lines || lines.length === 0)
        ? <span className="text-gray-500 italic">No log output captured.</span>
        : lines.map((l, i) => <div key={i} className={colorLine(l)}>{l || '\u00A0'}</div>)
      }
    </div>
  );
}

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
          <span className="text-gray-400 dark:text-gray-500 tabular-nums">batch {batches_done}/{batches_total}</span>
        )}
        <span className="font-semibold text-blue-600 dark:text-blue-400 tabular-nums">{pct}%</span>
      </div>
      <div className="w-full bg-gray-200 dark:bg-gray-700 rounded-full h-1.5 overflow-hidden">
        <div className="bg-blue-500 dark:bg-blue-400 h-1.5 rounded-full transition-all duration-500"
          style={{ width: `${Math.min(100, pct)}%` }} />
      </div>
    </div>
  );
}

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


/* ─── Sort icon ─── */
function SortIcon({ col, sortBy, sortDir }) {
  const active = sortBy === col;
  return (
    <span className={`inline-flex flex-col ml-1 gap-px align-middle ${active ? 'opacity-100' : 'opacity-30'}`}>
      <svg className={`w-2 h-2 ${active && sortDir === 'asc' ? 'text-blue-500' : 'text-current'}`} viewBox="0 0 8 5" fill="currentColor">
        <path d="M4 0L8 5H0z"/>
      </svg>
      <svg className={`w-2 h-2 ${active && sortDir === 'desc' ? 'text-blue-500' : 'text-current'}`} viewBox="0 0 8 5" fill="currentColor">
        <path d="M4 5L0 0h8z"/>
      </svg>
    </span>
  );
}

/* ─── Column header button ─── */
function ColHeader({ label, col, sortBy, sortDir, onSort, className = '' }) {
  return (
    <th
      onClick={() => onSort(col)}
      className={`px-3 py-2.5 text-left text-[11px] font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wider cursor-pointer select-none hover:text-gray-700 dark:hover:text-gray-200 whitespace-nowrap ${className}`}
    >
      {label}<SortIcon col={col} sortBy={sortBy} sortDir={sortDir} />
    </th>
  );
}

const LIMIT = 50;
const STATUS_FILTERS = ['', 'success', 'error', 'interrupted', 'running'];

/* ─── Main Component ─── */
const ProcessLog = () => {
  const { locale } = useLocale();

  /* Live jobs */
  const [liveJobs, setLiveJobs] = useState([]);
  const [expandedLive, setExpandedLive] = useState(null);

  /* Table data */
  const [items, setItems] = useState([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [expandedRow, setExpandedRow] = useState(null);  // row id with error/log expanded
  const [logData, setLogData] = useState({});            // {rowId: string}
  const [logLoading, setLogLoading] = useState(null);

  /* Filters */
  const [statusFilter, setStatusFilter] = useState('');
  const [stepInput, setStepInput] = useState('');      // raw input
  const [stepSearch, setStepSearch] = useState('');    // debounced
  const [dateFrom, setDateFrom] = useState('');
  const [dateTo, setDateTo] = useState('');

  /* Sort */
  const [sortBy, setSortBy] = useState('started_at');
  const [sortDir, setSortDir] = useState('desc');

  /* Pagination */
  const [offset, setOffset] = useState(0);

  const pollRef = useRef(null);

  /* Debounce step input */
  useEffect(() => {
    const t = setTimeout(() => { setStepSearch(stepInput); setOffset(0); }, 350);
    return () => clearTimeout(t);
  }, [stepInput]);

  /* Reset offset when filters/sort change */
  useEffect(() => { setOffset(0); }, [statusFilter, dateFrom, dateTo, sortBy, sortDir]);

  /* ── Fetch ── */
  const fetchData = useCallback(async (currentOffset) => {
    try {
      const params = new URLSearchParams({
        limit: LIMIT,
        offset: currentOffset ?? offset,
        sort_by: sortBy,
        sort_dir: sortDir,
      });
      if (statusFilter)  params.set('status', statusFilter);
      if (stepSearch)    params.set('step', stepSearch);
      if (dateFrom)      params.set('date_from', dateFrom);
      if (dateTo)        params.set('date_to', dateTo + 'T23:59:59');

      const [tableRes, jobsRes] = await Promise.all([
        api.get(`/process-log?${params}`).catch(() => ({ data: { items: [], total: 0 } })),
        api.get('/pipeline/jobs').catch(() => ({ data: [] })),
      ]);

      if (currentOffset === 0 || currentOffset == null) {
        setItems(tableRes.data.items || []);
      } else {
        setItems(prev => [...prev, ...(tableRes.data.items || [])]);
      }
      setTotal(tableRes.data.total || 0);
      setLiveJobs(Array.isArray(jobsRes.data) ? jobsRes.data : []);
      setError(null);
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  }, [statusFilter, stepSearch, dateFrom, dateTo, sortBy, sortDir, offset]);

  /* Initial + filter-driven fetches */
  useEffect(() => {
    setLoading(true);
    fetchData(0);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [statusFilter, stepSearch, dateFrom, dateTo, sortBy, sortDir]);

  /* Polling for live jobs only (every 5s, faster when running) */
  const hasRunning = liveJobs.some(j => j.status === 'running');
  useEffect(() => {
    clearInterval(pollRef.current);
    pollRef.current = setInterval(() => {
      api.get('/pipeline/jobs').catch(() => ({ data: [] })).then(r => {
        setLiveJobs(Array.isArray(r.data) ? r.data : []);
      });
    }, hasRunning ? 2000 : 8000);
    return () => clearInterval(pollRef.current);
  }, [hasRunning]);

  /* Sort handler */
  const handleSort = (col) => {
    if (col === sortBy) {
      setSortDir(d => d === 'desc' ? 'asc' : 'desc');
    } else {
      setSortBy(col);
      setSortDir('desc');
    }
  };

  /* Load more */
  const loadMore = () => {
    const next = offset + LIMIT;
    setOffset(next);
    fetchData(next);
  };

  /* Expand / collapse row */
  const toggleRow = async (row) => {
    const id = row.id;
    if (expandedRow === id) { setExpandedRow(null); return; }
    setExpandedRow(id);
    if (row.has_log_tail && !logData[id]) {
      setLogLoading(id);
      try {
        const r = await api.get(`/process-log/step/${id}/tail`);
        setLogData(prev => ({ ...prev, [id]: r.data.log_tail || '' }));
      } catch { setLogData(prev => ({ ...prev, [id]: 'Failed to load log.' })); }
      setLogLoading(null);
    }
  };

  /* Active live jobs */
  const activeLiveJobs = liveJobs.filter(j =>
    j.status === 'running' || j.status === 'pending' ||
    (j.ended_at && (Date.now() - new Date(j.ended_at).getTime()) < 60000)
  );

  const hasFilters = statusFilter || stepSearch || dateFrom || dateTo;

  return (
    <>
    <style>{INDETERMINATE_STYLE}</style>
    <div className="p-4 sm:p-6 max-w-7xl mx-auto">

      {/* ── Page header ── */}
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl sm:text-3xl font-bold text-gray-900 dark:text-white">Process Log</h1>
          <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
            Pipeline execution history and live running processes.
          </p>
        </div>
        <button
          onClick={() => { setLoading(true); fetchData(0); }}
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

      {/* ── Live Running Processes ── */}
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
                        <span className="ml-2 text-xs text-blue-500 dark:text-blue-400">▶ {job.current_step}</span>
                      )}
                    </div>
                    {hasProg && (
                      <span className="text-xs font-semibold text-blue-600 dark:text-blue-400 tabular-nums flex-shrink-0">{progPct}%</span>
                    )}
                    <span className="text-[10px] font-mono text-gray-400 dark:text-gray-500">{job.job_id}</span>
                    {job.started_at && (
                      <span className="text-xs text-gray-400 dark:text-gray-500 hidden sm:block">{fmtTime(job.started_at, locale)}</span>
                    )}
                    <svg className={`w-4 h-4 text-gray-400 flex-shrink-0 transition-transform ${expandedLive === job.job_id ? 'rotate-180' : ''}`}
                      fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                    </svg>
                  </button>
                  {(job.status === 'running' || job.status === 'pending' || hasProg) && (
                    <div className="px-4 pb-2 -mt-1">
                      {hasProg ? (
                        <>
                          <div className="flex items-center justify-between text-[10px] text-gray-400 dark:text-gray-500 mb-0.5">
                            <span>
                              {prog.current_step === 'forecast' ? '📊 Forecast' : prog.current_step === 'backtest' ? '🔁 Backtest' : '⚙️ Processing'}
                              {prog.batches_total > 1 && ` · batch ${prog.batches_done}/${prog.batches_total}`}
                            </span>
                            <span className="tabular-nums">{(prog.completed || 0).toLocaleString()} / {prog.total.toLocaleString()} series</span>
                          </div>
                          <div className="w-full bg-gray-100 dark:bg-gray-700 rounded-full h-1 overflow-hidden">
                            <div className="bg-blue-500 dark:bg-blue-400 h-1 rounded-full transition-all duration-500" style={{ width: `${progPct}%` }} />
                          </div>
                        </>
                      ) : (
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

      {/* ── Filter bar ── */}
      <div className="flex flex-wrap items-center gap-2 mb-3">
        {/* Status pills */}
        <div className="flex gap-1">
          {STATUS_FILTERS.map(s => (
            <button
              key={s}
              onClick={() => setStatusFilter(s)}
              className={`px-2.5 py-1 rounded text-[11px] font-medium transition-colors ${
                statusFilter === s
                  ? 'bg-blue-600 text-white'
                  : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-gray-600'
              }`}
            >
              {s === '' ? 'All' : s.charAt(0).toUpperCase() + s.slice(1)}
            </button>
          ))}
        </div>

        {/* Step search */}
        <div className="relative">
          <svg className="w-3 h-3 absolute left-2 top-1/2 -translate-y-1/2 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-4.35-4.35M17 11A6 6 0 115 11a6 6 0 0112 0z" />
          </svg>
          <input
            type="text"
            placeholder="Search process…"
            value={stepInput}
            onChange={e => setStepInput(e.target.value)}
            className="pl-6 pr-2 py-1 text-xs rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-gray-700 text-gray-700 dark:text-gray-300 placeholder-gray-400 dark:placeholder-gray-500 focus:outline-none focus:ring-1 focus:ring-blue-500 w-36"
          />
        </div>

        {/* Date range */}
        <input
          type="date"
          value={dateFrom}
          onChange={e => setDateFrom(e.target.value)}
          title="From date"
          className="py-1 px-2 text-xs rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-gray-700 text-gray-700 dark:text-gray-300 focus:outline-none focus:ring-1 focus:ring-blue-500"
        />
        <span className="text-xs text-gray-400">–</span>
        <input
          type="date"
          value={dateTo}
          onChange={e => setDateTo(e.target.value)}
          title="To date"
          className="py-1 px-2 text-xs rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-gray-700 text-gray-700 dark:text-gray-300 focus:outline-none focus:ring-1 focus:ring-blue-500"
        />

        {/* Clear filters */}
        {hasFilters && (
          <button
            onClick={() => { setStatusFilter(''); setStepInput(''); setStepSearch(''); setDateFrom(''); setDateTo(''); }}
            className="text-xs text-blue-500 dark:text-blue-400 hover:underline"
          >
            Clear filters
          </button>
        )}

        {/* Count */}
        <span className="ml-auto text-xs text-gray-400 dark:text-gray-500 tabular-nums">
          {loading ? <Spinner cls="w-3 h-3 inline" /> : `${items.length} / ${total.toLocaleString()}`}
        </span>
      </div>

      {/* ── Table ── */}
      {loading && items.length === 0 ? (
        <div className="flex items-center gap-2 text-gray-400 dark:text-gray-500 py-12 justify-center">
          <Spinner cls="w-5 h-5" />
          Loading process log...
        </div>
      ) : !loading && items.length === 0 ? (
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900/30 p-10 text-center">
          <svg className="w-12 h-12 mx-auto text-gray-300 dark:text-gray-600 mb-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5}
              d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2" />
          </svg>
          {hasFilters
            ? <p className="text-gray-500 dark:text-gray-400 text-sm">No entries match the current filters.</p>
            : <p className="text-gray-500 dark:text-gray-400 text-sm">No pipeline runs recorded yet.</p>
          }
        </div>
      ) : (
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900/30 overflow-hidden">
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead className="bg-gray-50 dark:bg-gray-700/50 border-b border-gray-200 dark:border-gray-700">
                <tr>
                  <ColHeader label="Status"   col="status"        sortBy={sortBy} sortDir={sortDir} onSort={handleSort} className="w-28" />
                  <th className="px-3 py-2.5 text-left text-[11px] font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wider w-24">Job</th>
                  <ColHeader label="Process"  col="step_name"     sortBy={sortBy} sortDir={sortDir} onSort={handleSort} />
                  <ColHeader label="Started"  col="started_at"    sortBy={sortBy} sortDir={sortDir} onSort={handleSort} className="w-40" />
                  <ColHeader label="Ended"    col="ended_at"      sortBy={sortBy} sortDir={sortDir} onSort={handleSort} className="w-40" />
                  <ColHeader label="Duration" col="duration_s"    sortBy={sortBy} sortDir={sortDir} onSort={handleSort} className="w-24" />
                  <ColHeader label="Rows"     col="rows_processed" sortBy={sortBy} sortDir={sortDir} onSort={handleSort} className="w-20 text-right" />
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100 dark:divide-gray-700/50">
                {items.map(row => {
                  const isExpanded = expandedRow === row.id;
                  const isJob = row.run_step_count > 1;
                  return (
                    <React.Fragment key={row.id}>
                      <tr
                        onClick={() => (row.error_message || row.has_log_tail) ? toggleRow(row) : undefined}
                        className={`transition-colors ${
                          row.error_message || row.has_log_tail
                            ? 'cursor-pointer hover:bg-gray-50 dark:hover:bg-gray-700/40'
                            : ''
                        } ${isExpanded ? 'bg-gray-50 dark:bg-gray-700/40' : ''}`}
                      >
                        {/* Status */}
                        <td className="px-3 py-2.5">
                          <StatusBadge status={row.status} />
                        </td>

                        {/* Job */}
                        <td className="px-3 py-2.5">
                          {isJob ? (
                            <span className="inline-block px-1.5 py-0.5 rounded bg-violet-100 dark:bg-violet-900/30 text-violet-700 dark:text-violet-300 text-[10px] font-mono font-semibold">
                              {row.run_id.slice(0, 8)}
                            </span>
                          ) : (
                            <span className="text-gray-300 dark:text-gray-600 text-xs">—</span>
                          )}
                        </td>

                        {/* Process */}
                        <td className="px-3 py-2.5">
                          <div className="flex items-center gap-1.5">
                            <span className="font-medium text-gray-800 dark:text-gray-200">{row.step_name}</span>
                            {(row.error_message || row.has_log_tail) && (
                              <svg className={`w-3 h-3 text-gray-400 flex-shrink-0 transition-transform ${isExpanded ? 'rotate-180' : ''}`}
                                fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                              </svg>
                            )}
                          </div>
                        </td>

                        {/* Started */}
                        <td className="px-3 py-2.5 text-xs text-gray-500 dark:text-gray-400 tabular-nums whitespace-nowrap">
                          {fmtTime(row.started_at, locale)}
                        </td>

                        {/* Ended */}
                        <td className="px-3 py-2.5 text-xs text-gray-500 dark:text-gray-400 tabular-nums whitespace-nowrap">
                          {row.status === 'running'
                            ? <span className="text-blue-500 dark:text-blue-400"><ElapsedTimer startedAt={row.started_at} /></span>
                            : fmtTime(row.ended_at, locale)
                          }
                        </td>

                        {/* Duration */}
                        <td className="px-3 py-2.5 text-xs text-gray-500 dark:text-gray-400 tabular-nums whitespace-nowrap">
                          {row.status === 'running'
                            ? <Spinner cls="w-3 h-3 text-blue-400" />
                            : fmtDuration(row.duration_s)
                          }
                        </td>

                        {/* Rows */}
                        <td className="px-3 py-2.5 text-xs text-gray-500 dark:text-gray-400 tabular-nums text-right">
                          {row.rows_processed != null ? row.rows_processed.toLocaleString() : '—'}
                        </td>
                      </tr>

                      {/* Expanded detail row */}
                      {isExpanded && (
                        <tr className="bg-gray-50 dark:bg-gray-700/30">
                          <td colSpan={7} className="px-4 py-3">
                            {row.error_message && (
                              <div className="mb-2 text-xs text-red-600 dark:text-red-400 bg-red-50 dark:bg-red-900/20 rounded px-3 py-2 font-mono break-all">
                                {row.error_message}
                              </div>
                            )}
                            {row.has_log_tail && (
                              logLoading === row.id
                                ? <div className="py-2 text-center"><Spinner cls="w-4 h-4 mx-auto text-gray-400" /></div>
                                : <LogViewer lines={logData[row.id] ? logData[row.id].split('\n') : []} />
                            )}
                          </td>
                        </tr>
                      )}
                    </React.Fragment>
                  );
                })}
              </tbody>
            </table>
          </div>

          {/* Load more */}
          {items.length < total && (
            <div className="px-4 py-3 border-t border-gray-100 dark:border-gray-700 text-center">
              <button
                onClick={loadMore}
                disabled={loading}
                className="text-xs text-blue-500 dark:text-blue-400 hover:underline disabled:opacity-50"
              >
                {loading ? <Spinner cls="w-3 h-3 inline mr-1" /> : null}
                Load more ({items.length} of {total.toLocaleString()})
              </button>
            </div>
          )}
        </div>
      )}
    </div>
    </>
  );
};

export default ProcessLog;

import { useState, useEffect, useCallback } from 'react';
import { useLocale } from '../contexts/LocaleContext';
import { formatDateTime } from '../utils/formatting';
import api from '../utils/api';

/* ─── Constants ─── */

const ENTITY_TYPES = ['parameter', 'segment', 'parameter_segment'];
const ACTIONS = ['create', 'update', 'delete', 'reorder'];

const ACTION_BADGE = {
  create: { bg: 'bg-emerald-100 dark:bg-emerald-900/30', text: 'text-emerald-700 dark:text-emerald-400', icon: '+' },
  update: { bg: 'bg-blue-100 dark:bg-blue-900/30',    text: 'text-blue-700 dark:text-blue-400',    icon: '~' },
  delete: { bg: 'bg-red-100 dark:bg-red-900/30',      text: 'text-red-700 dark:text-red-400',      icon: '\u2212' },
  reorder:{ bg: 'bg-amber-100 dark:bg-amber-900/30',  text: 'text-amber-700 dark:text-amber-400',  icon: '\u2195' },
};

const ENTITY_BADGE = {
  parameter:         { bg: 'bg-purple-100 dark:bg-purple-900/30', text: 'text-purple-700 dark:text-purple-400' },
  segment:           { bg: 'bg-indigo-100 dark:bg-indigo-900/30', text: 'text-indigo-700 dark:text-indigo-400' },
  parameter_segment: { bg: 'bg-gray-100 dark:bg-gray-700',       text: 'text-gray-600 dark:text-gray-300' },
};

const PAGE_SIZE = 30;


/* ─── Small helpers ─── */

function ActionBadge({ action }) {
  const cfg = ACTION_BADGE[action] || ACTION_BADGE.update;
  return (
    <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[10px] font-semibold ${cfg.bg} ${cfg.text}`}>
      {cfg.icon} {action}
    </span>
  );
}

function EntityBadge({ type }) {
  const cfg = ENTITY_BADGE[type] || ENTITY_BADGE.parameter;
  const label = type === 'parameter_segment' ? 'param-segment' : type;
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-[10px] font-medium ${cfg.bg} ${cfg.text}`}>
      {label}
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


/* ─── JSON diff viewer ─── */

function JsonDiff({ oldVal, newVal }) {
  if (!oldVal && !newVal) return null;

  // For create/delete just show the single value
  if (!oldVal) return <JsonBlock label="New" data={newVal} color="emerald" />;
  if (!newVal) return <JsonBlock label="Previous" data={oldVal} color="red" />;

  // For updates show both side by side
  return (
    <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
      <JsonBlock label="Before" data={oldVal} color="red" />
      <JsonBlock label="After" data={newVal} color="emerald" />
    </div>
  );
}

function JsonBlock({ label, data, color }) {
  const borderCls = color === 'emerald' ? 'border-emerald-200 dark:border-emerald-800/50' : 'border-red-200 dark:border-red-800/50';
  const labelCls  = color === 'emerald' ? 'text-emerald-600 dark:text-emerald-400' : 'text-red-600 dark:text-red-400';

  let content;
  if (typeof data === 'string') {
    try { content = JSON.stringify(JSON.parse(data), null, 2); }
    catch { content = data; }
  } else {
    content = JSON.stringify(data, null, 2);
  }

  return (
    <div className={`border rounded-lg overflow-hidden ${borderCls}`}>
      <div className={`px-2 py-1 text-[10px] font-semibold uppercase tracking-wider ${labelCls} bg-gray-50 dark:bg-gray-800 border-b ${borderCls}`}>
        {label}
      </div>
      <pre className="px-3 py-2 text-[11px] leading-4 font-mono text-gray-700 dark:text-gray-300 overflow-x-auto max-h-48 overflow-y-auto whitespace-pre-wrap break-all">
        {content}
      </pre>
    </div>
  );
}


/* ─── Main Component ─── */

const AuditLog = () => {
  const { locale } = useLocale();
  const [items, setItems] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [offset, setOffset] = useState(0);
  const [hasMore, setHasMore] = useState(true);

  // Filters
  const [filterEntity, setFilterEntity] = useState('');
  const [filterAction, setFilterAction] = useState('');
  const [expandedId, setExpandedId] = useState(null);

  const fetchLog = useCallback(async (newOffset = 0, append = false) => {
    setLoading(true);
    setError(null);
    try {
      const params = { limit: PAGE_SIZE, offset: newOffset };
      if (filterEntity) params.entity_type = filterEntity;
      if (filterAction) params.action = filterAction;

      const res = await api.get('/audit-log', { params });
      const rows = res.data?.items || [];
      setItems(prev => append ? [...prev, ...rows] : rows);
      setOffset(newOffset);
      setHasMore(rows.length === PAGE_SIZE);
    } catch (e) {
      setError(e.response?.data?.detail || e.message);
    } finally {
      setLoading(false);
    }
  }, [filterEntity, filterAction]);

  // Re-fetch when filters change
  useEffect(() => {
    fetchLog(0);
  }, [fetchLog]);

  const loadMore = () => fetchLog(offset + PAGE_SIZE, true);

  return (
    <div className="p-4 sm:p-6 max-w-4xl mx-auto">
      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl sm:text-3xl font-bold text-gray-900 dark:text-white">Audit Log</h1>
          <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
            Track all changes to parameters and segments.
          </p>
        </div>
        <button
          onClick={() => fetchLog(0)}
          className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium rounded-lg border border-gray-300 dark:border-gray-600 text-gray-600 dark:text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-700 transition-colors"
        >
          <svg className={`w-3.5 h-3.5 ${loading && items.length === 0 ? 'animate-spin' : ''}`} fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
          </svg>
          Refresh
        </button>
      </div>

      {/* Filters */}
      <div className="flex flex-wrap items-center gap-3 mb-4">
        <div className="flex items-center gap-2">
          <label className="text-xs font-medium text-gray-500 dark:text-gray-400">Entity</label>
          <select
            value={filterEntity}
            onChange={e => setFilterEntity(e.target.value)}
            className="text-xs px-2 py-1.5 rounded-lg border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-gray-700 dark:text-gray-300 focus:ring-1 focus:ring-blue-500"
          >
            <option value="">All</option>
            {ENTITY_TYPES.map(t => <option key={t} value={t}>{t}</option>)}
          </select>
        </div>
        <div className="flex items-center gap-2">
          <label className="text-xs font-medium text-gray-500 dark:text-gray-400">Action</label>
          <select
            value={filterAction}
            onChange={e => setFilterAction(e.target.value)}
            className="text-xs px-2 py-1.5 rounded-lg border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-gray-700 dark:text-gray-300 focus:ring-1 focus:ring-blue-500"
          >
            <option value="">All</option>
            {ACTIONS.map(a => <option key={a} value={a}>{a}</option>)}
          </select>
        </div>
        {(filterEntity || filterAction) && (
          <button
            onClick={() => { setFilterEntity(''); setFilterAction(''); }}
            className="text-[10px] text-blue-600 dark:text-blue-400 hover:underline"
          >
            Clear filters
          </button>
        )}
      </div>

      {/* Error */}
      {error && (
        <div className="mb-4 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg px-4 py-3 text-sm text-red-700 dark:text-red-300">
          {error}
        </div>
      )}

      {/* Loading */}
      {loading && items.length === 0 && (
        <div className="flex items-center gap-2 text-gray-400 dark:text-gray-500 py-8 justify-center">
          <Spinner cls="w-5 h-5" />
          Loading audit log...
        </div>
      )}

      {/* Empty state */}
      {!loading && items.length === 0 && (
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900/30 p-8 text-center">
          <svg className="w-12 h-12 mx-auto text-gray-300 dark:text-gray-600 mb-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5}
              d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
          </svg>
          <p className="text-gray-500 dark:text-gray-400 text-sm">No audit log entries found.</p>
          <p className="text-gray-400 dark:text-gray-500 text-xs mt-1">
            Changes to parameters and segments will appear here automatically.
          </p>
        </div>
      )}

      {/* Entries */}
      {items.length > 0 && (
        <div className="space-y-2">
          {items.map(entry => {
            const isExpanded = expandedId === entry.id;
            return (
              <div key={entry.id} className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900/30 overflow-hidden">
                <button
                  onClick={() => setExpandedId(isExpanded ? null : entry.id)}
                  className="w-full flex items-center gap-3 px-4 py-3 text-left hover:bg-gray-50 dark:hover:bg-gray-700/50 transition-colors"
                >
                  <ActionBadge action={entry.action} />
                  <EntityBadge type={entry.entity_type} />
                  {entry.entity_id != null && (
                    <span className="text-xs font-mono text-gray-400 dark:text-gray-500">
                      #{entry.entity_id}
                    </span>
                  )}

                  {/* Summary: try to extract name from new/old value */}
                  <span className="flex-1 text-sm text-gray-700 dark:text-gray-300 truncate">
                    {extractSummary(entry)}
                  </span>

                  <span className="text-xs text-gray-400 dark:text-gray-500 tabular-nums flex-shrink-0 hidden sm:block">
                    {entry.changed_by}
                  </span>
                  <span className="text-xs text-gray-400 dark:text-gray-500 tabular-nums flex-shrink-0">
                    {formatDateTime(entry.created_at, locale)}
                  </span>
                  <svg className={`w-4 h-4 text-gray-400 flex-shrink-0 transition-transform ${isExpanded ? 'rotate-180' : ''}`}
                    fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                  </svg>
                </button>
                {isExpanded && (
                  <div className="px-4 pb-4 border-t border-gray-100 dark:border-gray-700 pt-3">
                    <JsonDiff oldVal={entry.old_value} newVal={entry.new_value} />
                  </div>
                )}
              </div>
            );
          })}

          {/* Load more */}
          {hasMore && (
            <div className="text-center pt-2">
              <button
                onClick={loadMore}
                disabled={loading}
                className="inline-flex items-center gap-1.5 px-4 py-2 text-xs font-medium rounded-lg border border-gray-300 dark:border-gray-600 text-gray-600 dark:text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-700 transition-colors disabled:opacity-50"
              >
                {loading ? <Spinner cls="w-3 h-3" /> : null}
                Load more
              </button>
            </div>
          )}
        </div>
      )}
    </div>
  );
};


/** Try to pull a human-readable name from old/new JSON values. */
function extractSummary(entry) {
  const val = entry.new_value || entry.old_value;
  if (!val) return '';
  const obj = typeof val === 'string' ? tryParse(val) : val;
  if (!obj) return '';
  return obj.name || obj.label || '';
}

function tryParse(s) {
  try { return JSON.parse(s); } catch { return null; }
}

export default AuditLog;

/**
 * TimeSeriesViewer Component
 *
 * Displays time series with forecasts from all methods, quantile bands,
 * backtest metrics comparison, racing bars, forecast origin slider,
 * and an interactive date-range zoom.
 * All sections are individually collapsible.
 * Item/Site searchable dropdowns with recently-accessed history.
 */

import React, { useState, useEffect, useMemo, useCallback, useRef } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
// VegaLite removed — all charts now use Plotly (imported below)

import Plot from 'react-plotly.js';
import { useLocale } from '../contexts/LocaleContext';
import { useTheme } from '../contexts/ThemeContext';
import { formatNumber, formatDate, formatYearMonth, formatPercent, toISODate, formatDateTime } from '../utils/formatting';
import api from '../utils/api';
import DateRangePicker from './DateRangePicker';

const METHOD_COLORS = {
  AutoETS: '#2563eb',
  AutoARIMA: '#dc2626',
  AutoTheta: '#16a34a',
  SeasonalNaive: '#9333ea',
  HistoricAverage: '#ea580c',
  CrostonOptimized: '#0891b2',
  MSTL: '#c026d3',
  TimesFM: '#4f46e5',
  Historical: '#374151',
};
const getMethodColor = (method) => METHOD_COLORS[method] || '#6b7280';

/* ─── ListEditorPopup ──────────────────────────────────────────────────
 * A small popup for editing array values (e.g. confidence_levels).
 * Displays each item as an editable row with add / delete controls.
 * ─────────────────────────────────────────────────────────────────────── */
const ListEditorPopup = ({ values, onChange, onClose, label }) => {
  const [items, setItems] = useState(() =>
    (Array.isArray(values) ? values : []).map((v, i) => ({ id: i, value: v }))
  );
  const nextId = useRef((Array.isArray(values) ? values.length : 0));
  const popupRef = useRef(null);

  // Close on outside click or Escape
  useEffect(() => {
    const handleClick = (e) => { if (popupRef.current && !popupRef.current.contains(e.target)) onClose(); };
    const handleKey = (e) => { if (e.key === 'Escape') onClose(); };
    document.addEventListener('mousedown', handleClick);
    document.addEventListener('keydown', handleKey);
    return () => { document.removeEventListener('mousedown', handleClick); document.removeEventListener('keydown', handleKey); };
  }, [onClose]);

  const updateItem = (id, raw) => {
    setItems(prev => prev.map(it => it.id === id ? { ...it, value: raw } : it));
  };
  const removeItem = (id) => {
    setItems(prev => prev.filter(it => it.id !== id));
  };
  const addItem = () => {
    const id = nextId.current++;
    setItems(prev => [...prev, { id, value: '' }]);
  };
  const handleApply = () => {
    const parsed = items.map(it => {
      const trimmed = String(it.value).trim();
      const n = Number(trimmed);
      return trimmed === '' ? null : (isNaN(n) ? trimmed : n);
    }).filter(v => v !== null);
    onChange(parsed);
    onClose();
  };

  return (
    <div ref={popupRef}
      className="absolute z-50 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-600 rounded-lg shadow-xl p-3 min-w-[200px]"
      style={{ top: '100%', right: 0, marginTop: 4 }}
    >
      <div className="text-xs font-semibold text-gray-600 dark:text-gray-300 mb-2">{label || 'Edit list'}</div>
      <div className="flex flex-col gap-1 max-h-48 overflow-y-auto mb-2">
        {items.map((it, idx) => (
          <div key={it.id} className="flex items-center gap-1">
            <span className="text-[10px] text-gray-400 dark:text-gray-500 w-4 text-right flex-shrink-0">{idx + 1}</span>
            <input
              type="text"
              autoFocus={idx === items.length - 1}
              value={it.value}
              onChange={(e) => updateItem(it.id, e.target.value)}
              onKeyDown={(e) => { if (e.key === 'Enter') { e.preventDefault(); addItem(); } }}
              className="flex-1 text-xs font-mono border border-gray-200 dark:border-gray-600 rounded px-1.5 py-0.5 bg-white dark:bg-gray-900 dark:text-gray-200 focus:outline-none focus:ring-1 focus:ring-indigo-400 w-20"
            />
            <button
              onClick={() => removeItem(it.id)}
              className="text-red-400 hover:text-red-600 dark:text-red-500 dark:hover:text-red-400 text-xs px-1 flex-shrink-0"
              title="Remove"
            >&times;</button>
          </div>
        ))}
        {items.length === 0 && (
          <div className="text-xs text-gray-400 dark:text-gray-500 italic py-1">Empty list</div>
        )}
      </div>
      <div className="flex items-center justify-between border-t border-gray-100 dark:border-gray-700 pt-2">
        <button
          onClick={addItem}
          className="text-xs text-indigo-600 dark:text-indigo-400 hover:text-indigo-800 dark:hover:text-indigo-300 font-medium"
        >+ Add row</button>
        <div className="flex gap-1.5">
          <button
            onClick={onClose}
            className="text-xs px-2 py-1 text-gray-500 dark:text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-700 rounded"
          >Cancel</button>
          <button
            onClick={handleApply}
            className="text-xs px-2.5 py-1 bg-indigo-500 text-white rounded hover:bg-indigo-600 font-medium"
          >Apply</button>
        </div>
      </div>
    </div>
  );
};

const fmtDate = (d) => d.toISOString().split('T')[0];

// ---- Time aggregation helpers ----
const AGG_OPTS = [
  { value: 'native', label: 'Native' },
  { value: 'D',      label: 'Daily'  },
  { value: 'W',      label: 'Weekly' },
  { value: 'M',      label: 'Monthly'},
  { value: 'Q',      label: 'Quarterly' },
  { value: 'Y',      label: 'Yearly' },
];


/** Canonical period-start ISO date for any aggregation level. */
const getPeriodKey = (dateStr, agg) => {
  const d = new Date(dateStr);
  if (!agg || agg === 'native' || agg === 'D') return dateStr.slice(0, 10);
  if (agg === 'W') {
    const dow = d.getUTCDay();
    const mon = new Date(d);
    mon.setUTCDate(d.getUTCDate() - (dow === 0 ? 6 : dow - 1));
    return mon.toISOString().slice(0, 10);
  }
  if (agg === 'M') return `${d.getUTCFullYear()}-${String(d.getUTCMonth()+1).padStart(2,'0')}-01`;
  if (agg === 'Q') {
    const q = Math.floor(d.getUTCMonth() / 3);
    return `${d.getUTCFullYear()}-${String(q * 3 + 1).padStart(2,'0')}-01`;
  }
  if (agg === 'Y') return `${d.getUTCFullYear()}-01-01`;
  return dateStr.slice(0, 10);
};

/** Aggregate {date:string[], value:number[]} to a coarser period (sum). */
const aggHistData = (data, agg) => {
  if (!data?.date?.length || !agg || agg === 'native') return data;
  const map = new Map();
  data.date.forEach((d, i) => {
    const key = getPeriodKey(d, agg);
    map.set(key, (map.get(key) ?? 0) + (data.value[i] ?? 0));
  });
  const sorted = [...map.entries()].sort((a, b) => a[0].localeCompare(b[0]));
  return { date: sorted.map(([k]) => k), value: sorted.map(([, v]) => v) };
};

/**
 * Aggregate forecast step arrays (native dates + values) to a coarser period.
 * Returns { dates, pf, qs } all aggregated.
 */
const aggForecastSeries = (nativeDates, pointForecast, quantiles, agg) => {
  if (!agg || agg === 'native') return { dates: nativeDates, pf: pointForecast, qs: quantiles };
  const pfMap = new Map();
  const qsMap = {};
  Object.keys(quantiles).forEach(q => { qsMap[q] = new Map(); });
  nativeDates.forEach((d, i) => {
    const key = getPeriodKey(d, agg);
    pfMap.set(key, (pfMap.get(key) ?? 0) + (pointForecast[i] ?? 0));
    Object.keys(quantiles).forEach(q => {
      qsMap[q].set(key, (qsMap[q].get(key) ?? 0) + (quantiles[q]?.[i] ?? 0));
    });
  });
  const sorted = [...pfMap.keys()].sort();
  const aggQs = {};
  Object.keys(qsMap).forEach(q => { aggQs[q] = sorted.map(k => qsMap[q].get(k) ?? 0); });
  return { dates: sorted, pf: sorted.map(k => pfMap.get(k) ?? 0), qs: aggQs };
};

// ---- localStorage helpers ----
const getRecent = (key) => {
  try { return JSON.parse(localStorage.getItem(key) || '[]'); } catch { return []; }
};
const setRecent = (key, value, max = 5) => {
  const existing = getRecent(key);
  const filtered = existing.filter(v => v !== value);
  localStorage.setItem(key, JSON.stringify([value, ...filtered].slice(0, max)));
};

// ---- Parse unique_id into item + site (split on FIRST underscore) ----
const parseUniqueId = (uid) => {
  if (!uid) return { item: '', site: '' };
  const idx = uid.indexOf('_');
  if (idx === -1) return { item: uid, site: '' };
  return { item: uid.slice(0, idx), site: uid.slice(idx + 1) };
};

// ---- Section order — persisted drag-and-drop ----
const SECTION_ORDER_KEY = 'tsv_section_order';
const DEFAULT_SECTION_ORDER = [
  'toggles', 'main_chart', 'forecast_table', 'outlier',
  'rationale', 'parameters', 'metrics', 'hyperparameters', 'ridge', 'evolution',
];

function useSectionOrder() {
  const [order, setOrder] = useState(() => {
    try {
      const stored = JSON.parse(localStorage.getItem(SECTION_ORDER_KEY) || 'null');
      if (Array.isArray(stored) && stored.length > 0) {
        // Merge: keep stored order, append any new sections not yet in storage
        const merged = [...stored, ...DEFAULT_SECTION_ORDER.filter(s => !stored.includes(s))];
        return merged;
      }
    } catch { /* ignore */ }
    return DEFAULT_SECTION_ORDER;
  });

  const reorder = (dragId, overId) => {
    if (dragId === overId) return;
    setOrder(prev => {
      const next = [...prev];
      const from = next.indexOf(dragId);
      const to   = next.indexOf(overId);
      if (from === -1 || to === -1) return prev;
      next.splice(from, 1);
      next.splice(to, 0, dragId);
      localStorage.setItem(SECTION_ORDER_KEY, JSON.stringify(next));
      return next;
    });
  };

  return { order, reorder };
}

// ---- Collapsible section with drag-and-drop handle ----
const Section = ({
  title, storageKey, defaultOpen = true, children, badge, id,
  dragId, dragOver, onDragStart, onDragOver, onDrop, onDragEnd,
}) => {
  const [open, setOpen] = useState(() => {
    const stored = localStorage.getItem(storageKey);
    return stored === null ? defaultOpen : stored === 'true';
  });
  const toggle = () => setOpen(prev => {
    const next = !prev;
    localStorage.setItem(storageKey, String(next));
    return next;
  });

  const isDragTarget = dragOver === dragId;

  return (
    <div
      id={id}
      className={`mb-6 bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900/50 transition-all ${isDragTarget ? 'ring-2 ring-blue-400 ring-offset-1' : ''}`}
      onDragOver={dragId ? (e) => { e.preventDefault(); e.dataTransfer.dropEffect = 'move'; onDragOver(dragId); } : undefined}
      onDrop={dragId ? (e) => { e.preventDefault(); onDrop(dragId); } : undefined}
    >
      <div className="flex items-center rounded-t-lg hover:bg-gray-50 dark:hover:bg-gray-700/50 transition-colors">
        {/* Drag handle — draggable only on this icon, not the whole section,
            so chart interactions (Plotly 3D rotate, zoom slider) aren't stolen
            by the browser's HTML5 DnD system. */}
        {dragId && (
          <span
            draggable
            onDragStart={(e) => { e.dataTransfer.effectAllowed = 'move'; onDragStart(dragId); }}
            onDragEnd={onDragEnd}
            className="pl-3 pr-1 py-4 text-gray-300 dark:text-gray-600 hover:text-gray-500 dark:hover:text-gray-400 cursor-grab active:cursor-grabbing select-none text-lg flex-shrink-0"
            title="Drag to reorder"
          >
            {'\u2817'}
          </span>
        )}
        <button
          onClick={toggle}
          className="flex-1 flex items-center justify-between px-4 py-4 text-left"
        >
          <div className="flex items-center gap-2">
            <h2 className="text-lg font-semibold text-gray-900 dark:text-white">{title}</h2>
            {badge && <span className="text-xs bg-gray-100 dark:bg-gray-700 text-gray-500 dark:text-gray-400 px-2 py-0.5 rounded-full">{badge}</span>}
          </div>
          <span className="text-gray-400 dark:text-gray-500 text-xl flex-shrink-0">{open ? '\u25B2' : '\u25BC'}</span>
        </button>
      </div>
      {open && <div className="px-4 pb-4 sm:px-6 sm:pb-6">{children}</div>}
    </div>
  );
};

// ---- Parameter key-value compact display ----
// Renders a flat list of key=value pairs from a (possibly nested) object, max 2 levels deep
function ParameterKeyValues({ params, prefix = '', depth = 0 }) {
  if (!params || typeof params !== 'object' || depth > 1) return null;
  return (
    <div className="space-y-0.5">
      {Object.entries(params).map(([k, v]) => {
        const fullKey = prefix ? `${prefix}.${k}` : k;
        if (v !== null && typeof v === 'object' && !Array.isArray(v) && depth < 1) {
          return <ParameterKeyValues key={k} params={v} prefix={fullKey} depth={depth + 1} />;
        }
        const display = Array.isArray(v) ? (v.length > 0 ? v.join(', ') : '—') : String(v ?? '—');
        return (
          <div key={k} className="flex justify-between gap-2 text-[10px]">
            <span className="text-gray-400 dark:text-gray-500 truncate flex-shrink-0 max-w-[50%]">{fullKey}</span>
            <span className="text-gray-700 dark:text-gray-300 font-mono text-right truncate">{display}</span>
          </div>
        );
      })}
    </div>
  );
}

// ---- Searchable multi-select dropdown with recent history ----
// `values` is an array of selected strings; `onChange` receives the new array
// `getLabel` (optional) maps a value to its display label; defaults to identity
const SearchableDropdown = ({ label, values = [], onChange, options, recentOptions, disabled, placeholder, getLabel }) => {
  const [open, setOpen] = useState(false);
  const [search, setSearch] = useState('');
  const ref = useRef(null);
  const getLbl = getLabel || (v => v);

  // Close on outside click
  useEffect(() => {
    const handler = (e) => { if (ref.current && !ref.current.contains(e.target)) setOpen(false); };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, []);

  const toggleOption = (o) => {
    if (values.includes(o)) {
      onChange(values.filter(v => v !== o));
    } else {
      onChange([...values, o]);
    }
  };

  const filteredRecent = recentOptions.filter(o => getLbl(o).toLowerCase().includes(search.toLowerCase()));
  const filteredAll = options.filter(o =>
    getLbl(o).toLowerCase().includes(search.toLowerCase()) &&
    !recentOptions.includes(o)
  );
  const hasRecent = filteredRecent.length > 0 && search === '';
  const displayText = values.length === 0 ? '' : values.length === 1 ? getLbl(values[0]) : `${values.length} selected`;

  return (
    <div ref={ref} className="relative flex-1 min-w-0">
      <label className="block text-xs font-medium text-gray-500 dark:text-gray-400 mb-1">{label}</label>
      <div
        className={`flex items-center border rounded-lg px-3 py-2 gap-2 transition-colors min-h-[40px]
          ${disabled ? 'bg-gray-50 dark:bg-gray-800 border-gray-200 dark:border-gray-700 cursor-not-allowed opacity-60' : 'bg-white dark:bg-gray-800 border-gray-300 dark:border-gray-600 cursor-pointer hover:border-blue-400'}
          ${open ? 'border-blue-500 ring-2 ring-blue-100 dark:ring-blue-900' : ''}`}
        onClick={() => { if (!disabled) { setOpen(o => !o); setSearch(''); } }}
      >
        <svg className="w-3.5 h-3.5 text-gray-400 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-4.35-4.35M17 11A6 6 0 1 1 5 11a6 6 0 0 1 12 0z" />
        </svg>
        <input
          type="text"
          value={open ? search : displayText}
          onChange={e => { setSearch(e.target.value); if (!open) setOpen(true); }}
          onClick={e => { e.stopPropagation(); if (!disabled) setOpen(true); }}
          placeholder={disabled ? 'Select item first' : placeholder}
          disabled={disabled}
          className="flex-1 min-w-0 text-sm outline-none bg-transparent dark:text-gray-200 dark:placeholder-gray-500"
        />
        {values.length > 0 && !open && (
          <button onClick={e => { e.stopPropagation(); onChange([]); setSearch(''); }}
            className="text-gray-400 dark:text-gray-500 hover:text-gray-600 dark:hover:text-gray-300 flex-shrink-0 text-xs">✕</button>
        )}
        {values.length > 1 && (
          <span className="text-xs bg-blue-100 dark:bg-blue-900/40 text-blue-700 dark:text-blue-300 px-1.5 py-0.5 rounded-full font-semibold flex-shrink-0">{values.length}</span>
        )}
        <svg className={`w-4 h-4 text-gray-400 flex-shrink-0 transition-transform ${open ? 'rotate-180' : ''}`}
          fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
        </svg>
      </div>

      {open && !disabled && (
        <div className="absolute z-50 mt-1 left-0 right-0 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-600 rounded-lg shadow-lg max-h-64 overflow-y-auto">
          {/* Select all / clear */}
          <div className="flex items-center gap-2 px-3 py-2 border-b border-gray-100 dark:border-gray-700 sticky top-0 bg-white dark:bg-gray-800 z-10">
            <button onClick={() => onChange(options)} className="text-xs text-blue-600 dark:text-blue-400 hover:underline">All</button>
            <span className="text-gray-300 dark:text-gray-600">|</span>
            <button onClick={() => onChange([])} className="text-xs text-gray-500 dark:text-gray-400 hover:underline">Clear</button>
            <span className="ml-auto text-xs text-gray-400">{values.length} selected</span>
          </div>

          {/* Recent section */}
          {hasRecent && (
            <>
              <div className="px-3 py-1.5 text-xs font-semibold text-gray-400 uppercase tracking-wide bg-gray-50 dark:bg-gray-700/50">
                Recently accessed
              </div>
              {filteredRecent.map(o => (
                <button key={`recent-${o}`} onClick={() => toggleOption(o)}
                  className={`w-full text-left px-3 py-2 text-sm hover:bg-blue-50 dark:hover:bg-blue-900/30 hover:text-blue-700 dark:hover:text-blue-300 flex items-center gap-2 ${values.includes(o) ? 'bg-blue-50 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300' : 'dark:text-gray-300'}`}>
                  <span className={`w-4 h-4 rounded border flex-shrink-0 flex items-center justify-center text-xs ${values.includes(o) ? 'bg-blue-500 border-blue-500 text-white' : 'border-gray-300 dark:border-gray-600'}`}>{values.includes(o) ? '✓' : ''}</span>
                  <span className="text-gray-400 flex-shrink-0">🕐</span>
                  <span>{getLbl(o)}</span>
                </button>
              ))}
              {filteredAll.length > 0 && <div className="border-t border-gray-100 dark:border-gray-700" />}
            </>
          )}

          {/* All options */}
          {filteredAll.length > 0 && (
            <>
              {hasRecent && (
                <div className="px-3 py-1.5 text-xs font-semibold text-gray-400 uppercase tracking-wide bg-gray-50 dark:bg-gray-700/50">
                  All
                </div>
              )}
              {filteredAll.map(o => (
                <button key={o} onClick={() => toggleOption(o)}
                  className={`w-full text-left px-3 py-2 text-sm hover:bg-blue-50 dark:hover:bg-blue-900/30 hover:text-blue-700 dark:hover:text-blue-300 flex items-center gap-2 ${values.includes(o) ? 'bg-blue-50 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300' : 'dark:text-gray-300'}`}>
                  <span className={`w-4 h-4 rounded border flex-shrink-0 flex items-center justify-center text-xs ${values.includes(o) ? 'bg-blue-500 border-blue-500 text-white' : 'border-gray-300 dark:border-gray-600'}`}>{values.includes(o) ? '✓' : ''}</span>
                  <span>{getLbl(o)}</span>
                </button>
              ))}
            </>
          )}

          {filteredRecent.length === 0 && filteredAll.length === 0 && (
            <div className="px-3 py-4 text-sm text-gray-400 text-center">No matches found</div>
          )}
        </div>
      )}
    </div>
  );
};


// ── Forecast table with embedded adjustment rows ──────────────────────────────
//
// Renders the full forecast point values table.
// For the best (selected) method, two extra collapsible rows appear directly
// below the method row:
//   • "Adjustment (±)" — additive delta inputs, one cell per horizon month
//   • "Override"       — full-replacement inputs, one cell per horizon month
// Both rows fold under a single toggle button in the method label cell.
// Typing and leaving a cell auto-saves (400 ms debounce via saveAdjustment).

function ForecastTableWithAdjustments({
  activeForecasts, forecastDates, bestMethod, historicalData,
  isMultiMode, horizonLength, adjustments, adjSaving,
  saveAdjustment, resetAllAdjustments, locale, numberDecimals, isDark,
  dateRangeEnd, methodExplanation,
  // Multi-mode aggregated adjustment props
  multiSeriesData, saveMultiAdjustment, resetMultiAdjustments,
}) {
  const bestMethodName = bestMethod?.best_method;
  const [adjRowsOpen, setAdjRowsOpen] = React.useState(false);

  // Keep adj rows open automatically if there are any saved values
  const hasAnyAdj = isMultiMode
    ? Object.keys(multiSeriesData?.aggAdjustments || {}).length > 0
    : Object.keys(adjustments).length > 0;
  React.useEffect(() => {
    if (hasAnyAdj) setAdjRowsOpen(true);
  }, [hasAnyAdj]);

  // ---- Live draft state for instant Consensus updates ----
  // Keyed by dateStr, value is a raw string from the input (may be empty/"")
  const [draftAdj, setDraftAdj] = React.useState({});
  const [draftOv, setDraftOv] = React.useState({});

  // ---- Cell remark system ----
  const [remarkPopup, setRemarkPopup] = React.useState(null); // { dateStr, adjType, value, x, y, note }
  const [remarkDraft, setRemarkDraft] = React.useState('');
  const [contextMenu, setContextMenu] = React.useState(null); // { dateStr, adjType, value, x, y }
  const remarkRef = React.useRef(null);

  // Close remark popup on click outside
  React.useEffect(() => {
    if (!remarkPopup && !contextMenu) return;
    const handler = (e) => {
      if (remarkRef.current && !remarkRef.current.contains(e.target)) {
        setRemarkPopup(null);
        setContextMenu(null);
      }
    };
    const escHandler = (e) => { if (e.key === 'Escape') { setRemarkPopup(null); setContextMenu(null); } };
    document.addEventListener('mousedown', handler);
    document.addEventListener('keydown', escHandler);
    return () => { document.removeEventListener('mousedown', handler); document.removeEventListener('keydown', escHandler); };
  }, [remarkPopup, contextMenu]);

  const handleCellDoubleClick = (e, dateStr, adjType, currentValue) => {
    const adj = adjustments[`${dateStr}|${adjType}`];
    const rect = e.currentTarget.getBoundingClientRect();
    setRemarkDraft(adj?.note || '');
    setRemarkPopup({
      dateStr, adjType,
      value: currentValue,
      x: rect.left + rect.width / 2,
      y: rect.bottom + 4,
      note: adj?.note || '',
    });
    setContextMenu(null);
  };

  const handleCellContextMenu = (e, dateStr, adjType, currentValue) => {
    const adj = adjustments[`${dateStr}|${adjType}`];
    if (!adj?.note) return; // Only show context menu if there's a note to remove
    e.preventDefault();
    setContextMenu({
      dateStr, adjType, value: currentValue,
      x: e.clientX, y: e.clientY,
    });
    setRemarkPopup(null);
  };

  const saveRemark = () => {
    if (!remarkPopup) return;
    const { dateStr, adjType, value } = remarkPopup;
    const existing = adjustments[`${dateStr}|${adjType}`];
    // When no existing adjustment, use 0 for adjustment (no delta) or the forecast value for override
    const currentVal = existing ? existing.value : (adjType === 'override' ? value : 0);
    saveAdjustment(dateStr, adjType, String(currentVal), remarkDraft || null);
    setRemarkPopup(null);
  };

  const removeRemark = () => {
    if (!contextMenu) return;
    const { dateStr, adjType } = contextMenu;
    const existing = adjustments[`${dateStr}|${adjType}`];
    if (existing) {
      saveAdjustment(dateStr, adjType, String(existing.value), null);
    }
    setContextMenu(null);
  };

  // Sync drafts whenever the persisted adjustments change (load, save, reset)
  React.useEffect(() => {
    const source = isMultiMode ? (multiSeriesData?.aggAdjustments || {}) : adjustments;
    const adjMap = {};
    const ovMap  = {};
    Object.entries(source).forEach(([key, entry]) => {
      const [date, type] = key.split('|');
      if (type === 'adjustment') adjMap[date] = String(entry.value);
      if (type === 'override')   ovMap[date]  = String(entry.value);
    });
    setDraftAdj(adjMap);
    setDraftOv(ovMap);
  }, [adjustments, isMultiMode, multiSeriesData?.aggAdjustments]);

  // Resolve base date from historical tail, or fallback to dateRangeEnd
  const lastDate = historicalData?.date?.length
    ? new Date(historicalData.date[historicalData.date.length - 1])
    : (dateRangeEnd ? new Date(dateRangeEnd) : null);

  // The best-method (or first) forecast used as base for adjustments
  const bestFc = activeForecasts.find(f => f.method === bestMethodName) || activeForecasts[0];

  // Build per-month date strings (same logic as allData memo)
  const monthDates = React.useMemo(() => {
    if (!lastDate || !bestFc) return [];
    return bestFc.point_forecast.map((_, i) => {
      const d = new Date(lastDate);
      d.setUTCMonth(d.getUTCMonth() + i + 1);
      return d.toISOString().split('T')[0];
    });
  }, [lastDate?.toISOString(), bestFc?.method, bestFc?.point_forecast?.length]);

  // Compute consensus value for a given period index (uses live drafts)
  const consensusValue = React.useCallback((modelVal, dateStr) => {
    if (!dateStr) return modelVal;
    const ovRaw  = draftOv[dateStr];
    const adjRaw = draftAdj[dateStr];
    const ovNum  = ovRaw  !== undefined && ovRaw  !== '' ? parseFloat(ovRaw)  : null;
    const adjNum = adjRaw !== undefined && adjRaw !== '' ? parseFloat(adjRaw) : null;
    if (ovNum !== null && !isNaN(ovNum))  return ovNum;
    if (adjNum !== null && !isNaN(adjNum)) return (modelVal || 0) + adjNum;
    return modelVal;
  }, [draftAdj, draftOv]);

  // Is the consensus value different from the raw model for a given period?
  const consensusModified = React.useCallback((modelVal, dateStr) => {
    const cv = consensusValue(modelVal, dateStr);
    return cv !== modelVal;
  }, [consensusValue]);

  const adjCount = isMultiMode
    ? Object.keys(multiSeriesData?.aggAdjustments || {}).length
    : Object.keys(adjustments).length;
  const effectiveAdjustments = isMultiMode ? (multiSeriesData?.aggAdjustments || {}) : adjustments;
  const effectiveSave = isMultiMode ? saveMultiAdjustment : saveAdjustment;
  const effectiveReset = isMultiMode ? resetMultiAdjustments : resetAllAdjustments;

  // For multi-mode: build a single aggregated forecast row from best-method sums
  const aggFcRow = React.useMemo(() => {
    if (!isMultiMode || !multiSeriesData?.aggBestForecast) return null;
    return { method: 'Forecast', point_forecast: multiSeriesData.aggBestForecast, quantiles: {} };
  }, [isMultiMode, multiSeriesData?.aggBestForecast]);

  // Rows to render: single row in multi-mode, all methods in single-mode
  const displayForecasts = isMultiMode && aggFcRow ? [aggFcRow] : activeForecasts;

  return (
    <Section
      title={`Forecast Point Values${isMultiMode ? ` (aggregated sum \u2014 ${multiSeriesData?.uids?.length || 0} series)` : ''} (${horizonLength} months)`}
      storageKey="tsv_forecast_table_open"
    >
      {/* Reset button + adj count badge */}
      {adjCount > 0 && (
        <div className="flex items-center justify-end gap-3 mb-2">
          <span className="text-xs text-gray-400 dark:text-gray-500">{adjCount} adjustment{adjCount !== 1 ? 's' : ''} active{isMultiMode ? ` (across ${multiSeriesData?.uids?.length} series)` : ''}</span>
          <button
            onClick={effectiveReset}
            className="px-2.5 py-1 text-xs bg-red-50 dark:bg-red-900/20 text-red-700 dark:text-red-400 border border-red-200 dark:border-red-800 rounded hover:bg-red-100 dark:hover:bg-red-900/30"
          >
            ✕ Reset all adjustments
          </button>
        </div>
      )}

      <div className="overflow-x-auto max-h-[32rem]">
        <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-700 text-sm">
          <thead className="sticky top-0 z-10" style={{ backgroundColor: 'var(--color-surface-alt)' }}>
            <tr>
              <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase sticky left-0 z-20 min-w-[9rem]" style={{ backgroundColor: 'var(--color-surface-alt)', boxShadow: '2px 0 4px -1px rgba(0,0,0,0.1)' }}>
                {isMultiMode ? '' : 'Method'}
              </th>
              {forecastDates.map((d, i) => (
                <th key={i} className="px-2 py-2 text-right text-xs font-medium text-gray-500 dark:text-gray-400 uppercase whitespace-nowrap">
                  {d}
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-200 dark:divide-gray-700">
            {displayForecasts.map((f, idx) => {
              const isBest = isMultiMode ? true : f.method === bestMethodName;
              const methodNote = (methodExplanation?.included || []).find(m => m.method === f.method)?.backtest_note || '';
              const rowBg  = isBest ? 'bg-emerald-50 dark:bg-emerald-900/20' : '';
              const stickyBg = isBest
                ? (isDark ? '#064e3b33' : '#ecfdf5')
                : (isDark ? '#1f2937' : 'white');

              return (
                <React.Fragment key={f.method}>
                  {/* ── Forecast row ── */}
                  <tr className={rowBg}>
                    <td
                      className="px-3 py-2 font-medium whitespace-nowrap sticky left-0 z-10"
                      style={{ backgroundColor: stickyBg, boxShadow: '2px 0 4px -1px rgba(0,0,0,0.1)' }}
                    >
                      <div className="flex items-center gap-1.5">
                        <span
                          className="inline-block w-2.5 h-2.5 rounded-full flex-shrink-0"
                          style={{ backgroundColor: getMethodColor(f.method) }}
                        />
                        <span>{f.method}</span>
                        {/* Note indicator for methods with backtest notes */}
                        {methodNote && (
                          <span className="shrink-0 cursor-help" title={methodNote}>
                            <svg className="w-3.5 h-3.5 text-amber-500 dark:text-amber-400" viewBox="0 0 20 20" fill="currentColor">
                              <path fillRule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clipRule="evenodd" />
                            </svg>
                          </span>
                        )}
                        {/* Toggle adj rows — for the best/selected method, or always in multi-mode */}
                        {isBest && (
                          <button
                            onClick={() => setAdjRowsOpen(o => !o)}
                            title={adjRowsOpen ? 'Hide adjustment rows' : 'Show adjustment rows'}
                            className="ml-1 text-gray-400 hover:text-indigo-600 text-xs leading-none"
                          >
                            {adjRowsOpen ? '\u25B2' : '\u25BC'}
                          </button>
                        )}
                      </div>
                    </td>
                    {f.point_forecast.map((v, i) => {
                      // For the best method (or aggregated row), show the final (adjusted) value
                      if (isBest && monthDates[i]) {
                        const dateStr = monthDates[i];
                        const adj = effectiveAdjustments[`${dateStr}|adjustment`];
                        const ov  = effectiveAdjustments[`${dateStr}|override`];
                        const finalVal = ov
                          ? Number(ov.value)
                          : adj
                            ? v + Number(adj.value)
                            : v;
                        const noteText = adj?.note || ov?.note || '';
                        const saving = adjSaving[`${dateStr}|adjustment`] || adjSaving[`${dateStr}|override`];
                        return (
                          <td
                            key={i}
                            className={`px-2 py-2 text-right font-mono text-xs relative ${ov ? 'text-red-700 dark:text-red-400 font-semibold' : adj ? 'text-orange-700 dark:text-orange-400 font-semibold' : 'dark:text-gray-300'}`}
                            onDoubleClick={(e) => handleCellDoubleClick(e, dateStr, ov ? 'override' : 'adjustment', finalVal)}
                            onContextMenu={(e) => handleCellContextMenu(e, dateStr, ov ? 'override' : 'adjustment', finalVal)}
                          >
                            {saving && <span className="text-gray-300 dark:text-gray-600 mr-0.5 text-[10px]">{'\u27F3'}</span>}
                            {formatNumber(finalVal, locale, 0)}
                            {noteText && (
                              <span className="inline-block ml-0.5 align-top cursor-help" title={noteText}>
                                <svg className="inline w-3 h-3 text-indigo-400 dark:text-indigo-300" viewBox="0 0 20 20" fill="currentColor">
                                  <path fillRule="evenodd" d="M18 10c0 4.418-3.582 8-8 8s-8-3.582-8-8 3.582-8 8-8 8 3.582 8 8zm-4.293-3.707a1 1 0 010 1.414l-4 4a1 1 0 01-1.414 0l-2-2a1 1 0 111.414-1.414L9 9.586l3.293-3.293a1 1 0 011.414 0z" clipRule="evenodd" />
                                </svg>
                              </span>
                            )}
                          </td>
                        );
                      }
                      return (
                        <td key={i} className="px-2 py-2 text-right font-mono text-xs text-gray-600 dark:text-gray-400">
                          {formatNumber(v, locale, 0)}
                        </td>
                      );
                    })}
                  </tr>

                  {/* ── Adjustment rows (under best method or aggregated row, collapsible) ── */}
                  {isBest && adjRowsOpen && monthDates.length > 0 && (
                    <>
                      {/* Row 1: Adjustment (±) */}
                      <tr className="bg-orange-50/60 dark:bg-orange-900/10">
                        <td
                          className="px-3 py-1 text-xs font-medium text-orange-700 dark:text-orange-400 whitespace-nowrap sticky left-0 z-10 bg-orange-50 dark:bg-orange-900/20"
                          style={{ boxShadow: '2px 0 4px -1px rgba(0,0,0,0.1)' }}
                          title="Additive delta applied on top of model forecast"
                        >
                          <span className="flex items-center gap-1">
                            <span className="w-2 h-2 rounded-full bg-orange-400 inline-block flex-shrink-0" />
                            Adjustment (±)
                          </span>
                        </td>
                        {f.point_forecast.map((modelVal, i) => {
                          const dateStr = monthDates[i];
                          if (!dateStr) return <td key={i} />;
                          const adj = effectiveAdjustments[`${dateStr}|adjustment`];
                          return (
                            <td key={i} className="px-1 py-0.5">
                              <input
                                type="number"
                                step="1"
                                value={draftAdj[dateStr] ?? ''}
                                placeholder="±"
                                onChange={e => setDraftAdj(prev => ({ ...prev, [dateStr]: e.target.value }))}
                                onBlur={e => isMultiMode
                                  ? effectiveSave(dateStr, 'adjustment', e.target.value, adj?.note, i)
                                  : effectiveSave(dateStr, 'adjustment', e.target.value, adj?.note)}
                                className="w-full min-w-[3.5rem] text-right border border-orange-200 dark:border-orange-800 rounded px-1.5 py-0.5 text-xs font-mono focus:outline-none focus:ring-1 focus:ring-orange-400 bg-white dark:bg-gray-800 dark:text-gray-200"
                              />
                            </td>
                          );
                        })}
                      </tr>

                      {/* Row 2: Override */}
                      <tr className="bg-red-50/60 dark:bg-red-900/10">
                        <td
                          className="px-3 py-1 text-xs font-medium text-red-700 dark:text-red-400 whitespace-nowrap sticky left-0 z-10 bg-red-50 dark:bg-red-900/20"
                          style={{ boxShadow: '2px 0 4px -1px rgba(0,0,0,0.1)' }}
                          title="Fully replaces the model forecast for this period"
                        >
                          <span className="flex items-center gap-1">
                            <span className="w-2 h-2 bg-red-500 inline-block flex-shrink-0 rounded-sm" />
                            Override
                          </span>
                        </td>
                        {f.point_forecast.map((_, i) => {
                          const dateStr = monthDates[i];
                          if (!dateStr) return <td key={i} />;
                          const ov = effectiveAdjustments[`${dateStr}|override`];
                          return (
                            <td key={i} className="px-1 py-0.5">
                              <input
                                type="number"
                                step="1"
                                value={draftOv[dateStr] ?? ''}
                                placeholder="—"
                                onChange={e => setDraftOv(prev => ({ ...prev, [dateStr]: e.target.value }))}
                                onBlur={e => isMultiMode
                                  ? effectiveSave(dateStr, 'override', e.target.value, ov?.note, i)
                                  : effectiveSave(dateStr, 'override', e.target.value, ov?.note)}
                                className="w-full min-w-[3.5rem] text-right border border-red-200 dark:border-red-800 rounded px-1.5 py-0.5 text-xs font-mono focus:outline-none focus:ring-1 focus:ring-red-400 bg-white dark:bg-gray-800 dark:text-gray-200"
                              />
                            </td>
                          );
                        })}
                      </tr>
                    </>
                  )}
                </React.Fragment>
              );
            })}

            {/* ── Consensus row — visible when adjustments exist ── */}
            {(isMultiMode ? aggFcRow : bestFc) && monthDates.length > 0 && adjCount > 0 && (
              <tr className="border-t-2 border-indigo-200 dark:border-indigo-800 bg-indigo-50/70 dark:bg-indigo-900/20">
                <td
                  className="px-3 py-2 text-xs font-semibold text-indigo-800 dark:text-indigo-300 whitespace-nowrap sticky left-0 z-10"
                  style={{ backgroundColor: isDark ? 'rgba(49,46,129,0.2)' : '#eef2ff', boxShadow: '2px 0 4px -1px rgba(0,0,0,0.1)' }}
                  title="Model forecast with adjustments and overrides applied"
                >
                  <span className="flex items-center gap-1.5">
                    <span className="w-2.5 h-2.5 rounded-full bg-indigo-500 inline-block flex-shrink-0" />
                    Consensus
                  </span>
                </td>
                {(isMultiMode ? aggFcRow : bestFc).point_forecast.map((modelVal, i) => {
                  const dateStr = monthDates[i];
                  if (!dateStr) return <td key={i} />;
                  const cv = consensusValue(modelVal, dateStr);
                  const modified = cv !== modelVal;
                  const isOv  = draftOv[dateStr]  !== undefined && draftOv[dateStr]  !== '';
                  const isAdj = !isOv && draftAdj[dateStr] !== undefined && draftAdj[dateStr] !== '';
                  return (
                    <td
                      key={i}
                      className={`px-2 py-2 text-right font-mono text-xs font-semibold
                        ${isOv  ? 'text-red-700 dark:text-red-400'    : ''}
                        ${isAdj ? 'text-orange-700 dark:text-orange-400'  : ''}
                        ${!modified ? 'text-indigo-700 dark:text-indigo-300' : ''}
                      `}
                    >
                      {cv != null ? formatNumber(cv, locale, 0) : '\u2014'}
                    </td>
                  );
                })}
              </tr>
            )}
          </tbody>
        </table>
      </div>

      {/* Legend */}
      {(
        <div className="flex items-center gap-4 mt-2 text-xs text-gray-400 dark:text-gray-500 flex-wrap">
          <span className="flex items-center gap-1">
            <span className="inline-block w-2.5 h-2.5 rounded-full bg-indigo-500" />
            Consensus: final value (model + adjustments/overrides)
          </span>
          {adjRowsOpen && (
            <>
              <span className="flex items-center gap-1">
                <span className="inline-block w-2 h-2 rounded-full bg-orange-400" />
                Adjustment: additive ± delta (value shown in orange)
              </span>
              <span className="flex items-center gap-1">
                <span className="inline-block w-2 h-2 bg-red-500 rounded-sm" />
                Override: replaces model entirely (value shown in red)
              </span>
              <span className="text-gray-300 dark:text-gray-600">{'\u00B7'} leave blank to clear</span>
              <span className="text-gray-300 dark:text-gray-600">{'\u00B7'} double-click cell to add remark</span>
              <span className="flex items-center gap-1">
                <svg className="w-3 h-3 text-indigo-400" viewBox="0 0 20 20" fill="currentColor"><path fillRule="evenodd" d="M18 10c0 4.418-3.582 8-8 8s-8-3.582-8-8 3.582-8 8-8 8 3.582 8 8zm-4.293-3.707a1 1 0 010 1.414l-4 4a1 1 0 01-1.414 0l-2-2a1 1 0 111.414-1.414L9 9.586l3.293-3.293a1 1 0 011.414 0z" clipRule="evenodd" /></svg>
                <span className="text-gray-300 dark:text-gray-600">has remark (hover to read)</span>
              </span>
            </>
          )}
        </div>
      )}

      {/* ── Remark Popup (double-click) ── */}
      {remarkPopup && (
        <div
          ref={remarkRef}
          className="fixed z-[100] bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-600 rounded-lg shadow-xl p-3 w-64"
          style={{ left: Math.min(remarkPopup.x - 128, window.innerWidth - 280), top: remarkPopup.y }}
        >
          <div className="text-xs font-semibold text-gray-500 dark:text-gray-400 mb-1.5">
            Remark for {formatDate(remarkPopup.dateStr, locale)}
          </div>
          <textarea
            autoFocus
            value={remarkDraft}
            onChange={e => setRemarkDraft(e.target.value)}
            placeholder="Type a remark or note..."
            rows={3}
            maxLength={200}
            className="w-full border border-gray-200 dark:border-gray-600 dark:bg-gray-700 dark:text-white rounded px-2 py-1.5 text-xs focus:outline-none focus:ring-1 focus:ring-indigo-400 resize-none"
          />
          <div className="flex items-center justify-between mt-2">
            <span className="text-[10px] text-gray-400">{remarkDraft.length}/200</span>
            <div className="flex gap-1.5">
              <button
                onClick={() => setRemarkPopup(null)}
                className="px-2 py-1 text-xs rounded border border-gray-200 dark:border-gray-600 text-gray-500 dark:text-gray-400 hover:bg-gray-50 dark:hover:bg-gray-700"
              >Cancel</button>
              <button
                onClick={saveRemark}
                className="px-2 py-1 text-xs rounded bg-indigo-600 text-white hover:bg-indigo-700"
              >Save</button>
            </div>
          </div>
        </div>
      )}

      {/* ── Context Menu (right-click to remove remark) ── */}
      {contextMenu && (
        <div
          ref={remarkRef}
          className="fixed z-[100] bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-600 rounded-lg shadow-xl py-1 min-w-[140px]"
          style={{ left: contextMenu.x, top: contextMenu.y }}
        >
          <button
            onClick={removeRemark}
            className="w-full px-3 py-1.5 text-left text-xs text-red-600 dark:text-red-400 hover:bg-red-50 dark:hover:bg-red-900/20 flex items-center gap-2"
          >
            <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" />
            </svg>
            Remove remark
          </button>
        </div>
      )}
    </Section>
  );
}


/* ---------- Dual-range zoom slider (fully custom — no overlapping native inputs) ----------
 * Uses mouse/touch pointer-tracking on a single track div so Chrome can never
 * confuse the two handles.  Each thumb fires its own independent mousedown/touchstart
 * that attaches temporary document-level move+up listeners for the duration of the drag.
 * ---------- */
const ZoomSlider = ({ dates, start, end, onStartChange, onEndChange }) => {
  const clampedEnd   = Math.min(end,   dates.length - 1);
  const clampedStart = Math.min(start, clampedEnd);

  // Keep current boundary values available inside closure-captured event handlers
  // without causing stale reads.
  const startRef = useRef(clampedStart);
  const endRef   = useRef(clampedEnd);
  startRef.current = clampedStart;
  endRef.current   = clampedEnd;

  const trackRef = useRef(null);

  if (dates.length <= 1) return null;

  // ── Helpers ──────────────────────────────────────────────────────────
  const pct = idx => (idx / (dates.length - 1)) * 100;

  const idxFromClient = clientX => {
    if (!trackRef.current) return 0;
    const rect  = trackRef.current.getBoundingClientRect();
    const ratio = Math.max(0, Math.min(1, (clientX - rect.left) / rect.width));
    return Math.round(ratio * (dates.length - 1));
  };

  const findDateIdx = dateStr => {
    if (!dateStr) return -1;
    const idx = dates.findIndex(d => d >= dateStr);
    return idx >= 0 ? idx : dates.length - 1;
  };

  // ── Drag factory — one function per handle ────────────────────────────
  const makeDragHandlers = handle => {
    const onMove = ev => {
      const clientX = ev.touches ? ev.touches[0].clientX : ev.clientX;
      const newIdx  = idxFromClient(clientX);
      if (handle === 'start') {
        if (newIdx < endRef.current) onStartChange(newIdx);
      } else {
        if (newIdx > startRef.current) onEndChange(newIdx);
      }
    };
    const onUp = () => {
      document.removeEventListener('mousemove', onMove);
      document.removeEventListener('mouseup',   onUp);
      document.removeEventListener('touchmove', onMove);
      document.removeEventListener('touchend',  onUp);
    };
    return ev => {
      ev.preventDefault();
      ev.stopPropagation();
      document.addEventListener('mousemove', onMove);
      document.addEventListener('mouseup',   onUp);
      document.addEventListener('touchmove', onMove, { passive: false });
      document.addEventListener('touchend',  onUp);
    };
  };

  const THUMB = 'absolute top-1/2 -translate-y-1/2 w-5 h-5 rounded-full ' +
    'bg-blue-500 border-2 border-white shadow-md cursor-grab active:cursor-grabbing ' +
    'touch-none select-none -translate-x-1/2 hover:bg-blue-600 active:bg-blue-700 ' +
    'transition-colors z-10';

  return (
    <div className="mt-4 pt-4 border-t border-gray-100 dark:border-gray-700">
      <div className="flex items-center gap-2 sm:gap-3 flex-wrap">
        <span className="text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wide">Zoom</span>

        {/* Left date text input */}
        <input
          type="date"
          value={dates[clampedStart]?.slice(0, 10) || ''}
          min={dates[0]?.slice(0, 10)}
          max={dates[clampedEnd]?.slice(0, 10)}
          onChange={e => { const idx = findDateIdx(e.target.value); if (idx >= 0 && idx < clampedEnd) onStartChange(idx); }}
          className="text-xs font-mono bg-gray-100 dark:bg-gray-700 dark:text-gray-300 px-2 py-0.5 rounded border border-gray-200 dark:border-gray-600 w-[8.5rem]"
        />

        {/* Track + thumbs */}
        <div ref={trackRef}
             className="relative flex-1 min-w-32 h-8 select-none">
          {/* Background rail */}
          <div className="absolute top-1/2 left-0 right-0 h-1.5 -translate-y-1/2
                          bg-gray-200 dark:bg-gray-600 rounded-full" />
          {/* Active range highlight */}
          <div className="absolute top-1/2 h-1.5 -translate-y-1/2 bg-blue-500 rounded-full pointer-events-none"
               style={{ left: `${pct(clampedStart)}%`, right: `${100 - pct(clampedEnd)}%` }} />
          {/* Start thumb */}
          <div className={THUMB}
               style={{ left: `${pct(clampedStart)}%` }}
               onMouseDown={makeDragHandlers('start')}
               onTouchStart={makeDragHandlers('start')} />
          {/* End thumb */}
          <div className={THUMB}
               style={{ left: `${pct(clampedEnd)}%` }}
               onMouseDown={makeDragHandlers('end')}
               onTouchStart={makeDragHandlers('end')} />
        </div>

        {/* Right date text input */}
        <input
          type="date"
          value={dates[clampedEnd]?.slice(0, 10) || ''}
          min={dates[clampedStart]?.slice(0, 10)}
          max={dates[dates.length - 1]?.slice(0, 10)}
          onChange={e => { const idx = findDateIdx(e.target.value); if (idx >= 0 && idx > clampedStart) onEndChange(idx); }}
          className="text-xs font-mono bg-gray-100 dark:bg-gray-700 dark:text-gray-300 px-2 py-0.5 rounded border border-gray-200 dark:border-gray-600 w-[8.5rem]"
        />
        <button
          onClick={() => { onStartChange(0); onEndChange(dates.length - 1); }}
          className="text-xs bg-gray-200 dark:bg-gray-600 hover:bg-gray-300 dark:hover:bg-gray-500 dark:text-gray-200 px-2 py-1 rounded transition-colors"
        >
          Reset
        </button>
      </div>
    </div>
  );
};

export const TimeSeriesViewer = () => {
  const { uniqueId } = useParams();
  const decodedId = decodeURIComponent(uniqueId);
  const navigate = useNavigate();
  const { locale, numberDecimals } = useLocale();
  const { isDark } = useTheme();

  // ---- Item/Site dropdown state (multi-select: arrays) ----
  const [allSeriesList, setAllSeriesList] = useState([]);
  const [selectedItems, setSelectedItems] = useState([]); // array of item strings
  const [selectedSites, setSelectedSites] = useState([]); // array of site strings
  const [recentItems, setRecentItems] = useState([]);
  const [recentSites, setRecentSites] = useState([]);
  // Multi-series aggregated data (when more than 1 series selected)
  const [multiSeriesData, setMultiSeriesData] = useState(null); // null = use single-series mode
  const [multiLoading, setMultiLoading] = useState(false);

  // ---- Segment filter ----
  const [segments, setSegments] = useState([]);
  const [selectedSegmentId, setSelectedSegmentId] = useState(null);
  const [segmentMemberSet, setSegmentMemberSet] = useState(null); // null = no filter (All)
  const [segmentLoading, setSegmentLoading] = useState(false);

  // ---- Time series data ----
  const [historicalData, setHistoricalData] = useState(null);
  const [originalData, setOriginalData] = useState(null);
  const [outlierInfo, setOutlierInfo] = useState(null);
  const [hasOutlierCorrections, setHasOutlierCorrections] = useState(false);
  const [dateRangeEnd, setDateRangeEnd] = useState(null); // fallback for forecast dates when historical data missing
  const [nOutliers, setNOutliers] = useState(0);
  const [forecasts, setForecasts] = useState([]);
  const [characteristics, setCharacteristics] = useState(null);
  const [metrics, setMetrics] = useState([]);
  const [compositeRanking, setCompositeRanking] = useState(null);
  const [compositeWeights, setCompositeWeights] = useState(null);
  const [bestMethod, setBestMethod] = useState(null);
  const [btConfig, setBtConfig] = useState({ backtest_horizon: 60, window_size: 8, n_tests: 4 });
  const [methodExplanation, setMethodExplanation] = useState(null);
  const [distributions, setDistributions] = useState(null);

  // ---- Series parameters (applied parameter versions) ----
  const [seriesParameters, setSeriesParameters] = useState(null);

  // ---- Series browse table ----
  const [seriesTableOpen, setSeriesTableOpen] = useState(false);
  const [seriesTableSort, setSeriesTableSort] = useState({ col: '_item', dir: 'asc' });
  const [seriesTableSearch, setSeriesTableSearch] = useState('');
  const [seriesTablePage, setSeriesTablePage] = useState(0);
  const SERIES_TABLE_PAGE_SIZE = 50;

  // ---- Metrics table sorting ----
  const [metricsSortField, setMetricsSortField] = useState('mae');
  const [metricsSortDir, setMetricsSortDir] = useState('asc');

  // ---- Forecast origin slider ----
  const [origins, setOrigins] = useState([]);
  const [selectedOriginIdx, setSelectedOriginIdx] = useState(0);
  const [originForecasts, setOriginForecasts] = useState(null);
  const [selectedPeriod, setSelectedPeriod] = useState(1);
  const [isPlaying, setIsPlaying] = useState(false);
  const playTimerRef = useRef(null);

  // ---- Forecast convergence ----
  const [convergenceData, setConvergenceData] = useState(null);
  const [convergenceMethod, setConvergenceMethod] = useState(''); // '' = best method
  const [convergenceView, setConvergenceView] = useState('convergence'); // 'convergence' | 'racing'

  // ---- Method visibility ----
  const [visibleMethods, setVisibleMethods] = useState({});
  const [bandVisibleMethods, setBandVisibleMethods] = useState({});

  // ---- Date-range zoom ----
  const [zoomStart, setZoomStart] = useState(0);
  const [zoomEnd, setZoomEnd] = useState(99999);   // large sentinel → clamped to last date by slider max
  const [outlierZoomStart, setOutlierZoomStart] = useState(0);
  const [outlierZoomEnd, setOutlierZoomEnd] = useState(99999);

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  // ---- Planner adjustments ----
  // key: "YYYY-MM-DD|type" → {id, forecast_date, adjustment_type, value, note}
  const [adjustments, setAdjustments] = useState({});
  const [adjSaving, setAdjSaving] = useState({}); // key → true while saving
  const adjDebounceRef = useRef({});             // key → timeout id

  // ---- Run Forecast button state ----
  const [forecastJobId, setForecastJobId] = useState(null);
  const [forecastJobStatus, setForecastJobStatus] = useState(null); // null|pending|running|success|error
  const [forecastProgress, setForecastProgress] = useState(null);   // {current_step, completed, total, pct, ...}
  const [forecastStartedAt, setForecastStartedAt] = useState(null); // ISO timestamp of job start
  const forecastPollRef = useRef(null);

  // ---- Hyperparameter overrides (editable params) ----
  const [hpEdits, setHpEdits] = useState({});           // {method: {param: newValue}}
  const [hpSaving, setHpSaving] = useState(false);
  const [hpSavedOverrides, setHpSavedOverrides] = useState({}); // from DB: {method: {param: val}}
  const [openListPopup, setOpenListPopup] = useState(null); // "method:key" for which list popup is open

  // ---- Section drag-and-drop order ----
  const { order: sectionOrder, reorder: reorderSections } = useSectionOrder();
  const [draggingId, setDraggingId] = useState(null);
  const [dragOverId, setDragOverId] = useState(null);

  // ---- Display aggregation granularity ----
  const [displayAgg, setDisplayAgg] = useState('native');

  // ---- Period date-range filter ----
  const [periodStart, setPeriodStart] = useState(null); // null = no filter (all)
  const [periodEnd, setPeriodEnd] = useState(null);     // null = no filter (all)

  const handleDragStart = useCallback((id) => setDraggingId(id), []);
  const handleDragOver  = useCallback((id) => setDragOverId(id), []);
  const handleDrop      = useCallback((overId) => {
    if (draggingId && overId && draggingId !== overId) reorderSections(draggingId, overId);
    setDraggingId(null);
    setDragOverId(null);
  }, [draggingId, reorderSections]);
  const handleDragEnd   = useCallback(() => { setDraggingId(null); setDragOverId(null); }, []);

  // ---- Load all series list for dropdowns (once) ----
  useEffect(() => {
    api.get(`/series`, { params: { limit: 50000 } })
      .then(res => setAllSeriesList(res.data || []))
      .catch(() => {});
  }, []);

  // ---- Load segments (once) + auto-select the default ----
  useEffect(() => {
    api.get(`/segments`)
      .then(res => {
        const segs = res.data || [];
        setSegments(segs);
        const def = segs.find(s => s.is_default) || segs[0];
        if (def) setSelectedSegmentId(def.id);
      })
      .catch(() => {});
  }, []);

  // ---- Load segment members whenever selection changes ----
  useEffect(() => {
    if (!selectedSegmentId || segments.length === 0) return;
    const seg = segments.find(s => s.id === selectedSegmentId);
    if (!seg) return;
    if (seg.is_default) {
      // "All" segment — no filtering needed
      setSegmentMemberSet(null);
      return;
    }
    setSegmentLoading(true);
    api.get(`/segments/${selectedSegmentId}/members`, { params: { limit: 200000 } })
      .then(res => {
        const members = res.data.members || [];
        setSegmentMemberSet(new Set(members));
      })
      .catch(() => setSegmentMemberSet(null))
      .finally(() => setSegmentLoading(false));
  }, [selectedSegmentId, segments]);

  // ---- Parse current uniqueId into item/site on mount ----
  useEffect(() => {
    const { item, site } = parseUniqueId(decodedId);
    setSelectedItems([item]);
    setSelectedSites([site]);

    // Update localStorage recents
    setRecent('recent_items', item);
    setRecent('recent_sites', site);
    localStorage.setItem('last_series', decodedId);

    // Refresh recent state
    setRecentItems(getRecent('recent_items'));
    setRecentSites(getRecent('recent_sites'));
  }, [decodedId]);

  // ---- Navigate to single series when exactly 1 item + 1 site ----
  // For multi-select: load aggregated data instead of navigating
  const handleItemsChange = (items) => {
    setSelectedItems(items);
    setMultiSeriesData(null);
    // When clearing items (empty = "all"), keep sites as-is to allow "all items × selected sites"
    // When selecting multiple items, auto-select all their available sites
    if (items.length > 1) {
      const sites = filteredSeriesList
        .filter(s => items.includes(parseUniqueId(s.unique_id).item))
        .map(s => parseUniqueId(s.unique_id).site);
      const uniqueSites = [...new Set(sites)].sort((a, b) => a.localeCompare(b, undefined, { numeric: true }));
      setSelectedSites(uniqueSites);
    } else if (items.length === 1) {
      setSelectedSites([]); // reset sites when single item selected
    }
    // items.length === 0 → keep current sites (empty = "all items")
  };

  const handleSitesChange = (sites) => {
    setSelectedSites(sites);
    setMultiSeriesData(null);
    // Navigate to single series only when exactly 1 item × 1 site
    if (selectedItems.length === 1 && sites.length === 1) {
      const newId = `${selectedItems[0]}_${sites[0]}`;
      if (newId !== decodedId) navigate(`/series/${encodeURIComponent(newId)}`);
    }
    // sites.length === 0 → "all sites" → will trigger multi-series aggregation
  };

  const handleSegmentChange = (segId) => {
    setSelectedSegmentId(segId);
    setSelectedItems([]);
    setSelectedSites([]);
    setMultiSeriesData(null);
  };

  // ---- Derived dropdown options ----
  // filteredSeriesList: scoped to the active segment (null segmentMemberSet = All)
  const filteredSeriesList = useMemo(() => {
    if (!segmentMemberSet) return allSeriesList;
    return allSeriesList.filter(s => segmentMemberSet.has(s.unique_id));
  }, [allSeriesList, segmentMemberSet]);

  // ---- Name lookup maps: item_id → item_name, site_id → site_name ----
  const itemNameMap = useMemo(() => {
    const map = {};
    filteredSeriesList.forEach(s => {
      const { item } = parseUniqueId(s.unique_id);
      if (!(item in map) && s.item_name) map[item] = s.item_name;
    });
    return map;
  }, [filteredSeriesList]);

  const siteNameMap = useMemo(() => {
    const map = {};
    filteredSeriesList.forEach(s => {
      const { site } = parseUniqueId(s.unique_id);
      if (!(site in map) && s.site_name) map[site] = s.site_name;
    });
    return map;
  }, [filteredSeriesList]);

  const allItems = useMemo(() => {
    const items = [...new Set(filteredSeriesList.map(s => parseUniqueId(s.unique_id).item))];
    return items.sort((a, b) => {
      const na = itemNameMap[a] || a, nb = itemNameMap[b] || b;
      return na.localeCompare(nb, undefined, { numeric: true });
    });
  }, [filteredSeriesList, itemNameMap]);

  const availableSites = useMemo(() => {
    // When no items selected → show all sites in segment (empty = "all items")
    const source = selectedItems.length === 0
      ? filteredSeriesList
      : filteredSeriesList.filter(s => selectedItems.includes(parseUniqueId(s.unique_id).item));
    const sites = source.map(s => parseUniqueId(s.unique_id).site);
    return [...new Set(sites)].sort((a, b) => {
      const na = siteNameMap[a] || a, nb = siteNameMap[b] || b;
      return na.localeCompare(nb, undefined, { numeric: true });
    });
  }, [filteredSeriesList, selectedItems, siteNameMap]);

  // ---- Series browse table: filtered + sorted rows ----
  const seriesTableRows = useMemo(() => {
    let rows = filteredSeriesList;
    if (seriesTableSearch) {
      const q = seriesTableSearch.toLowerCase();
      rows = rows.filter(s =>
        s.unique_id.toLowerCase().includes(q) ||
        (s.item_name && s.item_name.toLowerCase().includes(q)) ||
        (s.site_name && s.site_name.toLowerCase().includes(q))
      );
    }
    rows = [...rows].sort((a, b) => {
      let va, vb;
      if (seriesTableSort.col === '_item') {
        va = a.item_name ?? parseUniqueId(a.unique_id).item;
        vb = b.item_name ?? parseUniqueId(b.unique_id).item;
      } else if (seriesTableSort.col === '_site') {
        va = a.site_name ?? parseUniqueId(a.unique_id).site;
        vb = b.site_name ?? parseUniqueId(b.unique_id).site;
      } else {
        va = a[seriesTableSort.col] ?? ''; vb = b[seriesTableSort.col] ?? '';
      }
      const cmp = String(va).localeCompare(String(vb), undefined, { numeric: true });
      return seriesTableSort.dir === 'asc' ? cmp : -cmp;
    });
    return rows;
  }, [filteredSeriesList, seriesTableSearch, seriesTableSort]);

  // ---- Trigger multi-series load when selection changes ----
  useEffect(() => {
    // Empty items = all items, empty sites = all sites (for current segment)
    const effectiveItems = selectedItems.length > 0 ? selectedItems : allItems;
    const effectiveSites = selectedSites.length > 0 ? selectedSites : availableSites;
    const selectedUids = [];
    effectiveItems.forEach(item => {
      effectiveSites.forEach(site => {
        // Only include UIDs that actually exist in filteredSeriesList
        const uid = `${item}_${site}`;
        if (filteredSeriesList.some(s => s.unique_id === uid)) selectedUids.push(uid);
      });
    });
    if (selectedUids.length <= 1) {
      setMultiSeriesData(null);
      return;
    }
    // Multi-series: fetch all and aggregate
    setMultiLoading(true);
    Promise.allSettled(selectedUids.map(uid =>
      Promise.allSettled([
        api.get(`/series/${encodeURIComponent(uid)}/data`),
        api.get(`/forecasts/${encodeURIComponent(uid)}`),
        api.get(`/metrics/${encodeURIComponent(uid)}`),
        api.get(`/series/${encodeURIComponent(uid)}/best-method`),
        api.get(`/adjustments/${encodeURIComponent(uid)}`),
      ])
    )).then(results => {
      // Aggregate: demand = sum, forecast = sum, metrics = weighted avg (by n_windows)
      const allHistorical = {}; // date -> sum
      const allForecasts = {};  // method -> [sum per horizon]
      const allMetrics = {};    // method -> {sum of metric*w, totalW, n}
      // Per-uid best-method forecast for disaggregation weights
      const perUidForecasts = {}; // uid -> { bestMethod, point_forecast[] }
      // Per-uid existing adjustments
      const perUidAdjustments = {}; // uid -> { "date|type" -> { value, ... } }

      results.forEach((r, uidIdx) => {
        if (r.status !== 'fulfilled') return;
        const uid = selectedUids[uidIdx];
        const [dataRes, fcRes, metricsRes, bestRes, adjRes] = r.value;

        // Historical sum
        if (dataRes.status === 'fulfilled') {
          const d = dataRes.value.data.data;
          (d.date || []).forEach((date, i) => {
            allHistorical[date] = (allHistorical[date] || 0) + (d.value[i] || 0);
          });
        }

        // Best method for this series
        const seriesBest = bestRes?.status === 'fulfilled' ? bestRes.value.data?.best_method : null;

        // Forecast sum + per-uid storage
        if (fcRes.status === 'fulfilled') {
          const fcasts = fcRes.value.data.forecasts || [];
          fcasts.forEach(f => {
            if (!allForecasts[f.method]) {
              allForecasts[f.method] = { point: new Array(f.point_forecast.length).fill(0), count: 0 };
            }
            f.point_forecast.forEach((v, i) => {
              if (allForecasts[f.method].point[i] !== undefined) allForecasts[f.method].point[i] += v || 0;
            });
            allForecasts[f.method].count++;
          });
          // Store the best-method forecast for this series (for disaggregation)
          const bestFc = fcasts.find(f => f.method === seriesBest) || fcasts[0];
          if (bestFc) {
            perUidForecasts[uid] = {
              bestMethod: bestFc.method,
              point_forecast: bestFc.point_forecast,
            };
          }
        }

        // Existing adjustments for this series
        if (adjRes?.status === 'fulfilled') {
          const map = {};
          (adjRes.value.data || []).forEach(a => {
            map[`${a.forecast_date}|${a.adjustment_type}`] = a;
          });
          perUidAdjustments[uid] = map;
        } else {
          perUidAdjustments[uid] = {};
        }

        // Metrics weighted average
        if (metricsRes.status === 'fulfilled') {
          const mlist = metricsRes.value.data.metrics || [];
          mlist.forEach(m => {
            const w = m.n_windows || 1;
            if (!allMetrics[m.method]) allMetrics[m.method] = { totalW: 0, n: 0, sums: {} };
            allMetrics[m.method].totalW += w;
            allMetrics[m.method].n++;
            ['mae', 'rmse', 'bias', 'mape', 'smape', 'mase', 'crps', 'winkler_score',
             'coverage_50', 'coverage_80', 'coverage_90', 'coverage_95', 'quantile_loss'].forEach(k => {
              if (m[k] != null) {
                allMetrics[m.method].sums[k] = (allMetrics[m.method].sums[k] || 0) + m[k] * w;
              }
            });
          });
        }
      });

      // Build aggregated data structures
      const sortedDates = Object.keys(allHistorical).sort();
      const aggregatedHistorical = { date: sortedDates, value: sortedDates.map(d => allHistorical[d]) };

      const aggregatedForecasts = Object.entries(allForecasts).map(([method, d]) => ({
        method,
        point_forecast: d.point,
        quantiles: {},
      }));

      const aggregatedMetrics = Object.entries(allMetrics).map(([method, d]) => {
        const entry = { method, n_windows: d.n };
        Object.entries(d.sums).forEach(([k, s]) => { entry[k] = d.totalW > 0 ? s / d.totalW : null; });
        return entry;
      });

      // Aggregate the per-uid best-method forecasts into a single "Forecast" row
      const uidList = Object.keys(perUidForecasts);
      let aggBestForecast = null;
      if (uidList.length > 0) {
        const len = perUidForecasts[uidList[0]].point_forecast.length;
        const summed = new Array(len).fill(0);
        uidList.forEach(uid => {
          perUidForecasts[uid].point_forecast.forEach((v, i) => { summed[i] += v || 0; });
        });
        aggBestForecast = summed;
      }

      // Aggregate existing adjustments from all series
      const aggAdjustments = {};
      Object.values(perUidAdjustments).forEach(adjMap => {
        Object.entries(adjMap).forEach(([key, entry]) => {
          if (!aggAdjustments[key]) aggAdjustments[key] = { ...entry, value: 0 };
          aggAdjustments[key].value += Number(entry.value) || 0;
        });
      });

      setMultiSeriesData({
        historical: aggregatedHistorical,
        forecasts: aggregatedForecasts,
        metrics: aggregatedMetrics,
        uids: selectedUids,
        perUidForecasts,     // for disaggregation weights
        perUidAdjustments,   // existing per-series adjustments
        aggBestForecast,     // single summed best-method forecast array
        aggAdjustments,      // aggregated adjustments across all series
      });
      setMultiLoading(false);
    });
  }, [selectedItems, selectedSites, allItems, availableSites, filteredSeriesList]);

  // Derive single item/site for single-series mode (backward compat)
  const selectedItem = selectedItems[0] || '';
  const selectedSite = selectedSites[0] || '';

  // All unique_ids from the current item × site selection (empty = "all")
  const forecastUids = useMemo(() => {
    if (multiSeriesData?.uids) return multiSeriesData.uids;
    const effItems = selectedItems.length > 0 ? selectedItems : allItems;
    const effSites = selectedSites.length > 0 ? selectedSites : availableSites;
    const uids = [];
    effItems.forEach(item => {
      effSites.forEach(site => {
        const uid = `${item}_${site}`;
        if (filteredSeriesList.some(s => s.unique_id === uid)) uids.push(uid);
      });
    });
    return uids;
  }, [selectedItems, selectedSites, allItems, availableSites, filteredSeriesList, multiSeriesData]);

  /* ---------- data loading ---------- */
  useEffect(() => {
    // Reset zoom to full range whenever the active series changes
    setZoomStart(0);
    setZoomEnd(99999);
    loadData();
    return () => { if (playTimerRef.current) clearInterval(playTimerRef.current); };
  }, [decodedId]); // eslint-disable-line react-hooks/exhaustive-deps

  const loadData = async () => {
    setLoading(true);
    setError(null);
    try {
      const [dataRes, forecastRes, seriesRes, metricsRes, bestRes, originsRes, outlierRes, explainRes, distRes] = await Promise.allSettled([
        api.get(`/series/${encodeURIComponent(decodedId)}/data`),
        api.get(`/forecasts/${encodeURIComponent(decodedId)}`),
        api.get(`/series`, { params: { search: decodedId, limit: 1 } }),
        api.get(`/metrics/${encodeURIComponent(decodedId)}`),
        api.get(`/series/${encodeURIComponent(decodedId)}/best-method`),
        api.get(`/forecasts/${encodeURIComponent(decodedId)}/origins`),
        api.get(`/series/${encodeURIComponent(decodedId)}/outliers`),
        api.get(`/series/${encodeURIComponent(decodedId)}/method-explanation`),
        api.get(`/series/${encodeURIComponent(decodedId)}/distributions`)
      ]);

      if (dataRes.status === 'fulfilled') {
        const d = dataRes.value.data;
        setHistoricalData(d.data);
        if (d.original_data) setOriginalData(d.original_data);
        setHasOutlierCorrections(d.has_outlier_corrections || false);
        setNOutliers(d.n_outliers || 0);
      }
      if (outlierRes.status === 'fulfilled') setOutlierInfo(outlierRes.value.data);
      if (forecastRes.status === 'fulfilled') {
        const fData = forecastRes.value.data;
        const fcasts = fData.forecasts || [];
        setForecasts(fcasts);
        // Store date_range_end as fallback for computing forecast dates
        if (fData.date_range_end) setDateRangeEnd(fData.date_range_end);
        // If /series/{uid}/data failed, use inline historical from forecast response
        if (dataRes.status !== 'fulfilled' && fData.historical) {
          setHistoricalData(fData.historical);
        }
        const vis = {};
        fcasts.forEach(f => { vis[f.method] = true; });
        setVisibleMethods(prev => {
          // Keep existing visibility preferences, add new methods as visible
          const merged = { ...vis };
          Object.entries(prev).forEach(([k, v]) => { if (k in merged) merged[k] = v; });
          return merged;
        });
      }
      if (seriesRes.status === 'fulfilled' && seriesRes.value.data.length > 0)
        setCharacteristics(seriesRes.value.data[0]);
      if (metricsRes.status === 'fulfilled') {
        setMetrics(metricsRes.value.data.metrics || []);
        setCompositeRanking(metricsRes.value.data.composite_ranking || null);
        setCompositeWeights(metricsRes.value.data.composite_weights || null);
        if (metricsRes.value.data.backtesting_config)
          setBtConfig(metricsRes.value.data.backtesting_config);
      }
      if (bestRes.status === 'fulfilled') {
        setBestMethod(bestRes.value.data);
        // Default band visibility: only best method shows confidence areas
        if (forecastRes.status === 'fulfilled') {
          const fcasts = forecastRes.value.data.forecasts || [];
          const bestName = bestRes.value.data?.best_method || fcasts[0]?.method;
          const bv = {};
          fcasts.forEach(f => { bv[f.method] = f.method === bestName; });
          setBandVisibleMethods(prev => {
            // On first load, use defaults; on reload, keep user preferences
            const hasExisting = Object.keys(prev).length > 0;
            if (hasExisting) {
              const merged = { ...bv };
              Object.entries(prev).forEach(([k, v]) => { if (k in merged) merged[k] = v; });
              return merged;
            }
            return bv;
          });
        }
      }
      if (explainRes.status === 'fulfilled') setMethodExplanation(explainRes.value.data);
      if (distRes.status === 'fulfilled') setDistributions(distRes.value.data);
      if (originsRes.status === 'fulfilled') {
        const o = originsRes.value.data.origins || [];
        setOrigins(o);
        if (o.length > 0) setSelectedOriginIdx(o.length - 1);
      }

      // Load hyperparameter overrides (non-blocking)
      try {
        const hpRes = await api.get(`/hyperparams/${encodeURIComponent(decodedId)}`);
        setHpSavedOverrides(hpRes.data.overrides || {});
        setHpEdits({});  // clear local edits on fresh load
      } catch { /* no overrides yet — that's fine */ }

      // Load applied parameter versions (non-blocking)
      api.get(`/series/${encodeURIComponent(decodedId)}/parameters`)
        .then(res => setSeriesParameters(res.data))
        .catch(() => setSeriesParameters(null));

      // Load forecast convergence data (non-blocking)
      try {
        const convRes = await api.get(`/series/${encodeURIComponent(decodedId)}/forecast-convergence`);
        console.log('[convergence] loaded', convRes.data?.targets?.length, 'targets,', convRes.data?.methods?.length, 'methods');
        setConvergenceData(convRes.data);
      } catch (convErr) {
        console.warn('[convergence] failed to load:', convErr?.response?.status, convErr?.response?.data?.detail || convErr.message);
      }

      setLoading(false);
    } catch (err) {
      setError(err.message);
      setLoading(false);
    }
  };

  // ---- Run Forecast handler ----
  const handleRunForecast = useCallback(async () => {
    if (forecastUids.length === 0) return;
    try {
      setForecastJobStatus('pending');
      setForecastProgress(null);
      setForecastStartedAt(new Date().toISOString());
      const res = await api.post(`/pipeline/run-forecast`, {
        series: forecastUids,
        all_methods: true,
      });
      const jobId = res.data.job_id;
      setForecastJobId(jobId);
      setForecastJobStatus('running');

      // Poll job status every 1.5 s
      if (forecastPollRef.current) clearInterval(forecastPollRef.current);
      forecastPollRef.current = setInterval(async () => {
        try {
          const r = await api.get(`/pipeline/jobs/${jobId}`);
          const st = r.data.status;
          setForecastJobStatus(st);
          // Capture progress and started_at from response
          if (r.data.progress) setForecastProgress(r.data.progress);
          if (r.data.started_at) setForecastStartedAt(r.data.started_at);
          if (st === 'success' || st === 'error') {
            clearInterval(forecastPollRef.current);
            forecastPollRef.current = null;
            setForecastProgress(null);
            if (st === 'success') {
              // Refresh the API data cache, then reload this series
              try { await api.post(`/reload`); } catch { /* non-fatal */ }
              loadData();
            }
          }
        } catch { /* ignore transient poll errors */ }
      }, 1500);
    } catch (err) {
      setForecastJobStatus('error');
      setForecastProgress(null);
      console.error('Run forecast failed:', err);
    }
  }, [forecastUids, loadData]);

  // Cleanup poll on unmount
  useEffect(() => {
    return () => { if (forecastPollRef.current) clearInterval(forecastPollRef.current); };
  }, []);

  // Auto-clear success/error badge after 8 seconds
  useEffect(() => {
    if (forecastJobStatus === 'success' || forecastJobStatus === 'error') {
      const timer = setTimeout(() => setForecastJobStatus(null), 8000);
      return () => clearTimeout(timer);
    }
  }, [forecastJobStatus]);

  // ---- Load / save adjustments ----
  const loadAdjustments = useCallback(async () => {
    try {
      const res = await api.get(`/adjustments/${encodeURIComponent(decodedId)}`);
      const map = {};
      (res.data || []).forEach(a => {
        map[`${a.forecast_date}|${a.adjustment_type}`] = a;
      });
      setAdjustments(map);
    } catch { /* non-fatal */ }
  }, [decodedId]);

  useEffect(() => { loadAdjustments(); }, [loadAdjustments]);

  const saveAdjustment = useCallback((forecastDate, adjType, value, note) => {
    const key = `${forecastDate}|${adjType}`;
    // Cancel pending debounce for this key
    if (adjDebounceRef.current[key]) clearTimeout(adjDebounceRef.current[key]);

    const strVal = String(value).trim();
    const isEmpty = strVal === '' || strVal === null;
    const numVal = isEmpty ? NaN : Number(strVal);
    const isInvalid = isEmpty || isNaN(numVal);

    if (isInvalid) {
      // Empty field → delete existing adjustment (if any)
      adjDebounceRef.current[key] = setTimeout(async () => {
        try {
          await api.delete(
            `/adjustments/${encodeURIComponent(decodedId)}/${forecastDate}/${adjType}`
          );
          setAdjustments(prev => {
            const next = { ...prev };
            delete next[key];
            return next;
          });
        } catch { /* non-fatal — row may not exist */ }
      }, 400);
      return;
    }

    // Upsert with debounce
    adjDebounceRef.current[key] = setTimeout(async () => {
      setAdjSaving(prev => ({ ...prev, [key]: true }));
      try {
        const res = await api.post(
          `/adjustments/${encodeURIComponent(decodedId)}`,
          { forecast_date: forecastDate, adjustment_type: adjType, value: numVal, note: note || null }
        );
        // Immediately update local state so chart re-renders without waiting for a reload
        setAdjustments(prev => ({ ...prev, [key]: { ...res.data, forecast_date: forecastDate, adjustment_type: adjType, value: numVal } }));
      } catch (e) {
        console.error('saveAdjustment failed:', e?.response?.data || e.message);
      } finally {
        setAdjSaving(prev => { const n = { ...prev }; delete n[key]; return n; });
      }
    }, 400);
  }, [decodedId]);

  const resetAllAdjustments = useCallback(async () => {
    if (!window.confirm('Reset ALL adjustments and overrides for this series?')) return;
    try {
      await api.delete(`/adjustments/${encodeURIComponent(decodedId)}`);
      setAdjustments({});
    } catch { /* non-fatal */ }
  }, [decodedId]);

  // ---- Multi-series adjustment: disaggregate proportionally and save to each series ----
  const multiAdjDebounceRef = useRef({});

  const saveMultiAdjustment = useCallback((forecastDate, adjType, value, note, periodIdx) => {
    if (!multiSeriesData) return;
    const key = `${forecastDate}|${adjType}`;
    if (multiAdjDebounceRef.current[key]) clearTimeout(multiAdjDebounceRef.current[key]);

    const strVal = String(value).trim();
    const isEmpty = strVal === '' || strVal === null;
    const numVal = isEmpty ? NaN : Number(strVal);
    const isInvalid = isEmpty || isNaN(numVal);
    const { perUidForecasts, uids, aggAdjustments, perUidAdjustments } = multiSeriesData;

    if (isInvalid) {
      // Delete all per-series adjustments for this date+type
      multiAdjDebounceRef.current[key] = setTimeout(async () => {
        setAdjSaving(prev => ({ ...prev, [key]: true }));
        try {
          await Promise.allSettled(uids.map(uid =>
            api.delete(`/adjustments/${encodeURIComponent(uid)}/${forecastDate}/${adjType}`)
          ));
          // Update local multi-series state
          setMultiSeriesData(prev => {
            if (!prev) return prev;
            const newAgg = { ...prev.aggAdjustments };
            delete newAgg[key];
            const newPerUid = { ...prev.perUidAdjustments };
            uids.forEach(uid => {
              newPerUid[uid] = { ...newPerUid[uid] };
              delete newPerUid[uid][key];
            });
            return { ...prev, aggAdjustments: newAgg, perUidAdjustments: newPerUid };
          });
        } catch { /* non-fatal */ }
        finally { setAdjSaving(prev => { const n = { ...prev }; delete n[key]; return n; }); }
      }, 400);
      return;
    }

    // Compute weights: each series' best-method forecast at this period / total
    multiAdjDebounceRef.current[key] = setTimeout(async () => {
      setAdjSaving(prev => ({ ...prev, [key]: true }));
      try {
        const weights = {};
        let total = 0;
        uids.forEach(uid => {
          const fc = perUidForecasts[uid];
          if (!fc) return;
          const val = Math.abs(fc.point_forecast[periodIdx] || 0);
          weights[uid] = val;
          total += val;
        });
        // Normalize weights; if total is 0, distribute equally
        const nUids = uids.length;
        uids.forEach(uid => {
          weights[uid] = total > 0 ? weights[uid] / total : 1 / nUids;
        });

        // For adjustments: distribute the delta proportionally
        // For overrides: distribute the total value proportionally
        const promises = uids.map(uid => {
          const perSeriesVal = adjType === 'override'
            ? numVal * weights[uid]                    // proportional share of the override total
            : numVal * weights[uid];                   // proportional share of the adjustment delta
          return api.post(`/adjustments/${encodeURIComponent(uid)}`, {
            forecast_date: forecastDate,
            adjustment_type: adjType,
            value: Math.round(perSeriesVal * 100) / 100, // round to 2 decimals
            note: note || null,
          });
        });
        await Promise.allSettled(promises);

        // Update local multi-series state
        setMultiSeriesData(prev => {
          if (!prev) return prev;
          const newAgg = { ...prev.aggAdjustments };
          newAgg[key] = { forecast_date: forecastDate, adjustment_type: adjType, value: numVal };
          const newPerUid = { ...prev.perUidAdjustments };
          uids.forEach(uid => {
            const perVal = adjType === 'override'
              ? numVal * weights[uid]
              : numVal * weights[uid];
            newPerUid[uid] = { ...newPerUid[uid] };
            newPerUid[uid][key] = { forecast_date: forecastDate, adjustment_type: adjType, value: Math.round(perVal * 100) / 100 };
          });
          return { ...prev, aggAdjustments: newAgg, perUidAdjustments: newPerUid };
        });
      } catch (e) {
        console.error('saveMultiAdjustment failed:', e?.response?.data || e.message);
      } finally {
        setAdjSaving(prev => { const n = { ...prev }; delete n[key]; return n; });
      }
    }, 400);
  }, [multiSeriesData]);

  const resetMultiAdjustments = useCallback(async () => {
    if (!multiSeriesData) return;
    if (!window.confirm(`Reset ALL adjustments for ${multiSeriesData.uids.length} series?`)) return;
    try {
      await Promise.allSettled(multiSeriesData.uids.map(uid =>
        api.delete(`/adjustments/${encodeURIComponent(uid)}`)
      ));
      setMultiSeriesData(prev => prev ? { ...prev, aggAdjustments: {}, perUidAdjustments: {} } : prev);
    } catch { /* non-fatal */ }
  }, [multiSeriesData]);

  useEffect(() => {
    if (origins.length === 0) return;
    const origin = origins[selectedOriginIdx];
    if (!origin) return;
    api.get(`/forecasts/${encodeURIComponent(decodedId)}/origins/${origin}`)
      .then(res => setOriginForecasts(res.data))
      .catch(() => setOriginForecasts(null));
  }, [selectedOriginIdx, origins, decodedId]);

  const togglePlay = useCallback(() => {
    if (isPlaying) {
      clearInterval(playTimerRef.current); playTimerRef.current = null; setIsPlaying(false);
    } else {
      setIsPlaying(true); setSelectedOriginIdx(0);
      playTimerRef.current = setInterval(() => {
        setSelectedOriginIdx(prev => {
          if (prev >= origins.length - 1) { clearInterval(playTimerRef.current); playTimerRef.current = null; setIsPlaying(false); return prev; }
          return prev + 1;
        });
      }, 800);
    }
  }, [isPlaying, origins.length]);

  const toggleMethod = (method) => setVisibleMethods(prev => ({ ...prev, [method]: !prev[method] }));
  const toggleBand = (method) => setBandVisibleMethods(prev => ({ ...prev, [method]: !prev[method] }));

  // Active data source: multi-series aggregated OR single series
  const activeHistoricalData = multiSeriesData ? multiSeriesData.historical : historicalData;
  const activeForecasts = multiSeriesData ? multiSeriesData.forecasts : forecasts;
  const activeMetrics = multiSeriesData ? multiSeriesData.metrics : metrics;
  const isMultiMode = !!multiSeriesData;

  const activeMethodDomain = useMemo(() => {
    const methods = ['Historical', ...activeForecasts.map(f => f.method)];
    return { domain: methods, range: methods.map(m => getMethodColor(m)) };
  }, [activeForecasts]);

  const horizonLength = useMemo(() => {
    if (activeForecasts.length === 0) return 0;
    return activeForecasts[0].point_forecast.length;
  }, [activeForecasts]);

  // Infer step size in days from median gap of first 20 historical date pairs.
  // Used to compute correct forecast future dates (e.g. 7 days/step for weekly).
  const daysPerPeriod = useMemo(() => {
    const dates = activeHistoricalData?.date;
    if (!dates || dates.length < 2) return 30;
    const diffs = [];
    for (let i = 1; i < Math.min(dates.length, 20); i++) {
      diffs.push((new Date(dates[i]) - new Date(dates[i-1])) / 86400000);
    }
    diffs.sort((a, b) => a - b);
    return Math.max(1, diffs[Math.floor(diffs.length / 2)]);
  }, [activeHistoricalData]);

  // Human-readable period label for the current step size
  const periodLabel = useMemo(() => {
    if (displayAgg !== 'native') return AGG_OPTS.find(o => o.value === displayAgg)?.label || '';
    if (daysPerPeriod <= 1.5) return 'day';
    if (daysPerPeriod <= 10)  return 'week';
    if (daysPerPeriod <= 40)  return 'month';
    if (daysPerPeriod <= 100) return 'quarter';
    return 'year';
  }, [daysPerPeriod, displayAgg]);

  // Date bounds for the period date-range picker
  const dateBounds = useMemo(() => {
    const dates = activeHistoricalData?.date;
    if (!dates || dates.length === 0) return { min: null, max: null };
    return { min: dates[0].slice(0, 10), max: dates[dates.length - 1].slice(0, 10) };
  }, [activeHistoricalData]);

  const handlePeriodChange = useCallback((start, end) => {
    setPeriodStart(start);
    setPeriodEnd(end);
  }, []);

  // Display-aggregated historical data (with optional period date-range filter)
  const dispHistData = useMemo(() => {
    let data = aggHistData(activeHistoricalData, displayAgg);
    if (!data?.date?.length || (!periodStart && !periodEnd)) return data;
    const lo = periodStart || data.date[0];
    const hi = periodEnd || data.date[data.date.length - 1];
    const startIdx = data.date.findIndex(d => d >= lo);
    const endIdx = data.date.findLastIndex(d => d <= hi);
    if (startIdx < 0 || endIdx < 0 || startIdx > endIdx) return data;
    return {
      date: data.date.slice(startIdx, endIdx + 1),
      value: data.value.slice(startIdx, endIdx + 1),
    };
  }, [activeHistoricalData, displayAgg, periodStart, periodEnd]);

  /* ---------- build combined data for main chart ---------- */
  const { allData, allDates } = useMemo(() => {
    const hasHistorical = dispHistData?.date?.length > 0;

    // We need either historical data or forecast data with a date reference to build the chart
    if (!hasHistorical && activeForecasts.length === 0) return { allData: [], allDates: [] };

    const data = [];
    const dateSet = new Set();

    // Historical demand → stacked bars (layer: 'bar', barSeries: 'Demand')
    if (hasHistorical) {
      dispHistData.date.forEach((date, i) => {
        dateSet.add(date);
        const val = dispHistData.value[i];
        data.push({ date, value: val, type: 'Actual', method: 'Historical',
          lo90: null, hi90: null, lo50: null, hi50: null,
          layer: 'bar', barSeries: 'Demand', barValue: val });
      });
    }

    // Forecast data + bars for best method + adjustment/override stacking
    if (activeForecasts.length > 0) {
      // Use the last historical date (from RAW data so step size is correct)
      const rawLastDate = activeHistoricalData?.date?.length
        ? activeHistoricalData.date[activeHistoricalData.date.length - 1]
        : null;
      const lastDate = rawLastDate
        ? new Date(rawLastDate)
        : (dateRangeEnd ? new Date(dateRangeEnd) : null);

      if (lastDate) {
        // Use only the best method's forecast to compute base values for adjustments
        const bestFc = activeForecasts.find(f => f.method === bestMethod?.best_method) || activeForecasts[0];

        // Compute native forecast dates for each horizon step (use daysPerPeriod, not months)
        const nativeFcDates = (bestFc?.point_forecast || []).map((_, i) => {
          const d = new Date(lastDate);
          d.setDate(d.getDate() + Math.round(daysPerPeriod * (i + 1)));
          return fmtDate(d);
        });

        // All forecast methods → lines + bands, optionally aggregated
        activeForecasts.forEach(forecast => {
          const rawQs = forecast.quantiles || {};
          const fcNativeDates = forecast.point_forecast.map((_, i) => {
            const d = new Date(lastDate);
            d.setDate(d.getDate() + Math.round(daysPerPeriod * (i + 1)));
            return fmtDate(d);
          });
          const { dates: fcDates, pf: fcPF, qs: fcQS } =
            aggForecastSeries(fcNativeDates, forecast.point_forecast, rawQs, displayAgg);

          fcDates.forEach((dateStr, i) => {
            dateSet.add(dateStr);
            const value = fcPF[i];
            const lo90 = fcQS['0.05']?.[i] ?? null;
            const hi90 = fcQS['0.95']?.[i] ?? null;
            data.push({ date: dateStr, value, type: 'Forecast', method: forecast.method, lo90, hi90, lo50: fcQS['0.25']?.[i] ?? lo90, hi50: fcQS['0.75']?.[i] ?? hi90, layer: 'line' });
            if (lo90 != null && hi90 != null)
              data.push({ date: dateStr, value: null, type: 'Band', method: forecast.method, lo90, hi90, lo50: fcQS['0.25']?.[i] ?? lo90, hi50: fcQS['0.75']?.[i] ?? hi90, layer: 'band' });
          });
        });

        // Adjustment / override markers — only when at native granularity
        if (bestFc && displayAgg === 'native') {
          nativeFcDates.forEach((dateStr, i) => {
            const baseValue = bestFc.point_forecast[i];
            const adjKey = `${dateStr}|adjustment`;
            const ovKey  = `${dateStr}|override`;
            const adj = adjustments[adjKey];
            const ov  = adjustments[ovKey];
            if (ov) {
              data.push({ date: dateStr, value: Number(ov.value), type: 'Override', method: 'Override',
                lo90: null, hi90: null, lo50: null, hi50: null, layer: 'marker',
                adjNote: ov.note || '' });
            }
            if (adj) {
              const adjVal = baseValue + Number(adj.value);
              data.push({ date: dateStr, value: adjVal, type: 'Adjustment', method: 'Adjustment',
                lo90: null, hi90: null, lo50: null, hi50: null, layer: 'marker',
                adjNote: adj.note || '', adjDelta: Number(adj.value) });
            }
          });
        }
      }
    }
    const sortedDates = [...dateSet].sort();
    return { allData: data, allDates: sortedDates };
  }, [dispHistData, activeHistoricalData, activeForecasts, adjustments, bestMethod, dateRangeEnd, daysPerPeriod, displayAgg]);

  useEffect(() => {
    if (allDates.length > 0) { setZoomStart(0); setZoomEnd(allDates.length - 1); }
  }, [allDates.length]);

  /* ---------- outlier data ---------- */
  const { outlierChartData, outlierDates } = useMemo(() => {
    if (!hasOutlierCorrections || !originalData || !historicalData) return { outlierChartData: [], outlierDates: [] };
    const data = [], dates = [];
    const outlierDateSet = new Set();
    if (outlierInfo?.outliers) outlierInfo.outliers.forEach(o => outlierDateSet.add(o.date?.split('T')[0]));
    originalData.date.forEach((date, i) => {
      const dateStr = date?.split('T')[0] || date;
      dates.push(dateStr);
      const origVal = originalData.value[i];
      const corrVal = historicalData.value[i];
      const isOutlier = outlierDateSet.has(dateStr);
      // For the stacked bar: push both series per date
      // "Corrected" base bar (always) + "Adjustment" bar showing the delta for outlier dates
      const delta = origVal - corrVal; // positive = original was clipped down
      data.push({ date: dateStr, value: corrVal, series: 'Corrected', isOutlier, origVal, corrVal, delta });
      if (isOutlier && Math.abs(delta) > 0) {
        // Show the adjustment as a separate stacked segment
        data.push({ date: dateStr, value: delta > 0 ? delta : Math.abs(delta), series: delta > 0 ? 'Clipped ↓' : 'Filled ↑', isOutlier: true, origVal, corrVal, delta });
      }
    });
    return { outlierChartData: data, outlierDates: dates };
  }, [hasOutlierCorrections, originalData, historicalData, outlierInfo]);

  useEffect(() => {
    if (outlierDates.length > 0) { setOutlierZoomStart(0); setOutlierZoomEnd(outlierDates.length - 1); }
  }, [outlierDates.length]);

  /* ---------- Plotly base layout for dark mode ---------- */
  const plotlyBase = useMemo(() => ({
    paper_bgcolor: 'rgba(0,0,0,0)',
    plot_bgcolor: 'rgba(0,0,0,0)',
    font: { color: isDark ? '#d1d5db' : '#374151', size: 11 },
    margin: { t: 10, r: 10, b: 40, l: 60, pad: 4 },
    xaxis: { gridcolor: isDark ? '#374151' : '#e5e7eb', zerolinecolor: isDark ? '#4b5563' : '#d1d5db', color: isDark ? '#d1d5db' : '#374151' },
    yaxis: { gridcolor: isDark ? '#374151' : '#e5e7eb', zerolinecolor: isDark ? '#4b5563' : '#d1d5db', color: isDark ? '#d1d5db' : '#374151' },
  }), [isDark]);
  const plotlyConfig = {
    responsive: true,
    displayModeBar: 'hover',
    displaylogo: false,
    modeBarButtonsToRemove: ['toImage', 'lasso2d', 'select2d'],
  };

  /* ---------- chart data (Plotly) ---------- */
  const outlierChartSpec = useMemo(() => {
    if (outlierChartData.length === 0 || outlierDates.length === 0) return null;
    const minDate = outlierDates[outlierZoomStart] || outlierDates[0];
    const maxDate = outlierDates[outlierZoomEnd] || outlierDates[outlierDates.length - 1];
    const filtered = outlierChartData.filter(d => d.date >= minDate && d.date <= maxDate);
    if (filtered.length === 0) return null;
    // Group by series type for stacked bar
    const seriesMap = { 'Corrected': { color: '#2563eb' }, 'Clipped \u2193': { color: '#ef4444' }, 'Filled \u2191': { color: '#f59e0b' } };
    const traces = Object.entries(seriesMap).map(([name, cfg]) => {
      const rows = filtered.filter(d => d.series === name);
      return {
        type: 'bar', name,
        x: rows.map(d => d.date), y: rows.map(d => d.value),
        marker: { color: cfg.color, opacity: rows.map(d => d.isOutlier ? 1.0 : 0.75) },
        customdata: rows.map(d => [d.corrVal, d.origVal, d.delta]),
        hovertemplate: '%{x|%Y-%m}<br>Corrected: %{customdata[0]:,.0f}<br>Original: %{customdata[1]:,.0f}<br>\u0394: %{customdata[2]:,.0f}<extra>%{fullData.name}</extra>',
      };
    }).filter(t => t.x.length > 0);
    return {
      data: traces,
      layout: {
        ...plotlyBase, height: 300, barmode: 'stack',
        xaxis: { ...plotlyBase.xaxis, type: 'date', tickformat: '%Y-%m', tickangle: -30 },
        yaxis: { ...plotlyBase.yaxis, title: 'Demand' },
        legend: { orientation: 'h', y: 1.08 },
      },
    };
  }, [outlierChartData, outlierDates, outlierZoomStart, outlierZoomEnd, plotlyBase]);

  const mainChartSpec = useMemo(() => {
    if (allData.length === 0 || allDates.length === 0) return null;
    const minDate = allDates[Math.min(zoomStart, allDates.length - 1)] || allDates[0];
    const maxDate = allDates[Math.min(zoomEnd, allDates.length - 1)] || allDates[allDates.length - 1];
    const filtered = allData.filter(d => {
      if (d.type !== 'Actual' && d.method !== 'Historical' && visibleMethods[d.method] === false) return false;
      if (d.layer === 'band' && bandVisibleMethods[d.method] === false) return false;
      return d.date >= minDate && d.date <= maxDate;
    });
    if (filtered.length === 0) return null;

    const methodColorMap = {};
    activeMethodDomain.domain.forEach((m, i) => { methodColorMap[m] = activeMethodDomain.range[i]; });
    const tickfmt = daysPerPeriod <= 10 ? '%b %d %Y' : (daysPerPeriod <= 95 ? '%b %Y' : '%Y');
    const traces = [];

    // ── Historical demand bars ──
    const barRows = filtered.filter(d => d.layer === 'bar');
    if (barRows.length > 0) {
      traces.push({
        type: 'bar', name: 'Historical', legendgroup: 'Historical', showlegend: false,
        x: barRows.map(d => d.date), y: barRows.map(d => d.value),
        marker: { color: isDark ? '#9ca3af' : '#374151', opacity: 0.55 },
        hovertemplate: '%{x|%Y-%m}<br>Demand: %{y:,.0f}<extra>Historical</extra>',
      });
    }

    // ── Confidence bands (90% then 50%) ──
    const bandRows = filtered.filter(d => d.layer === 'band');
    const bandMethods = [...new Set(bandRows.map(d => d.method))];
    bandMethods.forEach(m => {
      const rows = bandRows.filter(d => d.method === m).sort((a, b) => a.date.localeCompare(b.date));
      if (rows.length === 0) return;
      const color = methodColorMap[m] || '#6b7280';
      const r = parseInt(color.slice(1,3),16), g = parseInt(color.slice(3,5),16), b = parseInt(color.slice(5,7),16);
      // 90% band
      if (rows[0].hi90 != null) {
        traces.push({ type: 'scatter', mode: 'lines', name: `${m} 90%`, legendgroup: m, showlegend: false,
          x: [...rows.map(d => d.date), ...rows.map(d => d.date).reverse()],
          y: [...rows.map(d => d.hi90), ...rows.map(d => d.lo90).reverse()],
          fill: 'toself', fillcolor: `rgba(${r},${g},${b},0.1)`, line: { width: 0 }, hoverinfo: 'skip' });
      }
      // 50% band
      if (rows[0].hi50 != null) {
        traces.push({ type: 'scatter', mode: 'lines', name: `${m} 50%`, legendgroup: m, showlegend: false,
          x: [...rows.map(d => d.date), ...rows.map(d => d.date).reverse()],
          y: [...rows.map(d => d.hi50), ...rows.map(d => d.lo50).reverse()],
          fill: 'toself', fillcolor: `rgba(${r},${g},${b},0.22)`, line: { width: 0 }, hoverinfo: 'skip' });
      }
    });

    // ── Forecast/Actual lines per method ──
    const lineRows = filtered.filter(d => d.layer === 'line');
    const lineMethods = [...new Set(lineRows.map(d => d.method))];
    lineMethods.forEach(m => {
      const rows = lineRows.filter(d => d.method === m).sort((a, b) => a.date.localeCompare(b.date));
      // Split into Actual (solid) and Forecast (dash) segments
      const actRows = rows.filter(d => d.type === 'Actual');
      const fcRows = rows.filter(d => d.type === 'Forecast');
      const color = methodColorMap[m] || '#6b7280';
      if (actRows.length > 0) {
        traces.push({ type: 'scatter', mode: 'lines', name: m, legendgroup: m,
          x: actRows.map(d => d.date), y: actRows.map(d => d.value),
          line: { color, width: 2 }, opacity: 0.85,
          hovertemplate: `%{x|%Y-%m-%d}<br>Value: %{y:,.0f}<extra>${m} (Actual)</extra>` });
      }
      if (fcRows.length > 0) {
        // Connect to last actual point for continuity
        const bridge = actRows.length > 0 ? [actRows[actRows.length - 1]] : [];
        const pts = [...bridge, ...fcRows];
        traces.push({ type: 'scatter', mode: 'lines', name: m, legendgroup: m, showlegend: actRows.length === 0,
          x: pts.map(d => d.date), y: pts.map(d => d.value),
          line: { color, width: 2, dash: 'dash' }, opacity: 0.85,
          hovertemplate: `%{x|%Y-%m-%d}<br>Value: %{y:,.0f}<extra>${m} (Forecast)</extra>` });
      }
    });

    // ── Final Forecast overlay line ──
    const adjOvRows = filtered.filter(d => d.type === 'Adjustment' || d.type === 'Override').sort((a, b) => a.date.localeCompare(b.date));
    if (adjOvRows.length > 0) {
      traces.push({ type: 'scatter', mode: 'lines', name: 'Final Forecast', legendgroup: 'Final',
        x: adjOvRows.map(d => d.date), y: adjOvRows.map(d => d.value),
        line: { color: '#7c3aed', width: 2.5, dash: 'dot' },
        hovertemplate: '%{x|%Y-%m-%d}<br>Final: %{y:,.0f}<extra>Final Forecast</extra>' });
    }

    // ── Adjustment markers (orange triangle-up) ──
    const adjRows = filtered.filter(d => d.type === 'Adjustment');
    if (adjRows.length > 0) {
      traces.push({ type: 'scatter', mode: 'markers', name: 'Adjustments', legendgroup: 'Adj',
        x: adjRows.map(d => d.date), y: adjRows.map(d => d.value),
        marker: { color: '#f97316', symbol: 'triangle-up', size: 10 },
        customdata: adjRows.map(d => [d.adjDelta, d.adjNote || '']),
        hovertemplate: '%{x|%Y-%m-%d}<br>Adjusted: %{y:,.0f}<br>\u0394: %{customdata[0]:+,.0f}<br>%{customdata[1]}<extra>Adjustment</extra>' });
    }

    // ── Override markers (red square) ──
    const ovRows = filtered.filter(d => d.type === 'Override');
    if (ovRows.length > 0) {
      traces.push({ type: 'scatter', mode: 'markers', name: 'Overrides', legendgroup: 'Ov',
        x: ovRows.map(d => d.date), y: ovRows.map(d => d.value),
        marker: { color: '#dc2626', symbol: 'square', size: 9 },
        customdata: ovRows.map(d => [d.adjNote || '']),
        hovertemplate: '%{x|%Y-%m-%d}<br>Override: %{y:,.0f}<br>%{customdata[0]}<extra>Override</extra>' });
    }

    return {
      data: traces,
      layout: {
        ...plotlyBase, height: 380,
        xaxis: { ...plotlyBase.xaxis, type: 'date', tickformat: tickfmt, tickangle: -30 },
        yaxis: { ...plotlyBase.yaxis, title: 'Demand', rangemode: 'normal' },
        barmode: 'overlay',
        legend: { orientation: 'h', y: 1.05, font: { size: 10 } },
        hovermode: 'closest',
      },
    };
  }, [allData, allDates, zoomStart, zoomEnd, visibleMethods, bandVisibleMethods, activeMethodDomain, daysPerPeriod, plotlyBase, isDark]);

  const racingBarsSpec = useMemo(() => {
    const src = originForecasts?.forecasts?.length > 0 ? originForecasts.forecasts : activeForecasts;
    if (!src || src.length === 0) return null;
    const barData = src.filter(f => visibleMethods[f.method] !== false).map(f => ({ method: f.method, value: f.point_forecast[selectedPeriod - 1] || 0, actual: f.actual?.[selectedPeriod - 1] || null })).sort((a, b) => a.value - b.value); // ascending for horizontal bar
    if (barData.length === 0) return null;
    const methodColorMap = {};
    activeMethodDomain.domain.forEach((m, i) => { methodColorMap[m] = activeMethodDomain.range[i]; });
    const actualVal = barData.find(d => d.actual !== null)?.actual;
    const shapes = [];
    const annotations = [];
    if (actualVal != null) {
      shapes.push({ type: 'line', x0: actualVal, x1: actualVal, y0: -0.5, y1: barData.length - 0.5, line: { color: '#e11d48', width: 2, dash: 'dashdot' } });
      annotations.push({ x: actualVal, y: barData.length - 0.8, text: `Actual: ${formatNumber(actualVal, locale, 0)}`, showarrow: false, font: { color: '#e11d48', size: 11, weight: 'bold' }, xanchor: 'left', xshift: 4 });
    }
    return {
      data: [{
        type: 'bar', orientation: 'h',
        y: barData.map(d => d.method), x: barData.map(d => d.value),
        marker: { color: barData.map(d => methodColorMap[d.method] || '#6b7280') },
        customdata: barData.map(d => [d.actual]),
        hovertemplate: '%{y}<br>Forecast: %{x:,.0f}<br>Actual: %{customdata[0]:,.0f}<extra></extra>',
      }],
      layout: {
        ...plotlyBase,
        height: Math.max(150, barData.length * 40),
        margin: { t: 10, r: 10, b: 40, l: 120 },
        xaxis: { ...plotlyBase.xaxis, title: `Forecast (Month ${selectedPeriod})` },
        yaxis: { ...plotlyBase.yaxis, automargin: true },
        shapes, annotations,
      },
    };
  }, [originForecasts, activeForecasts, selectedPeriod, visibleMethods, activeMethodDomain, plotlyBase, locale]);

  // ---- Forecast Convergence chart (Plotly grouped bars) ----
  const convergenceChart = useMemo(() => {
    if (!convergenceData || !convergenceData.targets || convergenceData.targets.length === 0) return null;

    // Pick method: use convergenceMethod state, or fall back to best method, or first available
    const selectedMethod = convergenceMethod || bestMethod?.best_method || convergenceData.methods?.[0] || '';
    if (!selectedMethod) return null;

    const targets = convergenceData.targets;

    // Collect all unique origin dates across all targets
    const allOrigins = new Set();
    targets.forEach(t => t.origins.forEach(o => allOrigins.add(o.origin)));
    const originsSorted = [...allOrigins].sort();

    // Format date labels (shorter for x-axis)
    const fmtShort = (d) => {
      const dt = new Date(d + 'T00:00:00');
      return dt.toLocaleDateString('en-US', { month: 'short', year: '2-digit' });
    };

    // Color palette for origins (from older=lighter to newer=darker)
    const originColors = [
      '#93c5fd', '#60a5fa', '#3b82f6', '#2563eb', '#1d4ed8',
      '#1e40af', '#1e3a8a', '#172554', '#0f172a', '#6366f1',
      '#4f46e5', '#4338ca', '#3730a3', '#312e81'
    ];

    // Build one trace per origin
    const traces = originsSorted.map((origin, idx) => {
      const xLabels = [];
      const yValues = [];

      targets.forEach(t => {
        const oEntry = t.origins.find(o => o.origin === origin);
        const val = oEntry?.forecasts?.[selectedMethod];
        if (val !== undefined && val !== null) {
          xLabels.push(fmtShort(t.target_date));
          yValues.push(val);
        } else {
          xLabels.push(fmtShort(t.target_date));
          yValues.push(null);
        }
      });

      const color = originColors[idx % originColors.length];
      const monthsText = targets.map(t => {
        const oEntry = t.origins.find(o => o.origin === origin);
        return oEntry ? `${oEntry.months_ahead}m ahead` : '';
      });

      return {
        type: 'bar',
        name: fmtShort(origin),
        x: xLabels,
        y: yValues,
        marker: { color, line: { color: isDark ? '#1f2937' : '#fff', width: 0.5 } },
        hovertemplate: xLabels.map((x, i) =>
          `<b>${x}</b><br>Origin: ${fmtShort(origin)}<br>${monthsText[i]}<br>Forecast: %{y:,.0f}<extra></extra>`
        ),
      };
    });

    // Add actual values as a line overlay
    const actualX = [];
    const actualY = [];
    targets.forEach(t => {
      if (t.actual != null) {
        actualX.push(fmtShort(t.target_date));
        actualY.push(t.actual);
      }
    });

    if (actualX.length > 0) {
      traces.push({
        type: 'scatter',
        mode: 'lines+markers',
        name: 'Actual',
        x: actualX,
        y: actualY,
        line: { color: '#e11d48', width: 3, dash: 'dot' },
        marker: { color: '#e11d48', size: 8, symbol: 'diamond' },
        hovertemplate: '<b>%{x}</b><br>Actual: %{y:,.0f}<extra>Actual</extra>',
      });
    }

    const layout = {
      barmode: 'group',
      xaxis: {
        title: { text: 'Target Month', font: { size: 12, color: isDark ? '#d1d5db' : undefined } },
        tickangle: -45,
        gridcolor: isDark ? '#374151' : '#e5e7eb',
        tickfont: { color: isDark ? '#9ca3af' : '#6b7280' },
      },
      yaxis: {
        title: { text: 'Forecast Qty', font: { size: 12, color: isDark ? '#d1d5db' : undefined } },
        tickformat: ',.0f',
        gridcolor: isDark ? '#374151' : '#e5e7eb',
        tickfont: { color: isDark ? '#9ca3af' : '#6b7280' },
      },
      legend: {
        title: { text: 'Forecast Origin' },
        orientation: 'h',
        y: -0.3,
        x: 0.5,
        xanchor: 'center',
        font: { color: isDark ? '#d1d5db' : undefined },
      },
      margin: { l: 60, r: 20, t: 30, b: 80 },
      height: 400,
      hoverlabel: { bgcolor: '#1e293b', font: { color: '#fff', size: 12 } },
      plot_bgcolor: isDark ? '#1f2937' : '#fafafa',
      paper_bgcolor: isDark ? '#1f2937' : '#ffffff',
      font: { color: isDark ? '#d1d5db' : '#374151' },
    };

    return { traces, layout, method: selectedMethod };
  }, [convergenceData, convergenceMethod, bestMethod]);

  const ridgePlotData = useMemo(() => {
    if (!distributions || !distributions.horizons || distributions.horizons.length === 0) return null;
    const horizons = distributions.horizons;
    const nHorizons = horizons.length;
    const step = nHorizons > 24 ? Math.ceil(nHorizons / 24) : 1;
    const filtered = horizons.filter((_, i) => i % step === 0);
    if (filtered.length === 0) return null;

    // Build surface: z[horizonIdx][xIdx] = density
    // All horizons must share a common x grid for surface — use union of all x values sorted
    const allXSets = filtered.map(h => (h.density_points || []).map(p => p.x));
    const nPts = allXSets[0]?.length || 80;

    // For each horizon, build its own x/density arrays (they may differ — use per-row x for surface)
    // surface trace requires uniform grid: interpolate each row onto a shared x axis
    const globalXMin = Math.min(...allXSets.map(xs => Math.min(...xs)));
    const globalXMax = Math.max(...allXSets.map(xs => Math.max(...xs)));
    const sharedX = Array.from({ length: nPts }, (_, i) => globalXMin + (i / (nPts - 1)) * (globalXMax - globalXMin));

    // Linear interpolation helper
    const interp = (xs, ys, xNew) => {
      if (xNew <= xs[0]) return ys[0];
      if (xNew >= xs[xs.length - 1]) return ys[ys.length - 1];
      let lo = 0, hi = xs.length - 1;
      while (hi - lo > 1) { const mid = (lo + hi) >> 1; if (xs[mid] <= xNew) lo = mid; else hi = mid; }
      const t = (xNew - xs[lo]) / (xs[hi] - xs[lo]);
      return ys[lo] + t * (ys[hi] - ys[lo]);
    };

    const zRows = filtered.map(h => {
      const pts = h.density_points || [];
      const xs = pts.map(p => p.x);
      const ys = pts.map(p => p.y);
      return sharedX.map(xv => Math.max(0, interp(xs, ys, xv)));
    });

    const yLabels = filtered.map(h => `M${h.horizon_month}`);
    const means = filtered.map(h => h.mean);

    // Surface trace
    const surface = {
      type: 'surface',
      x: sharedX,
      y: filtered.map(h => h.horizon_month),
      z: zRows,
      colorscale: 'Viridis',
      opacity: 0.85,
      showscale: true,
      colorbar: { title: { text: 'Density', side: 'right' }, thickness: 14, len: 0.6 },
      contours: {
        z: { show: true, usecolormap: true, highlightcolor: '#fff', project: { z: false } }
      },
      hovertemplate: 'Horizon: M%{y}<br>Value: %{x:,.0f}<br>Density: %{z:.4f}<extra></extra>',
    };

    // Mean lines as scatter3d
    const meanLines = filtered.map((h, i) => ({
      type: 'scatter3d',
      mode: 'lines',
      x: [h.mean, h.mean],
      y: [h.horizon_month, h.horizon_month],
      z: [0, Math.max(...zRows[i]) * 1.05],
      line: { color: '#1e293b', width: 3, dash: 'dash' },
      showlegend: i === 0,
      name: i === 0 ? 'Mean' : '',
      hovertemplate: `M${h.horizon_month} mean: ${formatNumber(h.mean, locale, 0)}<extra></extra>`,
    }));

    return { traces: [surface, ...meanLines], yLabels, means };
  }, [distributions]);

  /* ---------- metrics helpers ---------- */
  const sortedMetrics = useMemo(() => {
    if (!activeMetrics || activeMetrics.length === 0) return [];
    return [...activeMetrics].sort((a, b) => {
      let va, vb;
      if (metricsSortField === 'composite') { va = compositeRanking?.[a.method] ?? Infinity; vb = compositeRanking?.[b.method] ?? Infinity; }
      else { va = a[metricsSortField]; vb = b[metricsSortField]; }
      if (metricsSortField === 'bias') { va = Math.abs(va || 0); vb = Math.abs(vb || 0); }
      if (va == null) va = Infinity;
      if (vb == null) vb = Infinity;
      return metricsSortDir === 'asc' ? va - vb : vb - va;
    });
  }, [activeMetrics, metricsSortField, metricsSortDir, compositeRanking]);

  const handleMetricsSort = (field) => {
    if (metricsSortField === field) setMetricsSortDir(metricsSortDir === 'asc' ? 'desc' : 'asc');
    else { setMetricsSortField(field); setMetricsSortDir('asc'); }
  };
  const metricsSortIndicator = (field) => metricsSortField === field ? (metricsSortDir === 'asc' ? ' ▲' : ' ▼') : '';

  const bestPerMetric = useMemo(() => {
    if (!activeMetrics || activeMetrics.length === 0) return {};
    const fields = ['mae', 'rmse', 'mape', 'smape', 'mase', 'crps', 'winkler_score', 'quantile_loss'];
    const result = {};
    fields.forEach(f => { const vals = activeMetrics.map(m => m[f]).filter(v => v != null && isFinite(v)); if (vals.length > 0) result[f] = Math.min(...vals); });
    const biasVals = activeMetrics.map(m => m.bias).filter(v => v != null && isFinite(v));
    if (biasVals.length > 0) result.bias = biasVals.reduce((best, v) => Math.abs(v) < Math.abs(best) ? v : best);
    ['coverage_50', 'coverage_80', 'coverage_90', 'coverage_95'].forEach(f => {
      const target = parseInt(f.split('_')[1]) / 100;
      const vals = activeMetrics.map(m => m[f]).filter(v => v != null && isFinite(v));
      if (vals.length > 0) result[f] = vals.reduce((best, v) => Math.abs(v - target) < Math.abs(best - target) ? v : best);
    });
    return result;
  }, [activeMetrics]);

  const isBestVal = (field, value) => {
    if (value == null || bestPerMetric[field] == null) return false;
    if (field === 'bias') return Math.abs(value) === Math.abs(bestPerMetric[field]);
    if (field.startsWith('coverage_')) { const target = parseInt(field.split('_')[1]) / 100; return Math.abs(value - target) === Math.abs(bestPerMetric[field] - target); }
    return value === bestPerMetric[field];
  };

  const fmtMetric = (value, pct = false) => {
    if (value == null || !isFinite(value)) return '-';
    if (pct) return formatPercent(value * 100, locale, 0);
    return formatNumber(value, locale, 1);
  };

  const forecastDates = useMemo(() => {
    if (!activeHistoricalData || !activeHistoricalData.date || activeHistoricalData.date.length === 0 || horizonLength === 0) return [];
    const lastDate = new Date(activeHistoricalData.date[activeHistoricalData.date.length - 1]);
    // Use inferred daysPerPeriod so weekly data shows weekly dates, not monthly
    return Array.from({ length: horizonLength }, (_, i) => {
      const d = new Date(lastDate);
      d.setDate(d.getDate() + Math.round(daysPerPeriod * (i + 1)));
      // For weekly/daily show full date; for monthly+ show YYYY-MM
      return daysPerPeriod < 20 ? d.toISOString().slice(0, 10) : d.toISOString().slice(0, 7);
    });
  }, [activeHistoricalData, horizonLength, daysPerPeriod]);

  /* ZoomSlider is defined at module level — see below TimeSeriesViewer */

  /* ---------- render ---------- */
  if (loading) return <div className="flex items-center justify-center h-64"><div className="animate-pulse text-xl text-gray-500 dark:text-gray-400">Loading time series...</div></div>;
  if (error) return <div className="flex items-center justify-center h-64"><div className="text-xl text-red-600 dark:text-red-400">Error: {error}</div></div>;

  return (
    <div className="p-4 sm:p-6">

      {/* Item / Site Selector — sticky on desktop, static on mobile */}
      <div id="tsv-selector" className="mb-6 bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900/50 sm:sticky sm:top-0 z-30">
        {/* ── Desktop layout: single row ── */}
        <div className="hidden sm:block p-4">
          <div className="flex flex-row gap-4 items-end">
            {/* Segment selector */}
            <div className="flex flex-col gap-1 flex-shrink-0">
              <label className="text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wide">Segment</label>
              <div className="relative">
                <select
                  value={selectedSegmentId || ''}
                  onChange={e => handleSegmentChange(Number(e.target.value))}
                  disabled={segments.length === 0}
                  className="pl-3 pr-8 py-2 text-sm border border-gray-200 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-800 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-violet-500 disabled:opacity-50 appearance-none min-w-[9rem]"
                >
                  {segments.map(s => (
                    <option key={s.id} value={s.id}>{s.name}</option>
                  ))}
                </select>
                {segmentLoading && (
                  <span className="absolute right-2 top-1/2 -translate-y-1/2">
                    <svg className="animate-spin w-3.5 h-3.5 text-violet-500" viewBox="0 0 24 24" fill="none">
                      <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"/>
                      <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8z"/>
                    </svg>
                  </span>
                )}
              </div>
            </div>
            <SearchableDropdown
              label="Item"
              values={selectedItems}
              onChange={handleItemsChange}
              options={allItems}
              recentOptions={recentItems}
              placeholder="Search item..."
              getLabel={id => itemNameMap[id] || id}
            />
            <SearchableDropdown
              label="Site"
              values={selectedSites}
              onChange={handleSitesChange}
              options={availableSites}
              recentOptions={recentSites.filter(s => availableSites.includes(s))}
              disabled={availableSites.length === 0}
              placeholder="Search site..."
              getLabel={id => siteNameMap[id] || id}
            />
            {/* Time aggregation granularity */}
            <div className="flex flex-col gap-1 flex-shrink-0">
              <label className="text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wide">Aggregation</label>
              <select
                value={displayAgg}
                onChange={e => setDisplayAgg(e.target.value)}
                className="px-3 py-2 text-sm border border-gray-200 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-800 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-indigo-500"
              >
                {AGG_OPTS.map(o => (
                  <option key={o.value} value={o.value}>{o.label}</option>
                ))}
              </select>
            </div>
            {/* Period date-range filter */}
            <div className="flex flex-col gap-1 flex-shrink-0">
              <label className="text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wide">Period</label>
              <DateRangePicker
                startDate={periodStart}
                endDate={periodEnd}
                minDate={dateBounds.min}
                maxDate={dateBounds.max}
                onChange={handlePeriodChange}
              />
            </div>
            <div className="flex items-center gap-2 flex-shrink-0 pb-0.5">
              <button
                onClick={handleRunForecast}
                disabled={forecastUids.length === 0 || forecastJobStatus === 'running' || forecastJobStatus === 'pending'}
                className={`px-4 py-2 rounded-lg text-sm font-semibold transition-all whitespace-nowrap ${
                  forecastUids.length === 0 || forecastJobStatus === 'running' || forecastJobStatus === 'pending'
                    ? 'bg-gray-200 dark:bg-gray-700 text-gray-400 dark:text-gray-500 cursor-not-allowed'
                    : 'bg-indigo-600 text-white hover:bg-indigo-700 active:scale-95 shadow-sm'
                }`}
                title={forecastUids.length === 0
                  ? 'Select at least one item and site'
                  : `Run all forecast methods for ${forecastUids.length} series`}
              >
                {forecastJobStatus === 'running' || forecastJobStatus === 'pending' ? (
                  <span className="flex items-center gap-1.5">
                    <svg className="animate-spin w-4 h-4" viewBox="0 0 24 24" fill="none">
                      <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"/>
                      <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8z"/>
                    </svg>
                    Running...
                  </span>
                ) : (
                  <span className="flex items-center gap-1.5">
                    <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth="2">
                      <path strokeLinecap="round" strokeLinejoin="round" d="M5 3l14 9-14 9V3z"/>
                    </svg>
                    Run Forecast
                  </span>
                )}
              </button>
              {forecastJobStatus === 'success' && (
                <span className="text-emerald-600 text-xs font-medium flex items-center gap-1 animate-fade-in">
                  {'\u2713'} Done
                </span>
              )}
              {forecastJobStatus === 'error' && (
                <span className="text-red-600 text-xs font-medium flex items-center gap-1">
                  {'\u2717'} Failed
                </span>
              )}
            </div>
            {/* ── Progress bar when job is running ── */}
            {(forecastJobStatus === 'running' || forecastJobStatus === 'pending') && (() => {
              const p = forecastProgress || {};
              const overallPct = p.overall_pct || 0;
              const stepLabel = { forecast: 'Forecasting', backtest: 'Backtesting', 'best-method': 'Selecting best', loading: 'Loading data' }[p.current_step] || 'Starting';
              // ETA calculation based on overall progress
              let etaText = '';
              if (overallPct > 2 && forecastStartedAt) {
                const elapsed = (Date.now() - new Date(forecastStartedAt).getTime()) / 1000;
                const remaining = elapsed * (100 - overallPct) / overallPct;
                if (remaining < 60) etaText = `~${Math.round(remaining)}s left`;
                else if (remaining < 3600) etaText = `~${Math.round(remaining / 60)}m left`;
                else etaText = `~${(remaining / 3600).toFixed(1)}h left`;
              }
              const stepDetail = p.completed != null && p.total != null && p.total > 1
                ? ` (${p.completed}/${p.total} series)` : '';
              return (
                <div className="mt-2 w-full max-w-md">
                  <div className="flex items-center justify-between text-xs mb-1">
                    <span className="font-medium text-indigo-700 dark:text-indigo-300">
                      {stepLabel}{stepDetail}
                    </span>
                    <span className="text-gray-500 dark:text-gray-400 tabular-nums">
                      {overallPct > 0 ? `${overallPct}%` : ''}
                      {etaText ? ` \u00b7 ${etaText}` : ''}
                    </span>
                  </div>
                  <div className="h-2 bg-gray-200 dark:bg-gray-700 rounded-full overflow-hidden">
                    {overallPct > 0 ? (
                      <div
                        className="h-full bg-indigo-500 rounded-full transition-all duration-700 ease-out"
                        style={{ width: `${Math.min(overallPct, 100)}%` }}
                      />
                    ) : (
                      <div className="h-full bg-indigo-400/60 rounded-full animate-pulse" style={{ width: '100%' }} />
                    )}
                  </div>
                </div>
              );
            })()}
          </div>
          <div className="mt-3 text-xs text-gray-400 dark:text-gray-500 flex flex-wrap gap-2 items-center">
            {/* Segment scope badge — only shown when non-default segment active */}
            {segmentMemberSet && (() => {
              const seg = segments.find(s => s.id === selectedSegmentId);
              return seg ? (
                <span className="bg-violet-100 dark:bg-violet-900/30 text-violet-700 dark:text-violet-300 px-2 py-0.5 rounded font-medium">
                  🗂️ {seg.name}: {filteredSeriesList.length} series
                </span>
              ) : null;
            })()}
            {isMultiMode ? (
              <>
                <span className="bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300 px-2 py-0.5 rounded font-medium">
                  Multi-series: {multiSeriesData?.uids?.length} series
                </span>
                <span className="text-gray-400 dark:text-gray-500">Demand &amp; Forecast = sum · Metrics = weighted average</span>
                {multiLoading && <span className="text-blue-500 animate-pulse">Loading...</span>}
              </>
            ) : (selectedItem && selectedSite && (
              <span>Current series: <span className="font-mono font-medium text-gray-600 dark:text-gray-300">{itemNameMap[selectedItem] ?? selectedItem}@{siteNameMap[selectedSite] ?? selectedSite}</span></span>
            ))}
          </div>
        </div>

        {/* ── Mobile layout: compact stacked ── */}
        <div className="sm:hidden p-3">
          <div className="space-y-2">
            {/* Segment selector (mobile) */}
            <div className="flex items-center gap-2">
              <label className="text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wide whitespace-nowrap">Segment</label>
              <select
                value={selectedSegmentId || ''}
                onChange={e => handleSegmentChange(Number(e.target.value))}
                disabled={segments.length === 0}
                className="flex-1 px-2 py-1.5 text-sm border border-gray-200 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-800 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-violet-500 disabled:opacity-50"
              >
                {segments.map(s => (
                  <option key={s.id} value={s.id}>{s.name}</option>
                ))}
              </select>
              {segmentLoading && (
                <svg className="animate-spin w-4 h-4 text-violet-500 flex-shrink-0" viewBox="0 0 24 24" fill="none">
                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"/>
                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8z"/>
                </svg>
              )}
            </div>
            <SearchableDropdown
              label="Item"
              values={selectedItems}
              onChange={handleItemsChange}
              options={allItems}
              recentOptions={recentItems}
              placeholder="Search item..."
              getLabel={id => itemNameMap[id] || id}
            />
            <SearchableDropdown
              label="Site"
              values={selectedSites}
              onChange={handleSitesChange}
              options={availableSites}
              recentOptions={recentSites.filter(s => availableSites.includes(s))}
              disabled={availableSites.length === 0}
              placeholder="Search site..."
              getLabel={id => siteNameMap[id] || id}
            />
            {/* Mobile time aggregation */}
            <div className="flex items-center gap-2">
              <label className="text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wide whitespace-nowrap">Agg</label>
              <select
                value={displayAgg}
                onChange={e => setDisplayAgg(e.target.value)}
                className="flex-1 px-2 py-1.5 text-sm border border-gray-200 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-800 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-indigo-500"
              >
                {AGG_OPTS.map(o => (
                  <option key={o.value} value={o.value}>{o.label}</option>
                ))}
              </select>
            </div>
            {/* Mobile period filter */}
            <div className="flex items-center gap-2">
              <label className="text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wide whitespace-nowrap">Period</label>
              <DateRangePicker
                startDate={periodStart}
                endDate={periodEnd}
                minDate={dateBounds.min}
                maxDate={dateBounds.max}
                onChange={handlePeriodChange}
              />
            </div>
          </div>
          <div className="flex items-center gap-2 mt-3">
            <button
              onClick={handleRunForecast}
              disabled={forecastUids.length === 0 || forecastJobStatus === 'running' || forecastJobStatus === 'pending'}
              className={`flex-1 px-3 py-2 rounded-lg text-sm font-semibold transition-all ${
                forecastUids.length === 0 || forecastJobStatus === 'running' || forecastJobStatus === 'pending'
                  ? 'bg-gray-200 dark:bg-gray-700 text-gray-400 dark:text-gray-500 cursor-not-allowed'
                  : 'bg-indigo-600 text-white hover:bg-indigo-700 active:scale-95 shadow-sm'
              }`}
            >
              {forecastJobStatus === 'running' || forecastJobStatus === 'pending' ? (
                <span className="flex items-center justify-center gap-1.5">
                  <svg className="animate-spin w-4 h-4" viewBox="0 0 24 24" fill="none">
                    <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"/>
                    <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8z"/>
                  </svg>
                  Running...
                </span>
              ) : (
                <span className="flex items-center justify-center gap-1.5">
                  <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth="2">
                    <path strokeLinecap="round" strokeLinejoin="round" d="M5 3l14 9-14 9V3z"/>
                  </svg>
                  Run Forecast
                </span>
              )}
            </button>
            {forecastJobStatus === 'success' && (
              <span className="text-emerald-600 text-xs font-medium">{'\u2713'} Done</span>
            )}
            {forecastJobStatus === 'error' && (
              <span className="text-red-600 text-xs font-medium">{'\u2717'} Failed</span>
            )}
          </div>
          {/* ── Mobile progress bar ── */}
          {(forecastJobStatus === 'running' || forecastJobStatus === 'pending') && (() => {
            const p = forecastProgress || {};
            const overallPct = p.overall_pct || 0;
            const stepLabel = { forecast: 'Forecasting', backtest: 'Backtesting', 'best-method': 'Selecting best', loading: 'Loading data' }[p.current_step] || 'Starting';
            let etaText = '';
            if (overallPct > 2 && forecastStartedAt) {
              const elapsed = (Date.now() - new Date(forecastStartedAt).getTime()) / 1000;
              const remaining = elapsed * (100 - overallPct) / overallPct;
              if (remaining < 60) etaText = `~${Math.round(remaining)}s left`;
              else if (remaining < 3600) etaText = `~${Math.round(remaining / 60)}m left`;
              else etaText = `~${(remaining / 3600).toFixed(1)}h left`;
            }
            const stepDetail = p.completed != null && p.total != null && p.total > 1
              ? ` (${p.completed}/${p.total} series)` : '';
            return (
              <div className="mt-2">
                <div className="flex items-center justify-between text-xs mb-1">
                  <span className="font-medium text-indigo-700 dark:text-indigo-300">
                    {stepLabel}{stepDetail}
                  </span>
                  <span className="text-gray-500 dark:text-gray-400 tabular-nums">
                    {overallPct > 0 ? `${overallPct}%` : ''}
                    {etaText ? ` \u00b7 ${etaText}` : ''}
                  </span>
                </div>
                <div className="h-2 bg-gray-200 dark:bg-gray-700 rounded-full overflow-hidden">
                  {overallPct > 0 ? (
                    <div
                      className="h-full bg-indigo-500 rounded-full transition-all duration-700 ease-out"
                      style={{ width: `${Math.min(overallPct, 100)}%` }}
                    />
                  ) : (
                    <div className="h-full bg-indigo-400/60 rounded-full animate-pulse" style={{ width: '100%' }} />
                  )}
                </div>
              </div>
            );
          })()}
          {isMultiMode && (
            <div className="mt-2 text-xs text-gray-400 dark:text-gray-500">
              <span className="bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300 px-2 py-0.5 rounded font-medium">
                Multi-series: {multiSeriesData?.uids?.length} series
              </span>
              {multiLoading && <span className="ml-2 text-blue-500 animate-pulse">Loading...</span>}
            </div>
          )}
          {!isMultiMode && selectedItem && selectedSite && (
            <div className="mt-2 text-xs text-gray-400 dark:text-gray-500">
              <span className="font-mono font-medium text-gray-600 dark:text-gray-300">{itemNameMap[selectedItem] ?? selectedItem}@{siteNameMap[selectedSite] ?? selectedSite}</span>
            </div>
          )}
        </div>

        {/* ── Browse all series (collapsible table) ── */}
        <div className="border-t border-gray-100 dark:border-gray-700">
          <button
            onClick={() => { setSeriesTableOpen(o => !o); setSeriesTablePage(0); }}
            className="w-full flex items-center justify-between px-4 py-2 text-xs text-gray-500 dark:text-gray-400 hover:bg-gray-50 dark:hover:bg-gray-700/50 transition-colors"
          >
            <span className="flex items-center gap-1.5 font-medium">
              <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth="2">
                <path strokeLinecap="round" strokeLinejoin="round" d="M4 6h16M4 10h16M4 14h16M4 18h16"/>
              </svg>
              Browse all series
              <span className="bg-gray-200 dark:bg-gray-600 text-gray-600 dark:text-gray-300 px-1.5 py-0.5 rounded-full font-mono text-[10px]">
                {filteredSeriesList.length}
              </span>
            </span>
            <svg className={`w-3.5 h-3.5 transition-transform ${seriesTableOpen ? 'rotate-180' : ''}`} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth="2">
              <path strokeLinecap="round" strokeLinejoin="round" d="M19 9l-7 7-7-7"/>
            </svg>
          </button>

          {seriesTableOpen && (() => {
            const COLS = [
              { key: '_item', label: 'Item' },
              { key: '_site', label: 'Site' },
              { key: 'n_observations', label: 'Obs' },
              { key: 'complexity_level', label: 'Complexity' },
              { key: 'is_intermittent', label: 'Interm.' },
              { key: 'has_seasonality', label: 'Seasonal' },
              { key: 'best_method', label: 'Best Method' },
            ];
            const totalPages = Math.ceil(seriesTableRows.length / SERIES_TABLE_PAGE_SIZE);
            const pageRows = seriesTableRows.slice(
              seriesTablePage * SERIES_TABLE_PAGE_SIZE,
              (seriesTablePage + 1) * SERIES_TABLE_PAGE_SIZE
            );
            const toggleSort = (col) => {
              setSeriesTableSort(prev =>
                prev.col === col
                  ? { col, dir: prev.dir === 'asc' ? 'desc' : 'asc' }
                  : { col, dir: 'asc' }
              );
              setSeriesTablePage(0);
            };
            return (
              <div className="px-4 pb-3">
                {/* Search */}
                <div className="mb-2">
                  <input
                    type="text"
                    value={seriesTableSearch}
                    onChange={e => { setSeriesTableSearch(e.target.value); setSeriesTablePage(0); }}
                    placeholder="Search by unique_id…"
                    className="w-full px-3 py-1.5 text-xs border border-gray-200 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-800 dark:text-gray-100 focus:outline-none focus:ring-1 focus:ring-indigo-400"
                  />
                </div>
                {/* Table */}
                <div className="overflow-x-auto rounded border border-gray-200 dark:border-gray-700">
                  <table className="w-full text-xs">
                    <thead className="bg-gray-50 dark:bg-gray-700/60">
                      <tr>
                        {COLS.map(c => (
                          <th
                            key={c.key}
                            onClick={() => toggleSort(c.key)}
                            className="px-2 py-1.5 text-left font-medium text-gray-500 dark:text-gray-400 cursor-pointer hover:text-gray-700 dark:hover:text-gray-200 select-none whitespace-nowrap"
                          >
                            {c.label}
                            {seriesTableSort.col === c.key && (
                              <span className="ml-0.5">{seriesTableSort.dir === 'asc' ? '↑' : '↓'}</span>
                            )}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-100 dark:divide-gray-700">
                      {pageRows.length === 0 ? (
                        <tr>
                          <td colSpan={COLS.length} className="px-2 py-3 text-center text-gray-400 dark:text-gray-500">No series found</td>
                        </tr>
                      ) : pageRows.map(s => {
                        const isActive = s.unique_id === decodedId;
                        return (
                          <tr
                            key={s.unique_id}
                            onClick={() => { navigate(`/series/${encodeURIComponent(s.unique_id)}`); setSeriesTableOpen(false); }}
                            className={`cursor-pointer transition-colors ${isActive
                              ? 'bg-indigo-50 dark:bg-indigo-900/30 font-semibold'
                              : 'hover:bg-gray-50 dark:hover:bg-gray-700/40'}`}
                          >
                            <td className="px-2 py-1.5 font-medium text-blue-600 dark:text-blue-400 whitespace-nowrap">
                              {isActive && <span className="text-indigo-500 mr-1">▶</span>}
                              {s.item_name ?? parseUniqueId(s.unique_id).item}
                            </td>
                            <td className="px-2 py-1.5 text-gray-500 dark:text-gray-400 whitespace-nowrap">{s.site_name ?? parseUniqueId(s.unique_id).site}</td>
                            <td className="px-2 py-1.5 text-gray-500 dark:text-gray-400 text-right">{s.n_observations ?? '—'}</td>
                            <td className="px-2 py-1.5">
                              {s.complexity_level ? (
                                <span className={`px-1.5 py-0.5 rounded text-[10px] font-medium ${
                                  s.complexity_level === 'high' ? 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400' :
                                  s.complexity_level === 'medium' ? 'bg-yellow-100 text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-400' :
                                  'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400'
                                }`}>{s.complexity_level}</span>
                              ) : '—'}
                            </td>
                            <td className="px-2 py-1.5 text-center">
                              {s.is_intermittent ? <span className="text-amber-500">✓</span> : <span className="text-gray-300 dark:text-gray-600">—</span>}
                            </td>
                            <td className="px-2 py-1.5 text-center">
                              {s.has_seasonality ? <span className="text-violet-500">✓</span> : <span className="text-gray-300 dark:text-gray-600">—</span>}
                            </td>
                            <td className="px-2 py-1.5 text-gray-500 dark:text-gray-400 font-mono whitespace-nowrap">{s.best_method ?? '—'}</td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
                {/* Pagination */}
                {totalPages > 1 && (
                  <div className="flex items-center justify-between mt-2 text-xs text-gray-400 dark:text-gray-500">
                    <span>{seriesTableRows.length} series · page {seriesTablePage + 1}/{totalPages}</span>
                    <div className="flex gap-1">
                      <button onClick={() => setSeriesTablePage(p => Math.max(0, p - 1))} disabled={seriesTablePage === 0}
                        className="px-2 py-0.5 rounded border border-gray-200 dark:border-gray-600 disabled:opacity-40 hover:bg-gray-100 dark:hover:bg-gray-700">‹</button>
                      <button onClick={() => setSeriesTablePage(p => Math.min(totalPages - 1, p + 1))} disabled={seriesTablePage >= totalPages - 1}
                        className="px-2 py-0.5 rounded border border-gray-200 dark:border-gray-600 disabled:opacity-40 hover:bg-gray-100 dark:hover:bg-gray-700">›</button>
                    </div>
                  </div>
                )}
              </div>
            );
          })()}
        </div>
      </div>

      {/* Header — hidden in multi-series mode */}
      {!isMultiMode && (
        <div id="tsv-header" className="mb-6">
          <h1 className="text-2xl sm:text-3xl font-bold mb-3 dark:text-white">
            {characteristics?.item_name ?? itemNameMap[parseUniqueId(decodedId).item] ?? parseUniqueId(decodedId).item}
            <span className="text-gray-400 dark:text-gray-500 font-normal mx-1">@</span>
            {characteristics?.site_name ?? siteNameMap[parseUniqueId(decodedId).site] ?? parseUniqueId(decodedId).site}
          </h1>
          {characteristics && (
            <div className="flex flex-wrap gap-2 text-sm">
              <span className="bg-gray-100 dark:bg-gray-700 dark:text-gray-300 px-3 py-1 rounded-full">{characteristics.n_observations} observations</span>
              <span className={`px-3 py-1 rounded-full ${characteristics.is_intermittent ? 'bg-amber-100 dark:bg-amber-900/30 text-amber-800 dark:text-amber-300' : 'bg-gray-100 dark:bg-gray-700 dark:text-gray-300'}`}>{characteristics.is_intermittent ? 'Intermittent' : 'Continuous'}</span>
              <span className={`px-3 py-1 rounded-full ${characteristics.has_seasonality ? 'bg-blue-100 dark:bg-blue-900/30 text-blue-800 dark:text-blue-300' : 'bg-gray-100 dark:bg-gray-700 dark:text-gray-300'}`}>{characteristics.has_seasonality ? 'Seasonal' : 'Non-Seasonal'}</span>
              <span className={`px-3 py-1 rounded-full ${characteristics.has_trend ? 'bg-purple-100 dark:bg-purple-900/30 text-purple-800 dark:text-purple-300' : 'bg-gray-100 dark:bg-gray-700 dark:text-gray-300'}`}>{characteristics.has_trend ? 'Trending' : 'Stationary'}</span>
              <span className={`px-3 py-1 rounded-full font-medium ${characteristics.complexity_level === 'high' ? 'bg-red-100 dark:bg-red-900/30 text-red-800 dark:text-red-300' : characteristics.complexity_level === 'medium' ? 'bg-yellow-100 dark:bg-yellow-900/30 text-yellow-800 dark:text-yellow-300' : 'bg-green-100 dark:bg-green-900/30 text-green-800 dark:text-green-300'}`}>{characteristics.complexity_level} complexity</span>
              {hasOutlierCorrections && <span className="bg-orange-100 dark:bg-orange-900/30 text-orange-800 dark:text-orange-300 px-3 py-1 rounded-full font-semibold">{nOutliers} outlier{nOutliers !== 1 ? 's' : ''} adjusted</span>}
              {bestMethod && <span className="bg-emerald-100 dark:bg-emerald-900/30 text-emerald-800 dark:text-emerald-300 px-3 py-1 rounded-full font-semibold">Winner: {bestMethod.best_method}</span>}
            </div>
          )}
        </div>
      )}

      {/* ── Draggable sections — rendered in user-defined order ── */}
      {(() => {
        // Helper: returns drag props for a section
        const dp = (id) => ({
          dragId: id,
          dragOver: dragOverId,
          onDragStart: handleDragStart,
          onDragOver: handleDragOver,
          onDrop: handleDrop,
          onDragEnd: handleDragEnd,
        });

        // Build map of section id → JSX (null = not applicable, skip)
        const sectionNodes = {};

        /* toggles — hidden in multi-series mode */
        sectionNodes['toggles'] = (activeForecasts.length > 0 && !isMultiMode) ? (
          <Section key="toggles" id="tsv-toggles" title="Method Toggles" storageKey="tsv_toggles_open" {...dp('toggles')}>
            <p className="text-xs text-gray-400 dark:text-gray-500 mb-2">Click a method to show/hide its line. Click the <span className="inline-block align-middle" style={{width:14,height:10,background:'rgba(99,102,241,0.25)',borderRadius:2,border:'1px solid rgba(99,102,241,0.5)'}}></span> icon to toggle its confidence bands.</p>
            <div className="flex flex-wrap gap-2">
              {activeForecasts.map(f => {
                const isVis = visibleMethods[f.method] !== false;
                const hasBands = f.quantiles && Object.keys(f.quantiles).length > 1;
                const bandOn = bandVisibleMethods[f.method] === true;
                return (
                  <div key={f.method} className="flex items-center gap-0.5">
                    <button onClick={() => toggleMethod(f.method)}
                      className={`px-3 py-1.5 min-w-[4rem] text-sm font-medium border-2 transition-all ${hasBands ? 'rounded-l-full' : 'rounded-full'} ${isVis ? 'text-white border-transparent' : 'bg-white dark:bg-gray-700 text-gray-400 dark:text-gray-500 border-gray-200 dark:border-gray-600'}`}
                      style={isVis ? { backgroundColor: getMethodColor(f.method), borderColor: getMethodColor(f.method) } : {}}>
                      {f.method}{bestMethod?.best_method === f.method && ' \u2605'}
                    </button>
                    {hasBands && (
                      <button onClick={() => toggleBand(f.method)}
                        title={bandOn ? `Hide confidence bands for ${f.method}` : `Show confidence bands for ${f.method}`}
                        className={`px-1.5 py-1.5 text-xs font-medium border-2 rounded-r-full transition-all ${bandOn ? 'text-white border-transparent' : 'bg-white dark:bg-gray-700 border-gray-200 dark:border-gray-600'}`}
                        style={bandOn ? { backgroundColor: getMethodColor(f.method), borderColor: getMethodColor(f.method), opacity: 0.7 } : { color: '#9ca3af' }}>
                        <svg width="16" height="14" viewBox="0 0 16 14" fill="none" xmlns="http://www.w3.org/2000/svg">
                          <path d="M0 10 Q4 2 8 7 Q12 12 16 4" stroke={bandOn ? 'white' : '#9ca3af'} strokeWidth="1.5" fill="none"/>
                          <path d="M0 10 Q4 2 8 7 Q12 12 16 4 L16 10 Q12 14 8 11 Q4 8 0 14 Z" fill={bandOn ? 'rgba(255,255,255,0.4)' : 'rgba(156,163,175,0.2)'}/>
                        </svg>
                      </button>
                    )}
                  </div>
                );
              })}
            </div>
            <div className="flex gap-3 mt-2">
              <button onClick={() => { const bv = {}; activeForecasts.forEach(f => { bv[f.method] = true; }); setBandVisibleMethods(bv); }}
                className="text-xs text-blue-500 hover:text-blue-700 dark:hover:text-blue-300 hover:underline">Show all bands</button>
              <button onClick={() => { const bv = {}; activeForecasts.forEach(f => { bv[f.method] = false; }); setBandVisibleMethods(bv); }}
                className="text-xs text-gray-400 dark:text-gray-500 hover:text-gray-600 dark:hover:text-gray-300 hover:underline">Hide all bands</button>
            </div>
          </Section>
        ) : null;

        /* outlier — hidden in multi-series mode */
        sectionNodes['outlier'] = (hasOutlierCorrections && outlierChartSpec && !isMultiMode) ? (
          <Section key="outlier" title="Demand Before & After Correction" storageKey="tsv_outlier_open" badge={`${nOutliers} outlier${nOutliers !== 1 ? 's' : ''}`} {...dp('outlier')}>
            <p className="text-sm text-gray-500 dark:text-gray-400 mb-4">
              Detected via <span className="font-medium">{outlierInfo?.detection_method || 'IQR'}</span>, corrected with <span className="font-medium">{outlierInfo?.correction_method || 'clip'}</span>.
              Gray dashed = original, blue solid = corrected, red dots = outlier points.
            </p>
            <div className="w-full overflow-x-auto"><Plot data={outlierChartSpec.data} layout={outlierChartSpec.layout} config={plotlyConfig} useResizeHandler style={{width:'100%'}} /></div>
            <ZoomSlider dates={outlierDates} start={outlierZoomStart} end={outlierZoomEnd} onStartChange={setOutlierZoomStart} onEndChange={setOutlierZoomEnd} />
          </Section>
        ) : null;

        /* main_chart */
        sectionNodes['main_chart'] = (
          <Section key="main_chart" id="tsv-main-chart" title={`Historical Data & Forecasts${horizonLength ? ` (${horizonLength}-${periodLabel} horizon)` : ''}`} storageKey="tsv_main_chart_open" {...dp('main_chart')}>
            <p className="text-sm text-gray-500 dark:text-gray-400 mb-4">Shaded bands: 50% (dark) and 90% (light) prediction intervals.</p>
            {mainChartSpec ? (
              <div className="w-full overflow-x-auto"><Plot data={mainChartSpec.data} layout={mainChartSpec.layout} config={plotlyConfig} useResizeHandler style={{width:'100%'}} /></div>
            ) : <div className="text-gray-400 dark:text-gray-500 py-8 text-center">No data available</div>}
            <ZoomSlider dates={allDates} start={zoomStart} end={zoomEnd} onStartChange={setZoomStart} onEndChange={setZoomEnd} />
          </Section>
        );

        /* rationale — hidden in multi-series mode */
        if (methodExplanation && !isMultiMode) {
          const chars = methodExplanation.characteristics || {};
          const acf   = methodExplanation.acf  || { lags: [], values: [], ci_upper: [], ci_lower: [] };
          const pacf  = methodExplanation.pacf || { lags: [], values: [] };

          const CorrelogramChart = ({ lags, values, ciUpper, ciLower, label, color }) => {
            if (!lags || lags.length === 0) return <p className="text-xs text-gray-400 italic">Not enough data to compute {label}.</p>;
            const W = 340, H = 110, padL = 28, padB = 20, padT = 10, padR = 8;
            const innerW = W - padL - padR;
            const innerH = H - padT - padB;
            const n = lags.length;
            const allVals = [...values, ...(ciUpper || []).map((u, i) => values[i] + u), ...(ciLower || []).map((l, i) => values[i] - l)];
            const yMin = Math.min(-0.5, ...allVals);
            const yMax = Math.max( 0.5, ...allVals);
            const yRange = yMax - yMin || 1;
            const toX = (i) => padL + (i + 0.5) * (innerW / n);
            const toY = (v) => padT + (1 - (v - yMin) / yRange) * innerH;
            const y0 = toY(0);
            const barW = Math.max(2, innerW / n - 2);
            const sigBand = 1.96 / Math.sqrt(values.length + 1);
            const ySigPos = toY(sigBand);
            const ySigNeg = toY(-sigBand);
            return (
              <svg width={W} height={H} className="overflow-visible">
                <rect x={padL} y={ySigPos} width={innerW} height={ySigNeg - ySigPos} fill={isDark ? '#1e3a5f' : '#dbeafe'} fillOpacity={0.5} />
                <line x1={padL} x2={padL + innerW} y1={ySigPos} y2={ySigPos} stroke={isDark ? '#3b82f6' : '#93c5fd'} strokeWidth={1} strokeDasharray="3,2"/>
                <line x1={padL} x2={padL + innerW} y1={ySigNeg} y2={ySigNeg} stroke={isDark ? '#3b82f6' : '#93c5fd'} strokeWidth={1} strokeDasharray="3,2"/>
                <line x1={padL} x2={padL + innerW} y1={y0} y2={y0} stroke={isDark ? '#6b7280' : '#94a3b8'} strokeWidth={1}/>
                {values.map((v, i) => {
                  const x  = toX(i) - barW / 2;
                  const yv = toY(v);
                  const significant = Math.abs(v) > sigBand;
                  return (
                    <g key={i}>
                      <rect x={x} y={Math.min(yv, y0)} width={barW} height={Math.abs(yv - y0)}
                            fill={significant ? color : (isDark ? '#6b7280' : '#cbd5e1')} fillOpacity={0.85} rx={1}/>
                      <title>Lag {lags[i]}: {v.toFixed(3)}</title>
                    </g>
                  );
                })}
                {[-0.5, 0, 0.5, 1].filter(v => v >= yMin && v <= yMax).map(v => (
                  <g key={v}>
                    <line x1={padL - 3} x2={padL} y1={toY(v)} y2={toY(v)} stroke={isDark ? '#6b7280' : '#94a3b8'} strokeWidth={1}/>
                    <text x={padL - 5} y={toY(v) + 3.5} textAnchor="end" fontSize={8} fill={isDark ? '#9ca3af' : '#64748b'}>{v}</text>
                  </g>
                ))}
                {lags.map((lg, i) => i % 2 === 0 && (
                  <text key={i} x={toX(i)} y={H - 4} textAnchor="middle" fontSize={8} fill={isDark ? '#9ca3af' : '#64748b'}>{lg}</text>
                ))}
                <text x={padL} y={padT - 2} fontSize={9} fontWeight="600" fill={isDark ? '#d1d5db' : '#475569'}>{label}</text>
              </svg>
            );
          };

          const GaugeBar = ({ value, max = 1, color, bgColor, height = 8 }) => {
            const pct = Math.min(1, Math.max(0, value / max)) * 100;
            const bg = bgColor || (isDark ? '#374151' : '#e5e7eb');
            return (
              <div style={{ background: bg, borderRadius: 4, height, overflow: 'hidden', width: '100%' }}>
                <div style={{ width: `${pct}%`, height: '100%', background: color, borderRadius: 4, transition: 'width 0.4s' }} />
              </div>
            );
          };

          // Map dark-hostile hex colors to lighter variants for dark mode
          const darkSafe = (c) => {
            if (!isDark || !c) return c;
            const map = { '#111827': '#f3f4f6', '#374151': '#d1d5db', '#1e293b': '#e2e8f0', '#6b7280': '#9ca3af' };
            return map[c.toLowerCase()] || c;
          };

          const StatCard = ({ label, value, sub, color, badge, badgeColor, gauge, gaugeMax, gaugeColor }) => (
            <div className="bg-gray-50 dark:bg-gray-700/50 border border-gray-200 dark:border-gray-600 rounded-lg p-3 flex flex-col gap-1.5">
              <div className="flex items-center justify-between gap-1">
                <span className="text-xs text-gray-500 dark:text-gray-400 font-medium">{label}</span>
                {badge && (
                  <span className={`text-xs px-1.5 py-0.5 rounded-full font-semibold ${badgeColor || 'bg-gray-100 dark:bg-gray-600 text-gray-600 dark:text-gray-300'}`}>{badge}</span>
                )}
              </div>
              <div className="flex items-baseline gap-1.5">
                <span className="text-lg font-bold" style={{ color: darkSafe(color) || (isDark ? '#f3f4f6' : '#111827') }}>{value}</span>
                {sub && <span className="text-xs text-gray-400 dark:text-gray-500">{sub}</span>}
              </div>
              {gauge !== undefined && (
                <GaugeBar value={gauge} max={gaugeMax || 1} color={gaugeColor || '#6366f1'} />
              )}
            </div>
          );

          const complexityColor = chars.complexity_level === 'high' ? '#dc2626' : chars.complexity_level === 'medium' ? '#d97706' : '#16a34a';
          const complexityBadgeColor = chars.complexity_level === 'high' ? 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400' : chars.complexity_level === 'medium' ? 'bg-yellow-100 text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-400' : 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400';
          const adfColor = chars.adf_pvalue <= 0.05 ? '#16a34a' : '#dc2626';
          const adfBadge = chars.is_stationary ? 'Stationary' : 'Non-stationary';
          const adfBadgeColor = chars.is_stationary ? 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400' : 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400';
          const trendBadgeColor = chars.has_trend ? 'bg-orange-100 text-orange-700 dark:bg-orange-900/30 dark:text-orange-400' : 'bg-gray-100 text-gray-500 dark:bg-gray-700 dark:text-gray-400';
          const seasonalBadgeColor = chars.has_seasonality ? 'bg-violet-100 text-violet-700 dark:bg-violet-900/30 dark:text-violet-400' : 'bg-gray-100 text-gray-500 dark:bg-gray-700 dark:text-gray-400';
          const intermittentBadgeColor = chars.is_intermittent ? 'bg-amber-100 text-amber-700 dark:bg-amber-900/30 dark:text-amber-400' : 'bg-emerald-100 text-emerald-700 dark:bg-emerald-900/30 dark:text-emerald-400';
          const cvLabel = chars.mean > 0 ? formatNumber(chars.std / chars.mean, locale, 2) : '—';

          sectionNodes['rationale'] = (
            <Section key="rationale" title="Method Selection Rationale" storageKey="tsv_rationale_open" defaultOpen={false} {...dp('rationale')}>
              {/* ── Demand Characteristics Grid ── */}
              <div className="mb-5">
                <h3 className="text-sm font-semibold text-gray-700 dark:text-gray-200 mb-3 flex items-center gap-2">
                  Demand Characteristics
                  <span className="text-xs font-normal text-gray-400 dark:text-gray-500">All signals used to select forecasting methods</span>
                </h3>
                <div className="grid grid-cols-2 sm:grid-cols-4 gap-2 mb-2">
                  <StatCard label="Observations" value={chars.n_observations} sub={`${chars.date_range_start?.slice(0,7)} → ${chars.date_range_end?.slice(0,7)}`} color="#111827" />
                  <StatCard label="Mean Demand" value={chars.mean != null ? formatNumber(chars.mean, locale, 1) : '—'} sub="units/period" color="#2563eb" />
                  <StatCard label="Std Deviation" value={chars.std != null ? formatNumber(chars.std, locale, 1) : '—'} sub="units/period" color="#7c3aed" />
                  <StatCard label="Coeff. of Variation" value={cvLabel} sub="σ / μ  (volatility)" color={parseFloat(cvLabel) > 1 ? '#dc2626' : parseFloat(cvLabel) > 0.5 ? '#d97706' : '#16a34a'} gauge={Math.min(parseFloat(cvLabel) || 0, 2)} gaugeMax={2} gaugeColor={parseFloat(cvLabel) > 1 ? '#dc2626' : parseFloat(cvLabel) > 0.5 ? '#d97706' : '#16a34a'} />
                </div>
                <div className="grid grid-cols-2 sm:grid-cols-4 gap-2 mb-2">
                  <StatCard label="Zero Ratio" value={formatPercent((chars.zero_ratio || 0) * 100, locale, 1)} sub="% periods with zero demand" color={chars.zero_ratio > 0.5 ? '#dc2626' : chars.zero_ratio > 0.2 ? '#d97706' : '#374151'} badge={chars.is_intermittent ? 'Intermittent' : 'Continuous'} badgeColor={intermittentBadgeColor} gauge={chars.zero_ratio || 0} gaugeMax={1} gaugeColor={chars.zero_ratio > 0.5 ? '#dc2626' : chars.zero_ratio > 0.2 ? '#d97706' : '#6b7280'} />
                  <StatCard label="ADI" value={formatNumber(chars.adi || 0, locale, 2)} sub="Avg Demand Interval (periods)" color={chars.adi > 1.32 ? '#dc2626' : '#374151'} gauge={Math.min(chars.adi || 0, 5)} gaugeMax={5} gaugeColor={chars.adi > 1.32 ? '#f59e0b' : '#6b7280'} />
                  <StatCard label="CoV (non-zero)" value={formatNumber(chars.cov || 0, locale, 2)} sub="Coeff. of Variation of demand sizes" color={chars.cov > 0.49 ? '#d97706' : '#374151'} gauge={Math.min(chars.cov || 0, 2)} gaugeMax={2} gaugeColor={chars.cov > 0.49 ? '#f59e0b' : '#6b7280'} />
                  <div className="bg-gray-50 dark:bg-gray-700/50 border border-gray-200 dark:border-gray-600 rounded-lg p-3 flex flex-col gap-1.5">
                    <span className="text-xs text-gray-500 dark:text-gray-400 font-medium">Demand Pattern</span>
                    <div className="flex flex-col gap-1 mt-1">
                      <div className="flex items-center gap-2">
                        <span className={`w-2 h-2 rounded-full flex-shrink-0 ${chars.is_intermittent ? 'bg-amber-400' : 'bg-emerald-400'}`}/>
                        <span className="text-xs font-semibold dark:text-gray-200">{chars.is_intermittent ? 'Intermittent' : 'Continuous'}</span>
                      </div>
                      <div className="text-xs text-gray-400 dark:text-gray-500">ADI &gt; 1.32 or &lt; 5 demand periods → intermittent</div>
                    </div>
                  </div>
                </div>
                <div className="grid grid-cols-2 sm:grid-cols-4 gap-2 mb-2">
                  <StatCard label="Trend" value={chars.has_trend ? `${chars.trend_direction === 'up' ? '↑' : '↓'} ${chars.trend_direction}` : 'None'} sub={`Kendall's τ = ${formatNumber(chars.trend_strength || 0, locale, 3)}`} color={chars.has_trend ? '#ea580c' : '#6b7280'} badge={chars.has_trend ? 'Trending' : 'No trend'} badgeColor={trendBadgeColor} gauge={chars.trend_strength || 0} gaugeMax={1} gaugeColor={chars.has_trend ? '#ea580c' : '#d1d5db'} />
                  <StatCard label="Seasonality" value={chars.has_seasonality ? `Periods: ${(chars.seasonal_periods || []).join(', ')}` : 'None detected'} sub={`ACF strength: ${formatNumber(chars.seasonal_strength || 0, locale, 3)}`} color={chars.has_seasonality ? '#7c3aed' : '#6b7280'} badge={chars.has_seasonality ? 'Seasonal' : 'Non-seasonal'} badgeColor={seasonalBadgeColor} gauge={chars.seasonal_strength || 0} gaugeMax={1} gaugeColor={chars.has_seasonality ? '#7c3aed' : '#d1d5db'} />
                  <StatCard label="ADF p-value" value={formatNumber(chars.adf_pvalue != null ? chars.adf_pvalue : 1, locale, 4)} sub="Augmented Dickey-Fuller test" color={adfColor} badge={adfBadge} badgeColor={adfBadgeColor} gauge={Math.max(0, 1 - (chars.adf_pvalue || 1))} gaugeMax={1} gaugeColor={adfColor} />
                  <StatCard label="Complexity Score" value={formatNumber(chars.complexity_score || 0, locale, 3)} sub="0 = simple · 1 = highly complex" color={complexityColor} badge={`${chars.complexity_level} complexity`} badgeColor={complexityBadgeColor} gauge={chars.complexity_score || 0} gaugeMax={1} gaugeColor={complexityColor} />
                </div>
                <div className="grid grid-cols-2 sm:grid-cols-4 gap-2">
                  <div className="col-span-2 sm:col-span-2 bg-gray-50 dark:bg-gray-700/50 border border-gray-200 dark:border-gray-600 rounded-lg p-3">
                    <span className="text-xs text-gray-500 dark:text-gray-400 font-medium block mb-2">Data Sufficiency</span>
                    <div className="flex flex-col gap-1.5">
                      {[
                        { label: 'Statistical models', ok: true, note: 'Always available' },
                        { label: 'Sparse check (obs/year)', ok: !chars.is_sparse, note: `${chars.obs_per_year != null ? formatNumber(chars.obs_per_year, locale, 1) : '—'} obs/yr — threshold < ${chars.sparse_obs_per_year_threshold ?? 5}` },
                        { label: 'ML models (LightGBM, XGBoost)', ok: chars.sufficient_for_ml, note: `≥${chars.min_for_ml ?? 100} obs — has ${chars.n_observations}` },
                        { label: 'Deep Learning (NHITS, NBEATS…)', ok: chars.sufficient_for_deep_learning, note: `≥${chars.min_for_dl ?? 200} obs — has ${chars.n_observations}` },
                      ].map(({ label, ok, note }) => (
                        <div key={label} className="flex items-center gap-2">
                          <span className={`flex-shrink-0 w-4 h-4 rounded-full flex items-center justify-center text-xs ${ok ? 'bg-emerald-100 dark:bg-emerald-900/30 text-emerald-700 dark:text-emerald-400' : 'bg-red-100 dark:bg-red-900/30 text-red-500 dark:text-red-400'}`}>
                            {ok ? '✓' : '✗'}
                          </span>
                          <span className="text-xs font-medium text-gray-700 dark:text-gray-300">{label}</span>
                          <span className="text-xs text-gray-400 dark:text-gray-500 ml-auto">{note}</span>
                        </div>
                      ))}
                    </div>
                  </div>
                  <div className="col-span-2 sm:col-span-2 bg-blue-50 dark:bg-blue-900/20 border border-blue-100 dark:border-blue-800 rounded-lg p-3">
                    <span className="text-xs font-medium text-blue-700 dark:text-blue-300 block mb-1">
                      Selection Category: <span className="font-bold">{methodExplanation.selection_category}</span>
                    </span>
                    <p className="text-xs text-blue-600 dark:text-blue-400 leading-relaxed">{methodExplanation.selection_reason}</p>
                  </div>
                </div>
              </div>

              {/* ── ACF + PACF charts ── */}
              {(acf.lags.length > 0 || pacf.lags.length > 0) && (
                <div className="mb-5">
                  <h3 className="text-sm font-semibold text-gray-700 dark:text-gray-200 mb-3 flex items-center gap-2">
                    Autocorrelation Analysis
                    <span className="text-xs font-normal text-gray-400 dark:text-gray-500">Bars outside blue band are statistically significant (95% CI)</span>
                  </h3>
                  <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                    <div className="bg-gray-50 dark:bg-gray-700/50 rounded-lg p-3 border border-gray-200 dark:border-gray-600">
                      <p className="text-xs text-gray-500 dark:text-gray-400 mb-2 font-medium">ACF — Autocorrelation Function</p>
                      <p className="text-xs text-gray-400 dark:text-gray-500 mb-2">Spikes at regular lags → seasonal pattern. Slow decay → trend or non-stationarity.</p>
                      <div className="overflow-x-auto">
                        <CorrelogramChart lags={acf.lags} values={acf.values} ciUpper={acf.ci_upper} ciLower={acf.ci_lower} label="ACF" color="#6366f1" />
                      </div>
                    </div>
                    <div className="bg-gray-50 dark:bg-gray-700/50 rounded-lg p-3 border border-gray-200 dark:border-gray-600">
                      <p className="text-xs text-gray-500 dark:text-gray-400 mb-2 font-medium">PACF — Partial Autocorrelation Function</p>
                      <p className="text-xs text-gray-400 dark:text-gray-500 mb-2">Removes indirect lag effects. Spike only at lag k → AR(k). Helps determine ARIMA order.</p>
                      <div className="overflow-x-auto">
                        <CorrelogramChart lags={pacf.lags} values={pacf.values} ciUpper={null} ciLower={null} label="PACF" color="#0891b2" />
                      </div>
                    </div>
                  </div>
                </div>
              )}

              {/* ── Included / Excluded methods ── */}
              <div>
                <h3 className="text-sm font-semibold text-gray-700 dark:text-gray-200 mb-3">Method Eligibility</h3>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  <div>
                    <h3 className="text-sm font-semibold text-emerald-700 dark:text-emerald-400 mb-2">Applied Methods ({methodExplanation.included?.length || 0})</h3>
                    <div className="space-y-1.5">
                      {(methodExplanation.included || []).map((m, i) => (
                        <div key={i} className="flex items-start gap-2 text-sm">
                          <span className={`mt-0.5 text-xs ${m.backtest_note ? 'text-amber-500 dark:text-amber-400' : m.status === 'forecasted' ? 'text-emerald-600 dark:text-emerald-400' : 'text-amber-500 dark:text-amber-400'}`}>
                            {m.status === 'forecasted' ? '\u2713' : '\u26A0'}
                          </span>
                          <div className="min-w-0">
                            <span className="font-medium text-gray-700 dark:text-gray-300">{m.method}</span>
                            <span className="text-gray-400 dark:text-gray-500 ml-1 text-xs">{m.reason}</span>
                          </div>
                          {m.backtest_note && (
                            <span className="shrink-0 mt-0.5 cursor-help" title={m.backtest_note}>
                              <svg className="w-4 h-4 text-amber-500 dark:text-amber-400" viewBox="0 0 20 20" fill="currentColor">
                                <path fillRule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clipRule="evenodd" />
                              </svg>
                            </span>
                          )}
                        </div>
                      ))}
                    </div>
                  </div>
                  <div>
                    <h3 className="text-sm font-semibold text-red-600 dark:text-red-400 mb-2">Excluded Methods ({methodExplanation.excluded?.length || 0})</h3>
                    <div className="space-y-1">
                      {(methodExplanation.excluded || []).map((m, i) => (
                        <div key={i} className="flex items-start gap-2 text-sm">
                          <span className="mt-0.5 text-xs text-red-400">✗</span>
                          <div><span className="font-medium text-gray-600 dark:text-gray-400">{m.method}</span><span className="text-gray-400 dark:text-gray-500 ml-1 text-xs">{m.reason}</span></div>
                        </div>
                      ))}
                    </div>
                  </div>
                </div>
              </div>
            </Section>
          );
        } else {
          sectionNodes['rationale'] = null;
        }

        /* parameters — applied parameter versions for each business type */
        sectionNodes['parameters'] = (!isMultiMode && seriesParameters) ? (
          <Section key="parameters" title="Parameters Applied" storageKey="tsv_parameters_open" defaultOpen={false} {...dp('parameters')}>
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-3 p-3">
              {['characterization', 'outlier_detection', 'forecasting', 'backtesting'].map(btype => {
                const info = seriesParameters.parameters?.[btype];
                if (!info) return null;
                const btypeLabel = {
                  characterization: 'Characterization',
                  outlier_detection: 'Outlier Detection',
                  forecasting: 'Forecasting',
                  backtesting: 'Backtesting',
                }[btype];
                return (
                  <div key={btype} className="bg-gray-50 dark:bg-gray-800 rounded-lg p-3">
                    <div className="flex items-center justify-between mb-2">
                      <span className="text-xs font-semibold text-gray-700 dark:text-gray-300 uppercase tracking-wide">
                        {btypeLabel}
                      </span>
                      <span className={`text-[10px] px-1.5 py-0.5 rounded font-medium ${
                        info.source === 'segment'
                          ? 'bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-400'
                          : 'bg-gray-200 text-gray-500 dark:bg-gray-700 dark:text-gray-400'
                      }`}>
                        {info.source === 'segment' ? info.name : 'Default'}
                      </span>
                    </div>
                    <ParameterKeyValues params={info.parameters_set} />
                  </div>
                );
              })}
            </div>
          </Section>
        ) : null;

        sectionNodes['scoring'] = null;

        /* metrics — hidden in multi-series mode */
        sectionNodes['metrics'] = (activeMetrics.length > 0 && !isMultiMode) ? (
          <Section key="metrics" title={`Comprehensive Metrics Comparison${isMultiMode ? ' (weighted avg)' : ''}`} storageKey="tsv_metrics_open" {...dp('metrics')}>

            {/* ── Backtesting Configuration sliders (single-series only) ── */}
            {!isMultiMode && (() => {
              const btEdits = hpEdits['_backtesting'] || {};
              const btSaved = hpSavedOverrides['_backtesting'] || {};
              const getBtValue = (key) => {
                if (key in btEdits) return btEdits[key];
                if (key in btSaved) return btSaved[key];
                return btConfig[key];
              };
              const setBtEdit = (key, value) => {
                setHpEdits(prev => ({
                  ...prev,
                  _backtesting: { ...(prev._backtesting || {}), [key]: value }
                }));
              };
              const handleBtSave = async () => {
                const merged = { ...btSaved, ...btEdits };
                if (Object.keys(merged).length === 0) return;
                try {
                  await api.put(`/hyperparams/${encodeURIComponent(decodedId)}`, {
                    overrides: { _backtesting: merged }
                  });
                  setHpSavedOverrides(prev => ({ ...prev, _backtesting: merged }));
                  setHpEdits(prev => { const next = { ...prev }; delete next._backtesting; return next; });
                } catch (err) {
                  console.error('Failed to save backtesting overrides:', err);
                }
              };
              const handleBtReset = async () => {
                try {
                  await api.delete(`/hyperparams/${encodeURIComponent(decodedId)}?method=_backtesting`);
                  setHpSavedOverrides(prev => { const next = { ...prev }; delete next._backtesting; return next; });
                  setHpEdits(prev => { const next = { ...prev }; delete next._backtesting; return next; });
                } catch (err) {
                  console.error('Failed to reset backtesting overrides:', err);
                }
              };

              const nObs = characteristics?.n_observations || 200;
              const forecastHorizon = activeForecasts?.[0]?.hyperparameters?.horizon || 52;
              // Slider ranges — ensure value is always clamped to [min, max]
              const maxHorizon = Math.max(4, nObs - 2);
              const btHorizon = Math.min(Math.max(4, getBtValue('backtest_horizon')), maxHorizon);
              const maxWindow = Math.min(btHorizon, forecastHorizon);
              const btWindow = Math.min(Math.max(1, getBtValue('window_size')), maxWindow);
              const maxTests = Math.max(1, btHorizon - btWindow + 1);
              const btNTests = Math.min(Math.max(0, getBtValue('n_tests')), maxTests);
              const hasBtEdits = Object.keys(btEdits).length > 0;
              const hasBtSaved = Object.keys(btSaved).length > 0;

              // Compute test positions for timeline visualization
              const availableRange = Math.max(0, btHorizon - btWindow);
              let step, actualTests;
              if (btNTests <= 0 || btNTests > availableRange + 1) {
                step = 1; actualTests = availableRange + 1;
              } else if (btNTests === 1) {
                step = 0; actualTests = 1;
              } else {
                step = Math.max(1, Math.floor(availableRange / (btNTests - 1)));
                actualTests = btNTests;
              }
              const testPositions = [];
              for (let i = 0; i < actualTests && i < 30; i++) {
                const left = btHorizon > 0 ? (i * step / btHorizon) * 100 : 0;
                const width = btHorizon > 0 ? (btWindow / btHorizon) * 100 : 100;
                if (left + width > 100.5) break;
                testPositions.push({ left, width: Math.min(width, 100 - left) });
              }
              const trainPct = nObs > 0 ? Math.max(10, Math.round(((nObs - btHorizon) / nObs) * 100)) : 50;

              return (
                <div className={`mb-4 p-4 rounded-lg border ${hasBtEdits ? 'border-amber-400' : hasBtSaved ? 'border-blue-400' : 'border-gray-200 dark:border-gray-600'} bg-white dark:bg-gray-800`}>
                  <div className="flex items-center justify-between mb-3">
                    <div className="flex items-center gap-2">
                      <h4 className="text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wide">
                        Backtesting Configuration
                      </h4>
                      {hasBtEdits && <span className="text-[9px] text-amber-500 font-normal">(unsaved)</span>}
                      {hasBtSaved && !hasBtEdits && <span className="text-[9px] text-blue-400 font-normal">(custom)</span>}
                    </div>
                    <div className="flex gap-2">
                      {hasBtEdits && (
                        <button onClick={handleBtSave} disabled={hpSaving}
                          className="text-xs bg-amber-500 hover:bg-amber-600 text-white px-2.5 py-1 rounded font-medium transition-colors">
                          Save
                        </button>
                      )}
                      {(hasBtSaved || hasBtEdits) && (
                        <button onClick={handleBtReset} disabled={hpSaving}
                          className="text-xs bg-gray-200 dark:bg-gray-600 hover:bg-gray-300 dark:hover:bg-gray-500 text-gray-700 dark:text-gray-200 px-2.5 py-1 rounded font-medium transition-colors">
                          Reset
                        </button>
                      )}
                    </div>
                  </div>

                  {/* Slider 1: Backtest Horizon */}
                  <div className="mb-3">
                    <div className="flex justify-between text-xs mb-1">
                      <span className="text-gray-500 dark:text-gray-400">Backtest Horizon</span>
                      <span className="font-mono text-blue-600 dark:text-blue-400 font-semibold">{btHorizon} periods</span>
                    </div>
                    <input type="range" min={4} max={maxHorizon} step={1}
                      value={btHorizon}
                      onChange={e => {
                        const val = parseInt(e.target.value, 10);
                        setBtEdit('backtest_horizon', val);
                        // Clamp window_size if needed
                        if (getBtValue('window_size') > val)
                          setBtEdit('window_size', val);
                      }}
                      className="w-full accent-blue-500 h-2 cursor-pointer"
                      style={{ minHeight: '20px' }}
                    />
                    <div className="flex justify-between text-[9px] text-gray-400 mt-0.5">
                      <span>4</span><span>{maxHorizon}</span>
                    </div>
                  </div>

                  {/* Slider 2: Window Size */}
                  <div className="mb-3">
                    <div className="flex justify-between text-xs mb-1">
                      <span className="text-gray-500 dark:text-gray-400">Window Size</span>
                      <span className="font-mono text-indigo-600 dark:text-indigo-400 font-semibold">{btWindow} periods</span>
                    </div>
                    <input type="range" min={1} max={maxWindow} step={1}
                      value={btWindow}
                      onChange={e => setBtEdit('window_size', parseInt(e.target.value, 10))}
                      className="w-full accent-indigo-500 h-2 cursor-pointer"
                      style={{ minHeight: '20px' }}
                    />
                    <div className="flex justify-between text-[9px] text-gray-400 mt-0.5">
                      <span>1</span><span>{maxWindow}</span>
                    </div>
                  </div>

                  {/* Slider 3: Number of Tests */}
                  <div className="mb-3">
                    <div className="flex justify-between text-xs mb-1">
                      <span className="text-gray-500 dark:text-gray-400">Number of Tests</span>
                      <span className="font-mono text-amber-600 dark:text-amber-400 font-semibold">
                        {btNTests === 0 ? `Auto (${availableRange + 1} tests, step=1)` : `${btNTests} tests${actualTests > 1 ? `, step=${step}` : ''}`}
                      </span>
                    </div>
                    <input type="range" min={0} max={Math.min(maxTests, 50)} step={1}
                      value={btNTests}
                      onChange={e => setBtEdit('n_tests', parseInt(e.target.value, 10))}
                      className="w-full accent-amber-500 h-2 cursor-pointer"
                      style={{ minHeight: '20px' }}
                    />
                    <div className="flex justify-between text-[9px] text-gray-400 mt-0.5">
                      <span>0 (auto)</span><span>{Math.min(maxTests, 50)}</span>
                    </div>
                  </div>

                  {/* Visual: test placement timeline */}
                  <div className="mt-3 flex h-5 rounded-full overflow-hidden border border-gray-200 dark:border-gray-600">
                    <div className="bg-gray-200 dark:bg-gray-600 flex items-center justify-center transition-all"
                      style={{ width: `${trainPct}%`, minWidth: '30px' }}>
                      <span className="text-[8px] text-gray-500 dark:text-gray-400 font-medium truncate px-1">Train</span>
                    </div>
                    <div className="bg-blue-50 dark:bg-blue-900/20 relative flex-1 overflow-hidden">
                      {testPositions.map((pos, i) => (
                        <div key={i} className="absolute top-0 h-full bg-indigo-400/50 dark:bg-indigo-500/40 border-r border-white/50 dark:border-gray-800/50 transition-all"
                          style={{ left: `${pos.left}%`, width: `${pos.width}%` }}
                          title={`Test ${i + 1}`}
                        />
                      ))}
                      <span className="absolute inset-0 flex items-center justify-center text-[8px] text-blue-600 dark:text-blue-400 font-medium pointer-events-none">
                        {actualTests} test{actualTests !== 1 ? 's' : ''}
                      </span>
                    </div>
                  </div>
                </div>
              );
            })()}

            <div className="overflow-x-auto">
              <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-700 text-sm">
                <thead><tr className="bg-gray-50 dark:bg-gray-700/50">
                  {[
                    ['method', 'Method', false],
                    ['mae', 'MAE', true], ['rmse', 'RMSE', true],
                    ['bias', 'Bias', true], ['mape', 'MAPE', true], ['smape', 'sMAPE', true],
                    ['mase', 'MASE', true],
                    ['crps', 'CRPS', true], ['winkler_score', 'Winkler', true],
                    ['coverage_50', 'Cov50', true], ['coverage_80', 'Cov80', true],
                    ['coverage_90', 'Cov90', true], ['coverage_95', 'Cov95', true],
                    ['quantile_loss', 'QLoss', true],
                    ['n_windows', 'Win', false],
                  ].map(([field, label, sortable]) => (
                    <th key={field} onClick={sortable ? () => handleMetricsSort(field) : undefined}
                      className={`px-2 py-2 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase whitespace-nowrap ${field === 'method' ? 'text-left' : 'text-right'} ${sortable ? 'cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-600 select-none' : ''}`}>
                      {label}{sortable ? metricsSortIndicator(field) : ''}
                    </th>
                  ))}
                  {compositeRanking && (
                    <th onClick={() => handleMetricsSort('composite')}
                      className="px-2 py-2 text-right text-xs font-medium text-gray-500 dark:text-gray-400 uppercase cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-600 whitespace-nowrap select-none">
                      Score{metricsSortIndicator('composite')}
                    </th>
                  )}
                </tr></thead>
                <tbody className="divide-y divide-gray-200 dark:divide-gray-700">
                  {sortedMetrics.map((m, idx) => {
                    const isBest = bestMethod?.best_method === m.method;
                    return (
                      <tr key={idx} className={isBest ? 'bg-emerald-50 dark:bg-emerald-900/20' : ''}>
                        <td className="px-2 py-2 font-medium whitespace-nowrap text-left dark:text-gray-200">
                          <span className="inline-block w-2.5 h-2.5 rounded-full mr-1.5" style={{ backgroundColor: getMethodColor(m.method) }}></span>
                          {m.method}
                          {isBest && <span className="ml-1.5 text-xs bg-emerald-200 dark:bg-emerald-800 text-emerald-800 dark:text-emerald-200 px-1 py-0.5 rounded font-semibold">Best</span>}
                        </td>
                        {['mae', 'rmse'].map(f => (<td key={f} className={`px-2 py-2 text-right font-mono ${isBestVal(f, m[f]) ? 'text-emerald-700 dark:text-emerald-400 font-bold' : 'dark:text-gray-300'}`}>{fmtMetric(m[f])}</td>))}
                        <td className={`px-2 py-2 text-right font-mono ${isBestVal('bias', m.bias) ? 'text-emerald-700 dark:text-emerald-400 font-bold' : 'dark:text-gray-300'}`}>{fmtMetric(m.bias)}</td>
                        {['mape', 'smape'].map(f => (<td key={f} className={`px-2 py-2 text-right font-mono ${isBestVal(f, m[f]) ? 'text-emerald-700 dark:text-emerald-400 font-bold' : 'dark:text-gray-300'}`}>{m[f] != null ? formatPercent(m[f], locale, 1) : '-'}</td>))}
                        <td className={`px-2 py-2 text-right font-mono ${isBestVal('mase', m.mase) ? 'text-emerald-700 dark:text-emerald-400 font-bold' : 'dark:text-gray-300'}`}>{fmtMetric(m.mase)}</td>
                        {['crps', 'winkler_score'].map(f => (<td key={f} className={`px-2 py-2 text-right font-mono ${isBestVal(f, m[f]) ? 'text-emerald-700 dark:text-emerald-400 font-bold' : 'dark:text-gray-300'}`}>{fmtMetric(m[f])}</td>))}
                        {['coverage_50', 'coverage_80', 'coverage_90', 'coverage_95'].map(f => (<td key={f} className={`px-2 py-2 text-right font-mono ${isBestVal(f, m[f]) ? 'text-emerald-700 dark:text-emerald-400 font-bold' : 'dark:text-gray-300'}`}>{fmtMetric(m[f], true)}</td>))}
                        <td className={`px-2 py-2 text-right font-mono ${isBestVal('quantile_loss', m.quantile_loss) ? 'text-emerald-700 dark:text-emerald-400 font-bold' : 'dark:text-gray-300'}`}>{fmtMetric(m.quantile_loss)}</td>
                        <td className="px-2 py-2 text-right dark:text-gray-300">{m.n_windows}</td>
                        {compositeRanking && (<td className={`px-2 py-2 text-right font-mono font-semibold ${isBest ? 'text-emerald-700 dark:text-emerald-400' : 'dark:text-gray-300'}`}>{compositeRanking[m.method] != null ? formatNumber(compositeRanking[m.method], locale, 4) : '-'}</td>)}
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </Section>
        ) : null;

        /* hyperparameters — per-method EDITABLE parameter cards — hidden in multi-series mode */
        sectionNodes['hyperparameters'] = (activeForecasts.some(f => f.hyperparameters) && !isMultiMode) ? (
          <Section key="hyperparameters" title="Model Hyperparameters &amp; Configuration" storageKey="tsv_hyperparams_open" {...dp('hyperparameters')}>
            <p className="text-sm text-gray-500 dark:text-gray-400 mb-4">
              Edit parameters below and click <strong className="dark:text-gray-300">Save</strong> to persist. Click <strong className="dark:text-gray-300">Run Forecast</strong> to re-run with your custom values.
            </p>
            <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
              {activeForecasts.filter(f => f.hyperparameters).map(f => {
                const hp = f.hyperparameters;
                const method = f.method;
                const isBest = bestMethod?.best_method === method;
                const description = hp.description;
                const commonKeys = ['horizon', 'frequency', 'confidence_levels', 'n_observations'];
                const metaKeys = ['description', 'method_family', 'training_time_seconds', 'prediction_intervals_available', 'has_overrides', 'overrides_applied'];
                const fittedKeys = Object.keys(hp).filter(k => k.startsWith('fitted_'));
                const sliderKeys = ['val_split'];  // rendered as custom slider, not inline input
                const specificKeys = Object.keys(hp).filter(k =>
                  !commonKeys.includes(k) && !metaKeys.includes(k) && !fittedKeys.includes(k) && !sliderKeys.includes(k)
                );

                // ALL keys are now editable (including fitted)
                const editableKeys = [...commonKeys.filter(k => hp[k] !== undefined), ...specificKeys];

                // Merge: saved DB overrides → local unsaved edits → original value
                // For fitted_ keys: overrides are stored WITHOUT the prefix (e.g. "p" not "fitted_p")
                const savedOvr = hpSavedOverrides[method] || {};
                const localEdits = hpEdits[method] || {};

                const getEffectiveValue = (k) => {
                  // For fitted keys, also check the unprefixed name in overrides
                  const ovrKey = k.startsWith('fitted_') ? k.replace('fitted_', '') : k;
                  if (k in localEdits) return localEdits[k];
                  if (ovrKey in localEdits) return localEdits[ovrKey];
                  if (k in savedOvr) return savedOvr[k];
                  if (ovrKey in savedOvr) return savedOvr[ovrKey];
                  return hp[k];
                };

                const hasMethodEdits = Object.keys(localEdits).length > 0;
                const hasMethodSaved = Object.keys(savedOvr).length > 0;

                const renderVal = (v) => {
                  if (v === null || v === undefined) return <span className="text-gray-400 dark:text-gray-500">null</span>;
                  if (typeof v === 'boolean') return <span className={v ? 'text-emerald-600 dark:text-emerald-400' : 'text-red-500 dark:text-red-400'}>{v.toString()}</span>;
                  if (Array.isArray(v)) return <span className="text-indigo-600 dark:text-indigo-400">[{v.join(', ')}]</span>;
                  if (typeof v === 'number') return <span className="text-blue-700 dark:text-blue-400">{Number.isInteger(v) ? v : formatNumber(v, locale, 4)}</span>;
                  return <span className="text-gray-800 dark:text-gray-200">{String(v)}</span>;
                };

                // Inline editable input for a param
                const renderEditableParam = (k, original) => {
                  const effective = getEffectiveValue(k);
                  const isEdited = k in localEdits;
                  const isSavedOverride = k in savedOvr && !(k in localEdits);
                  const borderCls = isEdited ? 'border-amber-400' : isSavedOverride ? 'border-blue-400' : 'border-gray-200 dark:border-gray-600';

                  if (typeof original === 'boolean') {
                    return (
                      <label className="flex items-center gap-1 justify-end cursor-pointer">
                        <input type="checkbox" checked={!!effective}
                          className="accent-indigo-600 w-3.5 h-3.5"
                          onChange={e => {
                            setHpEdits(prev => ({
                              ...prev,
                              [method]: { ...(prev[method] || {}), [k]: e.target.checked }
                            }));
                          }}
                        />
                        <span className={effective ? 'text-emerald-600' : 'text-red-500'}>{String(effective)}</span>
                      </label>
                    );
                  }
                  if (typeof original === 'number') {
                    const step = Number.isInteger(original) ? 1 : 0.001;
                    return (
                      <input type="number" step={step}
                        value={effective ?? ''}
                        className={`w-full text-right font-mono text-xs border rounded px-1 py-0.5 ${borderCls} bg-white dark:bg-gray-900 dark:text-gray-200 focus:outline-none focus:ring-1 focus:ring-indigo-400`}
                        onChange={e => {
                          const val = e.target.value === '' ? null : (Number.isInteger(original) ? parseInt(e.target.value, 10) : parseFloat(e.target.value));
                          setHpEdits(prev => ({
                            ...prev,
                            [method]: { ...(prev[method] || {}), [k]: val }
                          }));
                        }}
                      />
                    );
                  }
                  if (Array.isArray(original)) {
                    const popupKey = `${method}:${k}`;
                    const isPopupOpen = openListPopup === popupKey;
                    const displayArr = Array.isArray(effective) ? effective : [];
                    return (
                      <div className="relative">
                        <button
                          onClick={() => setOpenListPopup(isPopupOpen ? null : popupKey)}
                          className={`w-full text-right font-mono text-xs border rounded px-1.5 py-0.5 ${borderCls} bg-white dark:bg-gray-900 dark:text-gray-200 hover:bg-gray-50 dark:hover:bg-gray-800 cursor-pointer truncate flex items-center justify-end gap-1`}
                          title={`[${displayArr.join(', ')}] — click to edit`}
                        >
                          <span className="truncate text-indigo-600 dark:text-indigo-400">[{displayArr.join(', ')}]</span>
                          <svg className="w-3 h-3 text-gray-400 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15.232 5.232l3.536 3.536m-2.036-5.036a2.5 2.5 0 113.536 3.536L6.5 21.036H3v-3.572L16.732 3.732z" />
                          </svg>
                        </button>
                        {isPopupOpen && (
                          <ListEditorPopup
                            values={displayArr}
                            label={k}
                            onClose={() => setOpenListPopup(null)}
                            onChange={(newArr) => {
                              setHpEdits(prev => ({
                                ...prev,
                                [method]: { ...(prev[method] || {}), [k]: newArr }
                              }));
                            }}
                          />
                        )}
                      </div>
                    );
                  }
                  // string
                  return (
                    <input type="text"
                      value={effective ?? ''}
                      className={`w-full text-right font-mono text-xs border rounded px-1 py-0.5 ${borderCls} bg-white dark:bg-gray-900 dark:text-gray-200 focus:outline-none focus:ring-1 focus:ring-indigo-400`}
                      onChange={e => {
                        setHpEdits(prev => ({
                          ...prev,
                          [method]: { ...(prev[method] || {}), [k]: e.target.value }
                        }));
                      }}
                    />
                  );
                };

                // Save handler
                const handleSaveMethod = async () => {
                  // Merge localEdits with existing saved overrides
                  const merged = { ...savedOvr, ...localEdits };
                  if (Object.keys(merged).length === 0) return;
                  setHpSaving(true);
                  try {
                    await api.put(`/hyperparams/${encodeURIComponent(decodedId)}`, {
                      overrides: { [method]: merged }
                    });
                    setHpSavedOverrides(prev => ({ ...prev, [method]: merged }));
                    setHpEdits(prev => { const next = { ...prev }; delete next[method]; return next; });
                  } catch (err) {
                    console.error('Failed to save hyperparameter overrides:', err);
                  }
                  setHpSaving(false);
                };

                // Reset handler
                const handleResetMethod = async () => {
                  setHpSaving(true);
                  try {
                    await api.delete(`/hyperparams/${encodeURIComponent(decodedId)}?method=${encodeURIComponent(method)}`);
                    setHpSavedOverrides(prev => { const next = { ...prev }; delete next[method]; return next; });
                    setHpEdits(prev => { const next = { ...prev }; delete next[method]; return next; });
                  } catch (err) {
                    console.error('Failed to reset hyperparameter overrides:', err);
                  }
                  setHpSaving(false);
                };

                return (
                  <div key={method} className={`rounded-lg border p-4 text-sm ${isBest ? 'border-emerald-400 dark:border-emerald-600 bg-emerald-50/50 dark:bg-emerald-900/10' : 'border-gray-200 dark:border-gray-600 bg-white dark:bg-gray-800'}`}>
                    <div className="flex items-center gap-2 mb-2">
                      <span className="inline-block w-3 h-3 rounded-full" style={{ backgroundColor: getMethodColor(method) }}></span>
                      <span className="font-semibold text-gray-900 dark:text-gray-100">{method}</span>
                      {hp.method_family && <span className="text-xs bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400 px-1.5 py-0.5 rounded">{hp.method_family}</span>}
                      {isBest && <span className="text-xs bg-emerald-200 dark:bg-emerald-800 text-emerald-800 dark:text-emerald-200 px-1.5 py-0.5 rounded font-semibold">Best</span>}
                      {hasMethodSaved && <span className="text-xs bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300 px-1.5 py-0.5 rounded">Custom</span>}
                    </div>
                    {description && <p className="text-xs text-gray-500 dark:text-gray-400 mb-3 leading-relaxed">{description}</p>}

                    {/* Configuration — editable */}
                    <div className="mb-2">
                      <div className="text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wide mb-1">Configuration</div>
                      <div className="grid grid-cols-2 gap-x-3 gap-y-1 font-mono text-xs items-center">
                        {commonKeys.filter(k => hp[k] !== undefined).map(k => (
                          <React.Fragment key={k}>
                            <span className="text-gray-500 dark:text-gray-400">{k}</span>
                            {renderEditableParam(k, hp[k])}
                          </React.Fragment>
                        ))}
                      </div>
                    </div>

                    {/* Train / Validation split slider — ML methods only */}
                    {hp.method_family === 'ML' && (() => {
                      const effectiveValSplit = getEffectiveValue('val_split') ?? 0.2;
                      const trainPct = Math.round((1 - effectiveValSplit) * 100);
                      const valPct = Math.round(effectiveValSplit * 100);
                      const isSliderEdited = 'val_split' in localEdits;
                      const isSliderSaved = 'val_split' in savedOvr && !isSliderEdited;
                      return (
                        <div className="mb-3">
                          <div className="text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wide mb-1.5">
                            Train / Validation Split
                            {isSliderEdited && <span className="ml-1.5 text-amber-500 text-[9px] font-normal">(unsaved)</span>}
                            {isSliderSaved && <span className="ml-1.5 text-blue-400 text-[9px] font-normal">(custom)</span>}
                          </div>
                          <div className="flex h-3 rounded-full overflow-hidden mb-1.5 border border-gray-200 dark:border-gray-600">
                            <div className="bg-blue-500 transition-all" style={{ width: `${trainPct}%` }}
                              title={`Train: ${trainPct}%`} />
                            <div className="bg-amber-400 transition-all" style={{ width: `${valPct}%` }}
                              title={`Validation: ${valPct}%`} />
                          </div>
                          <div className="flex items-center gap-2">
                            <span className="text-xs text-blue-600 dark:text-blue-400 font-mono whitespace-nowrap">
                              Train {trainPct}%
                            </span>
                            <input type="range" min="5" max="50" step="5"
                              value={valPct}
                              onChange={e => {
                                const val = parseInt(e.target.value, 10) / 100;
                                setHpEdits(prev => ({
                                  ...prev,
                                  [method]: { ...(prev[method] || {}), val_split: val }
                                }));
                              }}
                              className="flex-1 accent-amber-500 h-2 cursor-pointer"
                            />
                            <span className="text-xs text-amber-600 dark:text-amber-400 font-mono whitespace-nowrap text-right">
                              Val {valPct}%
                            </span>
                          </div>
                        </div>
                      );
                    })()}

                    {/* Method-specific params — editable */}
                    {specificKeys.length > 0 && (
                      <div className="mb-2">
                        <div className="text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wide mb-1">Method Parameters</div>
                        <div className="grid grid-cols-2 gap-x-3 gap-y-1 font-mono text-xs items-center">
                          {specificKeys.map(k => (
                            <React.Fragment key={k}>
                              <span className="text-gray-500 dark:text-gray-400">{k}</span>
                              {renderEditableParam(k, hp[k])}
                            </React.Fragment>
                          ))}
                        </div>
                      </div>
                    )}

                    {/* Fitted params — now editable (overrides stored without fitted_ prefix) */}
                    {fittedKeys.length > 0 && (
                      <div className="mb-2">
                        <div className="text-xs font-semibold text-indigo-500 dark:text-indigo-400 uppercase tracking-wide mb-1">
                          Fitted (edit to override on next run)
                        </div>
                        <div className="grid grid-cols-2 gap-x-3 gap-y-1 font-mono text-xs items-center">
                          {fittedKeys.map(k => {
                            const stripKey = k.replace('fitted_', '');
                            const effective = getEffectiveValue(k);
                            const original = hp[k];
                            const isEdited = stripKey in localEdits || k in localEdits;
                            const isSavedOverride = (stripKey in savedOvr || k in savedOvr) && !isEdited;
                            const borderCls = isEdited ? 'border-amber-400' : isSavedOverride ? 'border-blue-400' : 'border-gray-200 dark:border-gray-600';

                            // Render editable input — same logic as renderEditableParam but stores key WITHOUT fitted_ prefix
                            const handleChange = (val) => {
                              setHpEdits(prev => ({
                                ...prev,
                                [method]: { ...(prev[method] || {}), [stripKey]: val }
                              }));
                            };

                            let input;
                            if (typeof original === 'boolean') {
                              input = (
                                <label className="flex items-center gap-1 justify-end cursor-pointer">
                                  <input type="checkbox" checked={!!effective}
                                    className="accent-indigo-600 w-3.5 h-3.5"
                                    onChange={e => handleChange(e.target.checked)} />
                                  <span className={effective ? 'text-emerald-600' : 'text-red-500'}>{String(effective)}</span>
                                </label>
                              );
                            } else if (typeof original === 'number') {
                              const step = Number.isInteger(original) ? 1 : 0.001;
                              input = (
                                <input type="number" step={step} value={effective ?? ''}
                                  className={`w-full text-right font-mono text-xs border rounded px-1 py-0.5 ${borderCls} bg-white dark:bg-gray-900 dark:text-gray-200 focus:outline-none focus:ring-1 focus:ring-indigo-400`}
                                  onChange={e => {
                                    const v = e.target.value === '' ? null : (Number.isInteger(original) ? parseInt(e.target.value, 10) : parseFloat(e.target.value));
                                    handleChange(v);
                                  }} />
                              );
                            } else if (typeof original === 'string') {
                              input = (
                                <input type="text" value={effective ?? ''}
                                  className={`w-full text-right font-mono text-xs border rounded px-1 py-0.5 ${borderCls} bg-white dark:bg-gray-900 dark:text-gray-200 focus:outline-none focus:ring-1 focus:ring-indigo-400`}
                                  onChange={e => handleChange(e.target.value)} />
                              );
                            } else {
                              // Fallback: read-only display
                              input = <span className="text-right">{renderVal(hp[k])}</span>;
                            }

                            return (
                              <React.Fragment key={k}>
                                <span className="text-gray-500 flex items-center gap-1" title={`Override: ${stripKey}`}>
                                  {stripKey}
                                  {isSavedOverride && <span className="text-blue-400 text-[8px]">*</span>}
                                </span>
                                {input}
                              </React.Fragment>
                            );
                          })}
                        </div>
                      </div>
                    )}

                    {/* Training time + PI + Save/Reset buttons */}
                    <div className="mt-2 pt-2 border-t border-gray-100 dark:border-gray-700 flex items-center justify-between">
                      <div className="flex items-center gap-3 text-xs text-gray-400 dark:text-gray-500">
                        {hp.training_time_seconds != null && <span>Training: {hp.training_time_seconds}s</span>}
                        {hp.prediction_intervals_available != null && (
                          <span className={hp.prediction_intervals_available ? 'text-emerald-500' : 'text-amber-500'}>
                            PI: {hp.prediction_intervals_available ? 'Yes' : 'No'}
                          </span>
                        )}
                      </div>
                      <div className="flex items-center gap-1.5">
                        {(hasMethodEdits || hasMethodSaved) && (
                          <button
                            onClick={handleResetMethod}
                            disabled={hpSaving}
                            className="px-2 py-0.5 text-xs rounded border border-gray-300 dark:border-gray-600 text-gray-600 dark:text-gray-400 hover:bg-gray-50 dark:hover:bg-gray-700 disabled:opacity-50"
                            title="Reset to defaults"
                          >Reset</button>
                        )}
                        {hasMethodEdits && (
                          <button
                            onClick={handleSaveMethod}
                            disabled={hpSaving}
                            className="px-2 py-0.5 text-xs rounded bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50"
                          >{hpSaving ? 'Saving...' : 'Save'}</button>
                        )}
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>
          </Section>
        ) : null;

        /* ridge — hidden in multi-series mode */
        sectionNodes['ridge'] = (ridgePlotData && !isMultiMode) ? (
          <Section key="ridge" title="Forecast Distribution Over Time (3D)" storageKey="tsv_ridge_open" {...dp('ridge')}>
            <p className="text-sm text-gray-500 dark:text-gray-400 mb-1">
              3D surface of forecast density by horizon ({distributions?.method || 'best method'}). X = forecast value, Y = horizon month, Z = density. Dashed lines = mean per horizon.
            </p>
            {distributions?.horizons?.some(h => h.is_bootstrap) && (
              <p className="text-xs text-amber-600 dark:text-amber-400 mb-3">Some horizons use bootstrap distributions — parametric fit was not available.</p>
            )}
            <div className="w-full" style={{ height: 520 }}>
              <Plot
                data={ridgePlotData.traces}
                layout={{
                  autosize: true,
                  margin: { l: 0, r: 0, t: 10, b: 0 },
                  paper_bgcolor: isDark ? '#1f2937' : 'rgba(0,0,0,0)',
                  font: { color: isDark ? '#d1d5db' : '#374151' },
                  scene: {
                    xaxis: { title: { text: 'Forecast Value', font: { size: 11 } }, tickformat: ',.0f', gridcolor: isDark ? '#374151' : '#e5e7eb', zerolinecolor: isDark ? '#4b5563' : '#cbd5e1', color: isDark ? '#d1d5db' : undefined },
                    yaxis: { title: { text: 'Horizon (month)', font: { size: 11 } }, tickformat: 'd', gridcolor: isDark ? '#374151' : '#e5e7eb', zerolinecolor: isDark ? '#4b5563' : '#cbd5e1', color: isDark ? '#d1d5db' : undefined },
                    zaxis: { title: { text: 'Density', font: { size: 11 } }, gridcolor: isDark ? '#374151' : '#e5e7eb', zerolinecolor: isDark ? '#4b5563' : '#cbd5e1', color: isDark ? '#d1d5db' : undefined },
                    camera: { eye: { x: -1.6, y: -1.6, z: 1.0 } },
                    bgcolor: isDark ? '#1f2937' : 'rgba(0,0,0,0)',
                  },
                  legend: { x: 0.02, y: 0.98, bgcolor: isDark ? 'rgba(31,41,55,0.9)' : 'rgba(255,255,255,0.7)', bordercolor: isDark ? '#4b5563' : '#e5e7eb', borderwidth: 1, font: { color: isDark ? '#d1d5db' : undefined } },
                }}
                config={{ responsive: true, displayModeBar: true, displaylogo: false, modeBarButtonsToRemove: ['toImage'] }}
                style={{ width: '100%', height: '100%' }}
                useResizeHandler
              />
            </div>
          </Section>
        ) : null;

        /* evolution — combined: Forecast Convergence + Racing Bars */
        {
          const hasConvergence = convergenceChart != null;
          const hasRacing = origins.length > 0 || activeForecasts.length > 0;
          // Auto-select available view if current selection is unavailable
          const effectiveView = (convergenceView === 'convergence' && !hasConvergence) ? 'racing'
                              : (convergenceView === 'racing' && !hasRacing) ? 'convergence'
                              : convergenceView;

          if ((hasConvergence || hasRacing) && !isMultiMode) {
            sectionNodes['evolution'] = (
              <Section key="evolution" title="Forecast Evolution" storageKey="tsv_evolution_open" {...dp('evolution')}>
                {/* View toggle tabs */}
                <div className="flex items-center gap-1 mb-4 border-b border-gray-200 dark:border-gray-700">
                  {hasConvergence && (
                    <button onClick={() => setConvergenceView('convergence')}
                      className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${effectiveView === 'convergence' ? 'border-blue-500 text-blue-600 dark:text-blue-400' : 'border-transparent text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300'}`}>
                      Convergence
                    </button>
                  )}
                  {hasRacing && (
                    <button onClick={() => setConvergenceView('racing')}
                      className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${effectiveView === 'racing' ? 'border-blue-500 text-blue-600 dark:text-blue-400' : 'border-transparent text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300'}`}>
                      Method Comparison
                    </button>
                  )}
                </div>

                {/* ── Convergence View ── */}
                {effectiveView === 'convergence' && hasConvergence && (
                  <div>
                    <p className="text-sm text-gray-500 dark:text-gray-400 mb-3">
                      How the forecast for each target month evolved as the forecast date approached.
                      Each bar group is a target month; bars within are forecasts made at different origins.
                    </p>
                    {/* Method selector */}
                    {convergenceData?.methods?.length > 1 && (
                      <div className="flex items-center gap-2 mb-4 flex-wrap">
                        <span className="text-sm text-gray-600 dark:text-gray-400">Method:</span>
                        {convergenceData.methods.map(m => (
                          <button key={m} onClick={() => setConvergenceMethod(m)}
                            className={`px-3 py-1.5 rounded-lg text-sm font-medium transition-colors ${(convergenceMethod || convergenceChart?.method) === m
                              ? 'text-white' : 'bg-gray-100 dark:bg-gray-700 hover:bg-gray-200 dark:hover:bg-gray-600 text-gray-700 dark:text-gray-300'}`}
                            style={(convergenceMethod || convergenceChart?.method) === m ? { backgroundColor: getMethodColor(m) } : {}}>
                            {m}
                          </button>
                        ))}
                      </div>
                    )}
                    {/* Convergence chart */}
                    <div className="w-full" style={{ minHeight: 400 }}>
                      <Plot
                        data={convergenceChart.traces}
                        layout={{ ...convergenceChart.layout, title: { text: `Forecast Convergence — ${convergenceChart.method}`, font: { size: 14 } } }}
                        config={plotlyConfig}
                        style={{ width: '100%', height: '100%' }}
                        useResizeHandler
                      />
                    </div>
                    {/* Legend explanation */}
                    <p className="text-xs text-gray-400 dark:text-gray-500 mt-2 text-center">
                      Lighter bars = older forecasts (further ahead) &middot; Darker bars = more recent forecasts &middot; Diamond line = actual demand
                    </p>
                  </div>
                )}

                {/* ── Racing Bars / Method Comparison View ── */}
                {effectiveView === 'racing' && hasRacing && (
                  <div>
                    <p className="text-sm text-gray-500 dark:text-gray-400 mb-4">
                      {origins.length > 0 ? 'Compare method forecasts at each origin date.' : 'Compare forecast values across methods for each horizon month.'}
                    </p>
                    {origins.length > 0 && (
                      <div className="flex items-center gap-3 mb-4 flex-wrap">
                        <button onClick={togglePlay} className={`px-4 py-2 rounded-lg text-white text-sm font-medium transition-colors ${isPlaying ? 'bg-red-500 hover:bg-red-600' : 'bg-blue-500 hover:bg-blue-600'}`}>
                          {isPlaying ? '■ Stop' : '▶ Play'}
                        </button>
                        <div className="flex-1 min-w-32">
                          <input type="range" min={0} max={origins.length - 1} value={selectedOriginIdx} onChange={e => setSelectedOriginIdx(parseInt(e.target.value))} className="w-full accent-blue-500" />
                        </div>
                        <div className="text-sm font-mono bg-blue-50 dark:bg-blue-900/30 text-blue-800 dark:text-blue-300 px-3 py-1.5 rounded-lg min-w-28 text-center font-medium">{origins[selectedOriginIdx] || '-'}</div>
                      </div>
                    )}
                    <div className="flex items-center gap-2 mb-4 flex-wrap">
                      <span className="text-sm text-gray-600 dark:text-gray-400">Horizon month:</span>
                      {horizonLength <= 12
                        ? Array.from({ length: horizonLength }, (_, i) => i + 1).map(p => (
                            <button key={p} onClick={() => setSelectedPeriod(p)}
                              className={`px-3 py-1.5 rounded-lg text-sm font-medium transition-colors ${selectedPeriod === p ? 'bg-blue-500 text-white' : 'bg-gray-100 dark:bg-gray-700 hover:bg-gray-200 dark:hover:bg-gray-600 text-gray-700 dark:text-gray-300'}`}>
                              M{p}
                            </button>
                          ))
                        : (
                          <>
                            {[1, 3, 6, 12, 18, 24].filter(p => p <= horizonLength).map(p => (
                              <button key={p} onClick={() => setSelectedPeriod(p)}
                                className={`px-3 py-1.5 rounded-lg text-sm font-medium transition-colors ${selectedPeriod === p ? 'bg-blue-500 text-white' : 'bg-gray-100 dark:bg-gray-700 hover:bg-gray-200 dark:hover:bg-gray-600 text-gray-700 dark:text-gray-300'}`}>
                                M{p}
                              </button>
                            ))}
                            <input type="range" min={1} max={horizonLength} value={selectedPeriod} onChange={e => setSelectedPeriod(parseInt(e.target.value))} className="w-28 accent-blue-500 ml-2" />
                            <span className="text-xs font-mono text-gray-500">M{selectedPeriod}</span>
                          </>
                        )
                      }
                    </div>
                    {racingBarsSpec
                      ? <div className="w-full overflow-x-auto"><Plot data={racingBarsSpec.data} layout={racingBarsSpec.layout} config={plotlyConfig} useResizeHandler style={{width:'100%'}} /></div>
                      : <div className="text-gray-400 dark:text-gray-500 py-4 text-center text-sm">No comparison data</div>
                    }
                  </div>
                )}
              </Section>
            );
          } else {
            sectionNodes['evolution'] = null;
          }
        }

        /* forecast_table */
        sectionNodes['forecast_table'] = activeForecasts.length > 0 ? (
          <div key="forecast_table" id="tsv-forecast-table">
            <ForecastTableWithAdjustments
              activeForecasts={activeForecasts}
              forecastDates={forecastDates}
              bestMethod={bestMethod}
              historicalData={historicalData}
              isMultiMode={isMultiMode}
              horizonLength={horizonLength}
              adjustments={adjustments}
              adjSaving={adjSaving}
              saveAdjustment={saveAdjustment}
              resetAllAdjustments={resetAllAdjustments}
              locale={locale}
              numberDecimals={numberDecimals}
              isDark={isDark}
              dateRangeEnd={dateRangeEnd}
              methodExplanation={methodExplanation}
              multiSeriesData={multiSeriesData}
              saveMultiAdjustment={saveMultiAdjustment}
              resetMultiAdjustments={resetMultiAdjustments}
            />
          </div>
        ) : null;

        return sectionOrder.map(id => sectionNodes[id] || null);
      })()}

      {activeMetrics.length === 0 && activeForecasts.length > 0 && !isMultiMode && (
        <div className="mb-6 bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900/50 p-6">
          <h2 className="text-lg font-semibold mb-2 dark:text-white">Backtest Metrics</h2>
          <p className="text-gray-500 dark:text-gray-400 text-sm">This series has insufficient history for rolling-window backtesting (needs {12 + horizonLength}+ monthly observations). Forecasts are still generated.</p>
        </div>
      )}

      {activeForecasts.length === 0 && activeMetrics.length === 0 && (
        <div className="bg-yellow-50 dark:bg-yellow-900/20 border border-yellow-200 dark:border-yellow-800 rounded-lg p-6 text-center">
          <p className="text-yellow-800 dark:text-yellow-300">No forecasts or backtest metrics available for this series.</p>
        </div>
      )}
    </div>
  );
};

export default TimeSeriesViewer;

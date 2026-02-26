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
import { VegaLite } from 'react-vega';
import Plot from 'react-plotly.js';
import axios from 'axios';
import { useLocale } from '../contexts/LocaleContext';
import { useTheme } from '../contexts/ThemeContext';
import { formatNumber, formatDate, formatYearMonth, formatPercent, toISODate, formatDateTime } from '../utils/formatting';

const API_BASE_URL = '/api';

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
  'rationale', 'scoring', 'metrics', 'hyperparameters', 'ridge', 'evolution',
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

// ---- Searchable multi-select dropdown with recent history ----
// `values` is an array of selected strings; `onChange` receives the new array
const SearchableDropdown = ({ label, values = [], onChange, options, recentOptions, disabled, placeholder }) => {
  const [open, setOpen] = useState(false);
  const [search, setSearch] = useState('');
  const ref = useRef(null);

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

  const filteredRecent = recentOptions.filter(o => o.toLowerCase().includes(search.toLowerCase()));
  const filteredAll = options.filter(o =>
    o.toLowerCase().includes(search.toLowerCase()) &&
    !recentOptions.includes(o)
  );
  const hasRecent = filteredRecent.length > 0 && search === '';
  const displayText = values.length === 0 ? '' : values.length === 1 ? values[0] : `${values.length} selected`;

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
                  <span>{o}</span>
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
                  <span>{o}</span>
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
  dateRangeEnd,
}) {
  const bestMethodName = bestMethod?.best_method;
  const [adjRowsOpen, setAdjRowsOpen] = React.useState(false);

  // Keep adj rows open automatically if there are any saved values
  const hasAnyAdj = Object.keys(adjustments).length > 0;
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
    const currentVal = existing ? existing.value : value || 0;
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
    const adjMap = {};
    const ovMap  = {};
    Object.entries(adjustments).forEach(([key, entry]) => {
      const [date, type] = key.split('|');
      if (type === 'adjustment') adjMap[date] = String(entry.value);
      if (type === 'override')   ovMap[date]  = String(entry.value);
    });
    setDraftAdj(adjMap);
    setDraftOv(ovMap);
  }, [adjustments]);

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

  const adjCount = Object.keys(adjustments).length;

  return (
    <Section
      title={`Forecast Point Values${isMultiMode ? ' (aggregated sum)' : ''} (${horizonLength} months)`}
      storageKey="tsv_forecast_table_open"
    >
      {/* Reset button + adj count badge */}
      {!isMultiMode && adjCount > 0 && (
        <div className="flex items-center justify-end gap-3 mb-2">
          <span className="text-xs text-gray-400 dark:text-gray-500">{adjCount} adjustment{adjCount !== 1 ? 's' : ''} active</span>
          <button
            onClick={resetAllAdjustments}
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
                Method
              </th>
              {forecastDates.map((d, i) => (
                <th key={i} className="px-2 py-2 text-right text-xs font-medium text-gray-500 dark:text-gray-400 uppercase whitespace-nowrap">
                  {d}
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-200 dark:divide-gray-700">
            {activeForecasts.map((f, idx) => {
              const isBest = f.method === bestMethodName;
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
                        {/* Toggle adj rows — only for the best/selected method */}
                        {isBest && !isMultiMode && (
                          <button
                            onClick={() => setAdjRowsOpen(o => !o)}
                            title={adjRowsOpen ? 'Hide adjustment rows' : 'Show adjustment rows'}
                            className="ml-1 text-gray-400 hover:text-indigo-600 text-xs leading-none"
                          >
                            {adjRowsOpen ? '▲' : '▼'}
                          </button>
                        )}
                      </div>
                    </td>
                    {f.point_forecast.map((v, i) => {
                      // For the best method, show the final (adjusted) value in the cell
                      if (isBest && monthDates[i]) {
                        const dateStr = monthDates[i];
                        const adj = adjustments[`${dateStr}|adjustment`];
                        const ov  = adjustments[`${dateStr}|override`];
                        const finalVal = ov
                          ? Number(ov.value)
                          : adj
                            ? v + Number(adj.value)
                            : v;
                        const hasNote = (adj?.note || ov?.note);
                        const saving = adjSaving[`${dateStr}|adjustment`] || adjSaving[`${dateStr}|override`];
                        return (
                          <td
                            key={i}
                            className={`px-2 py-2 text-right font-mono text-xs relative ${ov ? 'text-red-700 dark:text-red-400 font-semibold' : adj ? 'text-orange-700 dark:text-orange-400 font-semibold' : 'dark:text-gray-300'} ${hasNote ? 'cell-note-indicator' : ''}`}
                            title={hasNote ? `Note: ${adj?.note || ov?.note}` : undefined}
                            onDoubleClick={(e) => handleCellDoubleClick(e, dateStr, ov ? 'override' : 'adjustment', finalVal)}
                            onContextMenu={(e) => handleCellContextMenu(e, dateStr, ov ? 'override' : 'adjustment', finalVal)}
                          >
                            {saving && <span className="text-gray-300 dark:text-gray-600 mr-0.5 text-[10px]">\u27F3</span>}
                            {formatNumber(finalVal, locale, 0)}
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

                  {/* ── Adjustment rows (only under best method, collapsible) ── */}
                  {isBest && !isMultiMode && adjRowsOpen && monthDates.length > 0 && (
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
                          const adj = adjustments[`${dateStr}|adjustment`];
                          return (
                            <td key={i} className="px-1 py-0.5">
                              <input
                                type="number"
                                step="1"
                                value={draftAdj[dateStr] ?? ''}
                                placeholder="±"
                                onChange={e => setDraftAdj(prev => ({ ...prev, [dateStr]: e.target.value }))}
                                onBlur={e => saveAdjustment(dateStr, 'adjustment', e.target.value, adj?.note)}
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
                          const ov = adjustments[`${dateStr}|override`];
                          return (
                            <td key={i} className="px-1 py-0.5">
                              <input
                                type="number"
                                step="1"
                                value={draftOv[dateStr] ?? ''}
                                placeholder="—"
                                onChange={e => setDraftOv(prev => ({ ...prev, [dateStr]: e.target.value }))}
                                onBlur={e => saveAdjustment(dateStr, 'override', e.target.value, ov?.note)}
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

            {/* ── Consensus row — always visible when not in multi-mode ── */}
            {!isMultiMode && bestFc && monthDates.length > 0 && (
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
                {bestFc.point_forecast.map((modelVal, i) => {
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
      {!isMultiMode && (
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
  const [methodExplanation, setMethodExplanation] = useState(null);
  const [distributions, setDistributions] = useState(null);

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
  const forecastPollRef = useRef(null);

  // ---- Hyperparameter overrides (editable params) ----
  const [hpEdits, setHpEdits] = useState({});           // {method: {param: newValue}}
  const [hpSaving, setHpSaving] = useState(false);
  const [hpSavedOverrides, setHpSavedOverrides] = useState({}); // from DB: {method: {param: val}}

  // ---- Section drag-and-drop order ----
  const { order: sectionOrder, reorder: reorderSections } = useSectionOrder();
  const [draggingId, setDraggingId] = useState(null);
  const [dragOverId, setDragOverId] = useState(null);

  // ---- Display aggregation granularity ----
  const [displayAgg, setDisplayAgg] = useState('native');

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
    axios.get(`${API_BASE_URL}/series`, { params: { limit: 50000 } })
      .then(res => setAllSeriesList(res.data || []))
      .catch(() => {});
  }, []);

  // ---- Load segments (once) + auto-select the default ----
  useEffect(() => {
    axios.get(`${API_BASE_URL}/segments`)
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
    axios.get(`${API_BASE_URL}/segments/${selectedSegmentId}/members`, { params: { limit: 200000 } })
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
    setSelectedSites([]); // reset sites when items change
    setMultiSeriesData(null);
  };

  const handleSitesChange = (sites) => {
    setSelectedSites(sites);
    setMultiSeriesData(null);
    if (selectedItems.length === 1 && sites.length === 1) {
      const newId = `${selectedItems[0]}_${sites[0]}`;
      if (newId !== decodedId) navigate(`/series/${encodeURIComponent(newId)}`);
    }
  };

  const handleSegmentChange = (segId) => {
    setSelectedSegmentId(segId);
    setSelectedItems([]);
    setSelectedSites([]);
    setMultiSeriesData(null);
  };

  // ---- Trigger multi-series load when selection changes ----
  useEffect(() => {
    const selectedUids = [];
    selectedItems.forEach(item => {
      selectedSites.forEach(site => {
        if (item && site) selectedUids.push(`${item}_${site}`);
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
        axios.get(`${API_BASE_URL}/series/${encodeURIComponent(uid)}/data`),
        axios.get(`${API_BASE_URL}/forecasts/${encodeURIComponent(uid)}`),
        axios.get(`${API_BASE_URL}/metrics/${encodeURIComponent(uid)}`),
      ])
    )).then(results => {
      // Aggregate: demand = sum, forecast = sum, metrics = weighted avg (by n_windows)
      const allHistorical = {}; // date -> sum
      const allForecasts = {};  // method -> [sum per horizon]
      const allMetrics = {};    // method -> {sum of metric*w, totalW, n}
      let forecastDatesRef = null;

      results.forEach(r => {
        if (r.status !== 'fulfilled') return;
        const [dataRes, fcRes, metricsRes] = r.value;

        // Historical sum
        if (dataRes.status === 'fulfilled') {
          const d = dataRes.value.data.data;
          (d.date || []).forEach((date, i) => {
            allHistorical[date] = (allHistorical[date] || 0) + (d.value[i] || 0);
          });
        }

        // Forecast sum
        if (fcRes.status === 'fulfilled') {
          const fcasts = fcRes.value.data.forecasts || [];
          fcasts.forEach(f => {
            if (!allForecasts[f.method]) {
              allForecasts[f.method] = { point: new Array(f.point_forecast.length).fill(0), count: 0 };
              forecastDatesRef = forecastDatesRef || f;
            }
            f.point_forecast.forEach((v, i) => {
              if (allForecasts[f.method].point[i] !== undefined) allForecasts[f.method].point[i] += v || 0;
            });
            allForecasts[f.method].count++;
          });
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

      setMultiSeriesData({
        historical: aggregatedHistorical,
        forecasts: aggregatedForecasts,
        metrics: aggregatedMetrics,
        uids: selectedUids,
      });
      setMultiLoading(false);
    });
  }, [selectedItems, selectedSites]);

  // ---- Derived dropdown options ----
  // filteredSeriesList: scoped to the active segment (null segmentMemberSet = All)
  const filteredSeriesList = useMemo(() => {
    if (!segmentMemberSet) return allSeriesList;
    return allSeriesList.filter(s => segmentMemberSet.has(s.unique_id));
  }, [allSeriesList, segmentMemberSet]);

  const allItems = useMemo(() => {
    const items = [...new Set(filteredSeriesList.map(s => parseUniqueId(s.unique_id).item))];
    return items.sort((a, b) => a.localeCompare(b, undefined, { numeric: true }));
  }, [filteredSeriesList]);

  const availableSites = useMemo(() => {
    if (selectedItems.length === 0) return [];
    // Sites available for any of the selected items (within segment)
    const sites = filteredSeriesList
      .filter(s => selectedItems.includes(parseUniqueId(s.unique_id).item))
      .map(s => parseUniqueId(s.unique_id).site);
    return [...new Set(sites)].sort((a, b) => a.localeCompare(b, undefined, { numeric: true }));
  }, [filteredSeriesList, selectedItems]);

  // Derive single item/site for single-series mode (backward compat)
  const selectedItem = selectedItems[0] || '';
  const selectedSite = selectedSites[0] || '';

  // All unique_ids from the current item × site selection
  const forecastUids = useMemo(() => {
    const uids = [];
    selectedItems.forEach(item => {
      selectedSites.forEach(site => {
        if (item && site) uids.push(`${item}_${site}`);
      });
    });
    return uids;
  }, [selectedItems, selectedSites]);

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
        axios.get(`${API_BASE_URL}/series/${encodeURIComponent(decodedId)}/data`),
        axios.get(`${API_BASE_URL}/forecasts/${encodeURIComponent(decodedId)}`),
        axios.get(`${API_BASE_URL}/series`, { params: { search: decodedId, limit: 1 } }),
        axios.get(`${API_BASE_URL}/metrics/${encodeURIComponent(decodedId)}`),
        axios.get(`${API_BASE_URL}/series/${encodeURIComponent(decodedId)}/best-method`),
        axios.get(`${API_BASE_URL}/forecasts/${encodeURIComponent(decodedId)}/origins`),
        axios.get(`${API_BASE_URL}/series/${encodeURIComponent(decodedId)}/outliers`),
        axios.get(`${API_BASE_URL}/series/${encodeURIComponent(decodedId)}/method-explanation`),
        axios.get(`${API_BASE_URL}/series/${encodeURIComponent(decodedId)}/distributions`)
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
        const hpRes = await axios.get(`${API_BASE_URL}/hyperparams/${encodeURIComponent(decodedId)}`);
        setHpSavedOverrides(hpRes.data.overrides || {});
        setHpEdits({});  // clear local edits on fresh load
      } catch { /* no overrides yet — that's fine */ }

      // Load forecast convergence data (non-blocking)
      try {
        const convRes = await axios.get(`${API_BASE_URL}/series/${encodeURIComponent(decodedId)}/forecast-convergence`);
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
      const res = await axios.post(`${API_BASE_URL}/pipeline/run-forecast`, {
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
          const r = await axios.get(`${API_BASE_URL}/pipeline/jobs/${jobId}`);
          const st = r.data.status;
          setForecastJobStatus(st);
          if (st === 'success' || st === 'error') {
            clearInterval(forecastPollRef.current);
            forecastPollRef.current = null;
            if (st === 'success') {
              // Refresh the API data cache, then reload this series
              try { await axios.post(`${API_BASE_URL}/reload`); } catch { /* non-fatal */ }
              loadData();
            }
          }
        } catch { /* ignore transient poll errors */ }
      }, 1500);
    } catch (err) {
      setForecastJobStatus('error');
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
      const res = await axios.get(`${API_BASE_URL}/adjustments/${encodeURIComponent(decodedId)}`);
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
          await axios.delete(
            `${API_BASE_URL}/adjustments/${encodeURIComponent(decodedId)}/${forecastDate}/${adjType}`
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
        const res = await axios.post(
          `${API_BASE_URL}/adjustments/${encodeURIComponent(decodedId)}`,
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
      await axios.delete(`${API_BASE_URL}/adjustments/${encodeURIComponent(decodedId)}`);
      setAdjustments({});
    } catch { /* non-fatal */ }
  }, [decodedId]);

  useEffect(() => {
    if (origins.length === 0) return;
    const origin = origins[selectedOriginIdx];
    if (!origin) return;
    axios.get(`${API_BASE_URL}/forecasts/${encodeURIComponent(decodedId)}/origins/${origin}`)
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

  // Display-aggregated historical data
  const dispHistData = useMemo(
    () => aggHistData(activeHistoricalData, displayAgg),
    [activeHistoricalData, displayAgg]
  );

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

  /* ---------- Vega theme for dark mode ---------- */
  const vegaThemeConfig = useMemo(() => ({
    background: isDark ? '#1f2937' : '#ffffff',
    view: { stroke: isDark ? '#374151' : null },
    axis: {
      labelColor: isDark ? '#d1d5db' : '#374151',
      titleColor: isDark ? '#e5e7eb' : '#111827',
      gridColor: isDark ? '#374151' : '#e5e7eb',
      tickColor: isDark ? '#4b5563' : '#d1d5db',
      domainColor: isDark ? '#4b5563' : '#d1d5db',
    },
    legend: { labelColor: isDark ? '#d1d5db' : '#374151', titleColor: isDark ? '#e5e7eb' : '#111827' },
    title: { color: isDark ? '#e5e7eb' : '#111827' },
  }), [isDark]);

  /* ---------- chart specs ---------- */
  const outlierChartSpec = useMemo(() => {
    if (outlierChartData.length === 0 || outlierDates.length === 0) return null;
    const minDate = outlierDates[outlierZoomStart] || outlierDates[0];
    const maxDate = outlierDates[outlierZoomEnd] || outlierDates[outlierDates.length - 1];
    const filtered = outlierChartData.filter(d => d.date >= minDate && d.date <= maxDate);
    if (filtered.length === 0) return null;
    // Stacked bar: each date has Corrected + optional Adjustment stack
    return {
      $schema: 'https://vega.github.io/schema/vega-lite/v5.json',
      width: 'container', height: 300,
      autosize: { type: 'fit', contains: 'padding' },
      data: { values: filtered },
      mark: { type: 'bar', binSpacing: 1 },
      encoding: {
        x: { field: 'date', type: 'temporal', title: 'Date', axis: { format: '%Y-%m', labelAngle: -30, labelFontSize: 10 } },
        y: { field: 'value', type: 'quantitative', title: 'Demand', stack: 'zero' },
        color: {
          field: 'series', type: 'nominal',
          scale: { domain: ['Corrected', 'Clipped ↓', 'Filled ↑'], range: ['#2563eb', '#ef4444', '#f59e0b'] },
          legend: { title: null, orient: 'top', direction: 'horizontal' }
        },
        opacity: { condition: { test: "datum.isOutlier", value: 1.0 }, value: 0.75 },
        tooltip: [
          { field: 'date', type: 'temporal', title: 'Date' },
          { field: 'series', type: 'nominal', title: 'Type' },
          { field: 'corrVal', type: 'quantitative', title: 'Corrected', format: ',.0f' },
          { field: 'origVal', type: 'quantitative', title: 'Original', format: ',.0f' },
          { field: 'delta', type: 'quantitative', title: 'Δ Adjustment', format: ',.0f' },
        ]
      },
      config: vegaThemeConfig
    };
  }, [outlierChartData, outlierDates, outlierZoomStart, outlierZoomEnd]);

  const mainChartSpec = useMemo(() => {
    if (allData.length === 0 || allDates.length === 0) return null;
    const minDate = allDates[Math.min(zoomStart, allDates.length - 1)] || allDates[0];
    const maxDate = allDates[Math.min(zoomEnd, allDates.length - 1)] || allDates[allDates.length - 1];
    const filtered = allData.filter(d => {
      if (d.type !== 'Actual' && d.method !== 'Historical' && visibleMethods[d.method] === false) return false;
      // Filter bands by bandVisibleMethods
      if (d.layer === 'band' && bandVisibleMethods[d.method] === false) return false;
      return d.date >= minDate && d.date <= maxDate;
    });
    if (filtered.length === 0) return null;
    const colorScale = { field: 'method', type: 'nominal', scale: activeMethodDomain, legend: { title: 'Method' } };
    const hasBands = filtered.some(d => d.layer === 'band');
    const hasBars  = filtered.some(d => d.layer === 'bar');
    const layers = [];

    // Axis date format: fine for weekly/daily, coarse for monthly+
    const axisDateFmt = daysPerPeriod <= 10 ? '%b %d %Y' : (daysPerPeriod <= 95 ? '%b %Y' : '%Y');
    const ttipDateFmt = daysPerPeriod <= 10 ? '%Y-%m-%d' : '%Y-%m';

    // ---- Demand bars (historical only) ----
    // NOTE: no timeUnit — Vega-Lite's timeUnit:'yearmonth' would collapse weekly
    //       bars into monthly buckets, which is exactly what we want to avoid.
    //       Data is already at the correct granularity (weekly, monthly, etc.)
    //       via dispHistData / displayAgg aggregation done in JavaScript.
    if (hasBars) {
      layers.push({
        transform: [{ filter: "datum.layer === 'bar'" }],
        mark: { type: 'bar', tooltip: true, color: isDark ? '#9ca3af' : '#374151', opacity: 0.55 },
        encoding: {
          x: { field: 'date', type: 'temporal', title: 'Date',
               axis: { format: axisDateFmt, labelAngle: -30 } },
          y: { field: 'value', type: 'quantitative', title: 'Demand' },
          tooltip: [
            { field: 'date', type: 'temporal', title: 'Date', format: ttipDateFmt },
            { field: 'value', type: 'quantitative', title: 'Demand', format: ',.0f' },
          ]
        }
      });
    }

    // ---- Confidence bands (behind forecast lines) ----
    if (hasBands) {
      layers.push({ transform: [{ filter: "datum.layer === 'band'" }], mark: { type: 'area', opacity: 0.12 }, encoding: { x: { field: 'date', type: 'temporal' }, y: { field: 'lo90', type: 'quantitative' }, y2: { field: 'hi90' }, color: { ...colorScale, legend: null } } });
      layers.push({ transform: [{ filter: "datum.layer === 'band'" }], mark: { type: 'area', opacity: 0.25 }, encoding: { x: { field: 'date', type: 'temporal' }, y: { field: 'lo50', type: 'quantitative' }, y2: { field: 'hi50' }, color: { ...colorScale, legend: null } } });
    }

    // ---- Forecast method lines (excluding Historical which is now bars) ----
    layers.push({
      transform: [{ filter: "datum.layer === 'line'" }],
      mark: { type: 'line', point: false, strokeWidth: 2 },
      encoding: {
        x: { field: 'date', type: 'temporal', title: 'Date', axis: { format: axisDateFmt, labelAngle: -30 } },
        y: { field: 'value', type: 'quantitative', title: 'Demand', scale: { zero: false } },
        color: colorScale,
        strokeDash: { field: 'type', type: 'nominal',
          scale: { domain: ['Actual', 'Forecast'], range: [[1, 0], [5, 5]] }, legend: null },
        opacity: { value: 0.85 },
        tooltip: [
          { field: 'date', type: 'temporal', title: 'Date' },
          { field: 'value', type: 'quantitative', title: 'Value', format: ',.0f' },
          { field: 'method', type: 'nominal', title: 'Method' },
          { field: 'type', type: 'nominal', title: 'Type' },
        ]
      }
    });

    // "Final Forecast" line — only drawn when any adjustments/overrides exist.
    // It uses the best-method forecast as base, applies adjustments/overrides.
    const hasFinalOverlay = filtered.some(d => d.type === 'Adjustment' || d.type === 'Override');
    if (hasFinalOverlay) {
      // Build the final-forecast line from the existing marker values
      // (they already have the final value baked in from allData memo)
      const finalLineData = filtered
        .filter(d => d.type === 'Adjustment' || d.type === 'Override')
        .map(d => ({ date: d.date, value: d.value, type: 'Final Forecast', method: 'Final Forecast' }));
      if (finalLineData.length > 0) {
        layers.push({
          data: { values: finalLineData },
          mark: { type: 'line', strokeWidth: 2.5, strokeDash: [3, 2], color: '#7c3aed', point: false },
          encoding: {
            x: { field: 'date', type: 'temporal' },
            y: { field: 'value', type: 'quantitative' },
            tooltip: [
              { field: 'date', type: 'temporal', title: 'Date', format: '%Y-%m-%d' },
              { field: 'value', type: 'quantitative', title: 'Final forecast', format: ',.0f' },
            ]
          }
        });
      }
    }

    // Adjustment markers (orange circle ●) — use transform filter on shared data
    const hasAdjMarkers = filtered.some(d => d.type === 'Adjustment');
    if (hasAdjMarkers) {
      layers.push({
        transform: [{ filter: "datum.type === 'Adjustment'" }],
        mark: { type: 'point', shape: 'triangle-up', size: 160, filled: true, opacity: 1 },
        encoding: {
          x: { field: 'date', type: 'temporal' },
          y: { field: 'value', type: 'quantitative' },
          color: { value: '#f97316' },
          tooltip: [
            { field: 'date', type: 'temporal', title: 'Date', format: '%Y-%m-%d' },
            { field: 'value', type: 'quantitative', title: 'Adjusted forecast', format: ',.0f' },
            { field: 'adjDelta', type: 'quantitative', title: 'Δ (delta)', format: '+,.0f' },
            { field: 'adjNote', type: 'nominal', title: 'Note' },
          ]
        }
      });
    }

    // Override markers (red square ■) — use transform filter on shared data
    const hasOvMarkers = filtered.some(d => d.type === 'Override');
    if (hasOvMarkers) {
      layers.push({
        transform: [{ filter: "datum.type === 'Override'" }],
        mark: { type: 'point', shape: 'square', size: 160, filled: true, opacity: 1 },
        encoding: {
          x: { field: 'date', type: 'temporal' },
          y: { field: 'value', type: 'quantitative' },
          color: { value: '#dc2626' },
          tooltip: [
            { field: 'date', type: 'temporal', title: 'Date', format: '%Y-%m-%d' },
            { field: 'value', type: 'quantitative', title: 'Override value', format: ',.0f' },
            { field: 'adjNote', type: 'nominal', title: 'Note' },
          ]
        }
      });
    }

    return { $schema: 'https://vega.github.io/schema/vega-lite/v5.json', width: 'container', height: 380, autosize: { type: 'fit', contains: 'padding' }, data: { values: filtered }, layer: layers, config: vegaThemeConfig };
  }, [allData, allDates, zoomStart, zoomEnd, visibleMethods, bandVisibleMethods, activeMethodDomain, daysPerPeriod, vegaThemeConfig, isDark]);

  const racingBarsSpec = useMemo(() => {
    const src = originForecasts?.forecasts?.length > 0 ? originForecasts.forecasts : activeForecasts;
    if (!src || src.length === 0) return null;
    const barData = src.filter(f => visibleMethods[f.method] !== false).map(f => ({ method: f.method, value: f.point_forecast[selectedPeriod - 1] || 0, actual: f.actual?.[selectedPeriod - 1] || null })).sort((a, b) => b.value - a.value);
    if (barData.length === 0) return null;
    const layers = [{ mark: { type: 'bar', cornerRadiusEnd: 4 }, encoding: { y: { field: 'method', type: 'nominal', sort: '-x', title: 'Method' }, x: { field: 'value', type: 'quantitative', title: `Forecast (Month ${selectedPeriod})` }, color: { field: 'method', type: 'nominal', legend: null, scale: activeMethodDomain }, tooltip: [{ field: 'method', type: 'nominal', title: 'Method' }, { field: 'value', type: 'quantitative', title: 'Forecast', format: ',.0f' }, { field: 'actual', type: 'quantitative', title: 'Actual', format: ',.0f' }] } }];
    const actualVal = barData.find(d => d.actual !== null)?.actual;
    if (actualVal != null) {
      layers.push({ mark: { type: 'rule', color: '#e11d48', strokeWidth: 2, strokeDash: [6, 4] }, encoding: { x: { datum: actualVal } } });
      layers.push({ mark: { type: 'text', align: 'left', dx: 4, dy: -8, color: '#e11d48', fontSize: 11, fontWeight: 'bold' }, encoding: { x: { datum: actualVal }, text: { value: `Actual: ${formatNumber(actualVal, locale, 0)}` } } });
    }
    return { $schema: 'https://vega.github.io/schema/vega-lite/v5.json', width: 'container', height: Math.max(150, barData.length * 40), autosize: { type: 'fit', contains: 'padding' }, data: { values: barData }, layer: layers, config: vegaThemeConfig };
  }, [originForecasts, activeForecasts, selectedPeriod, visibleMethods, activeMethodDomain, vegaThemeConfig]);

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

  const targetChartSpec = useMemo(() => {
    if (!activeMetrics || activeMetrics.length === 0) return null;
    const data = activeMetrics.map(m => ({ method: m.method, accuracy: Math.abs(m.bias || 0), precision: m.rmse || 0, isBest: bestMethod?.best_method === m.method, composite: compositeRanking?.[m.method] ?? null }));
    const maxAccuracy = Math.max(...data.map(d => d.accuracy), 1);
    const maxPrecision = Math.max(...data.map(d => d.precision), 1);
    const bestStroke = isDark ? '#34d399' : '#059669';
    const ruleColor = isDark ? '#4b5563' : '#d1d5db';
    const pointStroke = isDark ? '#1f2937' : '#ffffff';
    return {
      $schema: 'https://vega.github.io/schema/vega-lite/v5.json', width: 'container', height: 380,
      autosize: { type: 'fit', contains: 'padding' },
      layer: [
        { data: { values: [{ x: 0, y: 0, x2: maxAccuracy * 0.5, y2: maxPrecision * 0.5 }] }, mark: { type: 'rect', opacity: isDark ? 0.10 : 0.06, color: isDark ? '#22c55e' : '#16a34a' }, encoding: { x: { field: 'x', type: 'quantitative', scale: { domain: [0, maxAccuracy * 1.15] }, title: '|Bias| (Accuracy)' }, x2: { field: 'x2' }, y: { field: 'y', type: 'quantitative', scale: { domain: [0, maxPrecision * 1.15] }, title: 'RMSE (Precision)' }, y2: { field: 'y2' } } },
        { data: { values: [{ x: maxAccuracy * 0.5, y: maxPrecision * 0.5 }] }, mark: { type: 'rule', strokeDash: [4, 4], color: ruleColor, strokeWidth: 1 }, encoding: { x: { field: 'x', type: 'quantitative' } } },
        { data: { values: [{ x: maxAccuracy * 0.5, y: maxPrecision * 0.5 }] }, mark: { type: 'rule', strokeDash: [4, 4], color: ruleColor, strokeWidth: 1 }, encoding: { y: { field: 'y', type: 'quantitative' } } },
        { data: { values: [{ x: maxAccuracy * 0.02, y: maxPrecision * 0.02, label: 'Best' }, { x: maxAccuracy * 1.05, y: maxPrecision * 0.02, label: 'Biased' }, { x: maxAccuracy * 0.02, y: maxPrecision * 1.05, label: 'Noisy' }, { x: maxAccuracy * 1.05, y: maxPrecision * 1.05, label: 'Worst' }] }, mark: { type: 'text', fontSize: 10, fontWeight: 'bold', opacity: isDark ? 0.35 : 0.25, align: 'left', baseline: 'top' }, encoding: { x: { field: 'x', type: 'quantitative' }, y: { field: 'y', type: 'quantitative' }, text: { field: 'label', type: 'nominal' } } },
        { data: { values: data }, mark: { type: 'point', filled: true, size: 200, opacity: 0.9 }, encoding: { x: { field: 'accuracy', type: 'quantitative' }, y: { field: 'precision', type: 'quantitative' }, color: { field: 'method', type: 'nominal', scale: activeMethodDomain, legend: null }, stroke: { condition: { test: 'datum.isBest', value: bestStroke }, value: pointStroke }, strokeWidth: { condition: { test: 'datum.isBest', value: 3 }, value: 1.5 }, tooltip: [{ field: 'method', type: 'nominal', title: 'Method' }, { field: 'accuracy', type: 'quantitative', title: '|Bias|', format: ',.1f' }, { field: 'precision', type: 'quantitative', title: 'RMSE', format: ',.1f' }, { field: 'composite', type: 'quantitative', title: 'Score', format: '.3f' }] } },
        { data: { values: data }, mark: { type: 'text', fontSize: 10, dy: -14, fontWeight: 500 }, encoding: { x: { field: 'accuracy', type: 'quantitative' }, y: { field: 'precision', type: 'quantitative' }, text: { field: 'method', type: 'nominal' }, color: { field: 'method', type: 'nominal', scale: activeMethodDomain, legend: null } } },
        { data: { values: data.filter(d => d.isBest) }, mark: { type: 'text', fontSize: 16, dy: 1, dx: 18 }, encoding: { x: { field: 'accuracy', type: 'quantitative' }, y: { field: 'precision', type: 'quantitative' }, text: { value: '★' }, color: { value: bestStroke } } }
      ],
      config: vegaThemeConfig
    };
  }, [activeMetrics, bestMethod, compositeRanking, activeMethodDomain, isDark, vegaThemeConfig]);

  const compositeScoreSpec = useMemo(() => {
    if (!compositeRanking || Object.keys(compositeRanking).length === 0) return null;
    const data = Object.entries(compositeRanking).map(([method, score]) => ({ method, score: score ?? 999, isBest: bestMethod?.best_method === method })).sort((a, b) => a.score - b.score);
    const bestStroke = isDark ? '#34d399' : '#059669';
    return { $schema: 'https://vega.github.io/schema/vega-lite/v5.json', width: 'container', height: Math.max(120, data.length * 36), autosize: { type: 'fit', contains: 'padding' }, data: { values: data }, mark: { type: 'bar', cornerRadiusEnd: 4 }, encoding: { y: { field: 'method', type: 'nominal', sort: { field: 'score', order: 'ascending' }, title: 'Method' }, x: { field: 'score', type: 'quantitative', title: 'Composite Score (lower is better)' }, color: { field: 'method', type: 'nominal', legend: null, scale: activeMethodDomain }, stroke: { condition: { test: 'datum.isBest', value: bestStroke }, value: null }, strokeWidth: { condition: { test: 'datum.isBest', value: 3 }, value: 0 }, tooltip: [{ field: 'method', type: 'nominal', title: 'Method' }, { field: 'score', type: 'quantitative', title: 'Composite Score', format: '.4f' }] }, config: vegaThemeConfig };
  }, [compositeRanking, bestMethod, activeMethodDomain, vegaThemeConfig, isDark]);

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
            />
            <SearchableDropdown
              label="Site"
              values={selectedSites}
              onChange={handleSitesChange}
              options={availableSites}
              recentOptions={recentSites.filter(s => availableSites.includes(s))}
              disabled={selectedItems.length === 0 || availableSites.length === 0}
              placeholder="Search site..."
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
              <span>Current series: <span className="font-mono font-medium text-gray-600 dark:text-gray-300">{selectedItem}_{selectedSite}</span></span>
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
            />
            <SearchableDropdown
              label="Site"
              values={selectedSites}
              onChange={handleSitesChange}
              options={availableSites}
              recentOptions={recentSites.filter(s => availableSites.includes(s))}
              disabled={selectedItems.length === 0 || availableSites.length === 0}
              placeholder="Search site..."
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
              <span className="font-mono font-medium text-gray-600 dark:text-gray-300">{selectedItem}_{selectedSite}</span>
            </div>
          )}
        </div>
      </div>

      {/* Header */}
      <div id="tsv-header" className="mb-6">
        <h1 className="text-2xl sm:text-3xl font-bold mb-3 dark:text-white">Series: {decodedId}</h1>
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

        /* toggles */
        sectionNodes['toggles'] = activeForecasts.length > 0 ? (
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

        /* outlier */
        sectionNodes['outlier'] = (hasOutlierCorrections && outlierChartSpec) ? (
          <Section key="outlier" title="Demand Before & After Correction" storageKey="tsv_outlier_open" badge={`${nOutliers} outlier${nOutliers !== 1 ? 's' : ''}`} {...dp('outlier')}>
            <p className="text-sm text-gray-500 dark:text-gray-400 mb-4">
              Detected via <span className="font-medium">{outlierInfo?.detection_method || 'IQR'}</span>, corrected with <span className="font-medium">{outlierInfo?.correction_method || 'clip'}</span>.
              Gray dashed = original, blue solid = corrected, red dots = outlier points.
            </p>
            <div className="w-full overflow-x-auto"><VegaLite spec={outlierChartSpec} actions={false} renderer="svg" style={{width:'100%'}} /></div>
            <ZoomSlider dates={outlierDates} start={outlierZoomStart} end={outlierZoomEnd} onStartChange={setOutlierZoomStart} onEndChange={setOutlierZoomEnd} />
          </Section>
        ) : null;

        /* main_chart */
        sectionNodes['main_chart'] = (
          <Section key="main_chart" id="tsv-main-chart" title={`Historical Data & Forecasts${horizonLength ? ` (${horizonLength}-${periodLabel} horizon)` : ''}`} storageKey="tsv_main_chart_open" {...dp('main_chart')}>
            <p className="text-sm text-gray-500 dark:text-gray-400 mb-4">Shaded bands: 50% (dark) and 90% (light) prediction intervals.</p>
            {mainChartSpec ? (
              <div className="w-full overflow-x-auto"><VegaLite spec={mainChartSpec} actions={false} renderer="svg" style={{width:'100%'}} /></div>
            ) : <div className="text-gray-400 dark:text-gray-500 py-8 text-center">No data available</div>}
            <ZoomSlider dates={allDates} start={zoomStart} end={zoomEnd} onStartChange={setZoomStart} onEndChange={setZoomEnd} />
          </Section>
        );

        /* rationale */
        if (methodExplanation) {
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
                    <div className="space-y-1">
                      {(methodExplanation.included || []).map((m, i) => (
                        <div key={i} className="flex items-start gap-2 text-sm">
                          <span className={`mt-0.5 text-xs ${m.status === 'forecasted' ? 'text-emerald-600 dark:text-emerald-400' : 'text-amber-500 dark:text-amber-400'}`}>{m.status === 'forecasted' ? '✓' : '⚠'}</span>
                          <div><span className="font-medium text-gray-700 dark:text-gray-300">{m.method}</span><span className="text-gray-400 dark:text-gray-500 ml-1 text-xs">{m.reason}</span></div>
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

        /* scoring */
        sectionNodes['scoring'] = (targetChartSpec || compositeScoreSpec) ? (
          <Section key="scoring" id="tsv-scoring" title="Accuracy vs Precision & Composite Score" storageKey="tsv_scoring_open" {...dp('scoring')}>
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              {targetChartSpec && (
                <div>
                  <h3 className="text-sm font-semibold text-gray-600 dark:text-gray-300 mb-1">Accuracy vs Precision</h3>
                  <p className="text-xs text-gray-400 dark:text-gray-500 mb-3">Bottom-left = best (low bias, low RMSE). Star = winner.</p>
                  <div className="w-full overflow-x-auto"><VegaLite spec={targetChartSpec} actions={false} renderer="svg" style={{width:'100%'}} /></div>
                </div>
              )}
              {compositeScoreSpec && (
                <div>
                  <h3 className="text-sm font-semibold text-gray-600 dark:text-gray-300 mb-1">Composite Score Ranking</h3>
                  <p className="text-xs text-gray-400 dark:text-gray-500 mb-1">Weighted score: lower is better. Green border = winner.</p>
                  {compositeWeights && (
                    <p className="text-xs text-gray-400 dark:text-gray-500 mb-3">
                      Weights: {Object.entries(compositeWeights).map(([k, v]) => `${k}=${(v * 100).toFixed(0)}%`).join(', ')}
                    </p>
                  )}
                  <div className="w-full overflow-x-auto"><VegaLite spec={compositeScoreSpec} actions={false} renderer="svg" style={{width:'100%'}} /></div>
                </div>
              )}
            </div>
          </Section>
        ) : null;

        /* metrics */
        sectionNodes['metrics'] = activeMetrics.length > 0 ? (
          <Section key="metrics" title={`Comprehensive Metrics Comparison${isMultiMode ? ' (weighted avg)' : ''}`} storageKey="tsv_metrics_open" {...dp('metrics')}>
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

        /* hyperparameters — per-method EDITABLE parameter cards */
        sectionNodes['hyperparameters'] = activeForecasts.some(f => f.hyperparameters) ? (
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
                const specificKeys = Object.keys(hp).filter(k =>
                  !commonKeys.includes(k) && !metaKeys.includes(k) && !fittedKeys.includes(k)
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
                    return (
                      <input type="text"
                        value={Array.isArray(effective) ? effective.join(', ') : String(effective ?? '')}
                        className={`w-full text-right font-mono text-xs border rounded px-1 py-0.5 ${borderCls} bg-white dark:bg-gray-900 dark:text-gray-200 focus:outline-none focus:ring-1 focus:ring-indigo-400`}
                        onChange={e => {
                          const val = e.target.value.split(',').map(s => {
                            const trimmed = s.trim();
                            const n = Number(trimmed);
                            return isNaN(n) ? trimmed : n;
                          });
                          setHpEdits(prev => ({
                            ...prev,
                            [method]: { ...(prev[method] || {}), [k]: val }
                          }));
                        }}
                      />
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
                    await axios.put(`${API_BASE_URL}/hyperparams/${encodeURIComponent(decodedId)}`, {
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
                    await axios.delete(`${API_BASE_URL}/hyperparams/${encodeURIComponent(decodedId)}?method=${encodeURIComponent(method)}`);
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

        /* ridge */
        sectionNodes['ridge'] = ridgePlotData ? (
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

          if (hasConvergence || hasRacing) {
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
                        config={{ responsive: true, displayModeBar: false }}
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
                      ? <div className="w-full overflow-x-auto"><VegaLite spec={racingBarsSpec} actions={false} renderer="svg" style={{width:'100%'}} /></div>
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

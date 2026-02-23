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
  'rationale', 'scoring', 'metrics', 'ridge', 'evolution',
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
  title, storageKey, defaultOpen = true, children, badge,
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
      className={`mb-6 bg-white rounded-lg shadow transition-all ${isDragTarget ? 'ring-2 ring-blue-400 ring-offset-1' : ''}`}
      draggable={!!dragId}
      onDragStart={dragId ? (e) => { e.dataTransfer.effectAllowed = 'move'; onDragStart(dragId); } : undefined}
      onDragOver={dragId ? (e) => { e.preventDefault(); e.dataTransfer.dropEffect = 'move'; onDragOver(dragId); } : undefined}
      onDrop={dragId ? (e) => { e.preventDefault(); onDrop(dragId); } : undefined}
      onDragEnd={dragId ? onDragEnd : undefined}
    >
      <div className="flex items-center rounded-t-lg hover:bg-gray-50 transition-colors">
        {/* Drag handle */}
        {dragId && (
          <span
            className="pl-3 pr-1 py-4 text-gray-300 hover:text-gray-500 cursor-grab active:cursor-grabbing select-none text-lg flex-shrink-0"
            title="Drag to reorder"
          >
            ⠿
          </span>
        )}
        <button
          onClick={toggle}
          className="flex-1 flex items-center justify-between px-4 py-4 text-left"
        >
          <div className="flex items-center gap-2">
            <h2 className="text-lg font-semibold">{title}</h2>
            {badge && <span className="text-xs bg-gray-100 text-gray-500 px-2 py-0.5 rounded-full">{badge}</span>}
          </div>
          <span className="text-gray-400 text-xl flex-shrink-0">{open ? '▲' : '▼'}</span>
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
      <label className="block text-xs font-medium text-gray-500 mb-1">{label}</label>
      <div
        className={`flex items-center border rounded-lg px-3 py-2 gap-2 transition-colors min-h-[40px]
          ${disabled ? 'bg-gray-50 border-gray-200 cursor-not-allowed opacity-60' : 'bg-white border-gray-300 cursor-pointer hover:border-blue-400'}
          ${open ? 'border-blue-500 ring-2 ring-blue-100' : ''}`}
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
          className="flex-1 min-w-0 text-sm outline-none bg-transparent"
        />
        {values.length > 0 && !open && (
          <button onClick={e => { e.stopPropagation(); onChange([]); setSearch(''); }}
            className="text-gray-400 hover:text-gray-600 flex-shrink-0 text-xs">✕</button>
        )}
        {values.length > 1 && (
          <span className="text-xs bg-blue-100 text-blue-700 px-1.5 py-0.5 rounded-full font-semibold flex-shrink-0">{values.length}</span>
        )}
        <svg className={`w-4 h-4 text-gray-400 flex-shrink-0 transition-transform ${open ? 'rotate-180' : ''}`}
          fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
        </svg>
      </div>

      {open && !disabled && (
        <div className="absolute z-50 mt-1 left-0 right-0 bg-white border border-gray-200 rounded-lg shadow-lg max-h-64 overflow-y-auto">
          {/* Select all / clear */}
          <div className="flex items-center gap-2 px-3 py-2 border-b border-gray-100 sticky top-0 bg-white z-10">
            <button onClick={() => onChange(options)} className="text-xs text-blue-600 hover:underline">All</button>
            <span className="text-gray-300">|</span>
            <button onClick={() => onChange([])} className="text-xs text-gray-500 hover:underline">Clear</button>
            <span className="ml-auto text-xs text-gray-400">{values.length} selected</span>
          </div>

          {/* Recent section */}
          {hasRecent && (
            <>
              <div className="px-3 py-1.5 text-xs font-semibold text-gray-400 uppercase tracking-wide bg-gray-50">
                Recently accessed
              </div>
              {filteredRecent.map(o => (
                <button key={`recent-${o}`} onClick={() => toggleOption(o)}
                  className={`w-full text-left px-3 py-2 text-sm hover:bg-blue-50 hover:text-blue-700 flex items-center gap-2 ${values.includes(o) ? 'bg-blue-50 text-blue-700' : ''}`}>
                  <span className={`w-4 h-4 rounded border flex-shrink-0 flex items-center justify-center text-xs ${values.includes(o) ? 'bg-blue-500 border-blue-500 text-white' : 'border-gray-300'}`}>{values.includes(o) ? '✓' : ''}</span>
                  <span className="text-gray-400 flex-shrink-0">🕐</span>
                  <span>{o}</span>
                </button>
              ))}
              {filteredAll.length > 0 && <div className="border-t border-gray-100" />}
            </>
          )}

          {/* All options */}
          {filteredAll.length > 0 && (
            <>
              {hasRecent && (
                <div className="px-3 py-1.5 text-xs font-semibold text-gray-400 uppercase tracking-wide bg-gray-50">
                  All
                </div>
              )}
              {filteredAll.map(o => (
                <button key={o} onClick={() => toggleOption(o)}
                  className={`w-full text-left px-3 py-2 text-sm hover:bg-blue-50 hover:text-blue-700 flex items-center gap-2 ${values.includes(o) ? 'bg-blue-50 text-blue-700' : ''}`}>
                  <span className={`w-4 h-4 rounded border flex-shrink-0 flex items-center justify-center text-xs ${values.includes(o) ? 'bg-blue-500 border-blue-500 text-white' : 'border-gray-300'}`}>{values.includes(o) ? '✓' : ''}</span>
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
  saveAdjustment, resetAllAdjustments,
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

  // Resolve base date from historical tail
  const lastDate = historicalData?.date?.length
    ? new Date(historicalData.date[historicalData.date.length - 1])
    : null;

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
          <span className="text-xs text-gray-400">{adjCount} adjustment{adjCount !== 1 ? 's' : ''} active</span>
          <button
            onClick={resetAllAdjustments}
            className="px-2.5 py-1 text-xs bg-red-50 text-red-700 border border-red-200 rounded hover:bg-red-100"
          >
            ✕ Reset all adjustments
          </button>
        </div>
      )}

      <div className="overflow-x-auto max-h-[32rem]">
        <table className="min-w-full divide-y divide-gray-200 text-sm">
          <thead className="sticky top-0 bg-gray-50 z-10">
            <tr>
              <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase sticky left-0 bg-gray-50 z-20 min-w-[9rem]">
                Method
              </th>
              {forecastDates.map((d, i) => (
                <th key={i} className="px-2 py-2 text-right text-xs font-medium text-gray-500 uppercase whitespace-nowrap">
                  {d}
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-200">
            {activeForecasts.map((f, idx) => {
              const isBest = f.method === bestMethodName;
              const rowBg  = isBest ? 'bg-emerald-50' : '';
              const stickyBg = isBest ? '#ecfdf5' : 'white';

              return (
                <React.Fragment key={f.method}>
                  {/* ── Forecast row ── */}
                  <tr className={rowBg}>
                    <td
                      className="px-3 py-2 font-medium whitespace-nowrap sticky left-0 z-10"
                      style={{ backgroundColor: stickyBg }}
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
                        const isMod = ov || adj;
                        const saving = adjSaving[`${dateStr}|adjustment`] || adjSaving[`${dateStr}|override`];
                        return (
                          <td
                            key={i}
                            className={`px-2 py-2 text-right font-mono text-xs ${ov ? 'text-red-700 font-semibold' : adj ? 'text-orange-700 font-semibold' : ''}`}
                          >
                            {saving && <span className="text-gray-300 mr-0.5 text-[10px]">⟳</span>}
                            {finalVal?.toLocaleString(undefined, { maximumFractionDigits: 0 })}
                          </td>
                        );
                      }
                      return (
                        <td key={i} className="px-2 py-2 text-right font-mono text-xs text-gray-600">
                          {v?.toLocaleString(undefined, { maximumFractionDigits: 0 })}
                        </td>
                      );
                    })}
                  </tr>

                  {/* ── Adjustment rows (only under best method, collapsible) ── */}
                  {isBest && !isMultiMode && adjRowsOpen && monthDates.length > 0 && (
                    <>
                      {/* Row 1: Adjustment (±) */}
                      <tr className="bg-orange-50/60">
                        <td
                          className="px-3 py-1 text-xs font-medium text-orange-700 whitespace-nowrap sticky left-0 z-10 bg-orange-50"
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
                                className="w-full min-w-[3.5rem] text-right border border-orange-200 rounded px-1.5 py-0.5 text-xs font-mono focus:outline-none focus:ring-1 focus:ring-orange-400 bg-white"
                              />
                            </td>
                          );
                        })}
                      </tr>

                      {/* Row 2: Override */}
                      <tr className="bg-red-50/60">
                        <td
                          className="px-3 py-1 text-xs font-medium text-red-700 whitespace-nowrap sticky left-0 z-10 bg-red-50"
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
                                className="w-full min-w-[3.5rem] text-right border border-red-200 rounded px-1.5 py-0.5 text-xs font-mono focus:outline-none focus:ring-1 focus:ring-red-400 bg-white"
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
              <tr className="border-t-2 border-indigo-200 bg-indigo-50/70">
                <td
                  className="px-3 py-2 text-xs font-semibold text-indigo-800 whitespace-nowrap sticky left-0 z-10 bg-indigo-50"
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
                        ${isOv  ? 'text-red-700'    : ''}
                        ${isAdj ? 'text-orange-700'  : ''}
                        ${!modified ? 'text-indigo-700' : ''}
                      `}
                    >
                      {cv != null ? cv.toLocaleString(undefined, { maximumFractionDigits: 0 }) : '—'}
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
        <div className="flex items-center gap-4 mt-2 text-xs text-gray-400 flex-wrap">
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
              <span className="text-gray-300">· leave blank to clear</span>
            </>
          )}
        </div>
      )}
    </Section>
  );
}


export const TimeSeriesViewer = () => {
  const { uniqueId } = useParams();
  const decodedId = decodeURIComponent(uniqueId);
  const navigate = useNavigate();

  // ---- Item/Site dropdown state (multi-select: arrays) ----
  const [allSeriesList, setAllSeriesList] = useState([]);
  const [selectedItems, setSelectedItems] = useState([]); // array of item strings
  const [selectedSites, setSelectedSites] = useState([]); // array of site strings
  const [recentItems, setRecentItems] = useState([]);
  const [recentSites, setRecentSites] = useState([]);
  // Multi-series aggregated data (when more than 1 series selected)
  const [multiSeriesData, setMultiSeriesData] = useState(null); // null = use single-series mode
  const [multiLoading, setMultiLoading] = useState(false);

  // ---- Time series data ----
  const [historicalData, setHistoricalData] = useState(null);
  const [originalData, setOriginalData] = useState(null);
  const [outlierInfo, setOutlierInfo] = useState(null);
  const [hasOutlierCorrections, setHasOutlierCorrections] = useState(false);
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

  // ---- Method visibility ----
  const [visibleMethods, setVisibleMethods] = useState({});

  // ---- Date-range zoom ----
  const [zoomStart, setZoomStart] = useState(0);
  const [zoomEnd, setZoomEnd] = useState(100);
  const [outlierZoomStart, setOutlierZoomStart] = useState(0);
  const [outlierZoomEnd, setOutlierZoomEnd] = useState(100);

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  // ---- Planner adjustments ----
  // key: "YYYY-MM-DD|type" → {id, forecast_date, adjustment_type, value, note}
  const [adjustments, setAdjustments] = useState({});
  const [adjSaving, setAdjSaving] = useState({}); // key → true while saving
  const adjDebounceRef = useRef({});             // key → timeout id

  // ---- Section drag-and-drop order ----
  const { order: sectionOrder, reorder: reorderSections } = useSectionOrder();
  const [draggingId, setDraggingId] = useState(null);
  const [dragOverId, setDragOverId] = useState(null);

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
  const allItems = useMemo(() => {
    const items = [...new Set(allSeriesList.map(s => parseUniqueId(s.unique_id).item))];
    return items.sort((a, b) => a.localeCompare(b, undefined, { numeric: true }));
  }, [allSeriesList]);

  const availableSites = useMemo(() => {
    if (selectedItems.length === 0) return [];
    // Sites available for any of the selected items
    const sites = allSeriesList
      .filter(s => selectedItems.includes(parseUniqueId(s.unique_id).item))
      .map(s => parseUniqueId(s.unique_id).site);
    return [...new Set(sites)].sort((a, b) => a.localeCompare(b, undefined, { numeric: true }));
  }, [allSeriesList, selectedItems]);

  // Derive single item/site for single-series mode (backward compat)
  const selectedItem = selectedItems[0] || '';
  const selectedSite = selectedSites[0] || '';

  /* ---------- data loading ---------- */
  useEffect(() => {
    loadData();
    return () => { if (playTimerRef.current) clearInterval(playTimerRef.current); };
  }, [decodedId]);

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
        const fcasts = forecastRes.value.data.forecasts || [];
        setForecasts(fcasts);
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
      if (bestRes.status === 'fulfilled') setBestMethod(bestRes.value.data);
      if (explainRes.status === 'fulfilled') setMethodExplanation(explainRes.value.data);
      if (distRes.status === 'fulfilled') setDistributions(distRes.value.data);
      if (originsRes.status === 'fulfilled') {
        const o = originsRes.value.data.origins || [];
        setOrigins(o);
        if (o.length > 0) setSelectedOriginIdx(o.length - 1);
      }
      setLoading(false);
    } catch (err) {
      setError(err.message);
      setLoading(false);
    }
  };

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

  /* ---------- build combined data for main chart ---------- */
  const { allData, allDates } = useMemo(() => {
    if (!activeHistoricalData || !activeHistoricalData.date || activeHistoricalData.date.length === 0)
      return { allData: [], allDates: [] };
    const data = [];
    const dateSet = new Set();
    activeHistoricalData.date.forEach((date, i) => {
      dateSet.add(date);
      data.push({ date, value: activeHistoricalData.value[i], type: 'Actual', method: 'Historical', lo90: null, hi90: null, lo50: null, hi50: null, layer: 'line' });
    });

    // Forecast data + adjustment/override marker points
    if (activeForecasts.length > 0) {
      const lastDate = new Date(activeHistoricalData.date[activeHistoricalData.date.length - 1]);

      // Use only the best method's forecast to compute base values for adjustments
      const bestFc = activeForecasts.find(f => f.method === bestMethod?.best_method) || activeForecasts[0];

      activeForecasts.forEach(forecast => {
        const quantiles = forecast.quantiles || {};
        forecast.point_forecast.forEach((value, i) => {
          const d = new Date(lastDate); d.setUTCMonth(d.getUTCMonth() + i + 1);
          const dateStr = fmtDate(d);
          dateSet.add(dateStr);
          const lo90 = quantiles['0.05']?.[i] ?? null;
          const hi90 = quantiles['0.95']?.[i] ?? null;
          data.push({ date: dateStr, value, type: 'Forecast', method: forecast.method, lo90, hi90, lo50: quantiles['0.25']?.[i] ?? lo90, hi50: quantiles['0.75']?.[i] ?? hi90, layer: 'line' });
          if (lo90 != null && hi90 != null)
            data.push({ date: dateStr, value: null, type: 'Band', method: forecast.method, lo90, hi90, lo50: quantiles['0.25']?.[i] ?? lo90, hi50: quantiles['0.75']?.[i] ?? hi90, layer: 'band' });
        });
      });

      // Add adjustment / override marker data points (plotted over forecast)
      if (bestFc) {
        bestFc.point_forecast.forEach((baseValue, i) => {
          const d = new Date(lastDate); d.setUTCMonth(d.getUTCMonth() + i + 1);
          const dateStr = fmtDate(d);
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
    const sortedDates = [...dateSet].sort();
    return { allData: data, allDates: sortedDates };
  }, [activeHistoricalData, activeForecasts, adjustments, bestMethod]);

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
      config: { view: { stroke: null } }
    };
  }, [outlierChartData, outlierDates, outlierZoomStart, outlierZoomEnd]);

  const mainChartSpec = useMemo(() => {
    if (allData.length === 0 || allDates.length === 0) return null;
    const minDate = allDates[Math.min(zoomStart, allDates.length - 1)] || allDates[0];
    const maxDate = allDates[Math.min(zoomEnd, allDates.length - 1)] || allDates[allDates.length - 1];
    const filtered = allData.filter(d => {
      if (d.type !== 'Actual' && d.method !== 'Historical' && visibleMethods[d.method] === false) return false;
      return d.date >= minDate && d.date <= maxDate;
    });
    if (filtered.length === 0) return null;
    const colorScale = { field: 'method', type: 'nominal', scale: activeMethodDomain, legend: { title: 'Method' } };
    const hasBands = filtered.some(d => d.layer === 'band');
    const layers = [];
    if (hasBands) {
      layers.push({ transform: [{ filter: "datum.layer === 'band'" }], mark: { type: 'area', opacity: 0.12 }, encoding: { x: { field: 'date', type: 'temporal' }, y: { field: 'lo90', type: 'quantitative' }, y2: { field: 'hi90' }, color: { ...colorScale, legend: null } } });
      layers.push({ transform: [{ filter: "datum.layer === 'band'" }], mark: { type: 'area', opacity: 0.25 }, encoding: { x: { field: 'date', type: 'temporal' }, y: { field: 'lo50', type: 'quantitative' }, y2: { field: 'hi50' }, color: { ...colorScale, legend: null } } });
    }
    layers.push({ transform: [{ filter: "datum.layer === 'line'" }], mark: { type: 'line', point: false, strokeWidth: 2 }, encoding: { x: { field: 'date', type: 'temporal', title: 'Date', axis: { format: '%Y-%m' } }, y: { field: 'value', type: 'quantitative', title: 'Demand', scale: { zero: false } }, color: colorScale, strokeDash: { field: 'type', type: 'nominal', scale: { domain: ['Actual', 'Forecast'], range: [[1, 0], [5, 5]] }, legend: null }, opacity: { condition: { test: "datum.type === 'Actual'", value: 1 }, value: 0.85 }, tooltip: [{ field: 'date', type: 'temporal', title: 'Date' }, { field: 'value', type: 'quantitative', title: 'Value', format: ',.0f' }, { field: 'method', type: 'nominal', title: 'Method' }, { field: 'type', type: 'nominal', title: 'Type' }] } });

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

    return { $schema: 'https://vega.github.io/schema/vega-lite/v5.json', width: 'container', height: 380, autosize: { type: 'fit', contains: 'padding' }, data: { values: filtered }, layer: layers, config: { view: { stroke: null } } };
  }, [allData, allDates, zoomStart, zoomEnd, visibleMethods, activeMethodDomain]);

  const racingBarsSpec = useMemo(() => {
    const src = originForecasts?.forecasts?.length > 0 ? originForecasts.forecasts : activeForecasts;
    if (!src || src.length === 0) return null;
    const barData = src.filter(f => visibleMethods[f.method] !== false).map(f => ({ method: f.method, value: f.point_forecast[selectedPeriod - 1] || 0, actual: f.actual?.[selectedPeriod - 1] || null })).sort((a, b) => b.value - a.value);
    if (barData.length === 0) return null;
    const layers = [{ mark: { type: 'bar', cornerRadiusEnd: 4 }, encoding: { y: { field: 'method', type: 'nominal', sort: '-x', title: 'Method' }, x: { field: 'value', type: 'quantitative', title: `Forecast (Month ${selectedPeriod})` }, color: { field: 'method', type: 'nominal', legend: null, scale: activeMethodDomain }, tooltip: [{ field: 'method', type: 'nominal', title: 'Method' }, { field: 'value', type: 'quantitative', title: 'Forecast', format: ',.0f' }, { field: 'actual', type: 'quantitative', title: 'Actual', format: ',.0f' }] } }];
    const actualVal = barData.find(d => d.actual !== null)?.actual;
    if (actualVal != null) {
      layers.push({ mark: { type: 'rule', color: '#e11d48', strokeWidth: 2, strokeDash: [6, 4] }, encoding: { x: { datum: actualVal } } });
      layers.push({ mark: { type: 'text', align: 'left', dx: 4, dy: -8, color: '#e11d48', fontSize: 11, fontWeight: 'bold' }, encoding: { x: { datum: actualVal }, text: { value: `Actual: ${actualVal.toLocaleString()}` } } });
    }
    return { $schema: 'https://vega.github.io/schema/vega-lite/v5.json', width: 'container', height: Math.max(150, barData.length * 40), autosize: { type: 'fit', contains: 'padding' }, data: { values: barData }, layer: layers };
  }, [originForecasts, activeForecasts, selectedPeriod, visibleMethods, activeMethodDomain]);

  const targetChartSpec = useMemo(() => {
    if (!activeMetrics || activeMetrics.length === 0) return null;
    const data = activeMetrics.map(m => ({ method: m.method, accuracy: Math.abs(m.bias || 0), precision: m.rmse || 0, isBest: bestMethod?.best_method === m.method, composite: compositeRanking?.[m.method] ?? null }));
    const maxAccuracy = Math.max(...data.map(d => d.accuracy), 1);
    const maxPrecision = Math.max(...data.map(d => d.precision), 1);
    return {
      $schema: 'https://vega.github.io/schema/vega-lite/v5.json', width: 'container', height: 380,
      autosize: { type: 'fit', contains: 'padding' },
      layer: [
        { data: { values: [{ x: 0, y: 0, x2: maxAccuracy * 0.5, y2: maxPrecision * 0.5 }] }, mark: { type: 'rect', opacity: 0.06, color: '#16a34a' }, encoding: { x: { field: 'x', type: 'quantitative', scale: { domain: [0, maxAccuracy * 1.15] }, title: '|Bias| (Accuracy)' }, x2: { field: 'x2' }, y: { field: 'y', type: 'quantitative', scale: { domain: [0, maxPrecision * 1.15] }, title: 'RMSE (Precision)' }, y2: { field: 'y2' } } },
        { data: { values: [{ x: maxAccuracy * 0.5, y: maxPrecision * 0.5 }] }, mark: { type: 'rule', strokeDash: [4, 4], color: '#d1d5db', strokeWidth: 1 }, encoding: { x: { field: 'x', type: 'quantitative' } } },
        { data: { values: [{ x: maxAccuracy * 0.5, y: maxPrecision * 0.5 }] }, mark: { type: 'rule', strokeDash: [4, 4], color: '#d1d5db', strokeWidth: 1 }, encoding: { y: { field: 'y', type: 'quantitative' } } },
        { data: { values: [{ x: maxAccuracy * 0.02, y: maxPrecision * 0.02, label: 'Best' }, { x: maxAccuracy * 1.05, y: maxPrecision * 0.02, label: 'Biased' }, { x: maxAccuracy * 0.02, y: maxPrecision * 1.05, label: 'Noisy' }, { x: maxAccuracy * 1.05, y: maxPrecision * 1.05, label: 'Worst' }] }, mark: { type: 'text', fontSize: 10, fontWeight: 'bold', opacity: 0.25, align: 'left', baseline: 'top' }, encoding: { x: { field: 'x', type: 'quantitative' }, y: { field: 'y', type: 'quantitative' }, text: { field: 'label', type: 'nominal' } } },
        { data: { values: data }, mark: { type: 'point', filled: true, size: 200, opacity: 0.9 }, encoding: { x: { field: 'accuracy', type: 'quantitative' }, y: { field: 'precision', type: 'quantitative' }, color: { field: 'method', type: 'nominal', scale: activeMethodDomain, legend: null }, stroke: { condition: { test: 'datum.isBest', value: '#059669' }, value: '#ffffff' }, strokeWidth: { condition: { test: 'datum.isBest', value: 3 }, value: 1.5 }, tooltip: [{ field: 'method', type: 'nominal', title: 'Method' }, { field: 'accuracy', type: 'quantitative', title: '|Bias|', format: ',.1f' }, { field: 'precision', type: 'quantitative', title: 'RMSE', format: ',.1f' }, { field: 'composite', type: 'quantitative', title: 'Score', format: '.3f' }] } },
        { data: { values: data }, mark: { type: 'text', fontSize: 10, dy: -14, fontWeight: 500 }, encoding: { x: { field: 'accuracy', type: 'quantitative' }, y: { field: 'precision', type: 'quantitative' }, text: { field: 'method', type: 'nominal' }, color: { field: 'method', type: 'nominal', scale: activeMethodDomain, legend: null } } },
        { data: { values: data.filter(d => d.isBest) }, mark: { type: 'text', fontSize: 16, dy: 1, dx: 18 }, encoding: { x: { field: 'accuracy', type: 'quantitative' }, y: { field: 'precision', type: 'quantitative' }, text: { value: '★' }, color: { value: '#059669' } } }
      ],
      config: { view: { stroke: null } }
    };
  }, [activeMetrics, bestMethod, compositeRanking, activeMethodDomain]);

  const compositeScoreSpec = useMemo(() => {
    if (!compositeRanking || Object.keys(compositeRanking).length === 0) return null;
    const data = Object.entries(compositeRanking).map(([method, score]) => ({ method, score: score ?? 999, isBest: bestMethod?.best_method === method })).sort((a, b) => a.score - b.score);
    return { $schema: 'https://vega.github.io/schema/vega-lite/v5.json', width: 'container', height: Math.max(120, data.length * 36), autosize: { type: 'fit', contains: 'padding' }, data: { values: data }, mark: { type: 'bar', cornerRadiusEnd: 4 }, encoding: { y: { field: 'method', type: 'nominal', sort: { field: 'score', order: 'ascending' }, title: 'Method' }, x: { field: 'score', type: 'quantitative', title: 'Composite Score (lower is better)' }, color: { field: 'method', type: 'nominal', legend: null, scale: activeMethodDomain }, stroke: { condition: { test: 'datum.isBest', value: '#059669' }, value: null }, strokeWidth: { condition: { test: 'datum.isBest', value: 3 }, value: 0 }, tooltip: [{ field: 'method', type: 'nominal', title: 'Method' }, { field: 'score', type: 'quantitative', title: 'Composite Score', format: '.4f' }] } };
  }, [compositeRanking, bestMethod, activeMethodDomain]);

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
      hovertemplate: `M${h.horizon_month} mean: ${h.mean.toLocaleString(undefined, { maximumFractionDigits: 0 })}<extra></extra>`,
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
    if (pct) return (value * 100).toFixed(0) + '%';
    return value.toLocaleString(undefined, { maximumFractionDigits: 1 });
  };

  const forecastDates = useMemo(() => {
    if (!activeHistoricalData || !activeHistoricalData.date || activeHistoricalData.date.length === 0 || horizonLength === 0) return [];
    const lastDate = new Date(activeHistoricalData.date[activeHistoricalData.date.length - 1]);
    return Array.from({ length: horizonLength }, (_, i) => { const d = new Date(lastDate); d.setUTCMonth(d.getUTCMonth() + i + 1); return d.toISOString().slice(0, 7); });
  }, [activeHistoricalData, horizonLength]);

  /* ---------- Dual-range zoom slider component ---------- */
  const ZoomSlider = ({ dates, start, end, onStartChange, onEndChange }) => {
    if (dates.length <= 1) return null;
    return (
      <div className="mt-4 pt-4 border-t border-gray-100">
        <style>{`
          .dual-range-container{position:relative;height:32px}
          .dual-range-track{position:absolute;top:50%;left:0;right:0;height:6px;transform:translateY(-50%);background:#e5e7eb;border-radius:3px}
          .dual-range-highlight{position:absolute;top:50%;height:6px;transform:translateY(-50%);background:#3b82f6;border-radius:3px}
          .dual-range-input{position:absolute;top:0;left:0;width:100%;height:100%;-webkit-appearance:none;appearance:none;background:transparent;pointer-events:none;margin:0}
          .dual-range-input::-webkit-slider-thumb{-webkit-appearance:none;appearance:none;width:18px;height:18px;border-radius:50%;background:#3b82f6;border:2px solid #fff;box-shadow:0 1px 3px rgba(0,0,0,.3);cursor:pointer;pointer-events:auto}
          .dual-range-input::-moz-range-thumb{width:18px;height:18px;border-radius:50%;background:#3b82f6;border:2px solid #fff;box-shadow:0 1px 3px rgba(0,0,0,.3);cursor:pointer;pointer-events:auto}
          .dual-range-input::-webkit-slider-runnable-track{height:0}
          .dual-range-input::-moz-range-track{height:0;background:transparent}
        `}</style>
        <div className="flex items-center gap-2 sm:gap-3 flex-wrap">
          <span className="text-xs font-medium text-gray-500 uppercase tracking-wide">Zoom</span>
          <span className="text-xs sm:text-sm font-mono bg-gray-100 px-2 py-0.5 rounded">{dates[start]?.slice(0, 7)}</span>
          <div className="flex-1 min-w-32 dual-range-container">
            <div className="dual-range-track" />
            <div className="dual-range-highlight" style={{ left: `${(start / (dates.length - 1)) * 100}%`, right: `${100 - (end / (dates.length - 1)) * 100}%` }} />
            <input type="range" min={0} max={dates.length - 1} value={start} onChange={e => { const v = parseInt(e.target.value); if (v < end) onStartChange(v); }} className="dual-range-input" style={{ zIndex: start > dates.length * 0.9 ? 5 : 3 }} />
            <input type="range" min={0} max={dates.length - 1} value={end} onChange={e => { const v = parseInt(e.target.value); if (v > start) onEndChange(v); }} className="dual-range-input" style={{ zIndex: 4 }} />
          </div>
          <span className="text-xs sm:text-sm font-mono bg-gray-100 px-2 py-0.5 rounded">{dates[end]?.slice(0, 7)}</span>
          <button onClick={() => { onStartChange(0); onEndChange(dates.length - 1); }} className="text-xs bg-gray-200 hover:bg-gray-300 px-2 py-1 rounded transition-colors">Reset</button>
        </div>
      </div>
    );
  };

  /* ---------- render ---------- */
  if (loading) return <div className="flex items-center justify-center h-64"><div className="animate-pulse text-xl text-gray-500">Loading time series...</div></div>;
  if (error) return <div className="flex items-center justify-center h-64"><div className="text-xl text-red-600">Error: {error}</div></div>;

  return (
    <div className="p-4 sm:p-6">

      {/* Item / Site Selector */}
      <div className="mb-6 bg-white rounded-lg shadow p-4">
        <div className="flex flex-col sm:flex-row gap-3 sm:gap-4">
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
        </div>
        <div className="mt-3 text-xs text-gray-400 flex flex-wrap gap-2 items-center">
          {isMultiMode ? (
            <>
              <span className="bg-blue-100 text-blue-700 px-2 py-0.5 rounded font-medium">
                Multi-series: {multiSeriesData?.uids?.length} series
              </span>
              <span className="text-gray-400">Demand &amp; Forecast = sum · Metrics = weighted average</span>
              {multiLoading && <span className="text-blue-500 animate-pulse">Loading...</span>}
            </>
          ) : (selectedItem && selectedSite && (
            <span>Current series: <span className="font-mono font-medium text-gray-600">{selectedItem}_{selectedSite}</span></span>
          ))}
        </div>
      </div>

      {/* Header */}
      <div className="mb-6">
        <h1 className="text-2xl sm:text-3xl font-bold mb-3">Series: {decodedId}</h1>
        {characteristics && (
          <div className="flex flex-wrap gap-2 text-sm">
            <span className="bg-gray-100 px-3 py-1 rounded-full">{characteristics.n_observations} observations</span>
            <span className={`px-3 py-1 rounded-full ${characteristics.is_intermittent ? 'bg-amber-100 text-amber-800' : 'bg-gray-100'}`}>{characteristics.is_intermittent ? 'Intermittent' : 'Continuous'}</span>
            <span className={`px-3 py-1 rounded-full ${characteristics.has_seasonality ? 'bg-blue-100 text-blue-800' : 'bg-gray-100'}`}>{characteristics.has_seasonality ? 'Seasonal' : 'Non-Seasonal'}</span>
            <span className={`px-3 py-1 rounded-full ${characteristics.has_trend ? 'bg-purple-100 text-purple-800' : 'bg-gray-100'}`}>{characteristics.has_trend ? 'Trending' : 'Stationary'}</span>
            <span className={`px-3 py-1 rounded-full font-medium ${characteristics.complexity_level === 'high' ? 'bg-red-100 text-red-800' : characteristics.complexity_level === 'medium' ? 'bg-yellow-100 text-yellow-800' : 'bg-green-100 text-green-800'}`}>{characteristics.complexity_level} complexity</span>
            {hasOutlierCorrections && <span className="bg-orange-100 text-orange-800 px-3 py-1 rounded-full font-semibold">{nOutliers} outlier{nOutliers !== 1 ? 's' : ''} adjusted</span>}
            {bestMethod && <span className="bg-emerald-100 text-emerald-800 px-3 py-1 rounded-full font-semibold">Winner: {bestMethod.best_method}</span>}
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
          <Section key="toggles" title="Method Toggles" storageKey="tsv_toggles_open" {...dp('toggles')}>
            <div className="flex flex-wrap gap-2">
              {activeForecasts.map(f => (
                <button key={f.method} onClick={() => toggleMethod(f.method)}
                  className={`px-3 py-1.5 rounded-full text-sm font-medium border-2 transition-all ${visibleMethods[f.method] ? 'text-white border-transparent' : 'bg-white text-gray-400 border-gray-200'}`}
                  style={visibleMethods[f.method] ? { backgroundColor: getMethodColor(f.method), borderColor: getMethodColor(f.method) } : {}}>
                  {f.method}{bestMethod?.best_method === f.method && ' ★'}
                </button>
              ))}
            </div>
          </Section>
        ) : null;

        /* outlier */
        sectionNodes['outlier'] = (hasOutlierCorrections && outlierChartSpec) ? (
          <Section key="outlier" title="Demand Before & After Correction" storageKey="tsv_outlier_open" badge={`${nOutliers} outlier${nOutliers !== 1 ? 's' : ''}`} {...dp('outlier')}>
            <p className="text-sm text-gray-500 mb-4">
              Detected via <span className="font-medium">{outlierInfo?.detection_method || 'IQR'}</span>, corrected with <span className="font-medium">{outlierInfo?.correction_method || 'clip'}</span>.
              Gray dashed = original, blue solid = corrected, red dots = outlier points.
            </p>
            <div className="w-full overflow-x-auto"><VegaLite spec={outlierChartSpec} actions={false} renderer="svg" style={{width:'100%'}} /></div>
            <ZoomSlider dates={outlierDates} start={outlierZoomStart} end={outlierZoomEnd} onStartChange={setOutlierZoomStart} onEndChange={setOutlierZoomEnd} />
          </Section>
        ) : null;

        /* main_chart */
        sectionNodes['main_chart'] = (
          <Section key="main_chart" title={`Historical Data & Forecasts${horizonLength ? ` (${horizonLength}-month horizon)` : ''}`} storageKey="tsv_main_chart_open" {...dp('main_chart')}>
            <p className="text-sm text-gray-500 mb-4">Shaded bands: 50% (dark) and 90% (light) prediction intervals.</p>
            {mainChartSpec ? (
              <div className="w-full overflow-x-auto"><VegaLite spec={mainChartSpec} actions={false} renderer="svg" style={{width:'100%'}} /></div>
            ) : <div className="text-gray-400 py-8 text-center">No data available</div>}
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
                <rect x={padL} y={ySigPos} width={innerW} height={ySigNeg - ySigPos} fill="#dbeafe" fillOpacity={0.5} />
                <line x1={padL} x2={padL + innerW} y1={ySigPos} y2={ySigPos} stroke="#93c5fd" strokeWidth={1} strokeDasharray="3,2"/>
                <line x1={padL} x2={padL + innerW} y1={ySigNeg} y2={ySigNeg} stroke="#93c5fd" strokeWidth={1} strokeDasharray="3,2"/>
                <line x1={padL} x2={padL + innerW} y1={y0} y2={y0} stroke="#94a3b8" strokeWidth={1}/>
                {values.map((v, i) => {
                  const x  = toX(i) - barW / 2;
                  const yv = toY(v);
                  const significant = Math.abs(v) > sigBand;
                  return (
                    <g key={i}>
                      <rect x={x} y={Math.min(yv, y0)} width={barW} height={Math.abs(yv - y0)}
                            fill={significant ? color : '#cbd5e1'} fillOpacity={0.85} rx={1}/>
                      <title>Lag {lags[i]}: {v.toFixed(3)}</title>
                    </g>
                  );
                })}
                {[-0.5, 0, 0.5, 1].filter(v => v >= yMin && v <= yMax).map(v => (
                  <g key={v}>
                    <line x1={padL - 3} x2={padL} y1={toY(v)} y2={toY(v)} stroke="#94a3b8" strokeWidth={1}/>
                    <text x={padL - 5} y={toY(v) + 3.5} textAnchor="end" fontSize={8} fill="#64748b">{v}</text>
                  </g>
                ))}
                {lags.map((lg, i) => i % 2 === 0 && (
                  <text key={i} x={toX(i)} y={H - 4} textAnchor="middle" fontSize={8} fill="#64748b">{lg}</text>
                ))}
                <text x={padL} y={padT - 2} fontSize={9} fontWeight="600" fill="#475569">{label}</text>
              </svg>
            );
          };

          const GaugeBar = ({ value, max = 1, color, bgColor = '#e5e7eb', height = 8 }) => {
            const pct = Math.min(1, Math.max(0, value / max)) * 100;
            return (
              <div style={{ background: bgColor, borderRadius: 4, height, overflow: 'hidden', width: '100%' }}>
                <div style={{ width: `${pct}%`, height: '100%', background: color, borderRadius: 4, transition: 'width 0.4s' }} />
              </div>
            );
          };

          const StatCard = ({ label, value, sub, color, badge, badgeColor, gauge, gaugeMax, gaugeColor }) => (
            <div className="bg-gray-50 border border-gray-200 rounded-lg p-3 flex flex-col gap-1.5">
              <div className="flex items-center justify-between gap-1">
                <span className="text-xs text-gray-500 font-medium">{label}</span>
                {badge && (
                  <span className={`text-xs px-1.5 py-0.5 rounded-full font-semibold ${badgeColor || 'bg-gray-100 text-gray-600'}`}>{badge}</span>
                )}
              </div>
              <div className="flex items-baseline gap-1.5">
                <span className="text-lg font-bold" style={{ color: color || '#111827' }}>{value}</span>
                {sub && <span className="text-xs text-gray-400">{sub}</span>}
              </div>
              {gauge !== undefined && (
                <GaugeBar value={gauge} max={gaugeMax || 1} color={gaugeColor || '#6366f1'} />
              )}
            </div>
          );

          const complexityColor = chars.complexity_level === 'high' ? '#dc2626' : chars.complexity_level === 'medium' ? '#d97706' : '#16a34a';
          const complexityBadgeColor = chars.complexity_level === 'high' ? 'bg-red-100 text-red-700' : chars.complexity_level === 'medium' ? 'bg-yellow-100 text-yellow-700' : 'bg-green-100 text-green-700';
          const adfColor = chars.adf_pvalue <= 0.05 ? '#16a34a' : '#dc2626';
          const adfBadge = chars.is_stationary ? 'Stationary' : 'Non-stationary';
          const adfBadgeColor = chars.is_stationary ? 'bg-green-100 text-green-700' : 'bg-red-100 text-red-700';
          const trendBadgeColor = chars.has_trend ? 'bg-orange-100 text-orange-700' : 'bg-gray-100 text-gray-500';
          const seasonalBadgeColor = chars.has_seasonality ? 'bg-violet-100 text-violet-700' : 'bg-gray-100 text-gray-500';
          const intermittentBadgeColor = chars.is_intermittent ? 'bg-amber-100 text-amber-700' : 'bg-emerald-100 text-emerald-700';
          const cvLabel = chars.mean > 0 ? (chars.std / chars.mean).toFixed(2) : '—';

          sectionNodes['rationale'] = (
            <Section key="rationale" title="Method Selection Rationale" storageKey="tsv_rationale_open" defaultOpen={false} {...dp('rationale')}>
              {/* ── Demand Characteristics Grid ── */}
              <div className="mb-5">
                <h3 className="text-sm font-semibold text-gray-700 mb-3 flex items-center gap-2">
                  Demand Characteristics
                  <span className="text-xs font-normal text-gray-400">All signals used to select forecasting methods</span>
                </h3>
                <div className="grid grid-cols-2 sm:grid-cols-4 gap-2 mb-2">
                  <StatCard label="Observations" value={chars.n_observations} sub={`${chars.date_range_start?.slice(0,7)} → ${chars.date_range_end?.slice(0,7)}`} color="#111827" />
                  <StatCard label="Mean Demand" value={chars.mean != null ? chars.mean.toFixed(1) : '—'} sub="units/period" color="#2563eb" />
                  <StatCard label="Std Deviation" value={chars.std != null ? chars.std.toFixed(1) : '—'} sub="units/period" color="#7c3aed" />
                  <StatCard label="Coeff. of Variation" value={cvLabel} sub="σ / μ  (volatility)" color={parseFloat(cvLabel) > 1 ? '#dc2626' : parseFloat(cvLabel) > 0.5 ? '#d97706' : '#16a34a'} gauge={Math.min(parseFloat(cvLabel) || 0, 2)} gaugeMax={2} gaugeColor={parseFloat(cvLabel) > 1 ? '#dc2626' : parseFloat(cvLabel) > 0.5 ? '#d97706' : '#16a34a'} />
                </div>
                <div className="grid grid-cols-2 sm:grid-cols-4 gap-2 mb-2">
                  <StatCard label="Zero Ratio" value={`${((chars.zero_ratio || 0) * 100).toFixed(1)}%`} sub="% periods with zero demand" color={chars.zero_ratio > 0.5 ? '#dc2626' : chars.zero_ratio > 0.2 ? '#d97706' : '#374151'} badge={chars.is_intermittent ? 'Intermittent' : 'Continuous'} badgeColor={intermittentBadgeColor} gauge={chars.zero_ratio || 0} gaugeMax={1} gaugeColor={chars.zero_ratio > 0.5 ? '#dc2626' : chars.zero_ratio > 0.2 ? '#d97706' : '#6b7280'} />
                  <StatCard label="ADI" value={(chars.adi || 0).toFixed(2)} sub="Avg Demand Interval (periods)" color={chars.adi > 1.32 ? '#dc2626' : '#374151'} gauge={Math.min(chars.adi || 0, 5)} gaugeMax={5} gaugeColor={chars.adi > 1.32 ? '#f59e0b' : '#6b7280'} />
                  <StatCard label="CoV (non-zero)" value={(chars.cov || 0).toFixed(2)} sub="Coeff. of Variation of demand sizes" color={chars.cov > 0.49 ? '#d97706' : '#374151'} gauge={Math.min(chars.cov || 0, 2)} gaugeMax={2} gaugeColor={chars.cov > 0.49 ? '#f59e0b' : '#6b7280'} />
                  <div className="bg-gray-50 border border-gray-200 rounded-lg p-3 flex flex-col gap-1.5">
                    <span className="text-xs text-gray-500 font-medium">Demand Pattern</span>
                    <div className="flex flex-col gap-1 mt-1">
                      <div className="flex items-center gap-2">
                        <span className={`w-2 h-2 rounded-full flex-shrink-0 ${chars.is_intermittent ? 'bg-amber-400' : 'bg-emerald-400'}`}/>
                        <span className="text-xs font-semibold">{chars.is_intermittent ? 'Intermittent' : 'Continuous'}</span>
                      </div>
                      <div className="text-xs text-gray-400">ADI &gt; 1.32 or &lt; 5 demand periods → intermittent</div>
                    </div>
                  </div>
                </div>
                <div className="grid grid-cols-2 sm:grid-cols-4 gap-2 mb-2">
                  <StatCard label="Trend" value={chars.has_trend ? `${chars.trend_direction === 'up' ? '↑' : '↓'} ${chars.trend_direction}` : 'None'} sub={`Kendall's τ = ${(chars.trend_strength || 0).toFixed(3)}`} color={chars.has_trend ? '#ea580c' : '#6b7280'} badge={chars.has_trend ? 'Trending' : 'No trend'} badgeColor={trendBadgeColor} gauge={chars.trend_strength || 0} gaugeMax={1} gaugeColor={chars.has_trend ? '#ea580c' : '#d1d5db'} />
                  <StatCard label="Seasonality" value={chars.has_seasonality ? `Periods: ${(chars.seasonal_periods || []).join(', ')}` : 'None detected'} sub={`ACF strength: ${(chars.seasonal_strength || 0).toFixed(3)}`} color={chars.has_seasonality ? '#7c3aed' : '#6b7280'} badge={chars.has_seasonality ? 'Seasonal' : 'Non-seasonal'} badgeColor={seasonalBadgeColor} gauge={chars.seasonal_strength || 0} gaugeMax={1} gaugeColor={chars.has_seasonality ? '#7c3aed' : '#d1d5db'} />
                  <StatCard label="ADF p-value" value={(chars.adf_pvalue != null ? chars.adf_pvalue : 1).toFixed(4)} sub="Augmented Dickey-Fuller test" color={adfColor} badge={adfBadge} badgeColor={adfBadgeColor} gauge={Math.max(0, 1 - (chars.adf_pvalue || 1))} gaugeMax={1} gaugeColor={adfColor} />
                  <StatCard label="Complexity Score" value={(chars.complexity_score || 0).toFixed(3)} sub="0 = simple · 1 = highly complex" color={complexityColor} badge={`${chars.complexity_level} complexity`} badgeColor={complexityBadgeColor} gauge={chars.complexity_score || 0} gaugeMax={1} gaugeColor={complexityColor} />
                </div>
                <div className="grid grid-cols-2 sm:grid-cols-4 gap-2">
                  <div className="col-span-2 sm:col-span-2 bg-gray-50 border border-gray-200 rounded-lg p-3">
                    <span className="text-xs text-gray-500 font-medium block mb-2">Data Sufficiency</span>
                    <div className="flex flex-col gap-1.5">
                      {[
                        { label: 'Statistical models', ok: true, note: 'Always available' },
                        { label: 'Sparse check (obs/year)', ok: !chars.is_sparse, note: `${chars.obs_per_year != null ? chars.obs_per_year.toFixed(1) : '—'} obs/yr — threshold < ${chars.sparse_obs_per_year_threshold ?? 5}` },
                        { label: 'ML models (LightGBM, XGBoost)', ok: chars.sufficient_for_ml, note: `≥100 obs — has ${chars.n_observations}` },
                        { label: 'Deep Learning (NHITS, NBEATS…)', ok: chars.sufficient_for_deep_learning, note: `≥200 obs — has ${chars.n_observations}` },
                      ].map(({ label, ok, note }) => (
                        <div key={label} className="flex items-center gap-2">
                          <span className={`flex-shrink-0 w-4 h-4 rounded-full flex items-center justify-center text-xs ${ok ? 'bg-emerald-100 text-emerald-700' : 'bg-red-100 text-red-500'}`}>
                            {ok ? '✓' : '✗'}
                          </span>
                          <span className="text-xs font-medium text-gray-700">{label}</span>
                          <span className="text-xs text-gray-400 ml-auto">{note}</span>
                        </div>
                      ))}
                    </div>
                  </div>
                  <div className="col-span-2 sm:col-span-2 bg-blue-50 border border-blue-100 rounded-lg p-3">
                    <span className="text-xs font-medium text-blue-700 block mb-1">
                      Selection Category: <span className="font-bold">{methodExplanation.selection_category}</span>
                    </span>
                    <p className="text-xs text-blue-600 leading-relaxed">{methodExplanation.selection_reason}</p>
                  </div>
                </div>
              </div>

              {/* ── ACF + PACF charts ── */}
              {(acf.lags.length > 0 || pacf.lags.length > 0) && (
                <div className="mb-5">
                  <h3 className="text-sm font-semibold text-gray-700 mb-3 flex items-center gap-2">
                    Autocorrelation Analysis
                    <span className="text-xs font-normal text-gray-400">Bars outside blue band are statistically significant (95% CI)</span>
                  </h3>
                  <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                    <div className="bg-gray-50 rounded-lg p-3 border border-gray-200">
                      <p className="text-xs text-gray-500 mb-2 font-medium">ACF — Autocorrelation Function</p>
                      <p className="text-xs text-gray-400 mb-2">Spikes at regular lags → seasonal pattern. Slow decay → trend or non-stationarity.</p>
                      <div className="overflow-x-auto">
                        <CorrelogramChart lags={acf.lags} values={acf.values} ciUpper={acf.ci_upper} ciLower={acf.ci_lower} label="ACF" color="#6366f1" />
                      </div>
                    </div>
                    <div className="bg-gray-50 rounded-lg p-3 border border-gray-200">
                      <p className="text-xs text-gray-500 mb-2 font-medium">PACF — Partial Autocorrelation Function</p>
                      <p className="text-xs text-gray-400 mb-2">Removes indirect lag effects. Spike only at lag k → AR(k). Helps determine ARIMA order.</p>
                      <div className="overflow-x-auto">
                        <CorrelogramChart lags={pacf.lags} values={pacf.values} ciUpper={null} ciLower={null} label="PACF" color="#0891b2" />
                      </div>
                    </div>
                  </div>
                </div>
              )}

              {/* ── Included / Excluded methods ── */}
              <div>
                <h3 className="text-sm font-semibold text-gray-700 mb-3">Method Eligibility</h3>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  <div>
                    <h3 className="text-sm font-semibold text-emerald-700 mb-2">Applied Methods ({methodExplanation.included?.length || 0})</h3>
                    <div className="space-y-1">
                      {(methodExplanation.included || []).map((m, i) => (
                        <div key={i} className="flex items-start gap-2 text-sm">
                          <span className={`mt-0.5 text-xs ${m.status === 'forecasted' ? 'text-emerald-600' : 'text-amber-500'}`}>{m.status === 'forecasted' ? '✓' : '⚠'}</span>
                          <div><span className="font-medium text-gray-700">{m.method}</span><span className="text-gray-400 ml-1 text-xs">{m.reason}</span></div>
                        </div>
                      ))}
                    </div>
                  </div>
                  <div>
                    <h3 className="text-sm font-semibold text-red-600 mb-2">Excluded Methods ({methodExplanation.excluded?.length || 0})</h3>
                    <div className="space-y-1">
                      {(methodExplanation.excluded || []).map((m, i) => (
                        <div key={i} className="flex items-start gap-2 text-sm">
                          <span className="mt-0.5 text-xs text-red-400">✗</span>
                          <div><span className="font-medium text-gray-600">{m.method}</span><span className="text-gray-400 ml-1 text-xs">{m.reason}</span></div>
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
          <Section key="scoring" title="Accuracy vs Precision & Composite Score" storageKey="tsv_scoring_open" {...dp('scoring')}>
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              {targetChartSpec && (
                <div>
                  <h3 className="text-sm font-semibold text-gray-600 mb-1">Accuracy vs Precision</h3>
                  <p className="text-xs text-gray-400 mb-3">Bottom-left = best (low bias, low RMSE). Star = winner.</p>
                  <div className="w-full overflow-x-auto"><VegaLite spec={targetChartSpec} actions={false} renderer="svg" style={{width:'100%'}} /></div>
                </div>
              )}
              {compositeScoreSpec && (
                <div>
                  <h3 className="text-sm font-semibold text-gray-600 mb-1">Composite Score Ranking</h3>
                  <p className="text-xs text-gray-400 mb-1">Weighted score: lower is better. Green border = winner.</p>
                  {compositeWeights && (
                    <p className="text-xs text-gray-400 mb-3">
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
              <table className="min-w-full divide-y divide-gray-200 text-sm">
                <thead><tr className="bg-gray-50">
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
                      className={`px-2 py-2 text-xs font-medium text-gray-500 uppercase whitespace-nowrap ${field === 'method' ? 'text-left' : 'text-right'} ${sortable ? 'cursor-pointer hover:bg-gray-100 select-none' : ''}`}>
                      {label}{sortable ? metricsSortIndicator(field) : ''}
                    </th>
                  ))}
                  {compositeRanking && (
                    <th onClick={() => handleMetricsSort('composite')}
                      className="px-2 py-2 text-right text-xs font-medium text-gray-500 uppercase cursor-pointer hover:bg-gray-100 whitespace-nowrap select-none">
                      Score{metricsSortIndicator('composite')}
                    </th>
                  )}
                </tr></thead>
                <tbody className="divide-y divide-gray-200">
                  {sortedMetrics.map((m, idx) => {
                    const isBest = bestMethod?.best_method === m.method;
                    return (
                      <tr key={idx} className={isBest ? 'bg-emerald-50' : ''}>
                        <td className="px-2 py-2 font-medium whitespace-nowrap text-left">
                          <span className="inline-block w-2.5 h-2.5 rounded-full mr-1.5" style={{ backgroundColor: getMethodColor(m.method) }}></span>
                          {m.method}
                          {isBest && <span className="ml-1.5 text-xs bg-emerald-200 text-emerald-800 px-1 py-0.5 rounded font-semibold">Best</span>}
                        </td>
                        {['mae', 'rmse'].map(f => (<td key={f} className={`px-2 py-2 text-right font-mono ${isBestVal(f, m[f]) ? 'text-emerald-700 font-bold' : ''}`}>{fmtMetric(m[f])}</td>))}
                        <td className={`px-2 py-2 text-right font-mono ${isBestVal('bias', m.bias) ? 'text-emerald-700 font-bold' : ''}`}>{fmtMetric(m.bias)}</td>
                        {['mape', 'smape'].map(f => (<td key={f} className={`px-2 py-2 text-right font-mono ${isBestVal(f, m[f]) ? 'text-emerald-700 font-bold' : ''}`}>{m[f] != null ? m[f].toFixed(1) + '%' : '-'}</td>))}
                        <td className={`px-2 py-2 text-right font-mono ${isBestVal('mase', m.mase) ? 'text-emerald-700 font-bold' : ''}`}>{fmtMetric(m.mase)}</td>
                        {['crps', 'winkler_score'].map(f => (<td key={f} className={`px-2 py-2 text-right font-mono ${isBestVal(f, m[f]) ? 'text-emerald-700 font-bold' : ''}`}>{fmtMetric(m[f])}</td>))}
                        {['coverage_50', 'coverage_80', 'coverage_90', 'coverage_95'].map(f => (<td key={f} className={`px-2 py-2 text-right font-mono ${isBestVal(f, m[f]) ? 'text-emerald-700 font-bold' : ''}`}>{fmtMetric(m[f], true)}</td>))}
                        <td className={`px-2 py-2 text-right font-mono ${isBestVal('quantile_loss', m.quantile_loss) ? 'text-emerald-700 font-bold' : ''}`}>{fmtMetric(m.quantile_loss)}</td>
                        <td className="px-2 py-2 text-right">{m.n_windows}</td>
                        {compositeRanking && (<td className={`px-2 py-2 text-right font-mono font-semibold ${isBest ? 'text-emerald-700' : ''}`}>{compositeRanking[m.method] != null ? compositeRanking[m.method].toFixed(4) : '-'}</td>)}
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </Section>
        ) : null;

        /* ridge */
        sectionNodes['ridge'] = ridgePlotData ? (
          <Section key="ridge" title="Forecast Distribution Over Time (3D)" storageKey="tsv_ridge_open" {...dp('ridge')}>
            <p className="text-sm text-gray-500 mb-1">
              3D surface of forecast density by horizon ({distributions?.method || 'best method'}). X = forecast value, Y = horizon month, Z = density. Dashed lines = mean per horizon.
            </p>
            {distributions?.horizons?.some(h => h.is_bootstrap) && (
              <p className="text-xs text-amber-600 mb-3">Some horizons use bootstrap distributions — parametric fit was not available.</p>
            )}
            <div className="w-full" style={{ height: 520 }}>
              <Plot
                data={ridgePlotData.traces}
                layout={{
                  autosize: true,
                  margin: { l: 0, r: 0, t: 10, b: 0 },
                  paper_bgcolor: 'rgba(0,0,0,0)',
                  scene: {
                    xaxis: { title: { text: 'Forecast Value', font: { size: 11 } }, tickformat: ',.0f', gridcolor: '#e5e7eb', zerolinecolor: '#cbd5e1' },
                    yaxis: { title: { text: 'Horizon (month)', font: { size: 11 } }, tickformat: 'd', gridcolor: '#e5e7eb', zerolinecolor: '#cbd5e1' },
                    zaxis: { title: { text: 'Density', font: { size: 11 } }, gridcolor: '#e5e7eb', zerolinecolor: '#cbd5e1' },
                    camera: { eye: { x: -1.6, y: -1.6, z: 1.0 } },
                    bgcolor: 'rgba(0,0,0,0)',
                  },
                  legend: { x: 0.02, y: 0.98, bgcolor: 'rgba(255,255,255,0.7)', bordercolor: '#e5e7eb', borderwidth: 1 },
                }}
                config={{ responsive: true, displayModeBar: true, displaylogo: false, modeBarButtonsToRemove: ['toImage'] }}
                style={{ width: '100%', height: '100%' }}
                useResizeHandler
              />
            </div>
          </Section>
        ) : null;

        /* evolution */
        if (isMultiMode && activeForecasts.length > 0) {
          sectionNodes['evolution'] = (
            <Section key="evolution" title="Method Comparison (aggregated)" storageKey="tsv_evolution_open" {...dp('evolution')}>
              <div className="flex items-center gap-2 mb-4 flex-wrap">
                <span className="text-sm text-gray-600">Horizon month:</span>
                {[1, 3, 6, 12, 18, 24].filter(p => p <= horizonLength).map(p => (
                  <button key={p} onClick={() => setSelectedPeriod(p)}
                    className={`px-3 py-1.5 rounded-lg text-sm font-medium transition-colors ${selectedPeriod === p ? 'bg-blue-500 text-white' : 'bg-gray-100 hover:bg-gray-200 text-gray-700'}`}>
                    M{p}
                  </button>
                ))}
              </div>
              {racingBarsSpec
                ? <div className="w-full overflow-x-auto"><VegaLite spec={racingBarsSpec} actions={false} renderer="svg" style={{width:'100%'}} /></div>
                : <div className="text-gray-400 py-4 text-center text-sm">No comparison data</div>
              }
            </Section>
          );
        } else if (origins.length > 0 || activeForecasts.length > 0) {
          sectionNodes['evolution'] = (
            <Section key="evolution" title={origins.length > 0 ? 'Forecast Evolution Over Time' : 'Method Comparison'} storageKey="tsv_evolution_open" {...dp('evolution')}>
              <p className="text-sm text-gray-500 mb-4">{origins.length > 0 ? 'See how forecasts changed at different points in time.' : 'Compare forecast values across methods for each horizon month.'}</p>
              {origins.length > 0 && (
                <div className="flex items-center gap-3 mb-4 flex-wrap">
                  <button onClick={togglePlay} className={`px-4 py-2 rounded-lg text-white text-sm font-medium transition-colors ${isPlaying ? 'bg-red-500 hover:bg-red-600' : 'bg-blue-500 hover:bg-blue-600'}`}>
                    {isPlaying ? '■ Stop' : '▶ Play'}
                  </button>
                  <div className="flex-1 min-w-32">
                    <input type="range" min={0} max={origins.length - 1} value={selectedOriginIdx} onChange={e => setSelectedOriginIdx(parseInt(e.target.value))} className="w-full accent-blue-500" />
                  </div>
                  <div className="text-sm font-mono bg-blue-50 text-blue-800 px-3 py-1.5 rounded-lg min-w-28 text-center font-medium">{origins[selectedOriginIdx] || '-'}</div>
                </div>
              )}
              <div className="flex items-center gap-2 mb-4 flex-wrap">
                <span className="text-sm text-gray-600">Horizon month:</span>
                {horizonLength <= 12
                  ? Array.from({ length: horizonLength }, (_, i) => i + 1).map(p => (
                      <button key={p} onClick={() => setSelectedPeriod(p)}
                        className={`px-3 py-1.5 rounded-lg text-sm font-medium transition-colors ${selectedPeriod === p ? 'bg-blue-500 text-white' : 'bg-gray-100 hover:bg-gray-200 text-gray-700'}`}>
                        M{p}
                      </button>
                    ))
                  : (
                    <>
                      {[1, 3, 6, 12, 18, 24].filter(p => p <= horizonLength).map(p => (
                        <button key={p} onClick={() => setSelectedPeriod(p)}
                          className={`px-3 py-1.5 rounded-lg text-sm font-medium transition-colors ${selectedPeriod === p ? 'bg-blue-500 text-white' : 'bg-gray-100 hover:bg-gray-200 text-gray-700'}`}>
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
                : <div className="text-gray-400 py-4 text-center text-sm">No comparison data</div>
              }
            </Section>
          );
        } else {
          sectionNodes['evolution'] = null;
        }

        /* forecast_table */
        sectionNodes['forecast_table'] = activeForecasts.length > 0 ? (
          <div key="forecast_table">
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
            />
          </div>
        ) : null;

        return sectionOrder.map(id => sectionNodes[id] || null);
      })()}

      {activeMetrics.length === 0 && activeForecasts.length > 0 && !isMultiMode && (
        <div className="mb-6 bg-white rounded-lg shadow p-6">
          <h2 className="text-lg font-semibold mb-2">Backtest Metrics</h2>
          <p className="text-gray-500 text-sm">This series has insufficient history for rolling-window backtesting (needs {12 + horizonLength}+ monthly observations). Forecasts are still generated.</p>
        </div>
      )}

      {activeForecasts.length === 0 && activeMetrics.length === 0 && (
        <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-6 text-center">
          <p className="text-yellow-800">No forecasts or backtest metrics available for this series.</p>
        </div>
      )}
    </div>
  );
};

export default TimeSeriesViewer;

/**
 * Dashboard Component
 *
 * Top-level view for all time-series.  Summary cards, aggregate charts,
 * and a fully interactive series table with per-column sort / filter /
 * hide and drag-to-reorder.  All sections are individually collapsible.
 */

import React, { useState, useEffect, useMemo, useCallback, useRef } from 'react';
import { useNavigate } from 'react-router-dom';
import Plot from 'react-plotly.js';
import { useLocale } from '../contexts/LocaleContext';
import { useTheme } from '../contexts/ThemeContext';
import { formatNumber } from '../utils/formatting';
import api from '../utils/api';

const TABLEAU10 = ['#4e79a7','#f28e2b','#e15759','#76b7b2','#59a14f','#edc948','#b07aa1','#ff9da7','#9c755f','#bab0ac'];
const ABC_COLORS = { A: '#22c55e', B: '#eab308', C: '#f97316', D: '#ef4444', X: '#3b82f6', Y: '#a855f7', Z: '#ec4899' };

/** Split unique_id on the first underscore into {item, site}. */
const parseSeriesId = (uid) => {
  if (!uid) return { item: '', site: '' };
  const idx = uid.indexOf('_');
  if (idx === -1) return { item: uid, site: '' };
  return { item: uid.slice(0, idx), site: uid.slice(idx + 1) };
};

// ─── Column definitions ───────────────────────────────────────────
// ABC columns are inserted dynamically after 'complexity_level'.
const FIXED_COLS = [
  { id: '_item',            label: 'Item',        type: 'text',    sortKey: '_item',            defaultHidden: false },
  { id: '_site',            label: 'Site',        type: 'text',    sortKey: '_site',            defaultHidden: false },
  { id: 'n_observations',   label: 'Obs',         type: 'number',  sortKey: 'n_observations',   defaultHidden: false },
  { id: 'complexity_level', label: 'Complexity',  type: 'enum',    sortKey: 'complexity_level', defaultHidden: false,
    opts: ['low', 'medium', 'high'] },
  // ABC cols inserted here by effectiveColOrder
  { id: 'is_intermittent',  label: 'Interm.',     type: 'boolean', sortKey: 'is_intermittent',  defaultHidden: false },
  { id: 'has_seasonality',  label: 'Seasonal',    type: 'boolean', sortKey: 'has_seasonality',  defaultHidden: false },
  { id: 'has_trend',        label: 'Trend',       type: 'boolean', sortKey: 'has_trend',        defaultHidden: true  },
  { id: 'mean',             label: 'Mean',        type: 'number',  sortKey: 'mean',             defaultHidden: false },
  { id: '_sparkline',       label: 'Demand',      type: null,      sortKey: null,               defaultHidden: false },
  { id: 'n_outliers',       label: 'Adj.',        type: 'adj',     sortKey: 'n_outliers',       defaultHidden: false },
  { id: 'best_method',      label: 'Best Method', type: 'text',    sortKey: 'best_method',      defaultHidden: false },
];

// ─── Default column widths (px) ──────────────────────────────────
const DEFAULT_COL_WIDTHS = {
  '_item': 130, '_site': 110, 'n_observations': 64,
  'complexity_level': 95, 'is_intermittent': 70, 'has_seasonality': 70,
  'has_trend': 60, 'mean': 80, '_sparkline': 120, 'n_outliers': 54,
  'best_method': 140,
};
const getDefaultColWidth = (colId) => {
  if (DEFAULT_COL_WIDTHS[colId] !== undefined) return DEFAULT_COL_WIDTHS[colId];
  if (colId.startsWith('_cls_')) return 70;
  return 100;
};

function buildDefaultOrder(abcColIds) {
  const base = FIXED_COLS.map(c => c.id);
  const cIdx = base.indexOf('complexity_level');
  return [...base.slice(0, cIdx + 1), ...abcColIds, ...base.slice(cIdx + 1)];
}
function buildDefaultHidden() {
  return new Set(FIXED_COLS.filter(c => c.defaultHidden).map(c => c.id));
}

// ─── Section ──────────────────────────────────────────────────────
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
        <span className="text-gray-400 dark:text-gray-500 text-xl">{open ? '\u25B2' : '\u25BC'}</span>
      </button>
      {open && <div className="px-4 pb-4 sm:px-6 sm:pb-6">{children}</div>}
    </div>
  );
};

// ─── Sparkline ────────────────────────────────────────────────────
const Sparkline = ({ historical = [], forecast = [], width = 100, height = 28 }) => {
  const all = [...historical, ...forecast];
  if (all.length === 0) return <span className="text-gray-300 dark:text-gray-600 text-xs">-</span>;
  const min = Math.min(...all), max = Math.max(...all), range = max - min || 1, pad = 1;
  const toX = (i, total) => pad + ((width - 2 * pad) * i) / Math.max(total - 1, 1);
  const toY = (v) => height - pad - ((v - min) / range) * (height - 2 * pad);
  const hLen = historical.length, totalLen = hLen + forecast.length;
  const histPoints = historical.map((v, i) => `${toX(i, totalLen)},${toY(v)}`).join(' ');
  const fcStart = hLen > 0 ? `${toX(hLen - 1, totalLen)},${toY(historical[hLen - 1])} ` : '';
  const fcPoints = fcStart + forecast.map((v, i) => `${toX(hLen + i, totalLen)},${toY(v)}`).join(' ');
  const divX = hLen > 0 ? toX(hLen - 1, totalLen) : null;
  return (
    <svg width={width} height={height} className="inline-block align-middle">
      {histPoints && <polyline points={histPoints} fill="none" stroke="#6b7280" strokeWidth="1.5" />}
      {forecast.length > 0 && <polyline points={fcPoints} fill="none" stroke="#2563eb" strokeWidth="1.5" strokeDasharray="3,2" />}
      {divX != null && forecast.length > 0 && <line x1={divX} y1={0} x2={divX} y2={height} stroke="#d1d5db" strokeWidth="0.5" strokeDasharray="2,2" />}
    </svg>
  );
};

// ─── Filter operator definitions ─────────────────────────────────
const TEXT_OPS = [
  { value: 'contains',   label: 'contains'   },
  { value: 'starts',     label: 'starts with' },
  { value: 'ends',       label: 'ends with'  },
  { value: 'equals',     label: '= equals'   },
  { value: 'not_equals', label: '\u2260 not equals' },
  { value: 'not_null',   label: '\u2713 has value'  },
  { value: 'is_null',    label: '\u00d8 is empty'   },
];
const NUM_OPS = [
  { value: '>=', label: '\u2265'         },
  { value: '>',  label: '>'             },
  { value: '<=', label: '\u2264'         },
  { value: '<',  label: '<'             },
  { value: '=',  label: '='             },
  { value: '!=', label: '\u2260'         },
  { value: 'not_null', label: '\u2713 not null' },
  { value: 'is_null',  label: '\u00d8 is null'  },
];
const NO_VAL_OPS = new Set(['not_null', 'is_null']);

/** Normalise a filter value into {op, val} for text/number columns. */
const normFilter = (val, defaultOp) => {
  if (!val || val === '') return { op: defaultOp, val: '' };
  if (typeof val === 'object') return val;
  return { op: defaultOp, val: String(val) };
};

/** Apply a text filter object (or legacy string) to a field value. */
const applyTextFilter = (fieldVal, filter) => {
  if (!filter || filter === '') return true;
  const { op, val } = typeof filter === 'string'
    ? { op: 'contains', val: filter }
    : filter;
  if (op === 'not_null') return fieldVal != null && fieldVal !== '';
  if (op === 'is_null')  return fieldVal == null || fieldVal === '';
  if (!val) return true;
  const v = (fieldVal ?? '').toLowerCase();
  const q = val.toLowerCase();
  if (op === 'starts')     return v.startsWith(q);
  if (op === 'ends')       return v.endsWith(q);
  if (op === 'equals')     return v === q;
  if (op === 'not_equals') return v !== q;
  return v.includes(q); // 'contains' (default)
};

/** Apply a numeric filter object (or legacy string) to a field value. */
const applyNumFilter = (numVal, filter) => {
  if (!filter || filter === '') return true;
  const { op, val } = typeof filter === 'string'
    ? { op: '>=', val: filter }
    : filter;
  if (op === 'not_null') return numVal != null && !isNaN(numVal);
  if (op === 'is_null')  return numVal == null || isNaN(numVal);
  if (!val) return true;
  const num = parseFloat(val);
  if (isNaN(num)) return true;
  const v = parseFloat(numVal) ?? 0;
  if (op === '>')  return v > num;
  if (op === '<=') return v <= num;
  if (op === '<')  return v < num;
  if (op === '=')  return v === num;
  if (op === '!=') return v !== num;
  return v >= num; // '>=' (default)
};

/** Check if a filter value (string or {op,val}) is active. */
const isFilterActive = (f) => {
  if (!f || f === '') return false;
  if (typeof f === 'object') {
    if (NO_VAL_OPS.has(f.op)) return true;
    return f.val !== '' && f.val != null;
  }
  return true;
};

// ─── Per-column filter input ──────────────────────────────────────
const ColFilter = ({ col, value, onChange }) => {
  const selCls = 'px-1 py-0.5 text-xs border rounded bg-white dark:bg-gray-700 text-gray-800 dark:text-gray-200 border-gray-300 dark:border-gray-600 focus:outline-none focus:ring-1 focus:ring-blue-400 cursor-pointer';
  const inpCls = 'min-w-0 flex-1 px-1.5 py-0.5 text-xs border rounded bg-white dark:bg-gray-700 text-gray-800 dark:text-gray-200 border-gray-300 dark:border-gray-600 focus:outline-none focus:ring-1 focus:ring-blue-400 placeholder-gray-400 dark:placeholder-gray-500';
  const stop = (e) => e.stopPropagation();

  if (!col.type) return <td className="px-1 py-1" />;

  // Enum / boolean / adj — simple select, unchanged
  if (col.type === 'enum') return (
    <td className="px-1 py-1">
      <select value={value || ''} onChange={e => onChange(e.target.value)} className={selCls + ' w-full'} onClick={stop}>
        <option value="">All</option>
        {(col.opts || []).map(o => <option key={o} value={o}>{o}</option>)}
      </select>
    </td>
  );
  if (col.type === 'boolean') return (
    <td className="px-1 py-1">
      <select value={value || ''} onChange={e => onChange(e.target.value)} className={selCls + ' w-full'} onClick={stop}>
        <option value="">All</option>
        <option value="true">Yes</option>
        <option value="false">No</option>
      </select>
    </td>
  );
  if (col.type === 'adj') return (
    <td className="px-1 py-1">
      <select value={value || ''} onChange={e => onChange(e.target.value)} className={selCls + ' w-full'} onClick={stop}>
        <option value="">All</option>
        <option value="true">Adjusted</option>
        <option value="false">Not adj.</option>
      </select>
    </td>
  );

  // Text / number — operator + value
  const isNum  = col.type === 'number';
  const ops    = isNum ? NUM_OPS : TEXT_OPS;
  const defOp  = isNum ? '>=' : 'contains';
  const { op, val } = normFilter(value, defOp);
  const needsVal = !NO_VAL_OPS.has(op);

  return (
    <td className="px-1 py-1">
      <div className="flex gap-0.5 items-center" onClick={stop}>
        <select
          value={op}
          onChange={e => onChange({ op: e.target.value, val })}
          className={selCls + ' shrink-0'}
          style={{ maxWidth: isNum ? '3rem' : '5.5rem' }}
          title="Filter operator"
        >
          {ops.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
        </select>
        {needsVal && (
          <input
            type={isNum ? 'number' : 'text'}
            placeholder={isNum ? 'value' : '\u2026'}
            value={val}
            onChange={e => onChange({ op, val: e.target.value })}
            className={inpCls}
          />
        )}
      </div>
    </td>
  );
};

// ─── Main component ───────────────────────────────────────────────
export const Dashboard = () => {
  const { locale, numberDecimals } = useLocale();
  const { isDark } = useTheme();
  const navigate = useNavigate();

  // ── Core data ──
  const [series, setSeries]       = useState([]);
  const [analytics, setAnalytics] = useState(null);
  const [sparklineData, setSparklineData] = useState({});
  const [loading, setLoading]     = useState(true);
  const [error, setError]         = useState(null);
  const [scenarios, setScenarios] = useState([]);
  const [activeScenarioId, setActiveScenarioId] = useState(1);

  const [accuracyPrecisionData, setAccuracyPrecisionData] = useState(null);
  const [selectedAccuracyMethod, setSelectedAccuracyMethod] = useState('');

  const [aggregateDemand, setAggregateDemand] = useState(null);
  const [aggLoading, setAggLoading] = useState(false);
  const [aggError, setAggError]     = useState(null);

  const [abcConfigs, setAbcConfigs] = useState([]);

  // Chart-interaction filter (separate from column filters)
  const [accuracyZoom, setAccuracyZoom] = useState(null);

  // ── Column management ──
  const abcColDefs = useMemo(() =>
    abcConfigs.map(cfg => ({
      id: `_cls_${cfg.name}`,
      label: cfg.name,
      type: 'enum',
      sortKey: `classifications.${cfg.name}`,
      defaultHidden: false,
      opts: cfg.class_labels || [],
    })), [abcConfigs]);

  const abcColIds = useMemo(() => abcColDefs.map(c => c.id), [abcColDefs]);

  const allColDefs = useMemo(() => {
    const map = {};
    FIXED_COLS.forEach(c => { map[c.id] = c; });
    abcColDefs.forEach(c => { map[c.id] = c; });
    return map;
  }, [abcColDefs]);

  const [colOrder, setColOrderRaw] = useState(() => {
    try { return JSON.parse(localStorage.getItem('dash_col_order') || 'null'); }
    catch { return null; }
  });
  const setColOrder = useCallback((order) => {
    setColOrderRaw(order);
    localStorage.setItem('dash_col_order', JSON.stringify(order));
  }, []);

  const effectiveColOrder = useMemo(() => {
    const def = buildDefaultOrder(abcColIds);
    if (!colOrder) return def;
    let order = colOrder.filter(id => id in allColDefs);
    const missing = abcColIds.filter(id => !order.includes(id));
    if (missing.length) {
      const cIdx = order.indexOf('complexity_level');
      order.splice(cIdx + 1, 0, ...missing);
    }
    FIXED_COLS.forEach(c => { if (!order.includes(c.id)) order.push(c.id); });
    return order;
  }, [colOrder, abcColIds, allColDefs]);

  const [hiddenCols, setHiddenColsRaw] = useState(() => {
    try {
      const stored = localStorage.getItem('dash_col_hidden');
      return stored ? new Set(JSON.parse(stored)) : buildDefaultHidden();
    } catch { return buildDefaultHidden(); }
  });
  const setHiddenCols = useCallback((fn) => {
    setHiddenColsRaw(prev => {
      const next = typeof fn === 'function' ? fn(prev) : fn;
      localStorage.setItem('dash_col_hidden', JSON.stringify([...next]));
      return next;
    });
  }, []);
  const toggleColHidden = useCallback((id) => {
    setHiddenCols(prev => {
      const next = new Set(prev);
      next.has(id) ? next.delete(id) : next.add(id);
      return next;
    });
  }, [setHiddenCols]);

  // Column widths (resizable)
  const [colWidths, setColWidthsRaw] = useState(() => {
    try { return JSON.parse(localStorage.getItem('dash_col_widths') || 'null') || {}; }
    catch { return {}; }
  });
  const setColWidth = useCallback((colId, width) => {
    setColWidthsRaw(prev => {
      const next = { ...prev, [colId]: width };
      localStorage.setItem('dash_col_widths', JSON.stringify(next));
      return next;
    });
  }, []);
  const handleResizeMouseDown = useCallback((e, colId, startWidth) => {
    e.stopPropagation();
    e.preventDefault();
    const startX = e.clientX;
    const onMove = (mv) => {
      const newW = Math.max(40, startWidth + mv.clientX - startX);
      setColWidth(colId, newW);
    };
    const onUp = () => {
      document.removeEventListener('mousemove', onMove);
      document.removeEventListener('mouseup', onUp);
      document.body.style.cursor = '';
      document.body.style.userSelect = '';
    };
    document.body.style.cursor = 'col-resize';
    document.body.style.userSelect = 'none';
    document.addEventListener('mousemove', onMove);
    document.addEventListener('mouseup', onUp);
  }, [setColWidth]);


  // Unified column filters
  const [colFilters, setColFilters] = useState({});
  const [page, setPage] = useState(0);
  const setColFilter = useCallback((colId, val) => {
    setColFilters(prev => ({ ...prev, [colId]: val }));
    setPage(0);
  }, []);
  const clearAllFilters = useCallback(() => {
    setColFilters({});
    setAccuracyZoom(null);
    setPage(0);
  }, []);

  // Sort
  const [sortField, setSortField] = useState('_item');
  const [sortDir, setSortDir]     = useState('asc');
  const handleSort = useCallback((field) => {
    if (!field) return;
    setSortField(prev => {
      if (prev === field) { setSortDir(d => d === 'asc' ? 'desc' : 'asc'); return field; }
      setSortDir('asc');
      return field;
    });
  }, []);
  const sortInd = (field) => sortField === field ? (sortDir === 'asc' ? ' \u25B2' : ' \u25BC') : '';

  // Drag-and-drop column reorder
  const [dragColId, setDragColId]         = useState(null);
  const [dragOverColId, setDragOverColId] = useState(null);
  const handleColDragStart = useCallback((e, id) => { setDragColId(id); e.dataTransfer.effectAllowed = 'move'; }, []);
  const handleColDragOver  = useCallback((e, id) => { e.preventDefault(); e.dataTransfer.dropEffect = 'move'; setDragOverColId(id); }, []);
  const handleColDrop = useCallback((e, targetId) => {
    e.preventDefault();
    setDragColId(src => {
      if (!src || src === targetId) return null;
      const order = [...effectiveColOrder];
      const fi = order.indexOf(src), ti = order.indexOf(targetId);
      if (fi !== -1 && ti !== -1) { order.splice(fi, 1); order.splice(ti, 0, src); setColOrder(order); }
      return null;
    });
    setDragOverColId(null);
  }, [effectiveColOrder, setColOrder]);
  const handleColDragEnd = useCallback(() => { setDragColId(null); setDragOverColId(null); }, []);

  // Columns dropdown
  const [colsMenuOpen, setColsMenuOpen] = useState(false);
  const colsMenuRef = useRef(null);
  useEffect(() => {
    if (!colsMenuOpen) return;
    const h = (e) => { if (colsMenuRef.current && !colsMenuRef.current.contains(e.target)) setColsMenuOpen(false); };
    document.addEventListener('mousedown', h);
    return () => document.removeEventListener('mousedown', h);
  }, [colsMenuOpen]);

  const pageSize = 50;

  // Load scenarios
  useEffect(() => {
    api.get('/forecast/scenarios').then(r => setScenarios(r.data)).catch(() => {});
  }, []);

  // ── Data loading ──
  useEffect(() => { loadData(); }, [activeScenarioId]);
  const loadData = async () => {
    setLoading(true); setError(null);
    try {
      const [seriesRes, analyticsRes, abcRes] = await Promise.allSettled([
        api.get('/series', { params: { limit: 50000, scenario_id: activeScenarioId } }),
        api.get('/analytics'),
        api.get('/abc/configurations'),
      ]);
      if (seriesRes.status === 'fulfilled') setSeries(seriesRes.value.data || []);
      else console.error('Failed to load series:', seriesRes.reason);
      if (analyticsRes.status === 'fulfilled') setAnalytics(analyticsRes.value.data);
      else console.error('Failed to load analytics:', analyticsRes.reason);
      if (abcRes.status === 'fulfilled') setAbcConfigs((abcRes.value.data || []).filter(c => c.is_active));
    } catch (err) { console.error('Dashboard load error:', err); setError(err.message); }
    finally { setLoading(false); }
  };

  useEffect(() => {
    const load = async () => {
      try {
        const params = selectedAccuracyMethod ? { method: selectedAccuracyMethod } : {};
        const res = await api.get('/analytics/accuracy-precision', { params });
        setAccuracyPrecisionData(res.data);
      } catch (err) { console.error('Failed to load accuracy/precision data:', err); setAccuracyPrecisionData(null); }
    };
    load();
  }, [selectedAccuracyMethod]);

  // ── Filtered + sorted series ──
  const filteredSeries = useMemo(() => {
    let result = series || [];
    Object.entries(colFilters).forEach(([colId, filter]) => {
      if (!isFilterActive(filter)) return;
      if (colId === '_item') {
        result = result.filter(s => applyTextFilter(s.item_name ?? parseSeriesId(s.unique_id).item, filter));
      } else if (colId === '_site') {
        result = result.filter(s => applyTextFilter(s.site_name ?? parseSeriesId(s.unique_id).site, filter));
      } else if (colId.startsWith('_cls_')) {
        // ABC classification — enum select (string value)
        const cfgName = colId.slice(5);
        result = result.filter(s => s.classifications?.[cfgName] === filter);
      } else if (colId === 'is_intermittent' || colId === 'has_seasonality' || colId === 'has_trend') {
        result = result.filter(s => s[colId] === (filter === 'true'));
      } else if (colId === 'n_outliers') {
        result = result.filter(s => s.has_outlier_corrections === (filter === 'true'));
      } else if (colId === 'complexity_level') {
        result = result.filter(s => s.complexity_level === filter);
      } else if (colId === 'best_method') {
        result = result.filter(s => applyTextFilter(s.best_method ?? '', filter));
      } else if (colId === 'n_observations' || colId === 'mean') {
        result = result.filter(s => applyNumFilter(s[colId], filter));
      }
    });
    if (accuracyZoom && accuracyPrecisionData?.points) {
      const zoomedIds = new Set(
        accuracyPrecisionData.points
          .filter(d => d.accuracy >= accuracyZoom.x[0] && d.accuracy <= accuracyZoom.x[1] &&
                       d.precision >= accuracyZoom.y[0] && d.precision <= accuracyZoom.y[1])
          .map(d => d.unique_id)
      );
      result = result.filter(s => zoomedIds.has(s.unique_id));
    }
    return [...result].sort((a, b) => {
      let va, vb;
      if (sortField.startsWith('classifications.')) {
        const cfgName = sortField.slice('classifications.'.length);
        va = a.classifications?.[cfgName] ?? ''; vb = b.classifications?.[cfgName] ?? '';
      } else if (sortField === '_item') {
        va = a.item_name ?? parseSeriesId(a.unique_id).item; vb = b.item_name ?? parseSeriesId(b.unique_id).item;
      } else if (sortField === '_site') {
        va = a.site_name ?? parseSeriesId(a.unique_id).site; vb = b.site_name ?? parseSeriesId(b.unique_id).site;
      } else { va = a[sortField]; vb = b[sortField]; }
      if (typeof va === 'string') va = va.toLowerCase();
      if (typeof vb === 'string') vb = vb.toLowerCase();
      if (va < vb) return sortDir === 'asc' ? -1 : 1;
      if (va > vb) return sortDir === 'asc' ? 1 : -1;
      return 0;
    });
  }, [series, colFilters, sortField, sortDir, accuracyZoom, accuracyPrecisionData]);

  // Aggregate demand (debounced 400 ms)
  const aggTimerRef = useRef(null);
  useEffect(() => {
    if (aggTimerRef.current) clearTimeout(aggTimerRef.current);
    setAggLoading(true); setAggError(null);
    aggTimerRef.current = setTimeout(async () => {
      try {
        const ids = filteredSeries.map(s => s.unique_id);
        console.log(`[Dashboard] aggregate-demand: filteredSeries=${ids.length}, series=${series?.length}`);
        let res;
        if (ids.length > 0 && ids.length < (series?.length || 0)) {
          res = await api.post('/analytics/aggregate-demand', { unique_ids: ids });
        } else {
          res = await api.get('/analytics/aggregate-demand');
        }
        console.log(`[Dashboard] aggregate-demand response: hist=${res.data?.historical?.length}, fc=${res.data?.forecast?.length}`);
        setAggregateDemand(res.data);
      } catch (err) {
        console.error('Failed to load aggregate demand:', err);
        setAggError(err.response?.data?.detail || err.message || 'Unknown error');
        setAggregateDemand(null);
      } finally { setAggLoading(false); }
    }, 400);
    return () => { if (aggTimerRef.current) clearTimeout(aggTimerRef.current); };
  }, [filteredSeries, series]);

  const pagedSeries = filteredSeries.slice(page * pageSize, (page + 1) * pageSize);
  const totalPages  = Math.ceil(filteredSeries.length / pageSize);

  const fetchSparklines = useCallback(async (ids) => {
    if (ids.length === 0) return;
    try { const res = await api.post('/sparklines', ids); setSparklineData(prev => ({ ...prev, ...res.data })); }
    catch { /* non-critical */ }
  }, []);
  useEffect(() => {
    const ids = pagedSeries.map(s => s.unique_id);
    const missing = ids.filter(id => !sparklineData[id]);
    if (missing.length > 0) fetchSparklines(missing);
  }, [pagedSeries.map(s => s.unique_id).join(',')]);

  // ── Plotly base layout ──
  const plotlyBase = useMemo(() => ({
    paper_bgcolor: 'rgba(0,0,0,0)', plot_bgcolor: 'rgba(0,0,0,0)',
    font: { color: isDark ? '#d1d5db' : '#374151', size: 11 },
    margin: { t: 30, r: 10, b: 40, l: 10, pad: 4 },
    xaxis: { gridcolor: isDark ? '#374151' : '#e5e7eb', zerolinecolor: isDark ? '#4b5563' : '#d1d5db', color: isDark ? '#d1d5db' : '#374151' },
    yaxis: { gridcolor: isDark ? '#374151' : '#e5e7eb', zerolinecolor: isDark ? '#4b5563' : '#d1d5db', color: isDark ? '#d1d5db' : '#374151' },
  }), [isDark]);

  // ── Best Method chart ──
  const bestMethodFilter = (() => {
    const f = colFilters.best_method;
    if (!f) return '';
    if (typeof f === 'string') return f;
    return NO_VAL_OPS.has(f.op) ? `[${f.op}]` : (f.val || '');
  })();
  const bestMethodChart = useMemo(() => {
    const withMethod = filteredSeries.filter(s => s.best_method);
    if (withMethod.length === 0) return null;
    const dist = {};
    withMethod.forEach(s => { dist[s.best_method] = (dist[s.best_method] || 0) + 1; });
    const entries = Object.entries(dist).sort((a, b) => a[1] - b[1]);
    const methods = entries.map(([m]) => m), counts = entries.map(([, c]) => c);
    const colors = methods.map((m, i) => {
      const base = TABLEAU10[i % TABLEAU10.length];
      if (bestMethodFilter && m !== bestMethodFilter) {
        const r = parseInt(base.slice(1,3),16), g = parseInt(base.slice(3,5),16), b = parseInt(base.slice(5,7),16);
        return `rgba(${r},${g},${b},0.3)`;
      }
      return base;
    });
    return {
      data: [{ type: 'bar', y: methods, x: counts, orientation: 'h', marker: { color: colors }, hovertemplate: '%{y}: %{x} series<extra></extra>' }],
      layout: {
        ...plotlyBase, height: Math.max(200, methods.length * 32),
        margin: { t: 30, r: 10, b: 30, l: 120 },
        title: { text: `Best method across ${formatNumber(withMethod.length, locale, 0)} backtested series`, font: { size: 12 } },
        yaxis: { ...plotlyBase.yaxis, automargin: true },
        xaxis: { ...plotlyBase.xaxis, title: 'Series Won' },
      },
    };
  }, [filteredSeries, plotlyBase, locale, bestMethodFilter]);

  // ── Accuracy vs Precision chart ──
  const accuracyChart = useMemo(() => {
    try {
      if (!accuracyPrecisionData?.points || accuracyPrecisionData.points.length === 0) return null;
      let data = accuracyPrecisionData.points;
      if (accuracyZoom) {
        data = data.filter(d =>
          d.accuracy >= accuracyZoom.x[0] && d.accuracy <= accuracyZoom.x[1] &&
          d.precision >= accuracyZoom.y[0] && d.precision <= accuracyZoom.y[1]);
      }
      if (data.length === 0) return null;
      const maxAcc = Math.max(...data.map(d => d.accuracy || 0), 1);
      const maxPrec = Math.max(...data.map(d => d.precision || 0), 1);
      const avgAcc  = accuracyPrecisionData.summary?.avg_accuracy || 0;
      const avgPrec = accuracyPrecisionData.summary?.avg_precision || 0;
      const byMethod = {};
      data.forEach(d => {
        if (!byMethod[d.method]) byMethod[d.method] = { x: [], y: [], ids: [], text: [] };
        byMethod[d.method].x.push(d.accuracy); byMethod[d.method].y.push(d.precision);
        byMethod[d.method].ids.push(d.unique_id); byMethod[d.method].text.push(d.unique_id);
      });
      const methodOrder = Object.entries(byMethod).sort((a, b) => b[1].x.length - a[1].x.length).map(([m]) => m);
      const traces = methodOrder.map((m, i) => ({
        type: 'scatter', mode: 'markers', name: m,
        x: byMethod[m].x, y: byMethod[m].y, customdata: byMethod[m].ids, text: byMethod[m].text,
        marker: { size: 8, color: TABLEAU10[i % TABLEAU10.length], opacity: 0.8 },
        hovertemplate: '<b>%{text}</b><br>|Bias|: %{x:.2f}<br>RMSE: %{y:.2f}<extra>%{fullData.name}</extra>',
      }));
      const xMax = maxAcc * 1.1, yMax = maxPrec * 1.1;
      return {
        data: traces,
        layout: {
          ...plotlyBase, height: 350,
          margin: { t: 35, r: 10, b: 50, l: 60 },
          title: { text: `Accuracy vs Precision (${formatNumber(data.length, locale, 0)} series${accuracyZoom ? ' \u2014 filtered' : ''})`, font: { size: 13 } },
          dragmode: 'select', selectdirection: 'any',
          xaxis: { ...plotlyBase.xaxis, title: '|Bias| (Accuracy)', range: [0, xMax], zeroline: false },
          yaxis: { ...plotlyBase.yaxis, title: 'RMSE (Precision)', range: [0, yMax], zeroline: false },
          legend: { orientation: 'v', x: 1.02, y: 1, font: { size: 10 } },
          shapes: [
            { type: 'rect', x0: 0, y0: 0, x1: avgAcc, y1: avgPrec, fillcolor: isDark ? 'rgba(34,197,94,0.1)' : 'rgba(22,163,106,0.06)', line: { width: 0 }, layer: 'below' },
            { type: 'line', x0: avgAcc, x1: avgAcc, y0: 0, y1: yMax, line: { dash: 'dash', color: isDark ? '#60a5fa' : '#3b82f6', width: 1.5 } },
            { type: 'line', x0: 0, x1: xMax, y0: avgPrec, y1: avgPrec, line: { dash: 'dash', color: isDark ? '#60a5fa' : '#3b82f6', width: 1.5 } },
          ],
          annotations: [
            { x: xMax*0.02, y: yMax*0.02, text: 'Best',   showarrow: false, font: { size: 11, color: isDark ? 'rgba(255,255,255,0.35)' : 'rgba(0,0,0,0.2)', weight: 'bold' } },
            { x: xMax*0.98, y: yMax*0.02, text: 'Biased', showarrow: false, xanchor: 'right', font: { size: 11, color: isDark ? 'rgba(255,255,255,0.35)' : 'rgba(0,0,0,0.2)', weight: 'bold' } },
            { x: xMax*0.02, y: yMax*0.98, text: 'Noisy',  showarrow: false, yanchor: 'top',   font: { size: 11, color: isDark ? 'rgba(255,255,255,0.35)' : 'rgba(0,0,0,0.2)', weight: 'bold' } },
            { x: xMax*0.98, y: yMax*0.98, text: 'Worst',  showarrow: false, xanchor: 'right', yanchor: 'top', font: { size: 11, color: isDark ? 'rgba(255,255,255,0.35)' : 'rgba(0,0,0,0.2)', weight: 'bold' } },
          ],
        },
      };
    } catch (err) { console.error('Error building accuracy chart:', err); return null; }
  }, [accuracyPrecisionData, plotlyBase, locale, isDark, accuracyZoom]);

  // ── Aggregate Demand chart ──
  const demandChart = useMemo(() => {
    if (!aggregateDemand) return null;
    const hist = aggregateDemand.historical || [], fc = aggregateDemand.forecast || [];
    if (hist.length === 0 && fc.length === 0) return null;
    const traces = [], shapes = [], annotations = [];
    if (hist.length > 0) {
      traces.push({ type: 'bar', name: 'Historical Demand',
        x: hist.map(d => d.date), y: hist.map(d => d.value),
        marker: { color: isDark ? '#9ca3af' : '#374151', opacity: 0.55 },
        hovertemplate: '%{x|%b %Y}<br>Demand: %{y:,.0f}<extra>Historical</extra>' });
    }
    if (fc.length > 0) {
      const bX = [], bY = [];
      if (hist.length > 0) { bX.push(hist[hist.length-1].date); bY.push(hist[hist.length-1].value); }
      fc.forEach(d => { bX.push(d.date); bY.push(d.value); });
      traces.push({ type: 'scatter', mode: 'lines+markers', name: 'Forecast (best method / series)',
        x: bX, y: bY, line: { color: '#2563eb', width: 2.5, dash: 'dash' },
        marker: { color: '#2563eb', size: 5, symbol: 'circle' },
        hovertemplate: '%{x|%b %Y}<br>Forecast: %{y:,.0f}<extra>Forecast</extra>' });
      if (hist.length > 0) {
        const boundary = hist[hist.length-1].date;
        shapes.push({ type: 'line', xref: 'x', yref: 'paper', x0: boundary, x1: boundary, y0: 0, y1: 1, line: { color: isDark ? '#6b7280' : '#9ca3af', width: 1.5, dash: 'dot' } });
        annotations.push({ x: boundary, y: 1, xref: 'x', yref: 'paper', text: 'Forecast \u2192', showarrow: false, xanchor: 'left', yanchor: 'bottom', xshift: 6, font: { size: 10, color: '#2563eb' } });
      }
    }
    return {
      data: traces,
      layout: {
        ...plotlyBase, height: 320, margin: { t: 20, r: 20, b: 45, l: 70 },
        xaxis: { ...plotlyBase.xaxis, type: 'date', tickformat: '%b %Y', tickangle: -30 },
        yaxis: { ...plotlyBase.yaxis, title: { text: 'Total Demand', standoff: 10 }, rangemode: 'tozero' },
        legend: { orientation: 'h', y: -0.18, font: { size: 10 } },
        barmode: 'overlay', shapes, annotations, hovermode: 'x unified',
      },
    };
  }, [aggregateDemand, plotlyBase, isDark]);

  const plotlyConfig   = { responsive: true, displayModeBar: 'hover', displaylogo: false, modeBarButtonsToRemove: ['toImage', 'lasso2d', 'select2d'] };
  const apPlotlyConfig = { responsive: true, displayModeBar: false };

  // ── Visible columns (ordered, non-hidden) ──
  const visibleCols = useMemo(() =>
    effectiveColOrder.map(id => allColDefs[id]).filter(c => c && !hiddenCols.has(c.id)),
    [effectiveColOrder, allColDefs, hiddenCols]);

  // Sum of all visible column widths — used to set an explicit table pixel width so
  // that table-layout:fixed gives each <col> its exact pixel value (rather than
  // treating them as proportional hints when min-width:100% makes the table stretch).
  const totalColWidth = useMemo(
    () => visibleCols.reduce((sum, col) => sum + (colWidths[col.id] ?? getDefaultColWidth(col.id)), 0),
    [visibleCols, colWidths]
  );

  // ── Cell renderer ──
  const renderCell = useCallback((col, s) => {
    const td = 'px-3 py-2';
    switch (col.id) {
      case '_item':
        return <td key={col.id} className={`${td} font-medium text-blue-600 dark:text-blue-400 whitespace-nowrap`}>
          {s.item_name ?? parseSeriesId(s.unique_id).item}
        </td>;
      case '_site':
        return <td key={col.id} className={`${td} text-gray-500 dark:text-gray-400 whitespace-nowrap`}>
          {s.site_name ?? parseSeriesId(s.unique_id).site}
        </td>;
      case 'n_observations':
        return <td key={col.id} className={`${td} text-right text-gray-700 dark:text-gray-300`}>{s.n_observations}</td>;
      case 'complexity_level':
        return <td key={col.id} className={td}>
          <span className={`px-1.5 py-0.5 rounded text-xs font-medium ${
            s.complexity_level === 'high'   ? 'bg-red-100 text-red-700 dark:bg-red-900/40 dark:text-red-400' :
            s.complexity_level === 'medium' ? 'bg-yellow-100 text-yellow-700 dark:bg-yellow-900/40 dark:text-yellow-400' :
            'bg-green-100 text-green-700 dark:bg-green-900/40 dark:text-green-400'
          }`}>{s.complexity_level}</span>
        </td>;
      case 'is_intermittent':
        return <td key={col.id} className={`${td} text-center text-gray-600 dark:text-gray-400`}>{s.is_intermittent ? '\u2713' : '-'}</td>;
      case 'has_seasonality':
        return <td key={col.id} className={`${td} text-center text-gray-600 dark:text-gray-400`}>{s.has_seasonality ? '\u2713' : '-'}</td>;
      case 'has_trend':
        return <td key={col.id} className={`${td} text-center text-gray-600 dark:text-gray-400`}>{s.has_trend ? '\u2713' : '-'}</td>;
      case 'mean':
        return <td key={col.id} className={`${td} text-right font-mono text-gray-700 dark:text-gray-300`}>{formatNumber(s.mean, locale, numberDecimals)}</td>;
      case '_sparkline':
        return <td key={col.id} className={td}>
          <Sparkline historical={sparklineData[s.unique_id]?.historical || []} forecast={sparklineData[s.unique_id]?.forecast || []} />
        </td>;
      case 'n_outliers':
        return <td key={col.id} className={`${td} text-center`}>
          {s.has_outlier_corrections
            ? <span className="bg-orange-100 text-orange-700 dark:bg-orange-900/40 dark:text-orange-400 px-1.5 py-0.5 rounded text-xs font-medium">{s.n_outliers}</span>
            : <span className="text-gray-300 dark:text-gray-600">-</span>}
        </td>;
      case 'best_method':
        return <td key={col.id} className={`${td} whitespace-nowrap`}>
          {s.best_method
            ? <span className={`inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-xs font-medium ${
                s.best_method_source === 'backtested'
                  ? 'bg-emerald-100 text-emerald-700 dark:bg-emerald-900/40 dark:text-emerald-400'
                  : 'bg-blue-50 text-blue-600 dark:bg-blue-900/30 dark:text-blue-400'
              }`}>
                {s.best_method_source === 'backtested' && <span title="Backtested">{'\u2713'}</span>}
                {s.best_method}
              </span>
            : <span className="text-gray-300 dark:text-gray-600">-</span>}
        </td>;
      default:
        if (col.id.startsWith('_cls_')) {
          const cfgName = col.id.slice(5);
          const cls = s.classifications?.[cfgName];
          return <td key={col.id} className={`${td} text-center`}>
            {cls
              ? <span className="inline-flex items-center justify-center w-6 h-6 rounded text-xs font-bold text-white" style={{ backgroundColor: ABC_COLORS[cls] || '#6b7280' }}>{cls}</span>
              : <span className="text-gray-300 dark:text-gray-600">-</span>}
          </td>;
        }
        return <td key={col.id} className={td}>-</td>;
    }
  }, [sparklineData, locale, numberDecimals]);

  const hasActiveFilters = Object.values(colFilters).some(isFilterActive) || !!accuracyZoom;

  // ── Early returns ──
  if (loading) return <div className="flex items-center justify-center h-64"><div className="text-xl text-gray-500 dark:text-gray-400 animate-pulse">Loading dashboard...</div></div>;
  if (error)   return <div className="flex items-center justify-center h-64"><div className="text-xl text-red-600 dark:text-red-400">Error: {error}</div></div>;
  if (!analytics && !series.length) return (
    <div className="p-4 sm:p-6">
      <h1 className="text-2xl sm:text-3xl font-bold mb-6 text-gray-900 dark:text-white">Forecasting Dashboard</h1>
      <div className="text-gray-500 dark:text-gray-400">No data available. Please run the pipeline first.</div>
    </div>
  );

  return (
    <div className="p-4 sm:p-6">
      <div className="flex flex-wrap items-center justify-between gap-3 mb-6">
        <h1 className="text-2xl sm:text-3xl font-bold text-gray-900 dark:text-white">Forecasting Dashboard</h1>
        {scenarios.length > 1 && (
          <div className="flex items-center gap-2">
            <label className="text-sm font-medium text-gray-600 dark:text-gray-400">Scenario:</label>
            <select
              value={activeScenarioId}
              onChange={e => setActiveScenarioId(Number(e.target.value))}
              className="px-3 py-1.5 text-sm rounded-lg border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-700 text-gray-900 dark:text-white focus:ring-2 focus:ring-blue-500"
            >
              {scenarios.map(s => (
                <option key={s.scenario_id} value={s.scenario_id}>
                  {s.name}{s.is_base ? ' (Base)' : ''}
                </option>
              ))}
            </select>
            {activeScenarioId !== 1 && (
              <span className="px-2 py-0.5 text-xs rounded-full bg-purple-100 dark:bg-purple-900/40 text-purple-700 dark:text-purple-300 font-medium">What-If</span>
            )}
          </div>
        )}
      </div>

      {/* Summary Cards */}
      {analytics && (
        <Section title="Summary" storageKey="dash_summary_open" id="dash-summary">
          <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 xl:grid-cols-7 gap-3">
            {[
              { label: 'Total Series',  value: formatNumber(analytics.total_series, locale, 0),                  color: 'text-blue-600 dark:text-blue-400' },
              { label: 'Backtested',    value: formatNumber(analytics.best_method_total_series || 0, locale, 0), color: 'text-emerald-600 dark:text-emerald-400' },
              { label: 'Seasonal',      value: formatNumber(analytics.seasonal_count, locale, 0),                color: '' },
              { label: 'Trending',      value: formatNumber(analytics.trending_count, locale, 0),                color: '' },
              { label: 'Intermittent',  value: formatNumber(analytics.intermittent_count, locale, 0),            color: '' },
              { label: 'Avg Obs',       value: formatNumber(analytics.avg_observations, locale, 0),              color: '' },
              { label: 'Outlier Adj.',  value: formatNumber(analytics.outlier_adjusted_count || 0, locale, 0),   color: 'text-orange-600 dark:text-orange-400' },
            ].map(({ label, value, color }) => (
              <div key={label} className="bg-gray-50 dark:bg-gray-700/50 rounded-lg p-3 border border-gray-100 dark:border-gray-600">
                <div className="text-xs text-gray-500 dark:text-gray-400 mb-1">{label}</div>
                <div className={`text-xl sm:text-2xl font-bold ${color || 'text-gray-900 dark:text-white'}`}>{value}</div>
              </div>
            ))}
          </div>
        </Section>
      )}

      {/* Charts */}
      {(accuracyChart || bestMethodChart) && (
        <Section title="Charts" storageKey="dash_charts_open" id="dash-charts">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            {accuracyChart && (
              <div>
                <div className="flex items-center justify-between mb-2">
                  <h3 className="text-sm font-semibold text-gray-600 dark:text-gray-400">Accuracy vs Precision</h3>
                  <div className="flex items-center gap-2">
                    <select value={selectedAccuracyMethod} onChange={(e) => { setSelectedAccuracyMethod(e.target.value); setPage(0); }}
                      className="text-xs border border-gray-300 dark:border-gray-600 rounded px-2 py-1 bg-white dark:bg-gray-700 dark:text-gray-200">
                      <option value="">All Methods</option>
                      {analytics?.best_method_distribution && Object.keys(analytics.best_method_distribution).map(m => (
                        <option key={m} value={m}>{m}</option>
                      ))}
                    </select>
                    {accuracyZoom && (
                      <button onClick={() => setAccuracyZoom(null)}
                        className="text-xs px-2 py-1 bg-gray-200 dark:bg-gray-600 hover:bg-gray-300 dark:hover:bg-gray-500 rounded text-gray-700 dark:text-gray-200">
                        Reset Zoom
                      </button>
                    )}
                  </div>
                </div>
                <Plot data={accuracyChart.data} layout={accuracyChart.layout} config={apPlotlyConfig}
                  useResizeHandler style={{ width: '100%' }}
                  onSelected={(event) => {
                    console.log('[Dashboard] onSelected event:', JSON.stringify(event?.range), 'points:', event?.points?.length);
                    if (event?.range?.x && event?.range?.y) { setAccuracyZoom({ x: event.range.x, y: event.range.y }); setPage(0); }
                  }}
                  onDeselect={() => { console.log('[Dashboard] onDeselect fired'); }}
                />
                <p className="text-xs text-gray-400 dark:text-gray-500 mt-1">Drag to select a region and filter the table below.</p>
              </div>
            )}
            {bestMethodChart && (
              <div>
                <h3 className="text-sm font-semibold text-gray-600 dark:text-gray-400 mb-2">Best Method Distribution</h3>
                <Plot data={bestMethodChart.data} layout={bestMethodChart.layout} config={plotlyConfig}
                  useResizeHandler style={{ width: '100%' }}
                  onClick={(event) => {
                    const pt = event?.points?.[0];
                    if (pt?.y) {
                      const cur = colFilters.best_method;
                      const curVal = cur && typeof cur === 'object' ? cur.val : (cur || '');
                      setColFilter('best_method', curVal === pt.y
                        ? { op: 'contains', val: '' }
                        : { op: 'equals', val: pt.y });
                    }
                  }}
                />
              </div>
            )}
          </div>
        </Section>
      )}

      {/* Aggregate Demand & Forecast */}
      <Section
        title={`Demand & Forecast${filteredSeries.length < (series?.length || 0) ? ` (${formatNumber(filteredSeries.length, locale, 0)} of ${formatNumber(series.length, locale, 0)} series)` : ` (${formatNumber(series.length, locale, 0)} series)`}`}
        storageKey="dash_demand_open" id="dash-demand">
        {aggLoading ? (
          <div className="flex items-center justify-center h-40 gap-3">
            <div className="animate-spin rounded-full h-5 w-5 border-b-2 border-blue-600" />
            <span className="text-sm text-gray-400 dark:text-gray-500">Loading demand data\u2026</span>
          </div>
        ) : aggError ? (
          <div className="flex items-center justify-center h-32 text-red-500 dark:text-red-400 text-sm">Error loading demand: {aggError}</div>
        ) : demandChart ? (
          <Plot data={demandChart.data} layout={demandChart.layout} config={plotlyConfig} useResizeHandler style={{ width: '100%' }} />
        ) : (
          <div className="flex items-center justify-center h-32 text-gray-400 dark:text-gray-500 text-sm">No demand data available for the current filter.</div>
        )}
        {(accuracyZoom || bestMethodFilter) && (
          <p className="text-xs text-blue-500 dark:text-blue-400 mt-1">
            Filtered by{accuracyZoom ? ' accuracy/precision selection' : ''}{accuracyZoom && bestMethodFilter ? ' + ' : ''}{bestMethodFilter ? ` method: ${bestMethodFilter}` : ''}
          </p>
        )}
      </Section>

      {/* Series Table */}
      <Section title={`Series Table (${formatNumber(filteredSeries.length, locale, 0)} series)`} storageKey="dash_table_open" id="dash-table">

        {/* Toolbar */}
        <div className="flex items-center justify-between gap-3 mb-3 flex-wrap">
          <div className="flex items-center gap-2 flex-wrap">
            {hasActiveFilters && (
              <button onClick={clearAllFilters}
                className="px-3 py-1.5 text-xs font-medium text-red-600 dark:text-red-400 border border-red-300 dark:border-red-700 rounded-lg hover:bg-red-50 dark:hover:bg-red-900/20 transition-colors">
                \u00d7 Clear all filters
              </button>
            )}
            {accuracyZoom && (
              <span className="text-xs text-blue-600 dark:text-blue-400 bg-blue-50 dark:bg-blue-900/30 px-2 py-1 rounded-lg">
                Accuracy/precision zoom active
              </span>
            )}
            {bestMethodFilter && (
              <span className="text-xs text-emerald-700 dark:text-emerald-400 bg-emerald-50 dark:bg-emerald-900/30 px-2 py-1 rounded-lg">
                Method: {bestMethodFilter}
              </span>
            )}
          </div>

          {/* Columns visibility dropdown */}
          <div className="relative" ref={colsMenuRef}>
            <button onClick={() => setColsMenuOpen(o => !o)}
              className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium border border-gray-300 dark:border-gray-600 rounded-lg text-gray-700 dark:text-gray-300 bg-white dark:bg-gray-700 hover:bg-gray-50 dark:hover:bg-gray-600 transition-colors">
              <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 17V7m0 10a2 2 0 01-2 2H5a2 2 0 01-2-2V7a2 2 0 012-2h2a2 2 0 012 2m0 10a2 2 0 002 2h2a2 2 0 002-2M9 7a2 2 0 012-2h2a2 2 0 012 2m0 10V7" />
              </svg>
              Columns {'\u25BE'}
            </button>
            {colsMenuOpen && (
              <div className="absolute right-0 top-full mt-1 z-20 w-48 bg-white dark:bg-gray-800 rounded-lg shadow-lg border border-gray-200 dark:border-gray-700 py-1 max-h-80 overflow-y-auto">
                <div className="px-3 py-1.5 text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase border-b border-gray-100 dark:border-gray-700 mb-1">
                  Show / hide columns
                </div>
                {effectiveColOrder.map(id => {
                  const col = allColDefs[id];
                  if (!col) return null;
                  return (
                    <label key={id} className="flex items-center gap-2 px-3 py-1.5 hover:bg-gray-50 dark:hover:bg-gray-700 cursor-pointer text-sm text-gray-700 dark:text-gray-300">
                      <input type="checkbox" checked={!hiddenCols.has(id)}
                        onChange={() => toggleColHidden(id)}
                        className="rounded border-gray-300 dark:border-gray-600 text-blue-600 focus:ring-blue-500" />
                      {col.label}
                    </label>
                  );
                })}
                <div className="border-t border-gray-100 dark:border-gray-700 mt-1 pt-1 px-3 pb-1 flex gap-3">
                  <button onClick={() => { setHiddenCols(buildDefaultHidden()); setColsMenuOpen(false); }}
                    className="text-xs text-blue-600 dark:text-blue-400 hover:underline">
                    Reset columns
                  </button>
                  <button onClick={() => { setColWidthsRaw({}); localStorage.removeItem('dash_col_widths'); setColsMenuOpen(false); }}
                    className="text-xs text-blue-600 dark:text-blue-400 hover:underline">
                    Reset widths
                  </button>
                </div>
              </div>
            )}
          </div>
        </div>

        {/* Table */}
        <div className="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
          <table
            className="divide-y divide-gray-200 dark:divide-gray-700 text-sm"
            style={{ tableLayout: 'fixed', width: totalColWidth || '100%' }}
          >
            <colgroup>
              {visibleCols.map(col => (
                <col key={col.id} style={{ width: colWidths[col.id] ?? getDefaultColWidth(col.id) }} />
              ))}
            </colgroup>
            <thead className="bg-gray-50 dark:bg-gray-900">

              {/* ── Column headers: click to sort, drag to reorder ── */}
              <tr>
                {visibleCols.map(col => (
                  <th key={col.id}
                    draggable
                    onDragStart={e => handleColDragStart(e, col.id)}
                    onDragOver={e => handleColDragOver(e, col.id)}
                    onDrop={e => handleColDrop(e, col.id)}
                    onDragEnd={handleColDragEnd}
                    onClick={() => handleSort(col.sortKey)}
                    title={col.sortKey ? 'Click to sort · Drag to reorder' : 'Drag to reorder'}
                    style={{ position: 'relative' }}
                    className={[
                      'px-3 py-2 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase whitespace-nowrap select-none transition-colors overflow-hidden',
                      col.sortKey ? 'cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-800' : 'cursor-grab',
                      dragOverColId === col.id && dragColId !== col.id ? 'bg-blue-50 dark:bg-blue-900/20 border-l-2 border-blue-400' : '',
                      dragColId === col.id ? 'opacity-40' : '',
                    ].filter(Boolean).join(' ')}
                  >
                    <span className="mr-1 text-gray-300 dark:text-gray-600 select-none" aria-hidden="true">{'\u2630'}</span>
                    {col.label}
                    {col.sortKey ? sortInd(col.sortKey) : ''}
                    {/* Resize handle */}
                    <div
                      draggable={false}
                      onMouseDown={(e) => handleResizeMouseDown(e, col.id, colWidths[col.id] ?? getDefaultColWidth(col.id))}
                      onDragStart={(e) => e.preventDefault()}
                      onClick={(e) => e.stopPropagation()}
                      title="Drag to resize column"
                      style={{ position: 'absolute', right: 0, top: 0, bottom: 0, width: '6px', cursor: 'col-resize', zIndex: 2 }}
                      className="resize-col-handle hover:bg-blue-400 dark:hover:bg-blue-500 opacity-0 hover:opacity-40 transition-opacity"
                    />
                  </th>
                ))}
              </tr>

              {/* ── Filter row ── */}
              <tr className="bg-white dark:bg-gray-800 border-t border-gray-200 dark:border-gray-700">
                {visibleCols.map(col => (
                  <ColFilter key={col.id} col={col}
                    value={colFilters[col.id] ?? ''}
                    onChange={val => setColFilter(col.id, val)} />
                ))}
              </tr>
            </thead>

            <tbody className="divide-y divide-gray-200 dark:divide-gray-700 bg-white dark:bg-gray-800">
              {pagedSeries.map(s => (
                <tr key={s.unique_id}
                  onClick={() => navigate(`/series/${encodeURIComponent(s.unique_id)}`)}
                  className="hover:bg-blue-50 dark:hover:bg-blue-900/20 cursor-pointer transition-colors">
                  {visibleCols.map(col => renderCell(col, s))}
                </tr>
              ))}
              {pagedSeries.length === 0 && (
                <tr>
                  <td colSpan={visibleCols.length || 1}
                    className="px-4 py-8 text-center text-gray-400 dark:text-gray-500 text-sm">
                    No series match the current filters.
                  </td>
                </tr>
              )}
            </tbody>
          </table>

          {/* Pagination */}
          {totalPages > 1 && (
            <div className="flex items-center justify-between px-4 py-3 bg-gray-50 dark:bg-gray-900 border-t border-gray-200 dark:border-gray-700">
              <button onClick={() => setPage(Math.max(0, page - 1))} disabled={page === 0}
                className="px-3 py-1.5 border border-gray-300 dark:border-gray-600 rounded-lg text-sm disabled:opacity-40 hover:bg-gray-100 dark:hover:bg-gray-800 dark:text-gray-300 transition-colors">
                {'\u2190'} Previous
              </button>
              <span className="text-sm text-gray-500 dark:text-gray-400">Page {page + 1} of {totalPages}</span>
              <button onClick={() => setPage(Math.min(totalPages - 1, page + 1))} disabled={page >= totalPages - 1}
                className="px-3 py-1.5 border border-gray-300 dark:border-gray-600 rounded-lg text-sm disabled:opacity-40 hover:bg-gray-100 dark:hover:bg-gray-800 dark:text-gray-300 transition-colors">
                Next {'\u2192'}
              </button>
            </div>
          )}
        </div>
      </Section>
    </div>
  );
};

export default Dashboard;

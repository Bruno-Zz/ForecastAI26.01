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

// ---- Collapsible section (same pattern as Method Rationale) ----
const Section = ({ title, storageKey, defaultOpen = true, children, badge }) => {
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
    <div className="mb-6 bg-white rounded-lg shadow">
      <button
        onClick={toggle}
        className="w-full flex items-center justify-between p-4 text-left hover:bg-gray-50 transition-colors rounded-lg"
      >
        <div className="flex items-center gap-2">
          <h2 className="text-lg font-semibold">{title}</h2>
          {badge && <span className="text-xs bg-gray-100 text-gray-500 px-2 py-0.5 rounded-full">{badge}</span>}
        </div>
        <span className="text-gray-400 text-xl flex-shrink-0">{open ? '▲' : '▼'}</span>
      </button>
      {open && <div className="px-4 pb-4 sm:px-6 sm:pb-6">{children}</div>}
    </div>
  );
};

// ---- Searchable dropdown with recent history ----
const SearchableDropdown = ({ label, value, onChange, options, recentOptions, disabled, placeholder }) => {
  const [open, setOpen] = useState(false);
  const [search, setSearch] = useState('');
  const ref = useRef(null);

  // Close on outside click
  useEffect(() => {
    const handler = (e) => { if (ref.current && !ref.current.contains(e.target)) setOpen(false); };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, []);

  // Sync display text with value
  useEffect(() => {
    if (!open) setSearch(value || '');
  }, [value, open]);

  const filteredRecent = recentOptions.filter(o => o.toLowerCase().includes(search.toLowerCase()) && o !== value);
  const filteredAll = options.filter(o =>
    o.toLowerCase().includes(search.toLowerCase()) &&
    !recentOptions.includes(o)
  );
  const hasRecent = filteredRecent.length > 0 && search === '';

  return (
    <div ref={ref} className="relative flex-1 min-w-0">
      <label className="block text-xs font-medium text-gray-500 mb-1">{label}</label>
      <div
        className={`flex items-center border rounded-lg px-3 py-2 gap-2 transition-colors
          ${disabled ? 'bg-gray-50 border-gray-200 cursor-not-allowed opacity-60' : 'bg-white border-gray-300 cursor-pointer hover:border-blue-400'}
          ${open ? 'border-blue-500 ring-2 ring-blue-100' : ''}`}
        onClick={() => { if (!disabled) { setOpen(o => !o); setSearch(''); } }}
      >
        <svg className="w-3.5 h-3.5 text-gray-400 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-4.35-4.35M17 11A6 6 0 1 1 5 11a6 6 0 0 1 12 0z" />
        </svg>
        <input
          type="text"
          value={open ? search : (value || '')}
          onChange={e => { setSearch(e.target.value); if (!open) setOpen(true); }}
          onClick={e => { e.stopPropagation(); if (!disabled) setOpen(true); }}
          placeholder={disabled ? 'Select item first' : placeholder}
          disabled={disabled}
          className="flex-1 min-w-0 text-sm outline-none bg-transparent"
        />
        {value && !open && (
          <button onClick={e => { e.stopPropagation(); onChange(''); setSearch(''); }}
            className="text-gray-400 hover:text-gray-600 flex-shrink-0 text-xs">✕</button>
        )}
        <svg className={`w-4 h-4 text-gray-400 flex-shrink-0 transition-transform ${open ? 'rotate-180' : ''}`}
          fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
        </svg>
      </div>

      {open && !disabled && (
        <div className="absolute z-50 mt-1 left-0 right-0 bg-white border border-gray-200 rounded-lg shadow-lg max-h-64 overflow-y-auto">
          {/* Recent section */}
          {hasRecent && (
            <>
              <div className="px-3 py-1.5 text-xs font-semibold text-gray-400 uppercase tracking-wide bg-gray-50 sticky top-0">
                Recently accessed
              </div>
              {filteredRecent.map(o => (
                <button key={`recent-${o}`} onClick={() => { onChange(o); setSearch(o); setOpen(false); }}
                  className="w-full text-left px-3 py-2 text-sm hover:bg-blue-50 hover:text-blue-700 flex items-center gap-2">
                  <span className="text-gray-400">🕐</span>
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
                <div className="px-3 py-1.5 text-xs font-semibold text-gray-400 uppercase tracking-wide bg-gray-50 sticky top-0">
                  All
                </div>
              )}
              {filteredAll.map(o => (
                <button key={o} onClick={() => { onChange(o); setSearch(o); setOpen(false); }}
                  className={`w-full text-left px-3 py-2 text-sm hover:bg-blue-50 hover:text-blue-700 ${o === value ? 'bg-blue-50 font-medium text-blue-700' : ''}`}>
                  {o}
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


export const TimeSeriesViewer = () => {
  const { uniqueId } = useParams();
  const decodedId = decodeURIComponent(uniqueId);
  const navigate = useNavigate();

  // ---- Item/Site dropdown state ----
  const [allSeriesList, setAllSeriesList] = useState([]);
  const [selectedItem, setSelectedItem] = useState('');
  const [selectedSite, setSelectedSite] = useState('');
  const [recentItems, setRecentItems] = useState([]);
  const [recentSites, setRecentSites] = useState([]);

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

  // ---- Load all series list for dropdowns (once) ----
  useEffect(() => {
    axios.get(`${API_BASE_URL}/series`, { params: { limit: 50000 } })
      .then(res => setAllSeriesList(res.data || []))
      .catch(() => {});
  }, []);

  // ---- Parse current uniqueId into item/site on mount ----
  useEffect(() => {
    const { item, site } = parseUniqueId(decodedId);
    setSelectedItem(item);
    setSelectedSite(site);

    // Update localStorage recents
    setRecent('recent_items', item);
    setRecent('recent_sites', site);
    localStorage.setItem('last_series', decodedId);

    // Refresh recent state
    setRecentItems(getRecent('recent_items'));
    setRecentSites(getRecent('recent_sites'));
  }, [decodedId]);

  // ---- Navigate when both item and site selected and differ from current ----
  const handleItemChange = (item) => {
    setSelectedItem(item);
    setSelectedSite(''); // reset site when item changes
  };

  const handleSiteChange = (site) => {
    setSelectedSite(site);
    if (selectedItem && site) {
      const newId = `${selectedItem}_${site}`;
      if (newId !== decodedId) navigate(`/series/${encodeURIComponent(newId)}`);
    }
  };

  // ---- Derived dropdown options ----
  const allItems = useMemo(() => {
    const items = [...new Set(allSeriesList.map(s => parseUniqueId(s.unique_id).item))];
    return items.sort((a, b) => a.localeCompare(b, undefined, { numeric: true }));
  }, [allSeriesList]);

  const availableSites = useMemo(() => {
    if (!selectedItem) return [];
    const sites = allSeriesList
      .filter(s => parseUniqueId(s.unique_id).item === selectedItem)
      .map(s => parseUniqueId(s.unique_id).site);
    return [...new Set(sites)].sort((a, b) => a.localeCompare(b, undefined, { numeric: true }));
  }, [allSeriesList, selectedItem]);

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
        setVisibleMethods(vis);
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

  const activeMethodDomain = useMemo(() => {
    const methods = ['Historical', ...forecasts.map(f => f.method)];
    return { domain: methods, range: methods.map(m => getMethodColor(m)) };
  }, [forecasts]);

  const horizonLength = useMemo(() => {
    if (forecasts.length === 0) return 0;
    return forecasts[0].point_forecast.length;
  }, [forecasts]);

  /* ---------- build combined data for main chart ---------- */
  const { allData, allDates } = useMemo(() => {
    if (!historicalData || !historicalData.date || historicalData.date.length === 0)
      return { allData: [], allDates: [] };
    const data = [];
    const dateSet = new Set();
    historicalData.date.forEach((date, i) => {
      dateSet.add(date);
      data.push({ date, value: historicalData.value[i], type: 'Actual', method: 'Historical', lo90: null, hi90: null, lo50: null, hi50: null, layer: 'line' });
    });
    if (forecasts.length > 0) {
      const lastDate = new Date(historicalData.date[historicalData.date.length - 1]);
      forecasts.forEach(forecast => {
        const quantiles = forecast.quantiles || {};
        forecast.point_forecast.forEach((value, i) => {
          const d = new Date(lastDate); d.setMonth(d.getMonth() + i + 1);
          const dateStr = fmtDate(d);
          dateSet.add(dateStr);
          const lo90 = quantiles['0.05']?.[i] ?? null;
          const hi90 = quantiles['0.95']?.[i] ?? null;
          data.push({ date: dateStr, value, type: 'Forecast', method: forecast.method, lo90, hi90, lo50: quantiles['0.25']?.[i] ?? lo90, hi50: quantiles['0.75']?.[i] ?? hi90, layer: 'line' });
          if (lo90 != null && hi90 != null)
            data.push({ date: dateStr, value: null, type: 'Band', method: forecast.method, lo90, hi90, lo50: quantiles['0.25']?.[i] ?? lo90, hi50: quantiles['0.75']?.[i] ?? hi90, layer: 'band' });
        });
      });
    }
    const sortedDates = [...dateSet].sort();
    return { allData: data, allDates: sortedDates };
  }, [historicalData, forecasts]);

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
      data.push({ date: dateStr, value: origVal, series: 'Original', isOutlier: false });
      data.push({ date: dateStr, value: corrVal, series: 'Corrected', isOutlier: false });
      if (isOutlier) data.push({ date: dateStr, value: origVal, series: 'Outlier', isOutlier: true });
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
    return {
      $schema: 'https://vega.github.io/schema/vega-lite/v5.json',
      width: 'container', height: 300,
      autosize: { type: 'fit', contains: 'padding' },
      data: { values: filtered },
      layer: [
        { transform: [{ filter: "datum.series === 'Original'" }], mark: { type: 'line', strokeDash: [6, 4], strokeWidth: 1.5, opacity: 0.6 }, encoding: { x: { field: 'date', type: 'temporal', title: 'Date', axis: { format: '%Y-%m' } }, y: { field: 'value', type: 'quantitative', title: 'Demand', scale: { zero: false } }, color: { datum: 'Original', scale: { domain: ['Original', 'Corrected', 'Outlier'], range: ['#9ca3af', '#2563eb', '#ef4444'] } }, tooltip: [{ field: 'date', type: 'temporal', title: 'Date' }, { field: 'value', type: 'quantitative', title: 'Original', format: ',.0f' }] } },
        { transform: [{ filter: "datum.series === 'Corrected'" }], mark: { type: 'line', strokeWidth: 2.5 }, encoding: { x: { field: 'date', type: 'temporal' }, y: { field: 'value', type: 'quantitative' }, color: { datum: 'Corrected' }, tooltip: [{ field: 'date', type: 'temporal', title: 'Date' }, { field: 'value', type: 'quantitative', title: 'Corrected', format: ',.0f' }] } },
        { transform: [{ filter: "datum.series === 'Outlier'" }], mark: { type: 'circle', size: 120, opacity: 0.9 }, encoding: { x: { field: 'date', type: 'temporal' }, y: { field: 'value', type: 'quantitative' }, color: { datum: 'Outlier' }, tooltip: [{ field: 'date', type: 'temporal', title: 'Date' }, { field: 'value', type: 'quantitative', title: 'Outlier Value', format: ',.0f' }] } }
      ],
      config: { view: { stroke: null }, legend: { title: null, orient: 'top', direction: 'horizontal' } }
    };
  }, [outlierChartData, outlierDates, outlierZoomStart, outlierZoomEnd]);

  const mainChartSpec = useMemo(() => {
    if (allData.length === 0 || allDates.length === 0) return null;
    const minDate = allDates[zoomStart], maxDate = allDates[zoomEnd];
    const filtered = allData.filter(d => {
      if (d.type !== 'Actual' && d.method !== 'Historical' && !visibleMethods[d.method]) return false;
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
    return { $schema: 'https://vega.github.io/schema/vega-lite/v5.json', width: 'container', height: 380, autosize: { type: 'fit', contains: 'padding' }, data: { values: filtered }, layer: layers, config: { view: { stroke: null } } };
  }, [allData, allDates, zoomStart, zoomEnd, visibleMethods, activeMethodDomain]);

  const racingBarsSpec = useMemo(() => {
    const src = originForecasts?.forecasts?.length > 0 ? originForecasts.forecasts : forecasts;
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
  }, [originForecasts, forecasts, selectedPeriod, visibleMethods, activeMethodDomain]);

  const targetChartSpec = useMemo(() => {
    if (!metrics || metrics.length === 0) return null;
    const data = metrics.map(m => ({ method: m.method, accuracy: Math.abs(m.bias || 0), precision: m.rmse || 0, isBest: bestMethod?.best_method === m.method, composite: compositeRanking?.[m.method] ?? null }));
    const maxAccuracy = Math.max(...data.map(d => d.accuracy), 1);
    const maxPrecision = Math.max(...data.map(d => d.precision), 1);
    return {
      $schema: 'https://vega.github.io/schema/vega-lite/v5.json', width: 'container', height: 380, autosize: { type: 'fit', contains: 'padding' },
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
  }, [metrics, bestMethod, compositeRanking, activeMethodDomain]);

  const compositeScoreSpec = useMemo(() => {
    if (!compositeRanking || Object.keys(compositeRanking).length === 0) return null;
    const data = Object.entries(compositeRanking).map(([method, score]) => ({ method, score: score ?? 999, isBest: bestMethod?.best_method === method })).sort((a, b) => a.score - b.score);
    return { $schema: 'https://vega.github.io/schema/vega-lite/v5.json', width: 'container', height: Math.max(120, data.length * 36), autosize: { type: 'fit', contains: 'padding' }, data: { values: data }, mark: { type: 'bar', cornerRadiusEnd: 4 }, encoding: { y: { field: 'method', type: 'nominal', sort: { field: 'score', order: 'ascending' }, title: 'Method' }, x: { field: 'score', type: 'quantitative', title: 'Composite Score (lower is better)' }, color: { field: 'method', type: 'nominal', legend: null, scale: activeMethodDomain }, stroke: { condition: { test: 'datum.isBest', value: '#059669' }, value: null }, strokeWidth: { condition: { test: 'datum.isBest', value: 3 }, value: 0 }, tooltip: [{ field: 'method', type: 'nominal', title: 'Method' }, { field: 'score', type: 'quantitative', title: 'Composite Score', format: '.4f' }] } };
  }, [compositeRanking, bestMethod, activeMethodDomain]);

  const ridgeChartSpec = useMemo(() => {
    if (!distributions || !distributions.horizons || distributions.horizons.length === 0) return null;
    const data = [];
    distributions.horizons.forEach(h => {
      (h.density_points || []).forEach(pt => { data.push({ horizon: `M${h.horizon_month}`, horizonNum: h.horizon_month, x: pt.x, density: pt.y, mean: h.mean, is_bootstrap: h.is_bootstrap }); });
    });
    if (data.length === 0) return null;
    const nHorizons = distributions.horizons.length;
    const step = nHorizons > 12 ? Math.ceil(nHorizons / 12) : 1;
    const filteredHorizons = distributions.horizons.filter((_, i) => i % step === 0).map(h => `M${h.horizon_month}`);
    const filteredData = data.filter(d => filteredHorizons.includes(d.horizon));
    return {
      $schema: 'https://vega.github.io/schema/vega-lite/v5.json', width: 'container', height: 40,
      data: { values: filteredData },
      facet: { row: { field: 'horizon', type: 'ordinal', sort: { field: 'horizonNum', order: 'ascending' }, header: { labelAngle: 0, labelAlign: 'right', labelFontSize: 10, title: null } } },
      spec: { width: 'container', height: 40, layer: [
        { mark: { type: 'area', interpolate: 'monotone', opacity: 0.6, line: { strokeWidth: 1 } }, encoding: { x: { field: 'x', type: 'quantitative', title: 'Forecast Value', axis: null }, y: { field: 'density', type: 'quantitative', title: null, axis: null, scale: { domain: [0, 1] } }, color: { field: 'horizonNum', type: 'quantitative', scale: { scheme: 'viridis' }, legend: null }, tooltip: [{ field: 'horizon', type: 'nominal', title: 'Horizon' }, { field: 'x', type: 'quantitative', title: 'Value', format: ',.0f' }, { field: 'density', type: 'quantitative', title: 'Density', format: '.3f' }] } },
        { mark: { type: 'rule', strokeWidth: 1.5, color: '#1e293b', strokeDash: [4, 3] }, encoding: { x: { field: 'mean', type: 'quantitative' } } }
      ] },
      config: { view: { stroke: null }, facet: { spacing: -8 } },
      resolve: { scale: { x: 'shared', y: 'independent' } }
    };
  }, [distributions]);

  /* ---------- metrics helpers ---------- */
  const sortedMetrics = useMemo(() => {
    if (!metrics || metrics.length === 0) return [];
    return [...metrics].sort((a, b) => {
      let va, vb;
      if (metricsSortField === 'composite') { va = compositeRanking?.[a.method] ?? Infinity; vb = compositeRanking?.[b.method] ?? Infinity; }
      else { va = a[metricsSortField]; vb = b[metricsSortField]; }
      if (metricsSortField === 'bias') { va = Math.abs(va || 0); vb = Math.abs(vb || 0); }
      if (va == null) va = Infinity;
      if (vb == null) vb = Infinity;
      return metricsSortDir === 'asc' ? va - vb : vb - va;
    });
  }, [metrics, metricsSortField, metricsSortDir, compositeRanking]);

  const handleMetricsSort = (field) => {
    if (metricsSortField === field) setMetricsSortDir(metricsSortDir === 'asc' ? 'desc' : 'asc');
    else { setMetricsSortField(field); setMetricsSortDir('asc'); }
  };
  const metricsSortIndicator = (field) => metricsSortField === field ? (metricsSortDir === 'asc' ? ' ▲' : ' ▼') : '';

  const bestPerMetric = useMemo(() => {
    if (!metrics || metrics.length === 0) return {};
    const fields = ['mae', 'rmse', 'mape', 'smape', 'mase', 'crps', 'winkler_score', 'quantile_loss'];
    const result = {};
    fields.forEach(f => { const vals = metrics.map(m => m[f]).filter(v => v != null && isFinite(v)); if (vals.length > 0) result[f] = Math.min(...vals); });
    const biasVals = metrics.map(m => m.bias).filter(v => v != null && isFinite(v));
    if (biasVals.length > 0) result.bias = biasVals.reduce((best, v) => Math.abs(v) < Math.abs(best) ? v : best);
    ['coverage_50', 'coverage_80', 'coverage_90', 'coverage_95'].forEach(f => {
      const target = parseInt(f.split('_')[1]) / 100;
      const vals = metrics.map(m => m[f]).filter(v => v != null && isFinite(v));
      if (vals.length > 0) result[f] = vals.reduce((best, v) => Math.abs(v - target) < Math.abs(best - target) ? v : best);
    });
    return result;
  }, [metrics]);

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
    if (!historicalData || !historicalData.date || historicalData.date.length === 0 || horizonLength === 0) return [];
    const lastDate = new Date(historicalData.date[historicalData.date.length - 1]);
    return Array.from({ length: horizonLength }, (_, i) => { const d = new Date(lastDate); d.setMonth(d.getMonth() + i + 1); return d.toISOString().slice(0, 7); });
  }, [historicalData, horizonLength]);

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
            value={selectedItem}
            onChange={handleItemChange}
            options={allItems}
            recentOptions={recentItems}
            placeholder="Search item..."
          />
          <SearchableDropdown
            label="Site"
            value={selectedSite}
            onChange={handleSiteChange}
            options={availableSites}
            recentOptions={recentSites.filter(s => availableSites.includes(s))}
            disabled={!selectedItem || availableSites.length === 0}
            placeholder="Search site..."
          />
        </div>
        {selectedItem && selectedSite && (
          <div className="mt-3 text-xs text-gray-400">
            Current series: <span className="font-mono font-medium text-gray-600">{selectedItem}_{selectedSite}</span>
          </div>
        )}
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

      {/* Method Toggles */}
      {forecasts.length > 0 && (
        <Section title="Method Toggles" storageKey="tsv_toggles_open">
          <div className="flex flex-wrap gap-2">
            {forecasts.map(f => (
              <button key={f.method} onClick={() => toggleMethod(f.method)}
                className={`px-3 py-1.5 rounded-full text-sm font-medium border-2 transition-all ${visibleMethods[f.method] ? 'text-white border-transparent' : 'bg-white text-gray-400 border-gray-200'}`}
                style={visibleMethods[f.method] ? { backgroundColor: getMethodColor(f.method), borderColor: getMethodColor(f.method) } : {}}>
                {f.method}{bestMethod?.best_method === f.method && ' ★'}
              </button>
            ))}
          </div>
        </Section>
      )}

      {/* Outlier Before/After Chart */}
      {hasOutlierCorrections && outlierChartSpec && (
        <Section title="Demand Before & After Correction" storageKey="tsv_outlier_open" badge={`${nOutliers} outlier${nOutliers !== 1 ? 's' : ''}`}>
          <p className="text-sm text-gray-500 mb-4">
            Detected via <span className="font-medium">{outlierInfo?.detection_method || 'IQR'}</span>, corrected with <span className="font-medium">{outlierInfo?.correction_method || 'clip'}</span>.
            Gray dashed = original, blue solid = corrected, red dots = outlier points.
          </p>
          <div className="w-full overflow-x-auto"><VegaLite spec={outlierChartSpec} actions={false} /></div>
          <ZoomSlider dates={outlierDates} start={outlierZoomStart} end={outlierZoomEnd} onStartChange={setOutlierZoomStart} onEndChange={setOutlierZoomEnd} />
        </Section>
      )}

      {/* Main Chart */}
      <Section title={`Historical Data & Forecasts${horizonLength ? ` (${horizonLength}-month horizon)` : ''}`} storageKey="tsv_main_chart_open">
        <p className="text-sm text-gray-500 mb-4">Shaded bands: 50% (dark) and 90% (light) prediction intervals.</p>
        {mainChartSpec ? (
          <div className="w-full overflow-x-auto"><VegaLite spec={mainChartSpec} actions={false} /></div>
        ) : <div className="text-gray-400 py-8 text-center">No data available</div>}
        <ZoomSlider dates={allDates} start={zoomStart} end={zoomEnd} onStartChange={setZoomStart} onEndChange={setZoomEnd} />
      </Section>

      {/* Method Selection Rationale */}
      {methodExplanation && (
        <Section title="Method Selection Rationale" storageKey="tsv_rationale_open" defaultOpen={false}>
          <div className="mb-3 text-sm bg-blue-50 text-blue-800 px-3 py-2 rounded">
            Selection category: <span className="font-semibold">{methodExplanation.selection_category}</span>
            <span className="mx-2">|</span>
            {methodExplanation.selection_reason}
            <span className="mx-2">|</span>
            {methodExplanation.n_observations} observations
          </div>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
              <h3 className="text-sm font-semibold text-emerald-700 mb-2">Applied Methods ({methodExplanation.included?.length || 0})</h3>
              <div className="space-y-1">
                {(methodExplanation.included || []).map((m, i) => (
                  <div key={i} className="flex items-start gap-2 text-sm">
                    <span className={`mt-0.5 text-xs ${m.status === 'forecasted' ? 'text-emerald-600' : 'text-amber-500'}`}>{m.status === 'forecasted' ? '✓' : '⚠'}</span>
                    <div><span className="font-medium">{m.method}</span><span className="text-gray-500 ml-1 text-xs">{m.reason}</span></div>
                  </div>
                ))}
              </div>
            </div>
            <div>
              <h3 className="text-sm font-semibold text-red-700 mb-2">Excluded Methods ({methodExplanation.excluded?.length || 0})</h3>
              <div className="space-y-1">
                {(methodExplanation.excluded || []).map((m, i) => (
                  <div key={i} className="flex items-start gap-2 text-sm">
                    <span className="mt-0.5 text-xs text-red-500">✗</span>
                    <div><span className="font-medium text-gray-600">{m.method}</span><span className="text-gray-400 ml-1 text-xs">{m.reason}</span></div>
                  </div>
                ))}
              </div>
            </div>
          </div>
        </Section>
      )}

      {/* Scoring Charts */}
      {(targetChartSpec || compositeScoreSpec) && (
        <Section title="Accuracy vs Precision & Composite Score" storageKey="tsv_scoring_open">
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            {targetChartSpec && (
              <div>
                <h3 className="text-sm font-semibold text-gray-600 mb-1">Accuracy vs Precision</h3>
                <p className="text-xs text-gray-400 mb-3">Bottom-left = best (low bias, low RMSE). Star = winner.</p>
                <div className="w-full overflow-x-auto"><VegaLite spec={targetChartSpec} actions={false} /></div>
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
                <div className="w-full overflow-x-auto"><VegaLite spec={compositeScoreSpec} actions={false} /></div>
              </div>
            )}
          </div>
        </Section>
      )}

      {/* Comprehensive Metrics Table */}
      {metrics.length > 0 && (
        <Section title="Comprehensive Metrics Comparison" storageKey="tsv_metrics_open">
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
      )}

      {metrics.length === 0 && forecasts.length > 0 && (
        <div className="mb-6 bg-white rounded-lg shadow p-6">
          <h2 className="text-lg font-semibold mb-2">Backtest Metrics</h2>
          <p className="text-gray-500 text-sm">This series has insufficient history for rolling-window backtesting (needs {12 + horizonLength}+ monthly observations). Forecasts are still generated.</p>
        </div>
      )}

      {/* Ridge Chart */}
      {ridgeChartSpec && (
        <Section title="Forecast Distribution Over Time" storageKey="tsv_ridge_open">
          <p className="text-sm text-gray-500 mb-1">
            Density curves for each horizon month ({distributions?.method || 'best method'}). Dashed line = mean. Color: near-term (cool) → far-term (warm).
          </p>
          {distributions?.horizons?.some(h => h.is_bootstrap) && (
            <p className="text-xs text-amber-600 mb-3">Some horizons use bootstrap distributions — parametric fit was not available.</p>
          )}
          <div className="w-full overflow-x-auto"><VegaLite spec={ridgeChartSpec} actions={false} /></div>
        </Section>
      )}

      {/* Forecast Evolution / Racing Bars */}
      {(origins.length > 0 || forecasts.length > 0) && (
        <Section title={origins.length > 0 ? 'Forecast Evolution Over Time' : 'Method Comparison'} storageKey="tsv_evolution_open">
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
            ? <div className="w-full overflow-x-auto"><VegaLite spec={racingBarsSpec} actions={false} /></div>
            : <div className="text-gray-400 py-4 text-center text-sm">No comparison data</div>
          }
        </Section>
      )}

      {/* Forecast Values Table */}
      {forecasts.length > 0 && (
        <Section title={`Forecast Point Values (${horizonLength} months)`} storageKey="tsv_forecast_table_open">
          <div className="overflow-x-auto max-h-96">
            <table className="min-w-full divide-y divide-gray-200 text-sm">
              <thead className="sticky top-0 bg-gray-50 z-10">
                <tr>
                  <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase sticky left-0 bg-gray-50 z-20">Method</th>
                  {forecastDates.map((d, i) => (
                    <th key={i} className="px-2 py-2 text-right text-xs font-medium text-gray-500 uppercase whitespace-nowrap">{d}</th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-200">
                {forecasts.map((f, idx) => (
                  <tr key={idx} className={bestMethod?.best_method === f.method ? 'bg-emerald-50' : ''}>
                    <td className="px-3 py-2 font-medium whitespace-nowrap sticky left-0 bg-white z-10" style={bestMethod?.best_method === f.method ? { backgroundColor: '#ecfdf5' } : {}}>
                      <span className="inline-block w-2.5 h-2.5 rounded-full mr-2" style={{ backgroundColor: getMethodColor(f.method) }}></span>
                      {f.method}
                    </td>
                    {f.point_forecast.map((v, i) => (
                      <td key={i} className="px-2 py-2 text-right font-mono text-xs">{v?.toLocaleString(undefined, { maximumFractionDigits: 0 })}</td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Section>
      )}

      {forecasts.length === 0 && metrics.length === 0 && (
        <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-6 text-center">
          <p className="text-yellow-800">No forecasts or backtest metrics available for this series.</p>
        </div>
      )}
    </div>
  );
};

export default TimeSeriesViewer;

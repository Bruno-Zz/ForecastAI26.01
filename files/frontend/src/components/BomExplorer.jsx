import React, { useState, useEffect, useCallback } from 'react';
import Plot from 'react-plotly.js';
import api from '../utils/api';
import { useTheme } from '../contexts/ThemeContext';

/**
 * BomExplorer — Plotly sunburst + collapsible tree list for BOM visualisation.
 *
 * Props:
 *   assetTypeId  {number|null}  — filter BOM by asset type
 *   assetId      {string|null}  — if set, overlay effectivity colours
 *   onSelectPart {function}     — called with item_id when a wedge is clicked
 */
export default function BomExplorer({ assetTypeId, assetId, onSelectPart }) {
  const { theme } = useTheme();
  const isDark = theme === 'dark';

  const [bom, setBom] = useState([]);
  const [effectivity, setEffectivity] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [treeOpen, setTreeOpen] = useState({});

  // Load BOM
  const loadBom = useCallback(async () => {
    if (!assetTypeId) { setBom([]); return; }
    setLoading(true);
    setError(null);
    try {
      const params = { asset_type_id: assetTypeId };
      const res = await api.get('/causal/bom', { params });
      setBom(res.data || []);
    } catch (e) {
      setError(e?.response?.data?.detail || e.message);
    } finally {
      setLoading(false);
    }
  }, [assetTypeId]);

  // Load effectivity (only when assetId provided)
  const loadEffectivity = useCallback(async () => {
    if (!assetId || !assetTypeId) { setEffectivity([]); return; }
    try {
      const res = await api.get('/causal/effectivity', {
        params: { asset_id: assetId, asset_type_id: assetTypeId }
      });
      setEffectivity(res.data || []);
    } catch {
      setEffectivity([]);
    }
  }, [assetId, assetTypeId]);

  useEffect(() => {
    loadBom();
    loadEffectivity();
  }, [loadBom, loadEffectivity]);

  // Build effectivity map: item_id -> { effective, qty_override }
  const effMap = {};
  for (const e of effectivity) {
    effMap[e.item_id] = e;
  }

  // Determine if a BOM item is effective
  const isEffective = (item_id) => {
    if (!assetId) return true; // no tail selected -> all effective
    const e = effMap[item_id];
    if (!e) return true; // no override -> effective by default
    return e.effective !== false;
  };

  // ── Sunburst data ─────────────────────────────────────────────────────────
  const sunburstData = (() => {
    if (!bom.length) return null;

    // Build id list: use bom_id as string for each item
    // Root node for each LRU (parent = '')
    const ids = [];
    const labels = [];
    const parents = [];
    const values = [];
    const colors = [];

    // Add a virtual root
    ids.push('__root__');
    labels.push(assetTypeId ? `Type ${assetTypeId}` : 'All');
    parents.push('');
    values.push(1);
    colors.push(isDark ? '#6b7280' : '#9ca3af');

    for (const row of bom) {
      const id = `bom_${row.bom_id}`;
      const label = row.item_name || `Item ${row.item_id}`;
      const parent = row.parent_bom_id ? `bom_${row.parent_bom_id}` : '__root__';
      const qty = Math.max(row.effective_qty || row.qty_per_asset || 1, 0.001);
      const effective = isEffective(row.item_id);
      const color = effective ? '#22c55e' : '#ef4444';

      ids.push(id);
      labels.push(`${label}\n(×${qty})`);
      parents.push(parent);
      values.push(qty);
      colors.push(color);
    }

    return [{
      type: 'sunburst',
      ids,
      labels,
      parents,
      values,
      marker: { colors },
      hovertemplate: '<b>%{label}</b><br>Qty: %{value}<extra></extra>',
      textinfo: 'label',
    }];
  })();

  // ── Tree list ─────────────────────────────────────────────────────────────
  // Build tree structure: lru -> [sru, ...]
  const lruItems = bom.filter(b => b.is_lru);
  const sruByParent = {};
  for (const b of bom) {
    if (b.parent_bom_id) {
      if (!sruByParent[b.parent_bom_id]) sruByParent[b.parent_bom_id] = [];
      sruByParent[b.parent_bom_id].push(b);
    }
  }

  const renderTreeNode = (node, depth = 0) => {
    const children = sruByParent[node.bom_id] || [];
    const hasChildren = children.length > 0;
    const effective = isEffective(node.item_id);
    const nodeKey = `node_${node.bom_id}`;
    const isOpen = treeOpen[nodeKey] !== false; // open by default

    return (
      <li key={node.bom_id} className="py-0.5">
        <div
          className={`flex items-center gap-1.5 px-2 py-0.5 rounded cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-700/50
            ${!effective ? 'opacity-50' : ''}`}
          style={{ paddingLeft: `${8 + depth * 16}px` }}
          onClick={() => {
            if (hasChildren) {
              setTreeOpen(prev => ({ ...prev, [nodeKey]: !isOpen }));
            }
            if (onSelectPart) onSelectPart(node.item_id);
          }}
        >
          {hasChildren && (
            <span className="text-gray-400 dark:text-gray-500 text-xs flex-shrink-0">
              {isOpen ? '▾' : '▸'}
            </span>
          )}
          {!hasChildren && <span className="w-3 flex-shrink-0" />}
          <span className={`w-2 h-2 rounded-full flex-shrink-0 ${effective ? 'bg-green-500' : 'bg-red-500'}`} />
          <span className="text-sm text-gray-800 dark:text-gray-200 truncate">
            {node.item_name || `Item ${node.item_id}`}
          </span>
          <span className="text-xs text-gray-400 dark:text-gray-500 flex-shrink-0 ml-1">
            ×{node.qty_per_asset} · {node.removal_driver}
          </span>
          {node.mdfh_mean > 0 && (
            <span className="text-xs text-blue-500 dark:text-blue-400 flex-shrink-0 ml-1">
              MDFH:{node.mdfh_mean.toFixed(4)}
            </span>
          )}
        </div>
        {hasChildren && isOpen && (
          <ul>
            {children.map(child => renderTreeNode(child, depth + 1))}
          </ul>
        )}
      </li>
    );
  };

  const plotBg = isDark ? '#1f2937' : '#ffffff';
  const plotPaper = isDark ? '#111827' : '#f9fafb';
  const plotFont = isDark ? '#d1d5db' : '#374151';

  if (!assetTypeId) {
    return (
      <p className="text-sm text-gray-400 dark:text-gray-500">Select an asset type to explore its BOM.</p>
    );
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center py-8">
        <div className="animate-spin rounded-full h-6 w-6 border-b-2 border-blue-600" />
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-700 rounded p-3 text-sm text-red-700 dark:text-red-400">
        {error}
      </div>
    );
  }

  if (!bom.length) {
    return (
      <p className="text-sm text-gray-400 dark:text-gray-500">No BOM lines found for this asset type.</p>
    );
  }

  return (
    <div className="space-y-4">
      {/* Sunburst chart */}
      {sunburstData && (
        <div>
          <Plot
            data={sunburstData}
            layout={{
              title: {
                text: `BOM Structure — Type ${assetTypeId}${assetId ? ` / Asset ${assetId}` : ''}`,
                font: { color: plotFont, size: 13 }
              },
              paper_bgcolor: plotPaper,
              plot_bgcolor: plotBg,
              font: { color: plotFont, size: 11 },
              margin: { l: 10, r: 10, t: 40, b: 10 },
              height: 380,
            }}
            config={{ displayModeBar: false, responsive: true }}
            style={{ width: '100%' }}
            onClick={(e) => {
              if (e.points && e.points[0] && onSelectPart) {
                const pointId = e.points[0].id;
                if (pointId && pointId !== '__root__') {
                  const bomId = parseInt(pointId.replace('bom_', ''), 10);
                  const found = bom.find(b => b.bom_id === bomId);
                  if (found) onSelectPart(found.item_id);
                }
              }
            }}
          />
          <div className="flex gap-4 justify-center mt-1 text-xs text-gray-500 dark:text-gray-400">
            <span className="flex items-center gap-1">
              <span className="w-3 h-3 rounded-full bg-green-500 inline-block" /> Effective
            </span>
            <span className="flex items-center gap-1">
              <span className="w-3 h-3 rounded-full bg-red-500 inline-block" /> Not effective
            </span>
          </div>
        </div>
      )}

      {/* Collapsible tree list */}
      <div>
        <button
          className="text-sm font-medium text-gray-700 dark:text-gray-300 hover:text-blue-600 dark:hover:text-blue-400 mb-2"
          onClick={() => setTreeOpen(prev => {
            // toggle all: if any are open, close all; otherwise open all
            const anyOpen = Object.values(prev).some(v => v !== false);
            if (anyOpen) {
              const closed = {};
              bom.forEach(b => { closed[`node_${b.bom_id}`] = false; });
              return closed;
            }
            return {};
          })}
        >
          {Object.values(treeOpen).some(v => v === false) ? 'Expand all' : 'Collapse all'}
        </button>
        <ul className="border border-gray-200 dark:border-gray-700 rounded-lg bg-white dark:bg-gray-800 py-1 max-h-64 overflow-y-auto">
          {lruItems.map(node => renderTreeNode(node, 0))}
        </ul>
      </div>
    </div>
  );
}

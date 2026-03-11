/**
 * Segments.jsx
 * Manage named groups of item/site series based on filter criteria.
 * Provides CRUD for segments, a recursive criteria builder, preview, and
 * "Run step for this segment" shortcuts.
 */

import { useState, useEffect, useCallback, useRef } from 'react';
import { useNavigate } from 'react-router-dom';
import api from '../utils/api';

/** Split unique_id on the first underscore into item and site. */
const parseSeriesId = (uid) => {
  if (!uid) return { item: '', site: '' };
  const idx = uid.indexOf('_');
  if (idx === -1) return { item: uid, site: '' };
  return { item: uid.slice(0, idx), site: uid.slice(idx + 1) };
};

// ─── Helpers ────────────────────────────────────────────────────────────────

/**
 * Build human-readable label from a column name.
 * e.g. "n_observations" → "N Observations", "abc_class" → "ABC Class"
 */
function colLabel(col) {
  return col
    .replace(/_/g, ' ')
    .replace(/\b\w/g, c => c.toUpperCase())
    .replace(/\bAbc\b/, 'ABC')
    .replace(/\bAdi\b/, 'ADI')
    .replace(/\bCov\b/, 'CoV')
    .replace(/\bAdf\b/, 'ADF')
    .replace(/\bMl\b/, 'ML')
    .replace(/\bId\b/, 'ID')
    .replace(/\bXuid\b/, 'Code');
}

/**
 * Build the allFields list from the API /segments/fields response.
 * Falls back to minimal hardcoded defaults when columns are not yet loaded.
 */
function buildFieldsFromApi(fieldsData) {
  const result = [];

  // ── Item table columns ──
  if (fieldsData.item_columns?.length) {
    for (const c of fieldsData.item_columns) {
      const label = c.column === 'type_id' && c.options_by_id ? 'Item Type' : `Item ${colLabel(c.column)}`;
      result.push({ key: `item.${c.column}`, label, type: c.type, options: c.options, optionsById: c.options_by_id });
    }
  } else {
    // Fallback if API hasn't returned column info yet
    result.push(
      { key: 'item.name',    label: 'Item Name', type: 'string' },
      { key: 'item.xuid',    label: 'Item Code', type: 'string' },
      { key: 'item.type_id', label: 'Item Type ID', type: 'number' },
    );
  }

  // ── Item JSONB attributes ──
  if (fieldsData.item_attr_info?.length) {
    for (const a of fieldsData.item_attr_info) {
      result.push({
        key: `item.attributes.${a.key}`,
        label: `Item: ${colLabel(a.key)}`,
        type: 'enum',
        ...(a.options?.length ? { options: a.options } : {}),
      });
    }
  } else {
    for (const k of (fieldsData.item || [])) {
      result.push({ key: `item.attributes.${k}`, label: `Item: ${colLabel(k)}`, type: 'enum' });
    }
  }

  // ── Site table columns ──
  if (fieldsData.site_columns?.length) {
    for (const c of fieldsData.site_columns) {
      const label = c.column === 'type_id' && c.options_by_id ? 'Site Type' : `Site ${colLabel(c.column)}`;
      result.push({ key: `site.${c.column}`, label, type: c.type, options: c.options, optionsById: c.options_by_id });
    }
  } else {
    result.push(
      { key: 'site.name',    label: 'Site Name', type: 'string' },
      { key: 'site.xuid',    label: 'Site Code', type: 'string' },
      { key: 'site.type_id', label: 'Site Type ID', type: 'number' },
    );
  }

  // ── Site JSONB attributes ──
  if (fieldsData.site_attr_info?.length) {
    for (const a of fieldsData.site_attr_info) {
      result.push({
        key: `site.attributes.${a.key}`,
        label: `Site: ${colLabel(a.key)}`,
        type: 'enum',
        ...(a.options?.length ? { options: a.options } : {}),
      });
    }
  } else {
    for (const k of (fieldsData.site || [])) {
      result.push({ key: `site.attributes.${k}`, label: `Site: ${colLabel(k)}`, type: 'enum' });
    }
  }

  // ── Demand / characteristics columns ──
  if (fieldsData.demand_columns?.length) {
    for (const c of fieldsData.demand_columns) {
      const field = { key: `demand.${c.column}`, label: colLabel(c.column), type: c.type };
      if (c.options) field.options = c.options;
      result.push(field);
    }
  } else {
    result.push(
      { key: 'demand.n_observations', label: 'N Observations', type: 'number' },
      { key: 'demand.mean',           label: 'Mean',           type: 'number' },
      { key: 'demand.abc_class',      label: 'ABC Class',      type: 'enum', options: ['A', 'B', 'C'] },
    );
  }

  // ── Dynamic ABC classification fields ──
  if (fieldsData.classification_fields?.length) {
    for (const cf of fieldsData.classification_fields) {
      result.push({
        key: cf.field_key,
        label: `Class: ${cf.column}`,
        type: 'enum',
        options: cf.options || [],
      });
    }
  }

  return result;
}

const OPERATORS_BY_TYPE = {
  string:  ['=','!=','contains','starts_with','is_null','is_not_null'],
  number:  ['=','!=','<','>','<=','>=','is_null','is_not_null'],
  boolean: ['is_true','is_false'],
  enum:    ['=','!=','in'],
};

const OP_LABELS = {
  '=': '=', '!=': '≠', '<': '<', '>': '>', '<=': '≤', '>=': '≥',
  'contains': 'contains', 'starts_with': 'starts with',
  'is_null': 'is empty', 'is_not_null': 'is not empty',
  'is_true': 'is true', 'is_false': 'is false', 'in': 'in list',
};

function uid() {
  return Math.random().toString(36).slice(2);
}

function makeCondition() {
  return { _id: uid(), type: 'condition', field: 'demand.n_observations',
           op: '>', valueType: 'literal', value: '' };
}

function makeGroup(operator = 'AND') {
  return { _id: uid(), type: 'group', operator, children: [makeCondition()] };
}

function stripIds(node) {
  if (!node) return node;
  const { _id, ...rest } = node;
  if (rest.children) rest.children = rest.children.map(stripIds);
  return rest;
}

function addIds(node) {
  if (!node || typeof node !== 'object') return node;
  const n = { _id: uid(), ...node };
  if (n.children) n.children = n.children.map(addIds);
  return n;
}

// ─── CriteriaCondition ───────────────────────────────────────────────────────

function CriteriaCondition({ node, onChange, onRemove, allFields }) {
  const field = allFields.find(f => f.key === node.field) || allFields[0];
  const ops = OPERATORS_BY_TYPE[field?.type ?? 'string'] ?? [];
  const noValue = ['is_null','is_not_null','is_true','is_false'].includes(node.op);

  function update(patch) { onChange({ ...node, ...patch }); }

  return (
    <div className="flex flex-wrap items-center gap-1.5 p-2 bg-white dark:bg-gray-800
                    border border-gray-200 dark:border-gray-600 rounded-lg">
      {/* Field */}
      <select
        value={node.field}
        onChange={e => {
          const f = allFields.find(x => x.key === e.target.value) || allFields[0];
          const newOps = OPERATORS_BY_TYPE[f?.type ?? 'string'] ?? [];
          update({ field: e.target.value, op: newOps[0] ?? '=', value: '' });
        }}
        className="text-xs border border-gray-300 dark:border-gray-600 rounded
                   px-2 py-1 bg-white dark:bg-gray-700 dark:text-gray-100"
      >
        {/* Group by category */}
        <optgroup label="Item">
          {allFields.filter(f => f.key.startsWith('item.')).map(f =>
            <option key={f.key} value={f.key}>{f.label}</option>
          )}
        </optgroup>
        <optgroup label="Site">
          {allFields.filter(f => f.key.startsWith('site.')).map(f =>
            <option key={f.key} value={f.key}>{f.label}</option>
          )}
        </optgroup>
        <optgroup label="Demand">
          {allFields.filter(f => f.key.startsWith('demand.')).map(f =>
            <option key={f.key} value={f.key}>{f.label}</option>
          )}
        </optgroup>
      </select>

      {/* Operator */}
      <select
        value={node.op}
        onChange={e => update({ op: e.target.value, value: '' })}
        className="text-xs border border-gray-300 dark:border-gray-600 rounded
                   px-2 py-1 bg-white dark:bg-gray-700 dark:text-gray-100"
      >
        {ops.map(op => <option key={op} value={op}>{OP_LABELS[op] ?? op}</option>)}
      </select>

      {/* Value input */}
      {!noValue && (
        field?.type === 'enum'
          ? field.key === node.field && node.op === 'in'
            ? <input
                type="text"
                placeholder="A, B, C"
                value={Array.isArray(node.value) ? node.value.join(', ') : node.value}
                onChange={e => {
                  const vals = e.target.value.split(',').map(s => s.trim());
                  update({ value: vals });
                }}
                className="text-xs border border-gray-300 dark:border-gray-600 rounded
                           px-2 py-1 w-28 bg-white dark:bg-gray-700 dark:text-gray-100"
              />
            : field.options?.length
              ? <select
                  value={node.value}
                  onChange={e => {
                    update({ value: e.target.value });
                  }}
                  className="text-xs border border-gray-300 dark:border-gray-600 rounded
                             px-2 py-1 bg-white dark:bg-gray-700 dark:text-gray-100"
                >
                  <option value="">—</option>
                  {field.options.map(o => <option key={o} value={o}>{o}</option>)}
                </select>
              : <input
                  type="text"
                  value={node.value ?? ''}
                  onChange={e => update({ value: e.target.value })}
                  placeholder="value"
                  className="text-xs border border-gray-300 dark:border-gray-600 rounded
                             px-2 py-1 w-28 bg-white dark:bg-gray-700 dark:text-gray-100"
                />
          : <input
              type={field?.type === 'number' ? 'number' : 'text'}
              value={node.value ?? ''}
              onChange={e => update({ value: e.target.value })}
              placeholder="value"
              className="text-xs border border-gray-300 dark:border-gray-600 rounded
                         px-2 py-1 w-28 bg-white dark:bg-gray-700 dark:text-gray-100"
            />
      )}

      {/* Remove */}
      <button
        onClick={onRemove}
        className="ml-1 text-gray-400 hover:text-red-500 text-sm font-bold"
        title="Remove condition"
      >✕</button>
    </div>
  );
}

// ─── CriteriaGroup ───────────────────────────────────────────────────────────

function CriteriaGroup({ node, onChange, onRemove, depth = 0, isRoot = false, allFields }) {
  const MAX_DEPTH = 4;

  function updateChild(index, newChild) {
    const children = node.children.map((c, i) => i === index ? newChild : c);
    onChange({ ...node, children });
  }

  function removeChild(index) {
    const children = node.children.filter((_, i) => i !== index);
    onChange({ ...node, children });
  }

  function addCondition() {
    onChange({ ...node, children: [...node.children, makeCondition()] });
  }

  function addGroup() {
    onChange({ ...node, children: [...node.children, makeGroup('OR')] });
  }

  const bgColors = ['bg-blue-50 border-blue-200 dark:bg-blue-900/20 dark:border-blue-700',
                    'bg-orange-50 border-orange-200 dark:bg-orange-900/20 dark:border-orange-700',
                    'bg-green-50 border-green-200 dark:bg-green-900/20 dark:border-green-700',
                    'bg-purple-50 border-purple-200 dark:bg-purple-900/20 dark:border-purple-700'];
  const bg = bgColors[depth % bgColors.length];

  const operatorPillClass = node.operator === 'AND'
    ? 'bg-blue-600 text-white'
    : 'bg-orange-500 text-white';

  return (
    <div className={`border rounded-lg p-3 space-y-2 ${bg}`}>
      <div className="flex items-center gap-2">
        {/* AND/OR toggle */}
        <button
          onClick={() => onChange({ ...node, operator: node.operator === 'AND' ? 'OR' : 'AND' })}
          className={`text-xs font-bold px-2.5 py-1 rounded-full cursor-pointer
                      transition-colors ${operatorPillClass}`}
        >
          {node.operator}
        </button>
        <span className="text-xs text-gray-500 dark:text-gray-400 italic">
          {node.operator === 'AND' ? 'All conditions must match' : 'Any condition must match'}
        </span>
        {!isRoot && (
          <button
            onClick={onRemove}
            className="ml-auto text-gray-400 hover:text-red-500 text-sm font-bold"
            title="Remove group"
          >✕</button>
        )}
      </div>

      {/* Children */}
      <div className="space-y-2 pl-2">
        {node.children.map((child, i) =>
          child.type === 'group'
            ? <CriteriaGroup
                key={child._id}
                node={child}
                onChange={n => updateChild(i, n)}
                onRemove={() => removeChild(i)}
                depth={depth + 1}
                isRoot={false}
                allFields={allFields}
              />
            : <CriteriaCondition
                key={child._id}
                node={child}
                onChange={n => updateChild(i, n)}
                onRemove={() => removeChild(i)}
                allFields={allFields}
              />
        )}
      </div>

      {/* Add buttons */}
      <div className="flex gap-2 pl-2 pt-1">
        <button
          onClick={addCondition}
          className="text-xs px-2 py-1 rounded border border-blue-300 text-blue-600
                     hover:bg-blue-50 dark:border-blue-600 dark:text-blue-400
                     dark:hover:bg-blue-900/30"
        >+ Add Condition</button>
        {depth < MAX_DEPTH && (
          <button
            onClick={addGroup}
            className="text-xs px-2 py-1 rounded border border-gray-300 text-gray-600
                       hover:bg-gray-50 dark:border-gray-600 dark:text-gray-400
                       dark:hover:bg-gray-700"
          >+ Add Group</button>
        )}
      </div>
    </div>
  );
}

// ─── CriteriaBuilder ─────────────────────────────────────────────────────────

function CriteriaBuilder({ rootCriteria, onChange, allFields }) {
  // If criteria is empty/null, show an empty root group
  const root = (rootCriteria && Object.keys(rootCriteria).length > 0)
    ? (rootCriteria._id ? rootCriteria : addIds(rootCriteria))
    : makeGroup('AND');

  const [localRoot, setLocalRoot] = useState(root);

  // Re-seed when criteria prop changes *externally* (e.g. switching to a different
  // saved segment).  We must NOT re-seed when the change came from inside the builder
  // itself, because that path goes:
  //   user types → handleChange → onChange(stripIds) → parent.setCriteria
  //   → rootCriteria prop changes → useEffect → addIds (new _ids) → key change
  //   → React unmounts input → focus lost every keystroke.
  const prevCriteriaRef  = useRef(JSON.stringify(rootCriteria));
  const internalChange   = useRef(false);   // set true before calling parent onChange

  useEffect(() => {
    if (internalChange.current) {
      // This prop update was triggered by our own onChange call — skip re-seed.
      internalChange.current = false;
      return;
    }
    const serialized = JSON.stringify(rootCriteria);
    if (serialized !== prevCriteriaRef.current) {
      prevCriteriaRef.current = serialized;
      const newRoot = (rootCriteria && Object.keys(rootCriteria).length > 0)
        ? (rootCriteria._id ? rootCriteria : addIds(rootCriteria))
        : makeGroup('AND');
      setLocalRoot(newRoot);
    }
  }, [rootCriteria]);

  function handleChange(newNode) {
    internalChange.current = true;   // tell the effect to ignore the echo
    prevCriteriaRef.current = JSON.stringify(stripIds(newNode)); // keep ref in sync
    setLocalRoot(newNode);
    onChange(stripIds(newNode));
  }

  return (
    <CriteriaGroup
      node={localRoot}
      onChange={handleChange}
      onRemove={() => {}}
      depth={0}
      isRoot={true}
      allFields={allFields}
    />
  );
}

// ─── EditModal ───────────────────────────────────────────────────────────────

function EditModal({ segment, onSave, onClose, allFields }) {
  const isNew = !segment?.id;
  const [name, setName] = useState(segment?.name ?? '');
  const [description, setDescription] = useState(segment?.description ?? '');
  const [criteria, setCriteria] = useState(segment?.criteria ?? {});
  const [preview, setPreview] = useState(null);  // {count, sample}
  const [previewing, setPreviewing] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState('');

  async function handlePreview() {
    if (!segment?.id) return;
    setPreviewing(true);
    try {
      const res = await api.post(`/segments/${segment.id}/preview`);
      setPreview(res.data);
    } catch (e) {
      setError(`Preview failed: ${e.response?.data?.detail || e.message}`);
    } finally {
      setPreviewing(false);
    }
  }

  // Live preview: temporarily update segment criteria and preview
  async function handleLivePreview() {
    setPreviewing(true);
    setError('');
    try {
      if (!segment?.id) {
        setError('Save the segment first to preview.');
        return;
      }
      // Update criteria silently, then preview
      await api.put(`/segments/${segment.id}`, { name, description, criteria });
      const pRes = await api.post(`/segments/${segment.id}/preview`);
      setPreview(pRes.data);
    } catch (e) {
      setError(`Preview failed: ${e.response?.data?.detail || e.message}`);
    } finally {
      setPreviewing(false);
    }
  }

  async function handleSave() {
    if (!name.trim()) { setError('Name is required'); return; }
    setSaving(true);
    setError('');
    try {
      const res = isNew
        ? await api.post('/segments', { name, description, criteria })
        : await api.put(`/segments/${segment.id}`, { name, description, criteria });
      const saved = res.data;
      onSave(saved);
    } catch (e) {
      setError(e.response?.data?.detail || e.message);
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4
                    bg-black/50 backdrop-blur-sm">
      <div className="bg-white dark:bg-gray-900 rounded-2xl shadow-2xl w-full max-w-3xl
                      max-h-[90vh] overflow-y-auto flex flex-col">
        {/* Header */}
        <div className="flex items-center justify-between p-5 border-b dark:border-gray-700">
          <h2 className="text-lg font-semibold dark:text-white">
            {isNew ? 'New Segment' : `Edit Segment: ${segment.name}`}
          </h2>
          <button onClick={onClose} className="text-gray-400 dark:text-gray-500 hover:text-gray-600 dark:hover:text-gray-300 text-xl">✕</button>
        </div>

        <div className="p-5 space-y-4 flex-1">
          {/* Name + Description */}
          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                Name <span className="text-red-500">*</span>
              </label>
              <input
                value={name}
                onChange={e => setName(e.target.value)}
                placeholder="e.g. German Sites"
                className="w-full border border-gray-300 dark:border-gray-600 rounded-lg px-3 py-2
                           text-sm bg-white dark:bg-gray-800 dark:text-white"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                Description
              </label>
              <input
                value={description}
                onChange={e => setDescription(e.target.value)}
                placeholder="Optional description"
                className="w-full border border-gray-300 dark:border-gray-600 rounded-lg px-3 py-2
                           text-sm bg-white dark:bg-gray-800 dark:text-white"
              />
            </div>
          </div>

          {/* Criteria builder */}
          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
              Criteria
              <span className="ml-2 text-xs text-gray-500 dark:text-gray-400 font-normal">
                (leave empty to match all series)
              </span>
            </label>
            <CriteriaBuilder
              rootCriteria={criteria}
              onChange={setCriteria}
              allFields={allFields}
            />
          </div>

          {/* Preview result */}
          {preview && (
            <div className="bg-blue-50 dark:bg-blue-900/20 border border-blue-200
                            dark:border-blue-700 rounded-lg p-3">
              <p className="text-sm font-semibold text-blue-800 dark:text-blue-300">
                Preview: <strong>{preview.count.toLocaleString()}</strong> matching series
              </p>
              {preview.sample?.length > 0 && (
                <p className="text-xs text-blue-600 dark:text-blue-400 mt-1">
                  Sample: {preview.sample.slice(0, 8).join(', ')}
                  {preview.sample.length > 8 ? ` … (+${preview.count - 8} more)` : ''}
                </p>
              )}
            </div>
          )}

          {error && (
            <p className="text-sm text-red-600 dark:text-red-400">{error}</p>
          )}
        </div>

        {/* Footer */}
        <div className="flex items-center justify-between p-5 border-t dark:border-gray-700">
          <button
            onClick={handleLivePreview}
            disabled={previewing || isNew}
            className="px-4 py-2 text-sm rounded-lg border border-blue-300 text-blue-600
                       hover:bg-blue-50 dark:border-blue-600 dark:text-blue-400
                       disabled:opacity-50"
          >
            {previewing ? 'Previewing…' : 'Preview'}
          </button>
          <div className="flex gap-2">
            <button
              onClick={onClose}
              className="px-4 py-2 text-sm rounded-lg border border-gray-300 dark:border-gray-600
                         text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-700"
            >Cancel</button>
            <button
              onClick={handleSave}
              disabled={saving}
              className="px-4 py-2 text-sm rounded-lg bg-blue-600 text-white
                         hover:bg-blue-700 disabled:opacity-50"
            >
              {saving ? 'Saving…' : isNew ? 'Create' : 'Save'}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

// ─── RunDropdown ─────────────────────────────────────────────────────────────

const RUNNABLE_STEPS = [
  { id: 'characterization', label: 'Characterization' },
  { id: 'forecast',         label: 'Forecast' },
  { id: 'backtest',         label: 'Backtest' },
];

function RunDropdown({ segment, onRun, onClose }) {
  const ref = useRef(null);

  useEffect(() => {
    function handler(e) {
      if (ref.current && !ref.current.contains(e.target)) onClose();
    }
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, [onClose]);

  return (
    <div ref={ref}
         className="absolute right-0 top-8 z-40 w-52 bg-white dark:bg-gray-800
                    border border-gray-200 dark:border-gray-700 rounded-xl shadow-lg
                    overflow-hidden text-sm">
      <div className="px-3 py-2 bg-gray-50 dark:bg-gray-700/50 text-xs font-semibold
                      text-gray-500 dark:text-gray-400 border-b dark:border-gray-700">
        Run for "{segment.name}"
      </div>
      {RUNNABLE_STEPS.map(s => (
        <button
          key={s.id}
          onClick={() => { onRun(s.id); onClose(); }}
          className="w-full text-left px-3 py-2 hover:bg-blue-50 dark:hover:bg-blue-900/30
                     text-gray-700 dark:text-gray-200"
        >
          ▶ {s.label}
        </button>
      ))}
    </div>
  );
}

// ─── DetailModal ─────────────────────────────────────────────────────────────

function CriteriaSummary({ node, allFields, depth = 0 }) {
  if (!node || typeof node !== 'object') return null;
  if (node.type === 'condition') {
    const f = allFields.find(x => x.key === node.field);
    const label = f?.label ?? node.field;
    const opLabel = OP_LABELS[node.op] ?? node.op;
    const val = Array.isArray(node.value) ? node.value.join(', ') : (node.value ?? '');
    const noValue = ['is_null','is_not_null','is_true','is_false'].includes(node.op);
    return (
      <span className="inline-flex items-center gap-1 text-xs bg-gray-100 dark:bg-gray-700
                        rounded px-2 py-0.5 text-gray-700 dark:text-gray-300">
        <span className="font-medium">{label}</span>
        <span className="text-gray-500 dark:text-gray-400">{opLabel}</span>
        {!noValue && <span className="font-semibold">{val}</span>}
      </span>
    );
  }
  // group
  const op = node.operator || 'AND';
  return (
    <div className={`flex flex-wrap items-center gap-1 ${depth > 0 ? 'ml-3 pl-2 border-l-2 border-gray-200 dark:border-gray-600' : ''}`}>
      {(node.children || []).map((child, i) => (
        <span key={i} className="flex items-center gap-1">
          {i > 0 && <span className={`text-xs font-bold px-1.5 py-0.5 rounded-full
                          ${op === 'AND' ? 'bg-blue-100 text-blue-700 dark:bg-blue-900/40 dark:text-blue-300'
                                         : 'bg-orange-100 text-orange-700 dark:bg-orange-900/40 dark:text-orange-300'}`}>{op}</span>}
          <CriteriaSummary node={child} allFields={allFields} depth={depth + 1} />
        </span>
      ))}
    </div>
  );
}

/** Extract which attribute keys are referenced in the segment criteria */
function extractCriteriaAttrKeys(node) {
  const keys = { item: new Set(), site: new Set() };
  if (!node) return keys;
  if (node.type === 'condition') {
    if (node.field?.startsWith('item.attributes.'))
      keys.item.add(node.field.replace('item.attributes.', ''));
    if (node.field?.startsWith('site.attributes.'))
      keys.site.add(node.field.replace('site.attributes.', ''));
  }
  if (node.children) {
    for (const c of node.children) {
      const sub = extractCriteriaAttrKeys(c);
      sub.item.forEach(k => keys.item.add(k));
      sub.site.forEach(k => keys.site.add(k));
    }
  }
  return keys;
}

function DetailModal({ segment, onClose, allFields }) {
  const [detail, setDetail] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [page, setPage] = useState(0);
  const PAGE_SIZE = 100;

  const fetchPage = useCallback(async (p) => {
    setLoading(true);
    setError('');
    try {
      const res = await api.get(`/segments/${segment.id}/details`, {
        params: { limit: PAGE_SIZE, offset: p * PAGE_SIZE },
      });
      setDetail(res.data);
    } catch (e) {
      setError(e.response?.data?.detail || e.message);
    } finally {
      setLoading(false);
    }
  }, [segment.id]);

  useEffect(() => { fetchPage(page); }, [fetchPage, page]);

  // Figure out which attribute keys are used in criteria
  const criteriaKeys = detail ? extractCriteriaAttrKeys(detail.criteria) : { item: new Set(), site: new Set() };

  // Build visible attribute columns: criteria-referenced attrs + a few defaults
  const defaultItemKeys = ['level1', 'Product', 'type_name'];
  const defaultSiteKeys = ['level1', 'type_name'];
  const extraItemCols = [...new Set([...criteriaKeys.item, ...defaultItemKeys])];
  const extraSiteCols = [...new Set([...criteriaKeys.site, ...defaultSiteKeys])];

  const totalPages = detail ? Math.max(1, Math.ceil(detail.total / PAGE_SIZE)) : 1;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4
                    bg-black/50 backdrop-blur-sm">
      <div className="bg-white dark:bg-gray-900 rounded-2xl shadow-2xl w-full max-w-6xl
                      max-h-[90vh] flex flex-col">
        {/* Header */}
        <div className="flex items-center justify-between p-5 border-b dark:border-gray-700 shrink-0">
          <div>
            <h2 className="text-lg font-semibold dark:text-white">
              {segment.name}
              <span className="ml-2 text-sm font-normal text-gray-500 dark:text-gray-400">
                {detail ? `${detail.total.toLocaleString()} series` : ''}
              </span>
            </h2>
            {segment.description && (
              <p className="text-sm text-gray-500 dark:text-gray-400 mt-0.5">{segment.description}</p>
            )}
          </div>
          <button onClick={onClose}
                  className="text-gray-400 dark:text-gray-500 hover:text-gray-600
                             dark:hover:text-gray-300 text-xl">✕</button>
        </div>

        {/* Criteria summary */}
        {detail?.criteria && Object.keys(detail.criteria).length > 0 && (
          <div className="px-5 py-3 border-b dark:border-gray-700 bg-gray-50 dark:bg-gray-800/50 shrink-0">
            <span className="text-xs font-medium text-gray-500 dark:text-gray-400 mr-2">Criteria:</span>
            <CriteriaSummary node={detail.criteria} allFields={allFields} />
          </div>
        )}

        {/* Content */}
        <div className="flex-1 overflow-auto">
          {loading && !detail ? (
            <div className="text-center py-16 text-gray-400">Loading…</div>
          ) : error ? (
            <div className="text-center py-16 text-red-500">{error}</div>
          ) : (
            <table className="w-full text-xs">
              <thead className="bg-gray-50 dark:bg-gray-800 sticky top-0">
                <tr>
                  <th className="text-left px-3 py-2 font-medium text-gray-600 dark:text-gray-300">Series (item@site)</th>
                  <th className="text-left px-3 py-2 font-medium text-gray-600 dark:text-gray-300">Item Name</th>
                  <th className="text-left px-3 py-2 font-medium text-gray-600 dark:text-gray-300">Site Name</th>
                  {extraItemCols.map(k => (
                    <th key={`ia_${k}`} className="text-left px-3 py-2 font-medium text-gray-600
                                                    dark:text-gray-300 whitespace-nowrap">
                      {colLabel(k)}
                    </th>
                  ))}
                  {extraSiteCols.map(k => (
                    <th key={`sa_${k}`} className="text-left px-3 py-2 font-medium text-gray-600
                                                    dark:text-gray-300 whitespace-nowrap">
                      Site: {colLabel(k)}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100 dark:divide-gray-700/50">
                {(detail?.members || []).map(m => (
                  <tr key={m.unique_id}
                      className="hover:bg-gray-50 dark:hover:bg-gray-800/50">
                    <td className="px-3 py-1.5 text-gray-600 dark:text-gray-400 whitespace-nowrap">
                      {(m.item_name ?? parseSeriesId(m.unique_id).item)}@{(m.site_name ?? parseSeriesId(m.unique_id).site)}
                    </td>
                    <td className="px-3 py-1.5 text-gray-800 dark:text-gray-200 max-w-[200px] truncate"
                        title={m.item_name ?? ''}>
                      {m.item_name ?? m.item_code ?? '—'}
                    </td>
                    <td className="px-3 py-1.5 text-gray-800 dark:text-gray-200 max-w-[200px] truncate"
                        title={m.site_name ?? ''}>
                      {m.site_name ?? m.site_code ?? '—'}
                    </td>
                    {extraItemCols.map(k => {
                      const val = m.item_attributes?.[k];
                      return (
                        <td key={`ia_${k}`}
                            className="px-3 py-1.5 text-gray-600 dark:text-gray-400 whitespace-nowrap">
                          {val != null && val !== '' && val !== '[]' ? String(val) : '—'}
                        </td>
                      );
                    })}
                    {extraSiteCols.map(k => {
                      const val = m.site_attributes?.[k];
                      return (
                        <td key={`sa_${k}`}
                            className="px-3 py-1.5 text-gray-600 dark:text-gray-400 whitespace-nowrap">
                          {val != null && val !== '' && val !== '[]' ? String(val) : '—'}
                        </td>
                      );
                    })}
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>

        {/* Footer / Pagination */}
        <div className="flex items-center justify-between px-5 py-3 border-t dark:border-gray-700 shrink-0">
          <span className="text-xs text-gray-500 dark:text-gray-400">
            {detail ? `Showing ${Math.min(page * PAGE_SIZE + 1, detail.total)}–${Math.min((page + 1) * PAGE_SIZE, detail.total)} of ${detail.total.toLocaleString()}` : ''}
          </span>
          <div className="flex gap-2">
            <button
              onClick={() => setPage(p => Math.max(0, p - 1))}
              disabled={page === 0 || loading}
              className="px-3 py-1.5 text-xs rounded-lg border border-gray-300 dark:border-gray-600
                         text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-700
                         disabled:opacity-40"
            >Prev</button>
            <span className="text-xs text-gray-500 dark:text-gray-400 self-center">
              {page + 1} / {totalPages}
            </span>
            <button
              onClick={() => setPage(p => Math.min(totalPages - 1, p + 1))}
              disabled={page >= totalPages - 1 || loading}
              className="px-3 py-1.5 text-xs rounded-lg border border-gray-300 dark:border-gray-600
                         text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-700
                         disabled:opacity-40"
            >Next</button>
            <button
              onClick={onClose}
              className="px-3 py-1.5 text-xs rounded-lg border border-gray-300 dark:border-gray-600
                         text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-700"
            >Close</button>
          </div>
        </div>
      </div>
    </div>
  );
}

// ─── Toast ───────────────────────────────────────────────────────────────────

function Toast({ msg, type, onClose }) {
  useEffect(() => {
    const t = setTimeout(onClose, 4000);
    return () => clearTimeout(t);
  }, [onClose]);

  const colors = type === 'error'
    ? 'bg-red-600 text-white'
    : 'bg-green-600 text-white';

  return (
    <div className={`fixed bottom-6 right-6 z-50 px-5 py-3 rounded-xl shadow-lg
                     flex items-center gap-3 text-sm ${colors}`}>
      <span>{msg}</span>
      <button onClick={onClose} className="opacity-70 hover:opacity-100">✕</button>
    </div>
  );
}

// ─── Main Segments page ───────────────────────────────────────────────────────

export default function Segments() {
  const navigate = useNavigate();
  const [segments, setSegments] = useState([]);
  const [fields, setFields] = useState({ item: [], site: [], item_columns: [], site_columns: [], demand_columns: [] });
  const [loading, setLoading] = useState(true);
  const [editModal, setEditModal] = useState(null);   // null | segment obj | {_isNew:true}
  const [detailModal, setDetailModal] = useState(null); // null | segment obj
  const [runDropdown, setRunDropdown] = useState(null); // segment id
  const [assigning, setAssigning] = useState(null);   // segment id
  const [deleting, setDeleting] = useState(null);
  const [toast, setToast] = useState(null);           // {msg, type}

  // Build allFields from API-detected columns + JSONB attribute keys
  const allFields = buildFieldsFromApi(fields);

  const loadAll = useCallback(async () => {
    setLoading(true);
    try {
      const [segRes, fldRes] = await Promise.all([
        api.get('/segments'),
        api.get('/segments/fields'),
      ]);
      setSegments(segRes.data);
      setFields(fldRes.data);
    } catch (e) {
      setToast({ msg: `Load failed: ${e.response?.data?.detail || e.message}`, type: 'error' });
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { loadAll(); }, [loadAll]);

  async function handleSave(saved) {
    setEditModal(null);
    await loadAll();
    setToast({ msg: `Segment "${saved.name}" saved`, type: 'success' });
  }

  async function handleDelete(seg) {
    if (!window.confirm(`Delete segment "${seg.name}"?`)) return;
    setDeleting(seg.id);
    try {
      await api.delete(`/segments/${seg.id}`);
      setToast({ msg: `Segment "${seg.name}" deleted`, type: 'success' });
      await loadAll();
    } catch (e) {
      setToast({ msg: `Delete failed: ${e.response?.data?.detail || e.message}`, type: 'error' });
    } finally {
      setDeleting(null);
    }
  }

  async function handleAssign(seg) {
    setAssigning(seg.id);
    try {
      const res = await api.post(`/segments/${seg.id}/assign`);
      setToast({ msg: `"${seg.name}": ${res.data.assigned.toLocaleString()} series assigned`, type: 'success' });
      await loadAll();
    } catch (e) {
      setToast({ msg: `Assign failed: ${e.response?.data?.detail || e.message}`, type: 'error' });
    } finally {
      setAssigning(null);
    }
  }

  async function handleRunStep(stepId, seg) {
    try {
      await api.post(`/pipeline/run/${stepId}`, { segment_id: seg.id });
      setToast({
        msg: `${stepId.charAt(0).toUpperCase() + stepId.slice(1)} job started for "${seg.name}"`,
        type: 'success',
      });
      // Navigate to pipeline page
      setTimeout(() => navigate('/pipeline'), 1000);
    } catch (e) {
      setToast({ msg: `Run failed: ${e.response?.data?.detail || e.message}`, type: 'error' });
    }
  }

  return (
    <div className="p-6 max-w-6xl mx-auto">
      {/* Header */}
      <div id="seg-header" className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-gray-900 dark:text-white">Segments</h1>
          <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
            Manage groups of item/site series based on filter criteria.
            Use segments to scope Forecast, Backtest, or Characterization runs.
          </p>
        </div>
        <div className="flex gap-2">
          <button
            onClick={loadAll}
            className="p-2 rounded-lg border border-gray-200 dark:border-gray-700
                       text-gray-500 hover:bg-gray-100 dark:hover:bg-gray-700"
            title="Refresh"
          >🔄</button>
          <button
            onClick={() => setEditModal({ _isNew: true, name: '', description: '', criteria: {} })}
            className="px-4 py-2 rounded-lg bg-blue-600 text-white text-sm
                       hover:bg-blue-700 flex items-center gap-1"
          >+ New Segment</button>
        </div>
      </div>

      {/* Table */}
      <div id="seg-table" className="bg-white dark:bg-gray-900 rounded-2xl border border-gray-200
                      dark:border-gray-700 shadow-sm overflow-visible">
        <table className="w-full text-sm">
          <thead className="bg-gray-50 dark:bg-gray-800 border-b dark:border-gray-700">
            <tr>
              <th className="text-left px-5 py-3 font-medium text-gray-600 dark:text-gray-300">Name</th>
              <th className="text-left px-5 py-3 font-medium text-gray-600 dark:text-gray-300">Description</th>
              <th className="text-right px-5 py-3 font-medium text-gray-600 dark:text-gray-300">Members</th>
              <th className="text-right px-5 py-3 font-medium text-gray-600 dark:text-gray-300">Actions</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100 dark:divide-gray-700/50">
            {loading
              ? <tr><td colSpan={4} className="text-center py-12 text-gray-400">Loading…</td></tr>
              : segments.length === 0
                ? <tr><td colSpan={4} className="text-center py-12 text-gray-400">No segments yet</td></tr>
                : segments.map(seg => (
                  <tr key={seg.id}
                      className="hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors">
                    <td className="px-5 py-3">
                      <div className="flex items-center gap-2">
                        <button
                          onClick={() => setDetailModal(seg)}
                          className="font-medium text-blue-700 dark:text-blue-400 hover:underline
                                     cursor-pointer text-left"
                        >{seg.name}</button>
                        {seg.is_default && (
                          <span className="text-xs px-1.5 py-0.5 rounded bg-gray-100
                                           dark:bg-gray-700 text-gray-500 dark:text-gray-400">
                            default
                          </span>
                        )}
                      </div>
                    </td>
                    <td className="px-5 py-3 text-gray-500 dark:text-gray-400 truncate max-w-xs">
                      {seg.description || '—'}
                    </td>
                    <td className="px-5 py-3 text-right">
                      <span className="font-mono text-gray-700 dark:text-gray-300">
                        {(seg.member_count ?? 0).toLocaleString()}
                      </span>
                    </td>
                    <td className="px-5 py-3">
                      <div className="flex items-center justify-end gap-2 relative">
                        {/* Run dropdown */}
                        <div className="relative">
                          <button
                            onClick={() => setRunDropdown(runDropdown === seg.id ? null : seg.id)}
                            className="px-3 py-1.5 rounded-lg bg-blue-50 dark:bg-blue-900/30
                                       text-blue-700 dark:text-blue-300 border border-blue-200
                                       dark:border-blue-700 hover:bg-blue-100 text-xs font-medium"
                          >Run ▾</button>
                          {runDropdown === seg.id && (
                            <RunDropdown
                              segment={seg}
                              onRun={stepId => handleRunStep(stepId, seg)}
                              onClose={() => setRunDropdown(null)}
                            />
                          )}
                        </div>

                        {!seg.is_default && (
                          <>
                            <button
                              onClick={() => setEditModal(seg)}
                              className="px-3 py-1.5 rounded-lg border border-gray-200
                                         dark:border-gray-700 text-gray-600 dark:text-gray-300
                                         hover:bg-gray-100 dark:hover:bg-gray-700 text-xs"
                            >Edit</button>
                            <button
                              onClick={() => handleAssign(seg)}
                              disabled={assigning === seg.id}
                              className="px-3 py-1.5 rounded-lg border border-green-200
                                         dark:border-green-700 text-green-600 dark:text-green-400
                                         hover:bg-green-50 dark:hover:bg-green-900/30
                                         text-xs disabled:opacity-50"
                            >
                              {assigning === seg.id ? '…' : 'Assign'}
                            </button>
                            <button
                              onClick={() => handleDelete(seg)}
                              disabled={deleting === seg.id}
                              className="px-3 py-1.5 rounded-lg border border-red-200
                                         dark:border-red-800 text-red-500 dark:text-red-400
                                         hover:bg-red-50 dark:hover:bg-red-900/30
                                         text-xs disabled:opacity-50"
                            >
                              {deleting === seg.id ? '…' : '✕'}
                            </button>
                          </>
                        )}
                      </div>
                    </td>
                  </tr>
                ))
            }
          </tbody>
        </table>
      </div>

      {/* Info box */}
      <div id="seg-info" className="mt-4 bg-blue-50 dark:bg-blue-900/20 border border-blue-200
                      dark:border-blue-700 rounded-xl p-4 text-xs text-blue-700
                      dark:text-blue-300 space-y-1">
        <p><strong>Tip:</strong> Run the <strong>Segmentation</strong> pipeline step to refresh ABC
           classification and reassign all segments automatically.</p>
        <p>Use <strong>Assign</strong> to re-evaluate a single segment's membership from its current
           criteria without running the full segmentation step.</p>
      </div>

      {/* Edit modal */}
      {editModal && (
        <EditModal
          segment={editModal}
          onSave={handleSave}
          onClose={() => setEditModal(null)}
          allFields={allFields}
        />
      )}

      {/* Detail modal */}
      {detailModal && (
        <DetailModal
          segment={detailModal}
          onClose={() => setDetailModal(null)}
          allFields={allFields}
        />
      )}

      {/* Toast */}
      {toast && <Toast msg={toast.msg} type={toast.type} onClose={() => setToast(null)} />}
    </div>
  );
}

import { useState, useEffect, useCallback, useRef } from 'react';
import { createPortal } from 'react-dom';
import { useTheme } from '../contexts/ThemeContext';
import { useLocale, LOCALE_PRESETS } from '../contexts/LocaleContext';
import { formatDate, formatNumber, formatDateTime } from '../utils/formatting';
import DateInput from './DateInput';
import axios from 'axios';

const API_BASE_URL = '/api';

const TABS = [
  { id: 'appearance', label: 'Appearance', icon: (
    <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
        d="M7 21a4 4 0 01-4-4V5a2 2 0 012-2h4a2 2 0 012 2v12a4 4 0 01-4 4zm0 0h12a2 2 0 002-2v-4a2 2 0 00-2-2h-2.343M11 7.343l1.657-1.657a2 2 0 012.828 0l2.829 2.829a2 2 0 010 2.828l-8.486 8.485M7 17h.01" />
    </svg>
  )},
  { id: 'locale', label: 'Locale & Formatting', icon: (
    <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
        d="M3.055 11H5a2 2 0 012 2v1a2 2 0 002 2 2 2 0 012 2v2.945M8 3.935V5.5A2.5 2.5 0 0010.5 8h.5a2 2 0 012 2 2 2 0 104 0 2 2 0 012-2h1.064M15 20.488V18a2 2 0 012-2h3.064M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
    </svg>
  )},
  { id: 'config', label: 'System Config', icon: (
    <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
        d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.066 2.573c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.573 1.066c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.066-2.573c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z" />
      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
    </svg>
  )},
];

export default function Settings() {
  const [activeTab, setActiveTab] = useState('appearance');

  return (
    <div className="p-4 sm:p-6 max-w-4xl mx-auto">
      <h1 className="text-2xl sm:text-3xl font-bold mb-6 text-gray-900 dark:text-white">Settings</h1>

      {/* Tab bar - responsive: icons on mobile, full labels on sm+ */}
      <div className="flex gap-1 mb-6 border-b border-gray-200 dark:border-gray-700 overflow-x-auto">
        {TABS.map(tab => (
          <button
            key={tab.id}
            onClick={() => setActiveTab(tab.id)}
            className={`flex items-center gap-2 px-3 sm:px-4 py-2.5 text-sm font-medium border-b-2 transition-colors whitespace-nowrap flex-shrink-0
              ${activeTab === tab.id
                ? 'border-blue-600 text-blue-600 dark:text-blue-400 dark:border-blue-400'
                : 'border-transparent text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300'
              }`}
          >
            {tab.icon}
            <span className="hidden sm:inline">{tab.label}</span>
          </button>
        ))}
      </div>

      {activeTab === 'appearance' && <AppearanceTab />}
      {activeTab === 'locale' && <LocaleTab />}
      {activeTab === 'config' && <ConfigTab />}
    </div>
  );
}

/* ─── Appearance Tab ─── */
function AppearanceTab() {
  const { theme, setTheme } = useTheme();

  const options = [
    { value: 'light', label: 'Light', desc: 'Bright background with dark text',
      icon: (
        <svg className="w-8 h-8" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5}
            d="M12 3v1m0 16v1m9-9h-1M4 12H3m15.364 6.364l-.707-.707M6.343 6.343l-.707-.707m12.728 0l-.707.707M6.343 17.657l-.707.707M16 12a4 4 0 11-8 0 4 4 0 018 0z" />
        </svg>
      ),
      previewBg: 'bg-gray-50', previewBorder: 'border-gray-200',
    },
    { value: 'dark', label: 'Dark', desc: 'Dark background, easier on the eyes',
      icon: (
        <svg className="w-8 h-8" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5}
            d="M20.354 15.354A9 9 0 018.646 3.646 9.003 9.003 0 0012 21a9.003 9.003 0 008.354-5.646z" />
        </svg>
      ),
      previewBg: 'bg-gray-800', previewBorder: 'border-gray-600',
    },
  ];

  return (
    <div className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900/50 p-6">
      <h2 className="text-lg font-semibold mb-2 text-gray-900 dark:text-white">Theme</h2>
      <p className="text-sm text-gray-500 dark:text-gray-400 mb-5">
        Choose your preferred color scheme. Your choice is saved automatically.
      </p>
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
        {options.map(opt => (
          <button
            key={opt.value}
            onClick={() => setTheme(opt.value)}
            className={`flex items-center gap-4 p-4 rounded-xl border-2 transition-all text-left
              ${theme === opt.value
                ? 'border-blue-500 bg-blue-50 dark:bg-blue-900/30 ring-1 ring-blue-500/20'
                : 'border-gray-200 dark:border-gray-600 hover:border-gray-300 dark:hover:border-gray-500'
              }`}
          >
            {/* Mini preview */}
            <div className={`w-16 h-12 rounded-lg ${opt.previewBg} ${opt.previewBorder} border flex items-center justify-center flex-shrink-0`}>
              <span className={opt.value === 'dark' ? 'text-gray-300' : 'text-gray-600'}>{opt.icon}</span>
            </div>
            <div>
              <div className="font-semibold text-sm text-gray-900 dark:text-white">{opt.label}</div>
              <div className="text-xs text-gray-500 dark:text-gray-400 mt-0.5">{opt.desc}</div>
            </div>
            {theme === opt.value && (
              <svg className="w-5 h-5 text-blue-500 ml-auto flex-shrink-0" fill="currentColor" viewBox="0 0 20 20">
                <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" clipRule="evenodd" />
              </svg>
            )}
          </button>
        ))}
      </div>
    </div>
  );
}

/* ─── Locale Tab ─── */
function LocaleTab() {
  const { locale, setLocale, numberDecimals, setNumberDecimals, preset } = useLocale();
  const [testDate, setTestDate] = useState('');

  // Live preview values
  const sampleDate = '2026-02-24';
  const sampleDatetime = '2026-02-24T14:30:45Z';
  const sampleNumber = 1234567.891;
  const sampleSmall = 0.0456;

  return (
    <div className="space-y-6">
      {/* Locale selector */}
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900/50 p-6">
        <h2 className="text-lg font-semibold mb-2 text-gray-900 dark:text-white">Regional Format</h2>
        <p className="text-sm text-gray-500 dark:text-gray-400 mb-4">
          Controls how dates, times, and numbers are displayed throughout the application.
        </p>
        <div className="grid grid-cols-2 sm:grid-cols-3 gap-3">
          {Object.entries(LOCALE_PRESETS).map(([key, p]) => (
            <button
              key={key}
              onClick={() => setLocale(key)}
              className={`text-left p-3 rounded-lg border-2 transition-all
                ${locale === key
                  ? 'border-blue-500 bg-blue-50 dark:bg-blue-900/30 ring-1 ring-blue-500/20'
                  : 'border-gray-200 dark:border-gray-600 hover:border-gray-300 dark:hover:border-gray-500'
                }`}
            >
              <div className="font-medium text-sm text-gray-900 dark:text-white">{p.label}</div>
              <div className="text-xs text-gray-500 dark:text-gray-400 mt-1 font-mono">{p.dateExample}</div>
              <div className="text-[10px] text-gray-400 dark:text-gray-500 mt-0.5">{p.dateFormat}</div>
            </button>
          ))}
        </div>
      </div>

      {/* Number precision */}
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900/50 p-6">
        <h2 className="text-lg font-semibold mb-2 text-gray-900 dark:text-white">Number Precision</h2>
        <p className="text-sm text-gray-500 dark:text-gray-400 mb-3">
          Default number of decimal places for numeric values in tables and charts.
        </p>
        <select
          value={numberDecimals}
          onChange={e => setNumberDecimals(Number(e.target.value))}
          className="border border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-white rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-400"
        >
          {[0, 1, 2, 3, 4].map(n => (
            <option key={n} value={n}>{n} decimal{n !== 1 ? 's' : ''}</option>
          ))}
        </select>
      </div>

      {/* Date input test */}
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900/50 p-6">
        <h2 className="text-lg font-semibold mb-2 text-gray-900 dark:text-white">Date Input Test</h2>
        <p className="text-sm text-gray-500 dark:text-gray-400 mb-3">
          Try typing a date in your regional format ({preset.dateFormat}) to see how it is parsed.
        </p>
        <div className="flex flex-col sm:flex-row items-start sm:items-center gap-3">
          <DateInput
            value={testDate}
            onChange={setTestDate}
            className="w-full sm:w-56"
          />
          {testDate && (
            <div className="text-sm">
              <span className="text-gray-500 dark:text-gray-400">Parsed (ISO):</span>{' '}
              <span className="font-mono text-blue-600 dark:text-blue-400">{testDate}</span>
            </div>
          )}
        </div>
      </div>

      {/* Live preview */}
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900/50 p-6">
        <h2 className="text-lg font-semibold mb-3 text-gray-900 dark:text-white">Formatting Preview</h2>
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-4 text-sm">
          <PreviewRow label="Date" value={formatDate(sampleDate, locale)} />
          <PreviewRow label="Date + Time" value={formatDateTime(sampleDatetime, locale)} />
          <PreviewRow label="Large number" value={formatNumber(sampleNumber, locale, numberDecimals)} />
          <PreviewRow label="Small number" value={formatNumber(sampleSmall, locale, 4)} />
          <PreviewRow label="Integer" value={formatNumber(42195, locale, 0)} />
          <PreviewRow label="Date input format" value={preset.dateFormat} />
        </div>
      </div>
    </div>
  );
}

function PreviewRow({ label, value }) {
  return (
    <div className="flex items-center justify-between gap-2 py-1.5 border-b border-gray-100 dark:border-gray-700 last:border-0">
      <span className="text-gray-500 dark:text-gray-400">{label}</span>
      <span className="font-mono text-gray-900 dark:text-white">{value}</span>
    </div>
  );
}

/* ─── JSON Table Modal (popup for editing arrays / objects as tables) ─── */
function JsonTableModal({ path, value, onSave, onClose }) {
  const isArray = Array.isArray(value);
  const [rows, setRows] = useState(() => {
    if (isArray) return value.map((v, i) => ({ key: String(i), value: v }));
    // object → key-value pairs
    return Object.entries(value).map(([k, v]) => ({ key: k, value: v }));
  });
  const [error, setError] = useState(null);
  const backdropRef = useRef(null);

  // Close on Escape
  useEffect(() => {
    const handler = (e) => { if (e.key === 'Escape') onClose(); };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [onClose]);

  // Detect the dominant scalar type in the existing values
  const guessType = useCallback(() => {
    const vals = rows.map(r => r.value).filter(v => v !== null && v !== undefined && v !== '');
    if (vals.length === 0) return 'string';
    if (vals.every(v => typeof v === 'number')) return 'number';
    if (vals.every(v => typeof v === 'boolean')) return 'boolean';
    return 'string';
  }, [rows]);

  const addRow = () => {
    const type = guessType();
    const defaultVal = type === 'number' ? 0 : type === 'boolean' ? false : '';
    if (isArray) {
      setRows(prev => [...prev, { key: String(prev.length), value: defaultVal }]);
    } else {
      setRows(prev => [...prev, { key: '', value: defaultVal }]);
    }
  };

  const removeRow = (idx) => {
    setRows(prev => {
      const next = prev.filter((_, i) => i !== idx);
      if (isArray) return next.map((r, i) => ({ ...r, key: String(i) }));
      return next;
    });
  };

  const moveRow = (idx, dir) => {
    setRows(prev => {
      const next = [...prev];
      const target = idx + dir;
      if (target < 0 || target >= next.length) return prev;
      [next[idx], next[target]] = [next[target], next[idx]];
      if (isArray) return next.map((r, i) => ({ ...r, key: String(i) }));
      return next;
    });
  };

  const updateRowValue = (idx, newVal) => {
    setRows(prev => prev.map((r, i) => i === idx ? { ...r, value: newVal } : r));
  };

  const updateRowKey = (idx, newKey) => {
    setRows(prev => prev.map((r, i) => i === idx ? { ...r, key: newKey } : r));
  };

  const handleSave = () => {
    setError(null);
    try {
      if (isArray) {
        const result = rows.map(r => r.value);
        onSave(path, result);
      } else {
        // Check for duplicate keys
        const keys = rows.map(r => r.key.trim()).filter(Boolean);
        if (new Set(keys).size !== keys.length) {
          setError('Duplicate keys found');
          return;
        }
        const result = {};
        for (const r of rows) {
          if (r.key.trim()) result[r.key.trim()] = r.value;
        }
        onSave(path, result);
      }
      onClose();
    } catch (e) {
      setError(e.message);
    }
  };

  /** Parse a raw string from the input into the right JS type */
  const parseVal = (raw, currentVal) => {
    if (raw === 'true') return true;
    if (raw === 'false') return false;
    if (raw === 'null') return null;
    // If current value is a number, try to parse as number
    if (typeof currentVal === 'number' || (raw !== '' && !isNaN(Number(raw)))) {
      const n = Number(raw);
      if (!isNaN(n) && raw.trim() !== '') return n;
    }
    return raw;
  };

  const pathLabel = path.split('.').pop();
  const typeLabel = isArray ? `Array [${rows.length}]` : `Object {${rows.length}}`;

  return createPortal(
    <div
      ref={backdropRef}
      onClick={e => { if (e.target === backdropRef.current) onClose(); }}
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm p-4"
    >
      <div className="bg-white dark:bg-gray-800 rounded-xl shadow-2xl dark:shadow-black/40 w-full max-w-lg max-h-[80vh] flex flex-col overflow-hidden border border-gray-200 dark:border-gray-700">
        {/* Header */}
        <div className="flex items-center justify-between px-5 py-4 border-b border-gray-200 dark:border-gray-700">
          <div>
            <h3 className="text-sm font-bold text-gray-900 dark:text-white">{pathLabel}</h3>
            <span className="text-[10px] text-gray-400 dark:text-gray-500 font-mono">{path} &mdash; {typeLabel}</span>
          </div>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-600 dark:hover:text-gray-300 p-1">
            <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>

        {/* Table body */}
        <div className="flex-1 overflow-y-auto px-5 py-3">
          {error && (
            <div className="mb-3 text-xs text-red-600 dark:text-red-400 bg-red-50 dark:bg-red-900/20 rounded px-3 py-2">
              {error}
            </div>
          )}

          <table className="w-full text-xs">
            <thead>
              <tr className="text-left text-gray-400 dark:text-gray-500 border-b border-gray-100 dark:border-gray-700">
                {isArray
                  ? <th className="py-1.5 pr-2 w-8 font-medium">#</th>
                  : <th className="py-1.5 pr-2 font-medium">Key</th>
                }
                <th className="py-1.5 pr-2 font-medium">Value</th>
                <th className="py-1.5 w-20 font-medium text-right">Actions</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row, idx) => (
                <tr key={idx} className="border-b border-gray-50 dark:border-gray-700/40 group">
                  {/* Key / index column */}
                  <td className="py-1.5 pr-2 align-middle">
                    {isArray ? (
                      <span className="text-gray-300 dark:text-gray-600 font-mono tabular-nums">{idx}</span>
                    ) : (
                      <input
                        type="text"
                        value={row.key}
                        onChange={e => updateRowKey(idx, e.target.value)}
                        className="w-full font-mono bg-transparent border-b border-dashed border-gray-200 dark:border-gray-600 text-gray-700 dark:text-gray-300 focus:outline-none focus:border-blue-400 py-0.5"
                        placeholder="key"
                      />
                    )}
                  </td>
                  {/* Value column */}
                  <td className="py-1.5 pr-2 align-middle">
                    {typeof row.value === 'boolean' ? (
                      <button
                        onClick={() => updateRowValue(idx, !row.value)}
                        className={`px-2 py-0.5 rounded text-[10px] font-semibold ${row.value ? 'bg-blue-100 dark:bg-blue-900/40 text-blue-700 dark:text-blue-300' : 'bg-gray-100 dark:bg-gray-700 text-gray-500 dark:text-gray-400'}`}
                      >
                        {String(row.value)}
                      </button>
                    ) : (
                      <input
                        type="text"
                        value={row.value === null ? 'null' : String(row.value)}
                        onChange={e => updateRowValue(idx, parseVal(e.target.value, row.value))}
                        className="w-full font-mono bg-gray-50 dark:bg-gray-700/60 border border-gray-200 dark:border-gray-600 rounded px-2 py-1 text-gray-700 dark:text-gray-300 focus:outline-none focus:ring-1 focus:ring-blue-400"
                      />
                    )}
                  </td>
                  {/* Actions column */}
                  <td className="py-1.5 align-middle">
                    <div className="flex items-center justify-end gap-0.5 opacity-40 group-hover:opacity-100 transition-opacity">
                      {isArray && (
                        <>
                          <button onClick={() => moveRow(idx, -1)} disabled={idx === 0}
                            className="p-0.5 text-gray-400 hover:text-gray-700 dark:hover:text-gray-200 disabled:opacity-20" title="Move up">
                            <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 15l7-7 7 7"/></svg>
                          </button>
                          <button onClick={() => moveRow(idx, 1)} disabled={idx === rows.length - 1}
                            className="p-0.5 text-gray-400 hover:text-gray-700 dark:hover:text-gray-200 disabled:opacity-20" title="Move down">
                            <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7"/></svg>
                          </button>
                        </>
                      )}
                      <button onClick={() => removeRow(idx)}
                        className="p-0.5 text-red-400 hover:text-red-600 dark:hover:text-red-300" title="Remove">
                        <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16"/></svg>
                      </button>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>

          {rows.length === 0 && (
            <div className="py-6 text-center text-gray-400 dark:text-gray-500 text-xs italic">
              Empty &mdash; click &ldquo;Add row&rdquo; to begin
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="flex items-center justify-between px-5 py-3 border-t border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-800/80">
          <button
            onClick={addRow}
            className="flex items-center gap-1 text-xs text-blue-600 dark:text-blue-400 hover:text-blue-800 dark:hover:text-blue-300 font-medium"
          >
            <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 4v16m8-8H4"/></svg>
            Add row
          </button>
          <div className="flex items-center gap-2">
            <button onClick={onClose} className="px-3 py-1.5 text-xs rounded-lg border border-gray-300 dark:border-gray-600 text-gray-600 dark:text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-700">
              Cancel
            </button>
            <button onClick={handleSave} className="px-4 py-1.5 text-xs rounded-lg bg-blue-600 text-white hover:bg-blue-700 font-medium shadow-sm">
              Save
            </button>
          </div>
        </div>
      </div>
    </div>,
    document.body
  );
}


/* ─── Editable parameter field ─── */
function ParamField({ label, path, value, onChange, saving }) {
  const [modalOpen, setModalOpen] = useState(false);
  const isBool = typeof value === 'boolean';
  const isNumber = typeof value === 'number';
  const isNull = value === null || value === undefined;
  const isArray = Array.isArray(value);
  const isObj = !isNull && !isArray && typeof value === 'object';
  const isSensitive = /password|secret|token|account_key/i.test(label);

  if (isSensitive) {
    return (
      <div className="flex items-center justify-between gap-3 py-2 border-b border-gray-100 dark:border-gray-700/50 last:border-0">
        <label className="text-xs text-gray-500 dark:text-gray-400 min-w-0 break-all">{label}</label>
        <span className="text-xs font-mono text-gray-400 dark:text-gray-500 italic">{'********'}</span>
      </div>
    );
  }

  if (isBool) {
    return (
      <div className="flex items-center justify-between gap-3 py-2 border-b border-gray-100 dark:border-gray-700/50 last:border-0">
        <label className="text-xs text-gray-500 dark:text-gray-400 min-w-0 break-all">{label}</label>
        <button
          onClick={() => onChange(path, !value)}
          disabled={saving === path}
          className={`relative w-9 h-5 rounded-full transition-colors flex-shrink-0 ${value ? 'bg-blue-500' : 'bg-gray-300 dark:bg-gray-600'}`}
        >
          <span className={`absolute top-0.5 w-4 h-4 rounded-full bg-white shadow transition-transform ${value ? 'translate-x-4' : 'translate-x-0.5'}`} />
        </button>
      </div>
    );
  }

  if (isArray || isObj) {
    const count = isArray ? value.length : Object.keys(value).length;
    const preview = isArray
      ? value.slice(0, 3).map(v => typeof v === 'string' ? v : JSON.stringify(v)).join(', ') + (value.length > 3 ? ` +${value.length - 3}` : '')
      : Object.entries(value).slice(0, 2).map(([k,v]) => `${k}: ${typeof v === 'string' ? v : JSON.stringify(v)}`).join(', ') + (Object.keys(value).length > 2 ? ' ...' : '');

    return (
      <div className="flex items-center justify-between gap-3 py-2 border-b border-gray-100 dark:border-gray-700/50 last:border-0">
        <label className="text-xs text-gray-500 dark:text-gray-400 min-w-0 break-all">{label}</label>
        <button
          onClick={() => setModalOpen(true)}
          className="flex items-center gap-1.5 text-xs font-mono bg-gray-50 dark:bg-gray-700 border border-gray-200 dark:border-gray-600 rounded px-2 py-1 max-w-[220px] text-left text-gray-600 dark:text-gray-300 hover:border-blue-400 dark:hover:border-blue-500 hover:bg-blue-50 dark:hover:bg-blue-900/20 transition-colors group"
        >
          <span className="text-[10px] font-semibold text-gray-400 dark:text-gray-500 flex-shrink-0">
            {isArray ? `[${count}]` : `{${count}}`}
          </span>
          <span className="truncate">{preview || (isArray ? '[]' : '{}')}</span>
          <svg className="w-3 h-3 text-gray-400 group-hover:text-blue-500 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15.232 5.232l3.536 3.536m-2.036-5.036a2.5 2.5 0 113.536 3.536L6.5 21.036H3v-3.572L16.732 3.732z" />
          </svg>
        </button>
        {modalOpen && (
          <JsonTableModal path={path} value={value} onSave={onChange} onClose={() => setModalOpen(false)} />
        )}
      </div>
    );
  }

  return (
    <div className="flex items-center justify-between gap-3 py-2 border-b border-gray-100 dark:border-gray-700/50 last:border-0">
      <label className="text-xs text-gray-500 dark:text-gray-400 min-w-0 break-all">{label}</label>
      <input
        type={isNumber ? 'number' : 'text'}
        defaultValue={isNull ? '' : String(value)}
        onBlur={e => {
          let v = e.target.value;
          if (isNumber) v = v.includes('.') ? parseFloat(v) : parseInt(v, 10);
          if (v === '' && isNull) return;
          if (v === 'null') v = null;
          onChange(path, v);
        }}
        placeholder={isNull ? 'null' : ''}
        className="text-xs font-mono bg-gray-50 dark:bg-gray-700 border border-gray-200 dark:border-gray-600 rounded px-2 py-1 w-48 text-right text-gray-700 dark:text-gray-300 focus:outline-none focus:ring-1 focus:ring-blue-400"
      />
    </div>
  );
}

/* ─── Flatten nested config into dot-path entries for a section ─── */
function flattenConfig(obj, prefix = '') {
  const entries = [];
  if (!obj || typeof obj !== 'object') return entries;
  for (const [k, v] of Object.entries(obj)) {
    const path = prefix ? `${prefix}.${k}` : k;
    if (v !== null && typeof v === 'object' && !Array.isArray(v)) {
      entries.push(...flattenConfig(v, path));
    } else {
      entries.push({ key: k, path, value: v });
    }
  }
  return entries;
}

/* ─── Config Tab ─── */
function ConfigTab() {
  const [configYaml, setConfigYaml] = useState(null);
  const [configObj, setConfigObj] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [searchTerm, setSearchTerm] = useState('');
  const [expandedSections, setExpandedSections] = useState({});
  const [yamlOpen, setYamlOpen] = useState(false);
  const [saving, setSaving] = useState(null);
  const [saveMsg, setSaveMsg] = useState(null);

  useEffect(() => {
    axios.get(`${API_BASE_URL}/config`)
      .then(res => {
        setConfigYaml(res.data.config_yaml);
        setConfigObj(res.data.config);
        const sections = {};
        Object.keys(res.data.config || {}).forEach(k => {
          if (typeof res.data.config[k] === 'object' && res.data.config[k] !== null) {
            sections[k] = true;
          }
        });
        setExpandedSections(sections);
        setLoading(false);
      })
      .catch(err => {
        setError(err.response?.data?.detail || err.message);
        setLoading(false);
      });
  }, []);

  const toggleSection = (key) => {
    setExpandedSections(prev => ({ ...prev, [key]: !prev[key] }));
  };

  const handleParamChange = async (path, value) => {
    setSaving(path);
    setSaveMsg(null);
    try {
      await axios.put(`${API_BASE_URL}/config`, { path, value });
      const parts = path.split('.');
      setConfigObj(prev => {
        const updated = JSON.parse(JSON.stringify(prev));
        let node = updated;
        for (let i = 0; i < parts.length - 1; i++) {
          if (node[parts[i]] === undefined) node[parts[i]] = {};
          node = node[parts[i]];
        }
        node[parts[parts.length - 1]] = value;
        return updated;
      });
      setSaveMsg({ type: 'ok', text: `${path} updated` });
      setTimeout(() => setSaveMsg(null), 3000);
    } catch (err) {
      setSaveMsg({ type: 'err', text: `Failed: ${err.response?.data?.detail || err.message}` });
    } finally {
      setSaving(null);
    }
  };

  const matchesSearch = (key, entries) => {
    if (!searchTerm) return true;
    const term = searchTerm.toLowerCase();
    if (key.toLowerCase().includes(term)) return true;
    return entries.some(e => e.key.toLowerCase().includes(term) || e.path.toLowerCase().includes(term) || String(e.value).toLowerCase().includes(term));
  };

  return (
    <div className="space-y-4">
      {/* Header */}
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900/50 p-6">
        <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3 mb-1">
          <div>
            <h2 className="text-lg font-semibold text-gray-900 dark:text-white">System Configuration</h2>
            <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
              Edit pipeline parameters below. Changes are saved to <code className="bg-gray-100 dark:bg-gray-700 px-1.5 py-0.5 rounded text-xs">config/config.yaml</code> immediately.
            </p>
          </div>
          {saveMsg && (
            <span className={`self-start text-xs px-2.5 py-1 rounded-full font-medium ${saveMsg.type === 'ok' ? 'bg-emerald-100 dark:bg-emerald-900/30 text-emerald-700 dark:text-emerald-400' : 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400'}`}>
              {saveMsg.text}
            </span>
          )}
        </div>
        <div className="mt-4">
          <input
            type="text"
            value={searchTerm}
            onChange={e => setSearchTerm(e.target.value)}
            placeholder="Filter parameters..."
            className="w-full sm:w-72 border border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-white rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-400"
          />
        </div>
      </div>

      {loading && (
        <div className="flex items-center gap-2 text-gray-400 dark:text-gray-500 py-8 justify-center">
          <svg className="animate-spin h-5 w-5" viewBox="0 0 24 24">
            <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" fill="none" />
            <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
          </svg>
          Loading configuration...
        </div>
      )}
      {error && (
        <div className="text-red-500 dark:text-red-400 bg-red-50 dark:bg-red-900/20 rounded-lg p-4 text-sm">
          Error loading config: {error}
        </div>
      )}

      {/* Editable parameter sections */}
      {configObj && (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
          {Object.entries(configObj).map(([sectionKey, sectionVal]) => {
            if (typeof sectionVal !== 'object' || sectionVal === null) return null;
            const entries = flattenConfig(sectionVal, sectionKey);
            if (!matchesSearch(sectionKey, entries)) return null;
            const isExpanded = expandedSections[sectionKey];
            const filteredEntries = searchTerm
              ? entries.filter(e => e.key.toLowerCase().includes(searchTerm.toLowerCase()) || e.path.toLowerCase().includes(searchTerm.toLowerCase()) || String(e.value).toLowerCase().includes(searchTerm.toLowerCase()))
              : entries;
            const sectionLabel = sectionKey.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());

            return (
              <div key={sectionKey} className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900/50 overflow-hidden">
                <button
                  onClick={() => toggleSection(sectionKey)}
                  className="flex items-center justify-between w-full px-5 py-3.5 text-left hover:bg-gray-50 dark:hover:bg-gray-700/50 transition-colors"
                >
                  <div className="flex items-center gap-3">
                    <span className="text-sm font-semibold text-gray-900 dark:text-white">{sectionLabel}</span>
                    <span className="text-[10px] bg-gray-100 dark:bg-gray-700 text-gray-500 dark:text-gray-400 px-1.5 py-0.5 rounded-full">{filteredEntries.length}</span>
                  </div>
                  <svg className={`w-4 h-4 text-gray-400 transition-transform flex-shrink-0 ${isExpanded ? 'rotate-180' : ''}`}
                    fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                  </svg>
                </button>
                {isExpanded && (
                  <div className="px-5 pb-4 border-t border-gray-100 dark:border-gray-700">
                    {filteredEntries.map(({ key, path, value }) => (
                      <ParamField key={path} label={key} path={path} value={value} onChange={handleParamChange} saving={saving} />
                    ))}
                  </div>
                )}
              </div>
            );
          })}
        </div>
      )}

      {/* Collapsible raw YAML */}
      {configYaml && (
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900/50 overflow-hidden">
          <button
            onClick={() => setYamlOpen(o => !o)}
            className="flex items-center justify-between w-full px-5 py-3.5 text-left hover:bg-gray-50 dark:hover:bg-gray-700/50 transition-colors"
          >
            <div className="flex items-center gap-2">
              <svg className="w-4 h-4 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 20l4-16m4 4l4 4-4 4M6 16l-4-4 4-4" />
              </svg>
              <span className="text-sm font-semibold text-gray-900 dark:text-white">Raw YAML</span>
              <span className="text-[10px] bg-gray-100 dark:bg-gray-700 text-gray-500 dark:text-gray-400 px-1.5 py-0.5 rounded-full">config.yaml</span>
            </div>
            <svg className={`w-4 h-4 text-gray-400 transition-transform flex-shrink-0 ${yamlOpen ? 'rotate-180' : ''}`}
              fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
            </svg>
          </button>
          {yamlOpen && (
            <div className="px-5 pb-5">
              <pre className="bg-gray-900 dark:bg-gray-950 text-gray-300 rounded-lg p-4 text-xs font-mono overflow-x-auto max-h-[600px] overflow-y-auto leading-5 border border-gray-700 dark:border-gray-600 selection:bg-blue-800">
                {configYaml}
              </pre>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

/**
 * useUserPrefs — user-scoped UI preference storage.
 *
 * All keys are namespaced by user.id so that different users who share
 * the same browser each get fully independent UI state (selected items,
 * filters, aggregation level, section order, etc.).
 *
 * Keys are stored as JSON values under `fai_{userId}_{key}` in
 * localStorage. When no user is authenticated the namespace falls back
 * to `fai_anon`.
 *
 * Usage:
 *   const { get, save, saveMany } = useUserPrefs();
 *   const agg = get('displayAgg', 'native');   // restore
 *   save('displayAgg', 'W');                   // persist
 *   saveMany({ sortField: 'mae', sortDir: 'asc' });
 */

import { useCallback } from 'react';
import { useAuth } from '../contexts/AuthContext';

export function useUserPrefs() {
  const { user } = useAuth();
  const prefix = `fai_${user?.id || 'anon'}`;

  /** Read one preference value. Returns defaultVal when not yet stored. */
  const get = useCallback(
    (key, defaultVal = null) => {
      try {
        const raw = localStorage.getItem(`${prefix}_${key}`);
        if (raw === null) return defaultVal;
        return JSON.parse(raw);
      } catch {
        return defaultVal;
      }
    },
    [prefix],
  );

  /** Write one preference value. Pass null/undefined to clear it. */
  const save = useCallback(
    (key, value) => {
      try {
        const fullKey = `${prefix}_${key}`;
        if (value === undefined || value === null) {
          localStorage.removeItem(fullKey);
        } else {
          localStorage.setItem(fullKey, JSON.stringify(value));
        }
      } catch { /* quota exceeded — ignore */ }
    },
    [prefix],
  );

  /** Write multiple preferences at once: { key: value, ... } */
  const saveMany = useCallback(
    (updates) => {
      Object.entries(updates).forEach(([k, v]) => {
        try {
          const fullKey = `${prefix}_${k}`;
          if (v === undefined || v === null) localStorage.removeItem(fullKey);
          else localStorage.setItem(fullKey, JSON.stringify(v));
        } catch { /* ignore */ }
      });
    },
    [prefix],
  );

  return { get, save, saveMany, prefix };
}

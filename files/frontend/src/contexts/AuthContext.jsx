/**
 * Authentication Context
 *
 * Manages JWT auth state: user, login (local + Microsoft + Google), logout.
 *
 * Multi-tenancy additions:
 *   - login() accepts optional account_id for superAdmin tenant selection
 *   - loginWithMicrosoft() / loginWithGoogle() accept optional account_id
 *   - isSuperAdmin  — true when role === 'superadmin'
 *   - currentAccount — { id: string } from the JWT's account_id claim
 */

import { createContext, useContext, useState, useCallback, useEffect } from 'react';
import api from '../utils/api';

const AuthContext = createContext(undefined);

const TOKEN_KEY = 'forecastai_token';
const USER_KEY = 'forecastai_user';

export function AuthProvider({ children }) {
  const [user, setUser] = useState(() => {
    try {
      const stored = localStorage.getItem(USER_KEY);
      return stored ? JSON.parse(stored) : null;
    } catch {
      return null;
    }
  });
  const [loading, setLoading] = useState(true);

  // Validate existing token on mount
  useEffect(() => {
    const token = localStorage.getItem(TOKEN_KEY);
    if (token) {
      api.get('/auth/me')
        .then(res => {
          setUser(res.data);
          localStorage.setItem(USER_KEY, JSON.stringify(res.data));
        })
        .catch(() => {
          localStorage.removeItem(TOKEN_KEY);
          localStorage.removeItem(USER_KEY);
          setUser(null);
        })
        .finally(() => setLoading(false));
    } else {
      setLoading(false);
    }
  }, []);

  /**
   * Local email/password login.
   * Pass account_id (UUID string) when logging in as a superAdmin.
   */
  const login = useCallback(async (email, password, account_id = null) => {
    const body = { email, password };
    if (account_id) body.account_id = account_id;
    const res = await api.post('/auth/login', body);
    const data = res.data;

    // Handle two-phase superAdmin flow: backend returns {status:'select_account'}
    if (data.status === 'select_account') {
      // Let the Login component handle the accounts list — re-throw as a
      // structured error so Login.jsx can read res.data
      const err = new Error('select_account');
      err.response = { data };
      throw err;
    }

    const { access_token, user: userData } = data;
    localStorage.setItem(TOKEN_KEY, access_token);
    localStorage.setItem(USER_KEY, JSON.stringify(userData));
    setUser(userData);
    return userData;
  }, []);

  /**
   * Microsoft OAuth login.
   * Pass account_id when the user has pre-selected a tenant.
   */
  const loginWithMicrosoft = useCallback(async (msalAccessToken, account_id) => {
    const body = { access_token: msalAccessToken };
    if (account_id) body.account_id = account_id;
    const res = await api.post('/auth/microsoft', body);
    const data = res.data;

    // Handle two-phase superAdmin flow: backend returns {status:'select_account'}
    if (data.status === 'select_account') {
      const err = new Error('select_account');
      err.response = { data };
      throw err;
    }

    const { access_token, user: userData } = data;
    localStorage.setItem(TOKEN_KEY, access_token);
    localStorage.setItem(USER_KEY, JSON.stringify(userData));
    setUser(userData);
    return userData;
  }, []);

  /**
   * Google OAuth login.
   * Pass account_id when the user has pre-selected a tenant.
   */
  const loginWithGoogle = useCallback(async (googleCredential, account_id) => {
    const body = { credential: googleCredential };
    if (account_id) body.account_id = account_id;
    const res = await api.post('/auth/google', body);
    const data = res.data;

    // Handle two-phase superAdmin flow
    if (data.status === 'select_account') {
      const err = new Error('select_account');
      err.response = { data };
      throw err;
    }

    const { access_token, user: userData } = data;
    localStorage.setItem(TOKEN_KEY, access_token);
    localStorage.setItem(USER_KEY, JSON.stringify(userData));
    setUser(userData);
    return userData;
  }, []);

  const logout = useCallback(async () => {
    try { await api.post('/auth/logout'); } catch { /* best-effort */ }
    localStorage.removeItem(TOKEN_KEY);
    localStorage.removeItem(USER_KEY);
    setUser(null);
  }, []);

  // ── Computed permission values ──────────────────────────────────────────
  const isSuperAdmin  = user?.role === 'superadmin';
  const isAdmin       = user?.role === 'admin' || isSuperAdmin;
  const isAuthenticated = !!user;
  const canRunProcess   = user?.can_run_process === true || isAdmin;
  const canCreateOverride = user?.can_create_override === true || isAdmin;
  const allowedSegments   = user?.allowed_segments || [];
  const allowedSegmentsEdit = user?.allowed_segments_edit || [];

  // The tenant account this session is scoped to (UUID)
  const currentAccount = user?.account_id ? { id: user.account_id } : null;

  const hasSegmentAccess = (segmentId, edit = false) => {
    if (isAdmin) return true;
    const list = edit ? allowedSegmentsEdit : allowedSegments;
    return list.includes(segmentId);
  };

  return (
    <AuthContext.Provider value={{
      user, loading,
      isAdmin, isSuperAdmin, isAuthenticated,
      canRunProcess, canCreateOverride, hasSegmentAccess,
      currentAccount,
      login, loginWithMicrosoft, loginWithGoogle, logout,
    }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  const ctx = useContext(AuthContext);
  if (ctx === undefined) {
    throw new Error('useAuth must be used within an AuthProvider');
  }
  return ctx;
}

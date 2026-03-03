/**
 * Authentication Context
 *
 * Manages JWT auth state: user, login (local + Microsoft), logout.
 * Follows the same pattern as ThemeContext and LocaleContext.
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

  const login = useCallback(async (email, password) => {
    const res = await api.post('/auth/login', { email, password });
    const { access_token, user: userData } = res.data;
    localStorage.setItem(TOKEN_KEY, access_token);
    localStorage.setItem(USER_KEY, JSON.stringify(userData));
    setUser(userData);
    return userData;
  }, []);

  const loginWithMicrosoft = useCallback(async (msalAccessToken) => {
    const res = await api.post('/auth/microsoft', { access_token: msalAccessToken });
    const { access_token, user: userData } = res.data;
    localStorage.setItem(TOKEN_KEY, access_token);
    localStorage.setItem(USER_KEY, JSON.stringify(userData));
    setUser(userData);
    return userData;
  }, []);

  const loginWithGoogle = useCallback(async (googleCredential) => {
    const res = await api.post('/auth/google', { credential: googleCredential });
    const { access_token, user: userData } = res.data;
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

  const isAdmin = user?.role === 'admin';
  const isAuthenticated = !!user;
  const canRunProcess = user?.can_run_process === true || isAdmin;
  const canCreateOverride = user?.can_create_override === true || isAdmin;
  const allowedSegments = user?.allowed_segments || [];
  const allowedSegmentsEdit = user?.allowed_segments_edit || [];

  const hasSegmentAccess = (segmentId, edit = false) => {
    if (isAdmin) return true;
    const list = edit ? allowedSegmentsEdit : allowedSegments;
    return list.includes(segmentId);
  };

  return (
    <AuthContext.Provider value={{
      user, loading, isAdmin, isAuthenticated,
      canRunProcess, canCreateOverride, hasSegmentAccess,
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

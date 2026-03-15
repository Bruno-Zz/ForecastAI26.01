/**
 * Login Page
 *
 * Three authentication methods:
 *   1. Local email / password
 *   2. Microsoft OAuth (Azure AD) via MSAL popup
 *   3. Google OAuth via Google Identity Services
 *
 * Multi-tenancy: on email blur the component probes the API to check
 * whether the email belongs to a superAdmin.  If so, an account dropdown
 * appears between the password field and the submit button.  Local login
 * then sends the selected account_id with the credentials.
 */

import React, { useState, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import { useAuth } from '../contexts/AuthContext';
import { useTheme } from '../contexts/ThemeContext';
import { PublicClientApplication } from '@azure/msal-browser';
import { msalConfig, loginRequest } from '../config/msalConfig';
import { GoogleLogin } from '@react-oauth/google';
import api from '../utils/api';

let msalInstance = null;

function getMsalInstance() {
  if (!msalInstance) {
    msalInstance = new PublicClientApplication(msalConfig);
  }
  return msalInstance;
}

export default function Login() {
  const { login, loginWithMicrosoft, loginWithGoogle } = useAuth();
  const { isDark, toggleTheme } = useTheme();
  const navigate = useNavigate();

  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const [msLoading, setMsLoading] = useState(false);
  const [googleLoading, setGoogleLoading] = useState(false);

  // SuperAdmin account selection state
  const [isSuperAdmin, setIsSuperAdmin] = useState(false);
  const [accounts, setAccounts] = useState([]);
  const [selectedAccountId, setSelectedAccountId] = useState('');
  const [probing, setProbing] = useState(false);

  // ── Probe: check if email is a superAdmin ──────────────────────────────
  const probeEmail = useCallback(async (emailValue) => {
    if (!emailValue || !emailValue.includes('@')) return;
    setProbing(true);
    try {
      const res = await api.get(`/auth/probe?email=${encodeURIComponent(emailValue)}`);
      const data = res.data;
      if (data.is_superadmin) {
        setIsSuperAdmin(true);
        setAccounts(data.accounts || []);
        if (data.accounts?.length === 1) setSelectedAccountId(data.accounts[0].id);
      } else if (data.accounts?.length > 1) {
        // Regular user with access to multiple accounts
        setIsSuperAdmin(false);
        setAccounts(data.accounts);
        setSelectedAccountId('');
      } else {
        setIsSuperAdmin(false);
        setAccounts([]);
        setSelectedAccountId('');
      }
    } catch {
      // Master DB not configured — silently ignore (single-tenant mode)
      setIsSuperAdmin(false);
    } finally {
      setProbing(false);
    }
  }, []);

  const handleEmailBlur = () => {
    probeEmail(email);
  };

  // ── Local login ────────────────────────────────────────────────────────
  const handleLocalLogin = async (e) => {
    e.preventDefault();
    if (!email || !password) { setError('Please enter email and password'); return; }
    if ((isSuperAdmin || accounts.length > 1) && !selectedAccountId) {
      setError('Please select an account to log into');
      return;
    }
    setLoading(true);
    setError('');
    try {
      await login(email, password, (isSuperAdmin || accounts.length > 1) ? selectedAccountId : null);
      navigate('/');
    } catch (err) {
      const detail = err.response?.data?.detail;
      // Backend may return {status:'select_account'} — handle gracefully
      if (err.response?.data?.status === 'select_account') {
        setAccounts(err.response.data.accounts || []);
        setIsSuperAdmin(true);
        setError('Please select an account from the list');
      } else {
        setError(detail || 'Login failed');
      }
    } finally {
      setLoading(false);
    }
  };

  // ── Microsoft OAuth ────────────────────────────────────────────────────
  const handleMicrosoftLogin = async () => {
    setMsLoading(true);
    setError('');
    try {
      const instance = getMsalInstance();
      await instance.initialize();
      const result = await instance.loginPopup(loginRequest);
      await loginWithMicrosoft(result.accessToken, selectedAccountId || undefined);
      navigate('/');
    } catch (err) {
      if (err?.errorCode === 'user_cancelled') {
        setError('');
      } else if (err.response?.data?.status === 'select_account') {
        // SuperAdmin: MS auth succeeded but no account selected — show dropdown
        setAccounts(err.response.data.accounts || []);
        setIsSuperAdmin(true);
        setSelectedAccountId('');
        setError('Please select an account, then click Sign in with Microsoft again');
      } else {
        setError(err.response?.data?.detail || 'Microsoft sign-in failed');
      }
    } finally {
      setMsLoading(false);
    }
  };

  // ── Google OAuth ───────────────────────────────────────────────────────
  const handleGoogleSuccess = async (credentialResponse) => {
    setGoogleLoading(true);
    setError('');
    try {
      await loginWithGoogle(credentialResponse.credential, selectedAccountId || undefined);
      navigate('/');
    } catch (err) {
      if (err.response?.data?.status === 'select_account') {
        setAccounts(err.response.data.accounts || []);
        setIsSuperAdmin(true);
        setSelectedAccountId('');
        setError('Please select an account, then sign in with Google again');
      } else {
        setError(err.response?.data?.detail || 'Google sign-in failed');
      }
    } finally {
      setGoogleLoading(false);
    }
  };

  const handleGoogleError = () => {
    setError('Google sign-in failed. Please try again.');
  };

  return (
    <div className="min-h-screen flex items-center justify-center bg-gray-100 dark:bg-gray-900 px-4">
      <div className="w-full max-w-md">
        {/* Logo / branding */}
        <div className="text-center mb-8">
          <h1 className="text-3xl font-bold text-gray-900 dark:text-white">
            ForecastAI
          </h1>
          <p className="mt-2 text-gray-600 dark:text-gray-400">
            Sign in to your account
          </p>
        </div>

        {/* Card */}
        <div className="bg-white dark:bg-gray-800 rounded-xl shadow-lg p-8">
          {/* Error */}
          {error && (
            <div className="mb-4 p-3 bg-red-50 dark:bg-red-900/30 border border-red-200 dark:border-red-800 rounded-lg text-red-700 dark:text-red-300 text-sm">
              {error}
            </div>
          )}

          {/* Microsoft sign-in */}
          <button
            onClick={handleMicrosoftLogin}
            disabled={msLoading}
            className="w-full flex items-center justify-center gap-3 px-4 py-3 border border-gray-300 dark:border-gray-600 rounded-lg text-gray-700 dark:text-gray-200 bg-white dark:bg-gray-700 hover:bg-gray-50 dark:hover:bg-gray-600 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {/* Microsoft logo SVG */}
            <svg width="20" height="20" viewBox="0 0 21 21">
              <rect x="1" y="1" width="9" height="9" fill="#f25022" />
              <rect x="11" y="1" width="9" height="9" fill="#7fba00" />
              <rect x="1" y="11" width="9" height="9" fill="#00a4ef" />
              <rect x="11" y="11" width="9" height="9" fill="#ffb900" />
            </svg>
            {msLoading ? 'Signing in...' : 'Sign in with Microsoft'}
          </button>

          {/* Google sign-in */}
          <div className="mt-3 flex justify-center">
            {googleLoading ? (
              <div className="w-full flex items-center justify-center gap-3 px-4 py-3 border border-gray-300 dark:border-gray-600 rounded-lg text-gray-500 dark:text-gray-400 bg-gray-50 dark:bg-gray-700">
                Signing in with Google...
              </div>
            ) : (
              <GoogleLogin
                onSuccess={handleGoogleSuccess}
                onError={handleGoogleError}
                theme={isDark ? 'filled_black' : 'outline'}
                size="large"
                width="400"
                text="signin_with"
              />
            )}
          </div>

          {/* Divider */}
          <div className="relative my-6">
            <div className="absolute inset-0 flex items-center">
              <div className="w-full border-t border-gray-300 dark:border-gray-600" />
            </div>
            <div className="relative flex justify-center text-sm">
              <span className="px-3 bg-white dark:bg-gray-800 text-gray-500 dark:text-gray-400">
                or sign in with email
              </span>
            </div>
          </div>

          {/* Local login form */}
          <form onSubmit={handleLocalLogin} className="space-y-4">
            <div>
              <label htmlFor="email" className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                Email
              </label>
              <input
                id="email"
                type="email"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                onBlur={handleEmailBlur}
                placeholder="admin@forecastai.local"
                className="w-full px-4 py-2.5 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-white placeholder-gray-400 dark:placeholder-gray-500 focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none transition-colors"
                autoComplete="email"
              />
              {probing && (
                <p className="mt-1 text-xs text-gray-400 dark:text-gray-500">Checking account…</p>
              )}
            </div>
            <div>
              <label htmlFor="password" className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                Password
              </label>
              <input
                id="password"
                type="password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                placeholder="Enter your password"
                className="w-full px-4 py-2.5 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-white placeholder-gray-400 dark:placeholder-gray-500 focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none transition-colors"
                autoComplete="current-password"
              />
            </div>

            {/* Account selector — for superAdmins and multi-account users */}
            {accounts.length > 0 && (
              <div>
                <label
                  htmlFor="account"
                  className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
                >
                  <span className="inline-flex items-center gap-1.5">
                    <span className={`inline-block w-2 h-2 rounded-full ${isSuperAdmin ? 'bg-purple-500' : 'bg-blue-500'}`} />
                    Select account
                  </span>
                </label>
                <select
                  id="account"
                  value={selectedAccountId}
                  onChange={(e) => setSelectedAccountId(e.target.value)}
                  className={`w-full px-4 py-2.5 border rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-white outline-none transition-colors focus:ring-2 ${
                    isSuperAdmin
                      ? 'border-purple-300 dark:border-purple-600 focus:ring-purple-500 focus:border-purple-500'
                      : 'border-blue-300 dark:border-blue-600 focus:ring-blue-500 focus:border-blue-500'
                  }`}
                >
                  <option value="">— choose an account —</option>
                  {accounts.map((acc) => (
                    <option key={acc.id} value={acc.id}>
                      {acc.display_name}
                    </option>
                  ))}
                </select>
                <p className={`mt-1 text-xs ${isSuperAdmin ? 'text-purple-600 dark:text-purple-400' : 'text-blue-600 dark:text-blue-400'}`}>
                  {isSuperAdmin ? 'SuperAdmin — you can log into any account' : 'Your account has access to multiple tenants'}
                </p>
              </div>
            )}

            <button
              type="submit"
              disabled={loading}
              className="w-full py-2.5 px-4 bg-blue-600 hover:bg-blue-700 text-white font-medium rounded-lg transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {loading ? 'Signing in...' : 'Sign in'}
            </button>
          </form>
        </div>

        {/* Theme toggle */}
        <div className="mt-4 text-center">
          <button
            onClick={toggleTheme}
            className="text-sm text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-200 transition-colors"
          >
            {isDark ? '☀️ Light mode' : '🌙 Dark mode'}
          </button>
        </div>
      </div>
    </div>
  );
}

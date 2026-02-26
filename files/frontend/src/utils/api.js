/**
 * Shared axios instance with JWT authentication interceptors.
 * All components should import this instead of raw axios.
 */

import axios from 'axios';

const api = axios.create({
  baseURL: '/api',
});

// ── Request interceptor: attach JWT token ──
api.interceptors.request.use((config) => {
  const token = localStorage.getItem('forecastai_token');
  if (token) {
    config.headers.Authorization = `Bearer ${token}`;
  }
  return config;
});

// ── Response interceptor: handle 401 ──
api.interceptors.response.use(
  (response) => response,
  (error) => {
    if (error.response?.status === 401) {
      localStorage.removeItem('forecastai_token');
      localStorage.removeItem('forecastai_user');
      if (window.location.pathname !== '/login') {
        window.location.href = '/login';
      }
    }
    return Promise.reject(error);
  }
);

export default api;

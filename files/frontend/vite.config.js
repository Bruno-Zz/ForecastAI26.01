import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// Plugin: guard against malformed percent-sequences in URLs.
// Vite 5's serveStaticMiddleware calls decodeURI(url.pathname) without a
// try-catch; OAuth libraries (MSAL, Google) can redirect the popup back to
// localhost with encoded state/nonce values that contain invalid UTF-16
// surrogate sequences (e.g. %ED%A0%80), causing an uncaught URIError and the
// HMR overlay to appear.  We sanitise the URL in a leading middleware so that
// Vite never sees the malformed sequence.
const fixMalformedUri = {
  name: 'fix-malformed-uri',
  configureServer(server) {
    server.middlewares.use((req, _res, next) => {
      if (req.url && req.url.includes('%')) {
        try {
          decodeURI(req.url);
        } catch {
          // Re-encode every bare % that is not followed by two hex digits so
          // the rest of the middleware chain receives a well-formed URL.
          req.url = req.url.replace(/%(?![0-9A-Fa-f]{2})/g, '%25');
        }
      }
      next();
    });
  },
};

export default defineConfig({
  plugins: [react(), fixMalformedUri],
  server: {
	allowedHosts: true,
    host: '0.0.0.0',
    port: 5173,
    proxy: {
      '/api': {
        target: 'http://localhost:8003',
        changeOrigin: true,
      },
    },
  },
  build: {
    rollupOptions: {
      output: {
        manualChunks: {
          // Heavy charting lib — cached independently from app code
          'vendor-plotly': ['plotly.js-dist-min', 'react-plotly.js'],
          // React core — rarely changes
          'vendor-react': ['react', 'react-dom', 'react-router-dom'],
        },
      },
    },
  },
});

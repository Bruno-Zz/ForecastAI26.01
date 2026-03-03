import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  server: {
	allowedHosts: true,
    host: '0.0.0.0',
    port: 5173,
    proxy: {
      '/api': {
        target: 'http://localhost:8002',
        changeOrigin: true,
      },
    },
  },
  build: {
    rollupOptions: {
      output: {
        manualChunks: {
          // Heavy charting libs — cached independently from app code
          'vendor-plotly': ['plotly.js-dist-min', 'react-plotly.js'],
          'vendor-vega': ['vega', 'vega-lite', 'react-vega'],
          // React core — rarely changes
          'vendor-react': ['react', 'react-dom', 'react-router-dom'],
        },
      },
    },
  },
});

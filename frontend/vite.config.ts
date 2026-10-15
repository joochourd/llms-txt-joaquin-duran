import { defineConfig } from 'vite';

export default defineConfig({
  server: {
    port: 5173,
    strictPort: true,
    proxy: { '/health': 'http://127.0.0.1:8080', '/api': 'http://127.0.0.1:8080', '/artifacts': 'http://127.0.0.1:8080' },
  },
  build: { sourcemap: false },
});

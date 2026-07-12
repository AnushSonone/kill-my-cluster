import { sveltekit } from '@sveltejs/kit/vite';
import { defineConfig } from 'vite';

export default defineConfig({
  plugins: [sveltekit()],
  server: {
    proxy: {
      // Local dev: browser → Vite → control plane container
      '/api': {
        target: process.env.CONTROLPLANE_URL || 'http://127.0.0.1:8080',
        changeOrigin: true
      }
    }
  }
});

import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

export default defineConfig({
  plugins: [react(), tailwindcss()],
  // The linked @shori/design-system package ships its own copy of react in
  // node_modules; dedupe forces a single React instance so its hook-using
  // components (e.g. NodeCard's split-button menu) don't hit "invalid hook call".
  resolve: {
    dedupe: ['react', 'react-dom'],
  },
  server: {
    proxy: {
      '/api': 'http://localhost:8000',
      // AI workspace UI + API live on the backend (docs/ai-workspace-model.md)
      '/ai': 'http://localhost:8000',
    },
  },
})

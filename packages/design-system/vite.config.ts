import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// Dev-only config for the component gallery (`npm run gallery`). The library
// build is tsup; this only serves gallery/ for visual review and screenshots.
export default defineConfig({
  plugins: [react()],
  server: { port: 5180, strictPort: true },
})

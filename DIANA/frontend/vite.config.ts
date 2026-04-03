import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/process': 'http://127.0.0.1:8000',
      '/transform': 'http://127.0.0.1:8000',
      '/download': 'http://127.0.0.1:8000',
      '/data-quality': 'http://127.0.0.1:8000',
      '/data-summary': 'http://127.0.0.1:8000',
      '/dashboard-data': 'http://127.0.0.1:8000',
      '/viz-suggestions': 'http://127.0.0.1:8000',
      '/chat': 'http://127.0.0.1:8000',
      '/generate-report': 'http://127.0.0.1:8000',
      '/export': 'http://127.0.0.1:8000',
    },
  },
})

import { defineConfig } from 'vite'
import solid from 'vite-plugin-solid'
import wasm from 'vite-plugin-wasm'

export default defineConfig({
  plugins: [solid(), wasm()],
  resolve: {
    alias: {
      rolldown: 'rollup',
    },
  },
  optimizeDeps: {
    exclude: ['engine'],
  },
})

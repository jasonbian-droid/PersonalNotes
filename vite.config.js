import { defineConfig } from 'vite'
import { resolve } from 'path'

export default defineConfig({
  base: '/PersonalNotes/',
  build: {
    rollupOptions: {
      input: {
        main:              resolve(__dirname, 'index.html'),
        emailFlow:         resolve(__dirname, 'email-flow.html'),
        eventApiActions:   resolve(__dirname, 'event-api-actions.html'),
        eventsReconMatrix: resolve(__dirname, 'events-recon-matrix.html'),
        nullTimeseries:    resolve(__dirname, 'null-timeseries.html'),
        step2Coverage:     resolve(__dirname, 'step2-coverage-audit.html'),
        journeys:          resolve(__dirname, 'journeys/index.html'),
        interactions:      resolve(__dirname, 'interactions/index.html'),
        nullEmails:        resolve(__dirname, 'null-emails/index.html'),
      },
    },
  },
})

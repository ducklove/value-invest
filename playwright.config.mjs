import { defineConfig } from '@playwright/test';

export default defineConfig({
  testDir: './tests/browser',
  timeout: 30_000,
  workers: 1,
  use: { baseURL: 'http://127.0.0.1:18765', trace: 'retain-on-failure', viewport: { width: 1440, height: 1000 } },
  webServer: {
    command: `${process.env.E2E_PYTHON || 'python'} -m uvicorn tests.browser.server:app --host 127.0.0.1 --port 18765`,
    url: 'http://127.0.0.1:18765/healthz',
    reuseExistingServer: false,
    timeout: 30_000,
  },
});

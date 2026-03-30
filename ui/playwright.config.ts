import { defineConfig, devices } from "@playwright/test";

export default defineConfig({
  testDir: "./tests/e2e",
  outputDir: "./tests/results",
  fullyParallel: false,
  retries: 1,
  workers: 1,
  reporter: "list",
  timeout: 30_000,
  use: {
    baseURL: "http://localhost:4173",
    trace: "on-first-retry",
    screenshot: "off",
  },
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],
  webServer: {
    command: "npm run preview -- --port 4173",
    port: 4173,
    reuseExistingServer: !process.env.CI,
    timeout: 10_000,
  },
});

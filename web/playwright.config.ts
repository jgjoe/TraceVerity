import path from "node:path";

import { defineConfig, devices } from "@playwright/test";

/**
 * The end-to-end workspace is local and gitignored. It is deliberately outside
 * the normal local workspace so repeated runs never touch the developer's own
 * dataset registry or import area.
 */
const workspace = path.join(import.meta.dirname, "..", "data", "e2e");

export default defineConfig({
  testDir: "./e2e",
  fullyParallel: false,
  retries: 0,
  reporter: "line",
  use: {
    baseURL: "http://127.0.0.1:8766",
    trace: "retain-on-failure",
  },
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],
  webServer: {
    command:
      "uv run --project .. piw-web --port 8766" +
      ` --registry-root "${path.join(workspace, "registry")}"` +
      ` --upload-root "${path.join(workspace, "imports")}"`,
    url: "http://127.0.0.1:8766/api/health",
    reuseExistingServer: false,
    timeout: 120_000,
  },
});

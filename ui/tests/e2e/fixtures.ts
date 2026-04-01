import { test as base, type Page, type Route } from "@playwright/test";

function hoursAgo(h: number): string {
  return new Date(Date.now() - h * 3600_000).toISOString();
}

const sampleCycles = [
  {
    cycle_id: 1,
    source: "pg-main",
    query: "users_sync",
    status: "success",
    started_at: hoursAgo(2),
    finished_at: hoursAgo(1.99),
    rows_created: 50,
    rows_updated: 10,
    rows_deleted: 2,
    duration_ms: 340,
    error: null,
  },
  {
    cycle_id: 2,
    source: "pg-main",
    query: "orders_sync",
    status: "success",
    started_at: hoursAgo(1.5),
    finished_at: hoursAgo(1.49),
    rows_created: 120,
    rows_updated: 30,
    rows_deleted: 0,
    duration_ms: 520,
    error: null,
  },
  {
    cycle_id: 3,
    source: "http-api",
    query: "events_pull",
    status: "failed",
    started_at: hoursAgo(1),
    finished_at: hoursAgo(0.99),
    rows_created: 0,
    rows_updated: 0,
    rows_deleted: 0,
    duration_ms: 1200,
    error: "Connection timeout",
  },
  {
    cycle_id: 4,
    source: "pg-main",
    query: "users_sync",
    status: "success",
    started_at: hoursAgo(0.5),
    finished_at: hoursAgo(0.49),
    rows_created: 15,
    rows_updated: 5,
    rows_deleted: 1,
    duration_ms: 290,
    error: null,
  },
];

const sampleSources = [
  {
    name: "pg-main",
    connector: "postgres",
    interval_secs: 60,
    queries: [
      { id: "users_sync", key_column: "id" },
      { id: "orders_sync", key_column: "order_id" },
    ],
    status: { last_cycle: sampleCycles[3], total_cycles: 42 },
  },
  {
    name: "http-api",
    connector: "http",
    interval_secs: 120,
    queries: [{ id: "events_pull", key_column: "event_id" }],
    status: { last_cycle: sampleCycles[2], total_cycles: 15 },
  },
];

const sampleSinks = [
  {
    name: "kafka-prod",
    sink_type: "kafka",
    config: { brokers: "localhost:9092", topic: "oversync-events" },
  },
  {
    name: "stdout-debug",
    sink_type: "stdout",
    config: { pretty: true },
  },
];

function getApiPath(url: string): string {
  const u = new URL(url);
  return u.pathname.replace(/^\/api/, "");
}

export async function setupMockApi(page: Page) {
  let sources = JSON.parse(JSON.stringify(sampleSources));
  let sinks = JSON.parse(JSON.stringify(sampleSinks));
  let paused = false;

  // Single catch-all route for /api/*
  await page.route("**/api/**", async (route: Route) => {
    const method = route.request().method();
    const path = getApiPath(route.request().url());

    // Health
    if (path === "/health" && method === "GET") {
      return route.fulfill({ json: { status: "ok", version: "0.1.0" } });
    }

    // Sync status
    if (path === "/sync/status" && method === "GET") {
      return route.fulfill({ json: { running: !paused, paused } });
    }
    if (path === "/sync/pause" && method === "POST") {
      paused = true;
      return route.fulfill({ json: { ok: true, message: "Syncing paused" } });
    }
    if (path === "/sync/resume" && method === "POST") {
      paused = false;
      return route.fulfill({ json: { ok: true, message: "Syncing resumed" } });
    }

    // History
    if (path === "/history" && method === "GET") {
      return route.fulfill({ json: { cycles: sampleCycles } });
    }

    // Sources trigger (must be before sources/:name)
    const triggerMatch = path.match(/^\/sources\/([^/]+)\/trigger$/);
    if (triggerMatch && method === "POST") {
      const name = decodeURIComponent(triggerMatch[1]);
      return route.fulfill({
        json: { source: name, message: `Sync triggered for "${name}"` },
      });
    }

    // Sources list
    if (path === "/sources" && method === "GET") {
      return route.fulfill({ json: { sources } });
    }
    if (path === "/sources" && method === "POST") {
      const body = route.request().postDataJSON();
      sources.push({
        name: body.name,
        connector: body.connector,
        interval_secs: 60,
        queries: body.queries ?? [],
        status: { last_cycle: null, total_cycles: 0 },
      });
      return route.fulfill({
        json: { ok: true, message: `Source "${body.name}" created` },
      });
    }

    // Sources single
    const sourceMatch = path.match(/^\/sources\/([^/]+)$/);
    if (sourceMatch) {
      const name = decodeURIComponent(sourceMatch[1]);

      if (method === "GET") {
        const source = sources.find((s: { name: string }) => s.name === name);
        if (!source) return route.fulfill({ status: 404, json: { error: "Not found" } });
        return route.fulfill({ json: source });
      }
      if (method === "PUT") {
        const idx = sources.findIndex((s: { name: string }) => s.name === name);
        if (idx === -1) return route.fulfill({ status: 404, json: { error: "Not found" } });
        const body = route.request().postDataJSON();
        if (body.connector) sources[idx].connector = body.connector;
        return route.fulfill({
          json: { ok: true, message: `Source "${name}" updated` },
        });
      }
      if (method === "DELETE") {
        sources = sources.filter((s: { name: string }) => s.name !== name);
        return route.fulfill({
          json: { ok: true, message: `Source "${name}" deleted` },
        });
      }
    }

    // Sinks list
    if (path === "/sinks" && method === "GET") {
      return route.fulfill({ json: { sinks } });
    }
    if (path === "/sinks" && method === "POST") {
      const body = route.request().postDataJSON();
      sinks.push({
        name: body.name,
        sink_type: body.sink_type,
        config: body.config ?? {},
      });
      return route.fulfill({
        json: { ok: true, message: `Sink "${body.name}" created` },
      });
    }

    // Sinks single
    const sinkMatch = path.match(/^\/sinks\/([^/]+)$/);
    if (sinkMatch) {
      const name = decodeURIComponent(sinkMatch[1]);

      if (method === "PUT") {
        return route.fulfill({
          json: { ok: true, message: `Sink "${name}" updated` },
        });
      }
      if (method === "DELETE") {
        sinks = sinks.filter((s: { name: string }) => s.name !== name);
        return route.fulfill({
          json: { ok: true, message: `Sink "${name}" deleted` },
        });
      }
    }

    // Fallback
    return route.fulfill({ status: 404, json: { error: "Not found" } });
  });
}

export const test = base.extend<{ mockApi: void }>({
  mockApi: async ({ page }, use) => {
    await setupMockApi(page);
    await use();
  },
});

export { expect } from "@playwright/test";

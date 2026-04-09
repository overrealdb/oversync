import { test as base, type Page, type Route } from "@playwright/test";

function hoursAgo(hours: number): string {
  return new Date(Date.now() - hours * 3600_000).toISOString();
}

const sampleCycles = [
  {
    cycle_id: 1,
    source: "catalog-sync",
    query: "catalog.public.customers",
    status: "success",
    started_at: hoursAgo(2),
    finished_at: hoursAgo(1.98),
    rows_created: 50,
    rows_updated: 10,
    rows_deleted: 2,
    duration_ms: 680,
    error: null,
  },
  {
    cycle_id: 2,
    source: "catalog-sync",
    query: "catalog.public.orders",
    status: "success",
    started_at: hoursAgo(1.5),
    finished_at: hoursAgo(1.47),
    rows_created: 120,
    rows_updated: 30,
    rows_deleted: 0,
    duration_ms: 910,
    error: null,
  },
];

const sampleQueries = [
  {
    id: "catalog.public.customers",
    sql: "select id, email from public.customers",
    key_column: "id",
  },
  {
    id: "catalog.public.orders",
    sql: "select id, customer_id, total_amount from public.orders",
    key_column: "id",
  },
];

const samplePipes = [
  {
    name: "catalog-sync",
    origin_connector: "postgres",
    origin_dsn: "postgres://readonly@pg-main:5432/catalog",
    targets: ["kafka-prod"],
    interval_secs: 60,
    query_count: 2,
    recipe: {
      type: "postgres_snapshot",
      prefix: "catalog",
      schema_id: "table",
      schemas: ["public"],
    },
    enabled: true,
  },
];

const samplePresets = [
  {
    name: "generic-postgres-snapshot",
    description: "Snapshot-style PostgreSQL onboarding",
    spec: {
      origin_connector: "postgres",
      origin_dsn: "{{dsn}}",
      origin_config: {},
      parameters: [
        { name: "dsn", required: true, secret: true },
        { name: "source_name", required: true, default: "catalog" },
      ],
      targets: ["kafka-prod"],
      queries: [],
      schedule: { interval_secs: 300, missed_tick_policy: "skip" },
      delta: { diff_mode: "db", fail_safe_threshold: 30 },
      retry: { max_retries: 3, retry_base_delay_secs: 5 },
      recipe: { type: "postgres_snapshot", prefix: "{{source_name}}", schema_id: "table", schemas: ["public"] },
      filters: [],
      transforms: [],
      links: [],
    },
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
  const parsed = new URL(url);
  return parsed.pathname.replace(/^\/api/, "");
}

function buildRuntimePipe(pipe: (typeof samplePipes)[number]) {
  return {
    pipe: {
      name: pipe.name,
      origin: {
        connector: pipe.origin_connector,
        dsn: pipe.origin_dsn,
        credential: null,
        trino_url: null,
        config: {},
      },
      targets: pipe.targets,
      queries: [],
      schedule: { interval_secs: pipe.interval_secs, missed_tick_policy: "skip" },
      delta: { diff_mode: "db", fail_safe_threshold: 30 },
      retry: { max_retries: 3, retry_base_delay_secs: 5 },
      recipe: pipe.recipe,
      filters: [],
      transforms: [],
      links: [],
      alert_webhook: null,
      enabled: pipe.enabled,
    },
    effective_queries: sampleQueries,
  };
}

export async function setupMockApi(page: Page) {
  let pipes = JSON.parse(JSON.stringify(samplePipes));
  let presets = JSON.parse(JSON.stringify(samplePresets));
  let sinks = JSON.parse(JSON.stringify(sampleSinks));
  let paused = false;

  await page.route("**/api/**", async (route: Route) => {
    const method = route.request().method();
    const path = getApiPath(route.request().url());

    if (path === "/health" && method === "GET") {
      return route.fulfill({ json: { status: "ok", version: "0.1.0" } });
    }

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

    if (path === "/history" && method === "GET") {
      return route.fulfill({ json: { cycles: sampleCycles } });
    }

    if (path === "/pipes" && method === "GET") {
      return route.fulfill({ json: { pipes } });
    }
    if (path === "/pipes" && method === "POST") {
      const body = route.request().postDataJSON();
      pipes.push({
        name: body.name,
        origin_connector: body.origin_connector,
        origin_dsn: body.origin_dsn,
        targets: body.targets ?? [],
        interval_secs: body.schedule?.interval_secs ?? 300,
        query_count: body.queries?.length ?? 0,
        recipe: body.recipe ?? null,
        enabled: true,
      });
      return route.fulfill({
        json: { ok: true, message: `Pipe "${body.name}" created` },
      });
    }

    if (path === "/pipes/dry-run" && method === "POST") {
      return route.fulfill({
        json: {
          mode: "live",
          pipe_name: "catalog-sync",
          query_id: "catalog.public.customers",
          source_row_count: 3,
          rows_examined: 3,
          delta: { created: 1, updated: 0, deleted: 0, unchanged: 2 },
          preview: {
            created: [{ id: "cust-101", email: "one@example.com" }],
            updated: [],
            deleted: [],
          },
          warnings: [],
        },
      });
    }

    const pipeResolveMatch = path.match(/^\/pipes\/([^/]+)\/resolve$/);
    if (pipeResolveMatch && method === "GET") {
      const name = decodeURIComponent(pipeResolveMatch[1]);
      const pipe = pipes.find((item: { name: string }) => item.name === name);
      if (!pipe) return route.fulfill({ status: 404, json: { error: "Not found" } });
      return route.fulfill({ json: buildRuntimePipe(pipe) });
    }

    const pipeMatch = path.match(/^\/pipes\/([^/]+)$/);
    if (pipeMatch) {
      const name = decodeURIComponent(pipeMatch[1]);

      if (method === "GET") {
        const pipe = pipes.find((item: { name: string }) => item.name === name);
        if (!pipe) return route.fulfill({ status: 404, json: { error: "Not found" } });
        return route.fulfill({ json: pipe });
      }

      if (method === "PUT") {
        const idx = pipes.findIndex((item: { name: string }) => item.name === name);
        if (idx === -1) return route.fulfill({ status: 404, json: { error: "Not found" } });
        const body = route.request().postDataJSON();
        pipes[idx] = {
          ...pipes[idx],
          origin_connector: body.origin_connector ?? pipes[idx].origin_connector,
          origin_dsn: body.origin_dsn ?? pipes[idx].origin_dsn,
          targets: body.targets ?? pipes[idx].targets,
          interval_secs: body.schedule?.interval_secs ?? pipes[idx].interval_secs,
          query_count: body.queries?.length ?? pipes[idx].query_count,
          recipe: body.recipe === undefined ? pipes[idx].recipe : body.recipe,
          enabled: body.enabled ?? pipes[idx].enabled,
        };
        return route.fulfill({
          json: { ok: true, message: `Pipe "${name}" updated` },
        });
      }

      if (method === "DELETE") {
        pipes = pipes.filter((item: { name: string }) => item.name !== name);
        return route.fulfill({
          json: { ok: true, message: `Pipe "${name}" deleted` },
        });
      }
    }

    if (path === "/pipe-presets" && method === "GET") {
      return route.fulfill({ json: { presets } });
    }
    if (path === "/pipe-presets" && method === "POST") {
      const body = route.request().postDataJSON();
      presets.push(body);
      return route.fulfill({
        json: { ok: true, message: `Saved recipe "${body.name}" created` },
      });
    }

    const presetMatch = path.match(/^\/pipe-presets\/([^/]+)$/);
    if (presetMatch) {
      const name = decodeURIComponent(presetMatch[1]);

      if (method === "GET") {
        const preset = presets.find((item: { name: string }) => item.name === name);
        if (!preset) return route.fulfill({ status: 404, json: { error: "Not found" } });
        return route.fulfill({ json: preset });
      }

      if (method === "PUT") {
        const idx = presets.findIndex((item: { name: string }) => item.name === name);
        if (idx === -1) return route.fulfill({ status: 404, json: { error: "Not found" } });
        const body = route.request().postDataJSON();
        presets[idx] = {
          ...presets[idx],
          description: body.description ?? presets[idx].description,
          spec: body.spec ?? presets[idx].spec,
        };
        return route.fulfill({
          json: { ok: true, message: `Saved recipe "${name}" updated` },
        });
      }

      if (method === "DELETE") {
        presets = presets.filter((item: { name: string }) => item.name !== name);
        return route.fulfill({
          json: { ok: true, message: `Saved recipe "${name}" deleted` },
        });
      }
    }

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

    const sinkMatch = path.match(/^\/sinks\/([^/]+)$/);
    if (sinkMatch) {
      const name = decodeURIComponent(sinkMatch[1]);

      if (method === "PUT") {
        return route.fulfill({
          json: { ok: true, message: `Sink "${name}" updated` },
        });
      }
      if (method === "DELETE") {
        sinks = sinks.filter((item: { name: string }) => item.name !== name);
        return route.fulfill({
          json: { ok: true, message: `Sink "${name}" deleted` },
        });
      }
    }

    if (path === "/config/export" && method === "GET") {
      return route.fulfill({
        json: {
          format: "toml",
          content: "[[pipes]]\nname = \"catalog-sync\"\n",
        },
      });
    }
    if (path === "/config/import" && method === "POST") {
      return route.fulfill({
        json: { ok: true, message: "Config imported", warnings: [] },
      });
    }

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

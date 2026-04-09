import { http, HttpResponse } from "msw";

import type {
  CycleInfo,
  HistoryResponse,
  MutationResponse,
  PipeInfo,
  PipePresetInfo,
  PipePresetListResponse,
  PipeQueryDefinition,
  ResolvePipeResponse,
  SinkInfo,
  SinkListResponse,
  StatusResponse,
} from "@/types/api";

const now = new Date();

function hoursAgo(hours: number): string {
  return new Date(now.getTime() - hours * 3600_000).toISOString();
}

const sampleCycles: CycleInfo[] = [
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
  {
    cycle_id: 3,
    source: "audit-snapshot",
    query: "audit.public.events",
    status: "failed",
    started_at: hoursAgo(1),
    finished_at: hoursAgo(0.97),
    rows_created: 0,
    rows_updated: 0,
    rows_deleted: 0,
    duration_ms: 1200,
    error: "Connection timeout",
  },
];

const samplePipeQueries: PipeQueryDefinition[] = [
  {
    id: "catalog.public.customers",
    sql: "select id, email, updated_at from public.customers",
    key_column: "id",
  },
  {
    id: "catalog.public.orders",
    sql: "select id, customer_id, total_amount from public.orders",
    key_column: "id",
  },
];

const samplePipes: PipeInfo[] = [
  {
    name: "catalog-sync",
    origin_connector: "postgres",
    origin_dsn: "postgres://readonly@pg-main:5432/catalog",
    targets: ["kafka-prod"],
    interval_secs: 60,
    query_count: 2,
    recipe: { type: "postgres_snapshot", prefix: "catalog", schema_id: "table", schemas: ["public"] },
    enabled: true,
  },
  {
    name: "audit-snapshot",
    origin_connector: "postgres",
    origin_dsn: "postgres://readonly@pg-audit:5432/audit",
    targets: ["stdout-debug"],
    interval_secs: 300,
    query_count: 1,
    recipe: null,
    enabled: false,
  },
];

const samplePresets: PipePresetInfo[] = [
  {
    name: "generic-postgres-snapshot",
    description: "Snapshot-style PostgreSQL onboarding",
    spec: {
      origin_connector: "postgres",
      origin_dsn: "{{dsn}}",
      origin_config: {},
      parameters: [
        {
          name: "dsn",
          label: "DSN",
          description: "Source PostgreSQL DSN",
          required: true,
          secret: true,
        },
        {
          name: "source_name",
          label: "Source Name",
          description: "Prefix for generated query ids",
          required: true,
          secret: false,
          default: "catalog",
        },
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

const sampleSinks: SinkInfo[] = [
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

function buildRuntimePipe(pipe: PipeInfo) {
  const effectiveQueries =
    pipe.name === "catalog-sync"
      ? samplePipeQueries
      : [
          {
            id: "audit.public.events",
            sql: "select id, event_type, created_at from public.events",
            key_column: "id",
          },
        ];

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
      queries: pipe.name === "catalog-sync" ? [] : effectiveQueries,
      schedule: { interval_secs: pipe.interval_secs, missed_tick_policy: "skip" },
      delta: { diff_mode: "db", fail_safe_threshold: 30 },
      retry: { max_retries: 3, retry_base_delay_secs: 5 },
      recipe: pipe.recipe ?? null,
      filters: [],
      transforms: [],
      links: [],
      alert_webhook: null,
      enabled: pipe.enabled,
    },
    effective_queries: effectiveQueries,
  } satisfies ResolvePipeResponse;
}

let syncPaused = false;
let pipes = [...samplePipes];
let presets = [...samplePresets];
let sinks = [...sampleSinks];

export function resetMockState() {
  pipes = [...samplePipes];
  presets = [...samplePresets];
  sinks = [...sampleSinks];
  syncPaused = false;
}

export const handlers = [
  http.get("/api/health", () => {
    return HttpResponse.json({ status: "ok", version: "0.1.0" });
  }),

  http.get("/api/pipes", () => {
    return HttpResponse.json({ pipes });
  }),

  http.get("/api/pipes/:name", ({ params }) => {
    const pipe = pipes.find((item) => item.name === params.name);
    if (!pipe) return HttpResponse.json({ error: "Not found" }, { status: 404 });
    return HttpResponse.json(pipe);
  }),

  http.get("/api/pipes/:name/resolve", ({ params }) => {
    const pipe = pipes.find((item) => item.name === params.name);
    if (!pipe) return HttpResponse.json({ error: "Not found" }, { status: 404 });
    return HttpResponse.json(buildRuntimePipe(pipe));
  }),

  http.post("/api/pipes", async ({ request }) => {
    const body = (await request.json()) as {
      name: string;
      origin_connector: string;
      origin_dsn: string;
      targets?: string[];
      schedule?: { interval_secs?: number };
      recipe?: PipeInfo["recipe"];
      queries?: PipeQueryDefinition[];
    };
    const newPipe: PipeInfo = {
      name: body.name,
      origin_connector: body.origin_connector,
      origin_dsn: body.origin_dsn,
      targets: body.targets ?? [],
      interval_secs: body.schedule?.interval_secs ?? 300,
      query_count: body.queries?.length ?? 0,
      recipe: body.recipe ?? null,
      enabled: true,
    };
    pipes.push(newPipe);
    return HttpResponse.json({
      ok: true,
      message: `Pipe "${body.name}" created`,
    } satisfies MutationResponse);
  }),

  http.put("/api/pipes/:name", async ({ params, request }) => {
    const idx = pipes.findIndex((item) => item.name === params.name);
    if (idx === -1) return HttpResponse.json({ error: "Not found" }, { status: 404 });
    const body = (await request.json()) as Partial<PipeInfo> & {
      queries?: PipeQueryDefinition[];
    };
    pipes[idx] = {
      ...pipes[idx],
      origin_connector: body.origin_connector ?? pipes[idx].origin_connector,
      origin_dsn: body.origin_dsn ?? pipes[idx].origin_dsn,
      targets: body.targets ?? pipes[idx].targets,
      interval_secs: body.interval_secs ?? pipes[idx].interval_secs,
      recipe: body.recipe === undefined ? pipes[idx].recipe : body.recipe,
      enabled: body.enabled ?? pipes[idx].enabled,
      query_count: body.queries?.length ?? pipes[idx].query_count,
    };
    return HttpResponse.json({
      ok: true,
      message: `Pipe "${params.name}" updated`,
    } satisfies MutationResponse);
  }),

  http.delete("/api/pipes/:name", ({ params }) => {
    const idx = pipes.findIndex((item) => item.name === params.name);
    if (idx === -1) return HttpResponse.json({ error: "Not found" }, { status: 404 });
    pipes.splice(idx, 1);
    return HttpResponse.json({
      ok: true,
      message: `Pipe "${params.name}" deleted`,
    } satisfies MutationResponse);
  }),

  http.post("/api/pipes/dry-run", async ({ request }) => {
    const body = (await request.json()) as { pipe?: { name?: string } | null };
    return HttpResponse.json({
      mode: "live",
      pipe_name: body.pipe?.name ?? "ad-hoc",
      query_id: "catalog.public.customers",
      source_row_count: 3,
      rows_examined: 3,
      delta: {
        created: 1,
        updated: 0,
        deleted: 0,
        unchanged: 2,
      },
      preview: {
        created: [{ id: "cust-101", email: "one@example.com" }],
        updated: [],
        deleted: [],
      },
      warnings: [],
    });
  }),

  http.get("/api/pipe-presets", () => {
    return HttpResponse.json({ presets } satisfies PipePresetListResponse);
  }),

  http.get("/api/pipe-presets/:name", ({ params }) => {
    const preset = presets.find((item) => item.name === params.name);
    if (!preset) return HttpResponse.json({ error: "Not found" }, { status: 404 });
    return HttpResponse.json(preset);
  }),

  http.post("/api/pipe-presets", async ({ request }) => {
    const body = (await request.json()) as PipePresetInfo;
    presets.push(body);
    return HttpResponse.json({
      ok: true,
      message: `Saved recipe "${body.name}" created`,
    } satisfies MutationResponse);
  }),

  http.put("/api/pipe-presets/:name", async ({ params, request }) => {
    const idx = presets.findIndex((item) => item.name === params.name);
    if (idx === -1) return HttpResponse.json({ error: "Not found" }, { status: 404 });
    const body = (await request.json()) as Partial<PipePresetInfo>;
    presets[idx] = {
      ...presets[idx],
      description: body.description ?? presets[idx].description,
      spec: body.spec ?? presets[idx].spec,
    };
    return HttpResponse.json({
      ok: true,
      message: `Saved recipe "${params.name}" updated`,
    } satisfies MutationResponse);
  }),

  http.delete("/api/pipe-presets/:name", ({ params }) => {
    const idx = presets.findIndex((item) => item.name === params.name);
    if (idx === -1) return HttpResponse.json({ error: "Not found" }, { status: 404 });
    presets.splice(idx, 1);
    return HttpResponse.json({
      ok: true,
      message: `Saved recipe "${params.name}" deleted`,
    } satisfies MutationResponse);
  }),

  http.get("/api/sinks", () => {
    return HttpResponse.json({ sinks } satisfies SinkListResponse);
  }),

  http.post("/api/sinks", async ({ request }) => {
    const body = (await request.json()) as { name: string; sink_type: string; config: Record<string, unknown> };
    const newSink: SinkInfo = {
      name: body.name,
      sink_type: body.sink_type,
      config: body.config,
    };
    sinks.push(newSink);
    return HttpResponse.json({
      ok: true,
      message: `Sink "${body.name}" created`,
    } satisfies MutationResponse);
  }),

  http.put("/api/sinks/:name", ({ params }) => {
    const idx = sinks.findIndex((item) => item.name === params.name);
    if (idx === -1) return HttpResponse.json({ error: "Not found" }, { status: 404 });
    return HttpResponse.json({
      ok: true,
      message: `Sink "${params.name}" updated`,
    } satisfies MutationResponse);
  }),

  http.delete("/api/sinks/:name", ({ params }) => {
    const idx = sinks.findIndex((item) => item.name === params.name);
    if (idx === -1) return HttpResponse.json({ error: "Not found" }, { status: 404 });
    sinks.splice(idx, 1);
    return HttpResponse.json({
      ok: true,
      message: `Sink "${params.name}" deleted`,
    } satisfies MutationResponse);
  }),

  http.get("/api/sync/status", () => {
    return HttpResponse.json({
      running: !syncPaused,
      paused: syncPaused,
    } satisfies StatusResponse);
  }),

  http.post("/api/sync/pause", () => {
    syncPaused = true;
    return HttpResponse.json({ ok: true, message: "Syncing paused" } satisfies MutationResponse);
  }),

  http.post("/api/sync/resume", () => {
    syncPaused = false;
    return HttpResponse.json({ ok: true, message: "Syncing resumed" } satisfies MutationResponse);
  }),

  http.get("/api/history", () => {
    return HttpResponse.json({ cycles: sampleCycles } satisfies HistoryResponse);
  }),

  http.get("/api/config/export", ({ request }) => {
    const format = new URL(request.url).searchParams.get("format") ?? "toml";
    return HttpResponse.json({
      format,
      content:
        format === "json"
          ? JSON.stringify({ pipes, sinks, pipe_presets: presets }, null, 2)
          : "[[pipes]]\nname = \"catalog-sync\"\n",
    });
  }),

  http.post("/api/config/import", () => {
    return HttpResponse.json({
      ok: true,
      message: "Config imported",
      warnings: [],
    });
  }),
];

import { http, HttpResponse } from "msw";
import type {
  SourceListResponse,
  SourceInfo,
  SinkListResponse,
  SinkInfo,
  HistoryResponse,
  StatusResponse,
  MutationResponse,
  TriggerResponse,
  CycleInfo,
} from "@/types/api";

const now = new Date();
function hoursAgo(h: number): string {
  return new Date(now.getTime() - h * 3600_000).toISOString();
}

const sampleCycles: CycleInfo[] = [
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

const sampleSources: SourceInfo[] = [
  {
    name: "pg-main",
    connector: "postgres",
    interval_secs: 60,
    queries: [
      { id: "users_sync", key_column: "id" },
      { id: "orders_sync", key_column: "order_id" },
    ],
    status: {
      last_cycle: sampleCycles[3],
      total_cycles: 42,
    },
  },
  {
    name: "http-api",
    connector: "http",
    interval_secs: 120,
    queries: [{ id: "events_pull", key_column: "event_id" }],
    status: {
      last_cycle: sampleCycles[2],
      total_cycles: 15,
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

let syncPaused = false;

// In-memory stores for CRUD
let sources = [...sampleSources];
let sinks = [...sampleSinks];

export function resetMockState() {
  sources = [...sampleSources];
  sinks = [...sampleSinks];
  syncPaused = false;
}

export const handlers = [
  // Health
  http.get("/api/health", () => {
    return HttpResponse.json({ status: "ok", version: "0.1.0" });
  }),

  // Sources
  http.get("/api/sources", () => {
    return HttpResponse.json({ sources } satisfies SourceListResponse);
  }),

  http.get("/api/sources/:name", ({ params }) => {
    const source = sources.find((s) => s.name === params.name);
    if (!source) return HttpResponse.json({ error: "Not found" }, { status: 404 });
    return HttpResponse.json(source satisfies SourceInfo);
  }),

  http.post("/api/sources", async ({ request }) => {
    const body = (await request.json()) as { name: string; connector: string; config: Record<string, unknown> };
    const newSource: SourceInfo = {
      name: body.name,
      connector: body.connector,
      interval_secs: 60,
      queries: [],
      status: { last_cycle: null, total_cycles: 0 },
    };
    sources.push(newSource);
    return HttpResponse.json({ ok: true, message: `Source "${body.name}" created` } satisfies MutationResponse);
  }),

  http.put("/api/sources/:name", async ({ params, request }) => {
    const idx = sources.findIndex((s) => s.name === params.name);
    if (idx === -1) return HttpResponse.json({ error: "Not found" }, { status: 404 });
    const body = (await request.json()) as Record<string, unknown>;
    if (body.connector) sources[idx].connector = body.connector as string;
    return HttpResponse.json({ ok: true, message: `Source "${params.name}" updated` } satisfies MutationResponse);
  }),

  http.delete("/api/sources/:name", ({ params }) => {
    const idx = sources.findIndex((s) => s.name === params.name);
    if (idx === -1) return HttpResponse.json({ error: "Not found" }, { status: 404 });
    sources.splice(idx, 1);
    return HttpResponse.json({ ok: true, message: `Source "${params.name}" deleted` } satisfies MutationResponse);
  }),

  http.post("/api/sources/:name/trigger", ({ params }) => {
    const source = sources.find((s) => s.name === params.name);
    if (!source) return HttpResponse.json({ error: "Not found" }, { status: 404 });
    return HttpResponse.json({
      source: params.name as string,
      message: `Sync triggered for "${params.name}"`,
    } satisfies TriggerResponse);
  }),

  // Sinks
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
    return HttpResponse.json({ ok: true, message: `Sink "${body.name}" created` } satisfies MutationResponse);
  }),

  http.put("/api/sinks/:name", async ({ params }) => {
    const idx = sinks.findIndex((s) => s.name === params.name);
    if (idx === -1) return HttpResponse.json({ error: "Not found" }, { status: 404 });
    return HttpResponse.json({ ok: true, message: `Sink "${params.name}" updated` } satisfies MutationResponse);
  }),

  http.delete("/api/sinks/:name", ({ params }) => {
    const idx = sinks.findIndex((s) => s.name === params.name);
    if (idx === -1) return HttpResponse.json({ error: "Not found" }, { status: 404 });
    sinks.splice(idx, 1);
    return HttpResponse.json({ ok: true, message: `Sink "${params.name}" deleted` } satisfies MutationResponse);
  }),

  // Sync status
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

  // History
  http.get("/api/history", () => {
    return HttpResponse.json({ cycles: sampleCycles } satisfies HistoryResponse);
  }),
];

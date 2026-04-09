import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";

import { useSettingsStore } from "@/stores/settings";
import { resetMockState } from "@/test/mocks/handlers";
import { server } from "@/test/mocks/server";

import { ApiError, api } from "./client";

beforeAll(() => {
  useSettingsStore.setState({ apiBaseUrl: "/api" });
  server.listen({ onUnhandledRequest: "error" });
});

afterEach(() => {
  server.resetHandlers();
  resetMockState();
});

afterAll(() => server.close());

describe("api client", () => {
  it("GET /health returns status ok", async () => {
    const data = await api.get<{ status: string }>("/health");
    expect(data.status).toBe("ok");
  });

  it("GET /pipes returns pipes array", async () => {
    const data = await api.get<{ pipes: unknown[] }>("/pipes");
    expect(Array.isArray(data.pipes)).toBe(true);
    expect(data.pipes.length).toBeGreaterThan(0);
  });

  it("GET /pipe-presets returns presets array", async () => {
    const data = await api.get<{ presets: unknown[] }>("/pipe-presets");
    expect(Array.isArray(data.presets)).toBe(true);
    expect(data.presets.length).toBeGreaterThan(0);
  });

  it("GET /sinks returns sinks array", async () => {
    const data = await api.get<{ sinks: unknown[] }>("/sinks");
    expect(Array.isArray(data.sinks)).toBe(true);
    expect(data.sinks.length).toBeGreaterThan(0);
  });

  it("GET /sync/status returns running state", async () => {
    const data = await api.get<{ running: boolean; paused: boolean }>("/sync/status");
    expect(data.running).toBe(true);
    expect(data.paused).toBe(false);
  });

  it("POST /sync/pause pauses syncing", async () => {
    await api.post("/sync/pause");
    const data = await api.get<{ running: boolean; paused: boolean }>("/sync/status");
    expect(data.paused).toBe(true);
  });

  it("POST /sync/resume resumes syncing", async () => {
    await api.post("/sync/pause");
    await api.post("/sync/resume");
    const data = await api.get<{ running: boolean; paused: boolean }>("/sync/status");
    expect(data.paused).toBe(false);
  });

  it("POST /pipes creates a new pipe", async () => {
    const result = await api.post<{ ok: boolean }>("/pipes", {
      name: "test-pipe",
      origin_connector: "postgres",
      origin_dsn: "postgres://localhost:5432/app",
      targets: ["kafka-prod"],
      schedule: { interval_secs: 60, missed_tick_policy: "skip" },
      delta: { diff_mode: "db", fail_safe_threshold: 30 },
      retry: { max_retries: 3, retry_base_delay_secs: 5 },
      queries: [
        {
          id: "public.users",
          sql: "select id, email from public.users",
          key_column: "id",
        },
      ],
    });
    expect(result.ok).toBe(true);

    const data = await api.get<{ pipes: { name: string }[] }>("/pipes");
    expect(data.pipes.some((pipe) => pipe.name === "test-pipe")).toBe(true);
  });

  it("DELETE /pipes/:name removes a pipe", async () => {
    const result = await api.del<{ ok: boolean }>("/pipes/catalog-sync");
    expect(result.ok).toBe(true);

    const data = await api.get<{ pipes: { name: string }[] }>("/pipes");
    expect(data.pipes.some((pipe) => pipe.name === "catalog-sync")).toBe(false);
  });

  it("POST /pipe-presets creates a new recipe", async () => {
    const result = await api.post<{ ok: boolean }>("/pipe-presets", {
      name: "generic-postgres",
      description: "Reusable onboarding template",
      spec: {
        origin_connector: "postgres",
        origin_dsn: "{{dsn}}",
        origin_config: {},
        parameters: [{ name: "dsn", required: true, secret: true }],
        targets: ["kafka-prod"],
        queries: [],
        schedule: { interval_secs: 300, missed_tick_policy: "skip" },
        delta: { diff_mode: "db", fail_safe_threshold: 30 },
        retry: { max_retries: 3, retry_base_delay_secs: 5 },
        recipe: { type: "postgres_snapshot", prefix: "{{source_name}}" },
        filters: [],
        transforms: [],
        links: [],
      },
    });
    expect(result.ok).toBe(true);
  });

  it("GET /history returns cycle history", async () => {
    const data = await api.get<{ cycles: unknown[] }>("/history");
    expect(Array.isArray(data.cycles)).toBe(true);
    expect(data.cycles.length).toBeGreaterThan(0);
  });

  it("throws ApiError on 404", async () => {
    try {
      await api.get("/pipes/nonexistent");
      expect.fail("Should have thrown");
    } catch (error) {
      expect(error).toBeInstanceOf(ApiError);
      expect((error as ApiError).status).toBe(404);
    }
  });
});

import { describe, it, expect, beforeAll, afterAll, afterEach } from "vitest";
import { server } from "@/test/mocks/server";
import { resetMockState } from "@/test/mocks/handlers";
import { api, ApiError } from "./client";
import { useSettingsStore } from "@/stores/settings";

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

  it("GET /sources returns sources array", async () => {
    const data = await api.get<{ sources: unknown[] }>("/sources");
    expect(Array.isArray(data.sources)).toBe(true);
    expect(data.sources.length).toBeGreaterThan(0);
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

  it("POST /sources creates a new source", async () => {
    const result = await api.post<{ ok: boolean }>("/sources", {
      name: "test-source",
      connector: "postgres",
      config: { host: "localhost" },
    });
    expect(result.ok).toBe(true);

    const data = await api.get<{ sources: { name: string }[] }>("/sources");
    expect(data.sources.some((s) => s.name === "test-source")).toBe(true);
  });

  it("DELETE /sources/:name removes a source", async () => {
    const result = await api.del<{ ok: boolean }>("/sources/pg-main");
    expect(result.ok).toBe(true);

    const data = await api.get<{ sources: { name: string }[] }>("/sources");
    expect(data.sources.some((s) => s.name === "pg-main")).toBe(false);
  });

  it("POST /sources/:name/trigger triggers sync", async () => {
    const result = await api.post<{ source: string; message: string }>(
      "/sources/pg-main/trigger",
    );
    expect(result.source).toBe("pg-main");
    expect(result.message).toContain("pg-main");
  });

  it("POST /sinks creates a new sink", async () => {
    const result = await api.post<{ ok: boolean }>("/sinks", {
      name: "test-sink",
      sink_type: "stdout",
      config: { pretty: true },
    });
    expect(result.ok).toBe(true);
  });

  it("DELETE /sinks/:name removes a sink", async () => {
    const result = await api.del<{ ok: boolean }>("/sinks/kafka-prod");
    expect(result.ok).toBe(true);
  });

  it("GET /history returns cycle history", async () => {
    const data = await api.get<{ cycles: unknown[] }>("/history");
    expect(Array.isArray(data.cycles)).toBe(true);
    expect(data.cycles.length).toBeGreaterThan(0);
  });

  it("throws ApiError on 404", async () => {
    try {
      await api.get("/sources/nonexistent");
      expect.fail("Should have thrown");
    } catch (e) {
      expect(e).toBeInstanceOf(ApiError);
      expect((e as ApiError).status).toBe(404);
    }
  });
});

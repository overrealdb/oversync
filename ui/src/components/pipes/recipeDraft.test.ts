import { describe, expect, it } from "vitest";
import {
  buildRuntimePipeFromPresetSpec,
  buildPipePayload,
  buildPipePresetSpec,
  buildPipePresetSpecWithParameters,
  draftFromPreset,
  materializePipePresetSpec,
  serializeRuntimePipeToml,
  suggestMaterializedPipeName,
  type RecipeDraftState,
} from "./recipeDraft";

function manualDraft(): RecipeDraftState {
  return {
    originDsn: "postgres://user:pass@db:5432/app",
    originCredential: "vault-postgres",
    pipeMode: "manual",
    prefix: "",
    schemas: "public",
    intervalSecs: 120,
    diffMode: "db",
    selectedTargets: ["catalog-kafka"],
    manualQueries: [
      {
        id: "aspect-columns",
        sql: "select 1 as entityId",
        key_column: "entityId",
      },
    ],
  };
}

describe("buildPipePayload", () => {
  it("keeps manual queries and clears recipe for manual mode", () => {
    const payload = buildPipePayload(manualDraft());

    expect(payload.recipe).toBeNull();
    expect(payload.queries).toHaveLength(1);
    expect(payload.queries?.[0]).toMatchObject({
      id: "aspect-columns",
      key_column: "entityId",
    });
  });
});

describe("buildPipePresetSpec", () => {
  it("emits recipe-backed presets without explicit query payload", () => {
    const spec = buildPipePresetSpec({
      ...manualDraft(),
      pipeMode: "postgres_snapshot",
      prefix: "some-postgresql-source",
      schemas: "public, analytics",
      manualQueries: [],
    });

    expect(spec.recipe).toMatchObject({
      type: "postgres_snapshot",
      prefix: "some-postgresql-source",
      schemas: ["public", "analytics"],
    });
    expect(spec.queries).toEqual([]);
  });
});

describe("draftFromPreset", () => {
  it("round-trips persisted manual recipe state back into editor fields", () => {
    const draft = draftFromPreset({
      name: "custom-aspect",
      description: "manual recipe",
      spec: buildPipePresetSpec(manualDraft()),
    });

    expect(draft.pipeMode).toBe("manual");
    expect(draft.originDsn).toBe("postgres://user:pass@db:5432/app");
    expect(draft.originCredential).toBe("vault-postgres");
    expect(draft.manualQueries).toHaveLength(1);
    expect(draft.manualQueries[0]?.sql).toContain("entityId");
  });
});

describe("materializePipePresetSpec", () => {
  it("expands placeholders inside recipe-backed defaults and SQL", () => {
    const spec = buildPipePresetSpecWithParameters(
      {
        ...manualDraft(),
        originDsn: "postgres://{{db_user}}:{{db_pass}}@db:5432/{{db_name}}",
        pipeMode: "postgres_snapshot",
        prefix: "{{source_name}}",
        schemas: "{{schema_name}}",
        manualQueries: [
          {
            id: "unused",
            sql: "select '{{schema_name}}'",
            key_column: "id",
          },
        ],
      },
      [
        { name: "db_user", required: true, secret: false },
        { name: "db_pass", required: true, secret: true },
        { name: "db_name", required: true, secret: false },
        { name: "source_name", required: true, secret: false },
        { name: "schema_name", required: true, secret: false },
      ],
    );

    const materialized = materializePipePresetSpec(spec, {
      db_user: "postgres",
      db_pass: "secret",
      db_name: "warehouse",
      source_name: "finance",
      schema_name: "analytics",
    });

    expect(materialized.origin_dsn).toBe("postgres://postgres:secret@db:5432/warehouse");
    expect(materialized.recipe?.prefix).toBe("finance");
    expect(materialized.recipe?.schemas).toEqual(["analytics"]);
  });
});

describe("suggestMaterializedPipeName", () => {
  it("prefers the materialized recipe prefix and normalizes it", () => {
    const suggestion = suggestMaterializedPipeName("preset-name", {
      ...buildPipePresetSpec(manualDraft()),
      recipe: {
        type: "postgres_snapshot",
        prefix: "Finance Warehouse / Tables",
        schemas: ["public"],
      },
    });

    expect(suggestion).toBe("Finance-Warehouse-Tables");
  });
});

describe("serializeRuntimePipeToml", () => {
  it("exports a runnable materialized pipe snippet", () => {
    const spec = materializePipePresetSpec(
      buildPipePresetSpecWithParameters(
        {
          ...manualDraft(),
          originDsn: "postgres://{{db_user}}:{{db_pass}}@db:5432/{{db_name}}",
          pipeMode: "postgres_snapshot",
          prefix: "{{source_name}}",
          schemas: "{{schema_name}}",
          selectedTargets: ["catalog-kafka"],
          manualQueries: [],
        },
        [
          { name: "db_user", required: true, secret: false },
          { name: "db_pass", required: true, secret: true },
          { name: "db_name", required: true, secret: false },
          { name: "source_name", required: true, secret: false },
          { name: "schema_name", required: true, secret: false },
        ],
      ),
      {
        db_user: "postgres",
        db_pass: "secret",
        db_name: "warehouse",
        source_name: "finance",
        schema_name: "analytics",
      },
    );

    const runtimePipe = buildRuntimePipeFromPresetSpec("finance-analytics", spec);
    const toml = serializeRuntimePipeToml(runtimePipe);

    expect(toml).toContain("[[pipes]]");
    expect(toml).toContain('name = "finance-analytics"');
    expect(toml).toContain('[pipes.origin]');
    expect(toml).toContain('connector = "postgres"');
    expect(toml).toContain('[pipes.recipe]');
    expect(toml).toContain('prefix = "finance"');
    expect(toml).toContain('schemas = ["analytics"]');
  });
});

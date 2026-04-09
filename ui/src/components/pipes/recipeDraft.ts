import type {
  CreatePipeRequest,
  PipePresetInfo,
  PipePresetParameter,
  PipePresetSpec,
  PipeQueryDefinition,
  PipeRecipe,
  RuntimePipeConfig,
  UpdatePipeRequest,
} from "@/types/api";

export type PipeMode = "manual" | "postgres_metadata" | "postgres_snapshot";
export type DiffMode = "db" | "memory";

export interface ManualQueryDraft {
  id: string;
  sql: string;
  key_column: string;
}

export interface RecipeDraftState {
  originDsn: string;
  originCredential: string;
  pipeMode: PipeMode;
  prefix: string;
  schemas: string;
  intervalSecs: number;
  diffMode: DiffMode;
  selectedTargets: string[];
  manualQueries: ManualQueryDraft[];
}

export const DEFAULT_INTERVAL_SECS = 300;
export const DEFAULT_SCHEMAS = "public";
const TEMPLATE_PATTERN = /\{\{\s*([a-zA-Z0-9_-]+)\s*\}\}/g;
const NON_PIPE_NAME_CHARS = /[^a-zA-Z0-9._-]+/g;

export function emptyManualQuery(): ManualQueryDraft {
  return {
    id: "",
    sql: "",
    key_column: "",
  };
}

export function createDefaultRecipeDraft(): RecipeDraftState {
  return {
    originDsn: "",
    originCredential: "",
    pipeMode: "postgres_snapshot",
    prefix: "",
    schemas: DEFAULT_SCHEMAS,
    intervalSecs: DEFAULT_INTERVAL_SECS,
    diffMode: "db",
    selectedTargets: [],
    manualQueries: [emptyManualQuery()],
  };
}

export function manualDraftsFromQueries(
  queries: PipeQueryDefinition[] | null | undefined,
): ManualQueryDraft[] {
  if (!queries || queries.length === 0) return [emptyManualQuery()];
  return queries.map((query) => ({
    id: query.id,
    sql: query.sql,
    key_column: query.key_column,
  }));
}

export function normalizeMode(recipe: PipeRecipe | null | undefined): PipeMode {
  return recipe?.type ?? "manual";
}

export function draftFromRuntimePipe(pipe: RuntimePipeConfig): RecipeDraftState {
  return {
    originDsn: pipe.origin.dsn ?? "",
    originCredential: pipe.origin.credential ?? "",
    pipeMode: normalizeMode(pipe.recipe),
    prefix: pipe.recipe?.prefix ?? "",
    schemas: (pipe.recipe?.schemas ?? [DEFAULT_SCHEMAS]).join(", "),
    intervalSecs: pipe.schedule?.interval_secs ?? DEFAULT_INTERVAL_SECS,
    diffMode: pipe.delta?.diff_mode === "memory" ? "memory" : "db",
    selectedTargets: pipe.targets ?? [],
    manualQueries: manualDraftsFromQueries(pipe.queries ?? []),
  };
}

export function draftFromPreset(preset: PipePresetInfo): RecipeDraftState {
  return {
    originDsn: preset.spec.origin_dsn ?? "",
    originCredential: preset.spec.origin_credential ?? "",
    pipeMode: normalizeMode(preset.spec.recipe),
    prefix: preset.spec.recipe?.prefix ?? "",
    schemas: (preset.spec.recipe?.schemas ?? [DEFAULT_SCHEMAS]).join(", "),
    intervalSecs: preset.spec.schedule?.interval_secs ?? DEFAULT_INTERVAL_SECS,
    diffMode: preset.spec.delta?.diff_mode === "memory" ? "memory" : "db",
    selectedTargets: preset.spec.targets ?? [],
    manualQueries: manualDraftsFromQueries(preset.spec.queries ?? []),
  };
}

export function normalizeManualQueries(draft: RecipeDraftState): PipeQueryDefinition[] {
  return draft.manualQueries
    .map((query) => ({
      id: query.id.trim(),
      sql: query.sql.trim(),
      key_column: query.key_column.trim(),
    }))
    .filter((query) => query.id || query.sql || query.key_column);
}

export function hasCompleteManualQueries(draft: RecipeDraftState): boolean {
  const queries = normalizeManualQueries(draft);
  return (
    queries.length > 0 &&
    queries.every((query) => query.id && query.sql && query.key_column)
  );
}

export function buildRecipe(draft: RecipeDraftState): PipeRecipe | null {
  if (draft.pipeMode === "manual") return null;
  return {
    type: draft.pipeMode,
    prefix: draft.prefix.trim(),
    entity_type_id: "postgres",
    schema_id: "table",
    schemas: draft.schemas
      .split(",")
      .map((value) => value.trim())
      .filter(Boolean),
  };
}

export function buildPipePresetSpec(draft: RecipeDraftState): PipePresetSpec {
  return buildPipePresetSpecWithParameters(draft, []);
}

export function buildPipePresetSpecWithParameters(
  draft: RecipeDraftState,
  parameters: PipePresetParameter[],
): PipePresetSpec {
  return {
    origin_connector: "postgres",
    origin_dsn: draft.originDsn.trim(),
    origin_credential: draft.originCredential.trim() || undefined,
    origin_config: {},
    parameters,
    targets: draft.selectedTargets,
    queries: draft.pipeMode === "manual" ? normalizeManualQueries(draft) : [],
    schedule: {
      interval_secs: draft.intervalSecs,
      missed_tick_policy: "skip",
    },
    delta: {
      diff_mode: draft.diffMode,
      fail_safe_threshold: 30,
    },
    retry: {
      max_retries: 3,
      retry_base_delay_secs: 5,
    },
    recipe: buildRecipe(draft) ?? undefined,
    filters: [],
    transforms: [],
    links: [],
  };
}

export function createDefaultPresetParameter(): PipePresetParameter {
  return {
    name: "",
    label: "",
    description: "",
    default: "",
    required: true,
    secret: false,
  };
}

export function normalizePresetParameters(
  parameters: PipePresetParameter[] | null | undefined,
): PipePresetParameter[] {
  return (parameters ?? [])
    .map((parameter) => ({
      name: parameter.name.trim(),
      label: parameter.label?.trim() || undefined,
      description: parameter.description?.trim() || undefined,
      default: parameter.default?.trim() || undefined,
      required: parameter.required !== false,
      secret: parameter.secret === true,
    }))
    .filter((parameter) => parameter.name);
}

export function parameterInitialValues(
  parameters: PipePresetParameter[] | null | undefined,
): Record<string, string> {
  return Object.fromEntries(
    normalizePresetParameters(parameters).map((parameter) => [
      parameter.name,
      parameter.default ?? "",
    ]),
  );
}

function expandTemplateString(
  value: string,
  parameterValues: Record<string, string>,
): string {
  return value.replace(TEMPLATE_PATTERN, (_, rawName: string) => {
    const name = rawName.trim();
    return parameterValues[name] ?? "";
  });
}

function expandTemplateValue(
  value: unknown,
  parameterValues: Record<string, string>,
): unknown {
  if (typeof value === "string") {
    return expandTemplateString(value, parameterValues);
  }
  if (Array.isArray(value)) {
    return value.map((item) => expandTemplateValue(item, parameterValues));
  }
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>).map(([key, nested]) => [
        key,
        expandTemplateValue(nested, parameterValues),
      ]),
    );
  }
  return value;
}

export function materializePipePresetSpec(
  spec: PipePresetSpec,
  parameterValues: Record<string, string>,
): PipePresetSpec {
  return expandTemplateValue(spec, parameterValues) as PipePresetSpec;
}

function normalizeSuggestedPipeName(value: string): string {
  return value
    .trim()
    .replace(NON_PIPE_NAME_CHARS, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");
}

export function suggestMaterializedPipeName(
  presetName: string,
  spec: PipePresetSpec,
): string {
  const candidate =
    spec.recipe?.prefix?.trim() ||
    spec.queries?.[0]?.id?.trim() ||
    presetName.trim() ||
    "generated-pipe";

  return normalizeSuggestedPipeName(candidate) || "generated-pipe";
}

export function buildRuntimePipeFromPresetSpec(
  name: string,
  spec: PipePresetSpec,
): RuntimePipeConfig {
  return {
    name: name.trim(),
    origin: {
      connector: spec.origin_connector,
      dsn: spec.origin_dsn,
      credential: spec.origin_credential?.trim() || null,
      trino_url: spec.trino_url?.trim() || null,
      config: spec.origin_config ?? {},
    },
    targets: spec.targets ?? [],
    queries: spec.queries ?? [],
    schedule: spec.schedule,
    delta: spec.delta,
    retry: spec.retry,
    recipe: spec.recipe ?? null,
    filters: spec.filters ?? [],
    transforms: spec.transforms ?? [],
    links: spec.links ?? [],
    alert_webhook: null,
    enabled: true,
  };
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function isArrayOfObjects(value: unknown): value is Array<Record<string, unknown>> {
  return Array.isArray(value) && value.every((item) => isPlainObject(item));
}

function lastLine(lines: string[]): string | undefined {
  return lines.length > 0 ? lines[lines.length - 1] : undefined;
}

function formatTomlPrimitive(value: unknown): string {
  if (typeof value === "string") return JSON.stringify(value);
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  if (Array.isArray(value)) {
    return `[${value.map((item) => formatTomlPrimitive(item)).join(", ")}]`;
  }
  return JSON.stringify(value);
}

function appendTomlTableBody(
  lines: string[],
  path: string,
  value: Record<string, unknown>,
) {
  for (const [key, nested] of Object.entries(value)) {
    if (nested === undefined || nested === null) continue;
    if (isPlainObject(nested) || isArrayOfObjects(nested)) continue;
    lines.push(`${key} = ${formatTomlPrimitive(nested)}`);
  }

  for (const [key, nested] of Object.entries(value)) {
    if (!isPlainObject(nested)) continue;
    if (lastLine(lines) !== "") lines.push("");
    const nestedPath = `${path}.${key}`;
    lines.push(`[${nestedPath}]`);
    appendTomlTableBody(lines, nestedPath, nested);
  }

  for (const [key, nested] of Object.entries(value)) {
    if (!isArrayOfObjects(nested)) continue;
    const nestedPath = `${path}.${key}`;
    for (const item of nested) {
      if (lastLine(lines) !== "") lines.push("");
      lines.push(`[[${nestedPath}]]`);
      appendTomlTableBody(lines, nestedPath, item);
    }
  }
}

export function serializeRuntimePipeToml(pipe: RuntimePipeConfig): string {
  const lines = ["[[pipes]]"];
  appendTomlTableBody(lines, "pipes", {
    name: pipe.name,
    targets: pipe.targets,
    enabled: pipe.enabled,
    origin: pipe.origin,
    schedule: pipe.schedule,
    delta: pipe.delta,
    retry: pipe.retry,
    recipe: pipe.recipe,
    queries: pipe.queries,
    filters: pipe.filters,
    transforms: pipe.transforms,
    links: pipe.links,
    alert_webhook: pipe.alert_webhook,
  });
  return `${lines.join("\n").trim()}\n`;
}

export function buildPipePayload(
  draft: RecipeDraftState,
): Omit<CreatePipeRequest, "name"> & Omit<UpdatePipeRequest, "enabled"> {
  const recipe = buildRecipe(draft);
  return {
    origin_connector: "postgres",
    origin_dsn: draft.originDsn.trim(),
    origin_credential: draft.originCredential.trim() || undefined,
    origin_config: {},
    targets: draft.selectedTargets,
    schedule: {
      interval_secs: draft.intervalSecs,
      missed_tick_policy: "skip",
    },
    delta: {
      diff_mode: draft.diffMode,
      fail_safe_threshold: 30,
    },
    retry: {
      max_retries: 3,
      retry_base_delay_secs: 5,
    },
    recipe,
    filters: [],
    transforms: [],
    links: [],
    queries: draft.pipeMode === "manual" ? normalizeManualQueries(draft) : [],
  };
}

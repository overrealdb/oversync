export interface HealthResponse {
  status: string;
  version: string;
}

export interface SourceInfo {
  name: string;
  connector: string;
  interval_secs: number;
  queries: QueryInfo[];
  status: SourceStatus;
}

export interface QueryInfo {
  id: string;
  key_column: string;
}

export interface SourceStatus {
  last_cycle: CycleInfo | null;
  total_cycles: number;
}

export interface CycleInfo {
  cycle_id: number;
  source: string;
  query: string;
  status: CycleStatus;
  started_at: string;
  finished_at: string | null;
  rows_created: number;
  rows_updated: number;
  rows_deleted: number;
  duration_ms: number | null;
  error: string | null;
}

export interface SinkInfo {
  name: string;
  sink_type: string;
  config?: Record<string, unknown>;
}

export interface TriggerResponse {
  source: string;
  message: string;
}

export interface ErrorResponse {
  error: string;
}

export interface SourceListResponse {
  sources: SourceInfo[];
}

export interface SinkListResponse {
  sinks: SinkInfo[];
}

export interface PipeRecipe {
  type: "postgres_metadata" | "postgres_snapshot";
  prefix: string;
  entity_type_id?: string;
  schema_id?: string;
  schemas?: string[];
}

export interface PipeInfo {
  name: string;
  origin_connector: string;
  origin_dsn: string;
  targets: string[];
  interval_secs: number;
  recipe?: PipeRecipe | null;
  enabled: boolean;
}

export interface PipeListResponse {
  pipes: PipeInfo[];
}

export interface PipeScheduleDefinition {
  interval_secs?: number;
  missed_tick_policy?: "skip" | "burst";
  max_requests_per_minute?: number | null;
}

export interface PipeDeltaDefinition {
  diff_mode?: "db" | "memory";
  fail_safe_threshold?: number;
}

export interface PipeRetryDefinition {
  max_retries?: number;
  retry_base_delay_secs?: number;
}

export interface PipePresetSpec {
  origin_connector: string;
  origin_dsn: string;
  origin_credential?: string;
  trino_url?: string;
  origin_config?: Record<string, unknown>;
  targets: string[];
  queries: PipeQueryDefinition[];
  schedule?: PipeScheduleDefinition;
  delta?: PipeDeltaDefinition;
  retry?: PipeRetryDefinition;
  recipe?: PipeRecipe | null;
  filters: unknown[];
  transforms: unknown[];
  links: unknown[];
}

export interface PipePresetInfo {
  name: string;
  description?: string | null;
  spec: PipePresetSpec;
}

export interface PipePresetListResponse {
  presets: PipePresetInfo[];
}

export interface PipeQueryDefinition {
  id: string;
  sql: string;
  key_column: string;
  sinks?: string[] | null;
  transform?: string | null;
}

export interface PipeOriginDefinition {
  connector: string;
  dsn: string;
  credential?: string | null;
  trino_url?: string | null;
  config?: Record<string, unknown> | null;
}

export interface RuntimePipeConfig {
  name: string;
  origin: PipeOriginDefinition;
  targets: string[];
  queries: PipeQueryDefinition[];
  schedule?: PipeScheduleDefinition;
  delta?: PipeDeltaDefinition;
  retry?: PipeRetryDefinition;
  recipe?: PipeRecipe | null;
  filters: unknown[];
  transforms: unknown[];
  links: unknown[];
  alert_webhook?: string | null;
  enabled: boolean;
}

export interface ResolvePipeResponse {
  pipe: RuntimePipeConfig;
  effective_queries: PipeQueryDefinition[];
}

export interface CreateSourceRequest {
  name: string;
  connector: string;
  config: Record<string, unknown>;
}

export interface UpdateSourceRequest {
  connector?: string;
  config?: Record<string, unknown>;
  enabled?: boolean;
}

export interface CreateSinkRequest {
  name: string;
  sink_type: string;
  config: Record<string, unknown>;
}

export interface UpdateSinkRequest {
  sink_type?: string;
  config?: Record<string, unknown>;
  enabled?: boolean;
}

export interface MutationResponse {
  ok: boolean;
  message: string;
}

export interface CreatePipeRequest {
  name: string;
  origin_connector: string;
  origin_dsn: string;
  origin_credential?: string;
  trino_url?: string;
  origin_config?: Record<string, unknown>;
  targets?: string[];
  schedule?: PipeScheduleDefinition;
  delta?: PipeDeltaDefinition;
  retry?: PipeRetryDefinition;
  recipe?: PipeRecipe;
  filters?: unknown[];
  transforms?: unknown[];
  links?: unknown[];
  queries?: PipeQueryDefinition[];
}

export interface UpdatePipeRequest {
  origin_connector?: string;
  origin_dsn?: string;
  origin_credential?: string;
  trino_url?: string;
  origin_config?: Record<string, unknown>;
  targets?: string[];
  schedule?: PipeScheduleDefinition;
  delta?: PipeDeltaDefinition;
  retry?: PipeRetryDefinition;
  recipe?: PipeRecipe;
  filters?: unknown[];
  transforms?: unknown[];
  links?: unknown[];
  queries?: PipeQueryDefinition[];
  enabled?: boolean;
}

export interface CreatePipePresetRequest {
  name: string;
  description?: string;
  spec: PipePresetSpec;
}

export interface UpdatePipePresetRequest {
  description?: string;
  spec?: PipePresetSpec;
}

export interface CreatePipePresetRequest {
  name: string;
  description?: string;
  spec: PipePresetSpec;
}

export interface UpdatePipePresetRequest {
  description?: string;
  spec?: PipePresetSpec;
}

export interface HistoryResponse {
  cycles: CycleInfo[];
}

export interface StatusResponse {
  running: boolean;
  paused: boolean;
}

export type ExportConfigFormat = "toml" | "json";

export interface ExportConfigResponse {
  format: ExportConfigFormat;
  content: string;
}

export interface ImportConfigRequest {
  format: ExportConfigFormat;
  content: string;
}

export interface ImportConfigResponse {
  ok: boolean;
  message: string;
  warnings: string[];
}

export type DryRunMode = "mock" | "live";

export interface DryRunCredentials {
  username: string;
  password: string;
}

export interface DryRunChanges {
  created: number;
  updated: number;
  deleted: number;
}

export interface DryRunStats {
  rows_fetched: number;
  events_before_transform: number;
  events_after_transform: number;
  events_filtered_out: number;
}

export interface DryRunRequest {
  pipe: RuntimePipeConfig;
  query_id: string;
  mode: DryRunMode;
  mock_data?: Array<Record<string, unknown>>;
  row_limit: number;
  transforms?: unknown[];
  use_existing_state: boolean;
  credentials?: DryRunCredentials;
}

export interface DryRunResult {
  input_rows: number;
  input_sample: Array<Record<string, unknown>>;
  changes: DryRunChanges;
  after_transform: Array<Record<string, unknown>>;
  stats: DryRunStats;
}

export type CycleStatus = "success" | "failed" | "aborted" | "running";

export type ConnectorType =
  | "postgres"
  | "mysql"
  | "http"
  | "graphql"
  | "trino"
  | "flight_sql";

export type SinkType = "stdout" | "kafka" | "surrealdb" | "http";

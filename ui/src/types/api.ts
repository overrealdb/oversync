import type {
  ApiLinkDef,
  ApiOriginConfig,
  ApiOriginConfigDef,
  ApiPipeRecipeDef,
  ApiSinkConfig,
  ApiTransformStep,
} from "../api/generated/types.gen";

export interface HealthResponse {
  status: string;
  version: string;
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
  config?: ApiSinkConfig;
}

export interface ErrorResponse {
  error: string;
}

export interface SinkListResponse {
  sinks: SinkInfo[];
}

export type PipeRecipe = ApiPipeRecipeDef;

export type PipeTransformDefinition = ApiTransformStep;

export type PipeLinkDefinition = ApiLinkDef;

export interface PipeInfo {
  name: string;
  origin_connector: string;
  origin_dsn: string;
  targets: string[];
  interval_secs: number;
  query_count: number;
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

export interface PipePresetParameter {
  name: string;
  label?: string | null;
  description?: string | null;
  default?: string | null;
  required?: boolean;
  secret?: boolean;
}

export interface PipePresetSpec {
  origin_connector: string;
  origin_dsn: string;
  origin_credential?: string;
  trino_url?: string;
  origin_config?: ApiOriginConfig;
  parameters?: PipePresetParameter[];
  targets: string[];
  queries: PipeQueryDefinition[];
  schedule?: PipeScheduleDefinition;
  delta?: PipeDeltaDefinition;
  retry?: PipeRetryDefinition;
  recipe?: PipeRecipe | null;
  filters: PipeTransformDefinition[];
  transforms: PipeTransformDefinition[];
  links: PipeLinkDefinition[];
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
  config?: ApiOriginConfigDef | null;
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
  filters: PipeTransformDefinition[];
  transforms: PipeTransformDefinition[];
  links: PipeLinkDefinition[];
  alert_webhook?: string | null;
  enabled: boolean;
}

export interface ResolvePipeResponse {
  pipe: RuntimePipeConfig;
  effective_queries: PipeQueryDefinition[];
}

export interface CreateSinkRequest {
  name: string;
  sink_type: string;
  config: ApiSinkConfig;
}

export interface UpdateSinkRequest {
  sink_type?: string;
  config?: ApiSinkConfig;
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
  origin_config?: ApiOriginConfig;
  targets?: string[];
  schedule?: PipeScheduleDefinition;
  delta?: PipeDeltaDefinition;
  retry?: PipeRetryDefinition;
  recipe?: PipeRecipe | null;
  filters?: PipeTransformDefinition[];
  transforms?: PipeTransformDefinition[];
  links?: PipeLinkDefinition[];
  queries?: PipeQueryDefinition[];
}

export interface UpdatePipeRequest {
  origin_connector?: string;
  origin_dsn?: string;
  origin_credential?: string;
  trino_url?: string;
  origin_config?: ApiOriginConfig;
  targets?: string[];
  schedule?: PipeScheduleDefinition;
  delta?: PipeDeltaDefinition;
  retry?: PipeRetryDefinition;
  recipe?: PipeRecipe | null;
  filters?: PipeTransformDefinition[];
  transforms?: PipeTransformDefinition[];
  links?: PipeLinkDefinition[];
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

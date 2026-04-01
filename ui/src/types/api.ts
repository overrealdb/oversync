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

export interface HistoryResponse {
  cycles: CycleInfo[];
}

export interface StatusResponse {
  running: boolean;
  paused: boolean;
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

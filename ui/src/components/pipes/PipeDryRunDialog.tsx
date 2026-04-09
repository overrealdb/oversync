import { useEffect, useEffectEvent, useMemo, useState } from "react";
import { Play, X } from "lucide-react";
import { useDryRunPipe, useResolvePipe } from "@/api/pipes";
import { useToast } from "@/components/shared/useToast";
import type {
  DryRunMode,
  DryRunRequest,
  PipeQueryDefinition,
} from "@/types/api";

const INPUT_CLS =
  "w-full rounded-2xl border border-white/10 bg-white/[0.04] px-4 py-3 text-sm text-slate-100 outline-none transition-colors focus:border-emerald-300/35";

interface PipeDryRunDialogProps {
  pipeName: string | null;
  open: boolean;
  onClose: () => void;
}

function prettyJson(value: unknown) {
  return JSON.stringify(value, null, 2);
}

export function PipeDryRunDialog({
  pipeName,
  open,
  onClose,
}: PipeDryRunDialogProps) {
  const { toast } = useToast();
  const { data, isLoading, error, refetch, isFetching } = useResolvePipe(pipeName, open);
  const dryRun = useDryRunPipe();

  const [mode, setMode] = useState<DryRunMode>("live");
  const [selectedQuery, setSelectedQuery] = useState("");
  const [rowLimit, setRowLimit] = useState(100);
  const [useExistingState, setUseExistingState] = useState(true);
  const [credentialsUsername, setCredentialsUsername] = useState("");
  const [credentialsPassword, setCredentialsPassword] = useState("");
  const [mockDataText, setMockDataText] = useState('[\n  {\n    "id": "demo-key"\n  }\n]');

  const resetDialogState = useEffectEvent(() => {
    setMode("live");
    setSelectedQuery("");
    setRowLimit(100);
    setUseExistingState(true);
    setCredentialsUsername("");
    setCredentialsPassword("");
    setMockDataText('[\n  {\n    "id": "demo-key"\n  }\n]');
    dryRun.reset();
  });

  useEffect(() => {
    if (!open) return;
    resetDialogState();
  }, [open, pipeName]);

  useEffect(() => {
    if (!data) return;
    const firstQuery = data.effective_queries[0]?.id ?? "";
    if (!selectedQuery || !data.effective_queries.some((query) => query.id === selectedQuery)) {
      setSelectedQuery(firstQuery);
    }
  }, [data, selectedQuery]);

  const selectedQueryDef = useMemo<PipeQueryDefinition | null>(() => {
    if (!data) return null;
    return data.effective_queries.find((query) => query.id === selectedQuery) ?? null;
  }, [data, selectedQuery]);

  if (!open) return null;

  function buildPayload(): DryRunRequest | null {
    if (!data || !selectedQuery) {
      toast("error", "No query selected for dry-run");
      return null;
    }

    let mockData: Array<Record<string, unknown>> | undefined;
    if (mode === "mock") {
      try {
        const parsed = JSON.parse(mockDataText);
        if (!Array.isArray(parsed)) {
          toast("error", "Mock data must be a JSON array of rows");
          return null;
        }
        mockData = parsed;
      } catch {
        toast("error", "Mock data is not valid JSON");
        return null;
      }
    }

    const credentials =
      credentialsUsername.trim() || credentialsPassword.trim()
        ? {
            username: credentialsUsername,
            password: credentialsPassword,
          }
        : undefined;

    return {
      pipe: data.pipe,
      query_id: selectedQuery,
      mode,
      mock_data: mockData,
      row_limit: rowLimit,
      transforms: [...data.pipe.filters, ...data.pipe.transforms],
      use_existing_state: useExistingState,
      credentials,
    };
  }

  function handleRun() {
    const payload = buildPayload();
    if (!payload) return;

    dryRun.mutate(payload, {
      onError: (err) => toast("error", err.message),
    });
  }

  const result = dryRun.data;
  const queryCount = data?.effective_queries.length ?? 0;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      <div className="fixed inset-0 bg-black/60" onClick={onClose} />
      <div className="panel-surface relative max-h-[92vh] w-full max-w-6xl overflow-y-auto p-6 shadow-2xl">
        <div className="mb-6 flex items-center justify-between gap-4">
          <div>
            <div className="eyebrow">Pipeline Preview</div>
            <h2 className="mt-2 text-2xl font-semibold tracking-[-0.03em] text-white">
              Dry Run {pipeName ? `· ${pipeName}` : ""}
            </h2>
            <p className="mt-3 max-w-3xl text-sm leading-6 text-slate-400">
              Preview fetch, diff, and transform output without writing to sinks or mutating state.
              This route runs against the real backend contract.
            </p>
          </div>
          <button
            onClick={onClose}
            className="rounded-full border border-white/10 bg-white/[0.04] p-2 text-slate-400 transition-colors hover:text-white"
          >
            <X className="h-5 w-5" />
          </button>
        </div>

        <div className="grid gap-6 xl:grid-cols-[360px_minmax(0,1fr)]">
          <section className="space-y-4">
            <div className="panel-subtle p-4">
              <div className="text-xs font-semibold uppercase tracking-[0.22em] text-slate-500">
                Runtime pipe
              </div>
              {isLoading || isFetching ? (
                <p className="mt-3 text-sm text-slate-400">Resolving pipe and effective queries...</p>
              ) : error ? (
                <div className="mt-3 space-y-3">
                  <p className="text-sm leading-6 text-rose-300">{error.message}</p>
                  <button
                    type="button"
                    onClick={() => refetch()}
                    className="action-button-secondary"
                  >
                    Retry Resolve
                  </button>
                </div>
              ) : data ? (
                <div className="mt-3 space-y-3 text-sm text-slate-300">
                  <div className="flex items-center justify-between rounded-2xl border border-white/8 bg-white/[0.03] px-4 py-3">
                    <span className="text-slate-500">Connector</span>
                    <span className="font-mono text-slate-100">{data.pipe.origin.connector}</span>
                  </div>
                  <div className="flex items-center justify-between rounded-2xl border border-white/8 bg-white/[0.03] px-4 py-3">
                    <span className="text-slate-500">Effective queries</span>
                    <span className="font-mono text-slate-100">{queryCount}</span>
                  </div>
                  <div className="rounded-2xl border border-white/8 bg-white/[0.03] px-4 py-3">
                    <div className="text-slate-500">Recipe</div>
                    <div className="mt-2 font-mono text-xs text-slate-100">
                      {data.pipe.recipe?.type ?? "manual"}
                    </div>
                    <div className="mt-1 text-xs text-slate-500">
                      {data.pipe.recipe?.prefix ?? "explicit queries"}
                    </div>
                  </div>
                </div>
              ) : null}
            </div>

            <div className="panel-subtle p-4">
              <label className="mb-2 block text-sm font-medium text-slate-200">
                Query
              </label>
              <select
                value={selectedQuery}
                onChange={(e) => setSelectedQuery(e.target.value)}
                disabled={!data || queryCount === 0}
                className={INPUT_CLS}
              >
                {data?.effective_queries.map((query) => (
                  <option key={query.id} value={query.id}>
                    {query.id}
                  </option>
                ))}
              </select>
              <p className="mt-2 text-xs leading-6 text-slate-500">
                {selectedQueryDef
                  ? `Key column: ${selectedQueryDef.key_column}`
                  : "Resolve a pipe to load its effective query set."}
              </p>
            </div>

            <div className="grid gap-4 sm:grid-cols-2">
              <div className="panel-subtle p-4">
                <label className="mb-2 block text-sm font-medium text-slate-200">
                  Mode
                </label>
                <select
                  value={mode}
                  onChange={(e) => setMode(e.target.value as DryRunMode)}
                  className={INPUT_CLS}
                >
                  <option value="live">live</option>
                  <option value="mock">mock</option>
                </select>
              </div>

              <div className="panel-subtle p-4">
                <label className="mb-2 block text-sm font-medium text-slate-200">
                  Row limit
                </label>
                <input
                  type="number"
                  min={1}
                  max={10000}
                  value={rowLimit}
                  onChange={(e) => setRowLimit(Number(e.target.value))}
                  className={INPUT_CLS}
                />
              </div>
            </div>

            <label className="panel-subtle flex items-start gap-3 p-4">
              <input
                type="checkbox"
                checked={useExistingState}
                onChange={(e) => setUseExistingState(e.target.checked)}
                className="mt-1 h-4 w-4 rounded border-white/10 bg-slate-950 text-emerald-300 focus:ring-emerald-300/35"
              />
              <span>
                <span className="block text-sm font-medium text-slate-200">
                  Diff against current state
                </span>
                <span className="mt-1 block text-sm leading-6 text-slate-400">
                  Compare against persisted snapshot keys instead of treating all fetched rows as new.
                </span>
              </span>
            </label>

            <div className="panel-subtle p-4">
              <div className="mb-3 text-sm font-medium text-slate-200">
                Transient credentials override
              </div>
              <div className="grid gap-3">
                <input
                  type="text"
                  value={credentialsUsername}
                  onChange={(e) => setCredentialsUsername(e.target.value)}
                  className={INPUT_CLS}
                  placeholder="optional username"
                />
                <input
                  type="password"
                  value={credentialsPassword}
                  onChange={(e) => setCredentialsPassword(e.target.value)}
                  className={INPUT_CLS}
                  placeholder="optional password"
                />
              </div>
              <p className="mt-2 text-xs leading-6 text-slate-500">
                Only used for this preview. Nothing is persisted.
              </p>
            </div>

            {mode === "mock" ? (
              <div className="panel-subtle p-4">
                <label className="mb-2 block text-sm font-medium text-slate-200">
                  Mock rows JSON
                </label>
                <textarea
                  value={mockDataText}
                  onChange={(e) => setMockDataText(e.target.value)}
                  rows={12}
                  spellCheck={false}
                  className="w-full rounded-2xl border border-white/10 bg-[#0a1320] px-4 py-3 font-mono text-sm text-slate-100 outline-none"
                />
              </div>
            ) : null}

            <button
              type="button"
              onClick={handleRun}
              disabled={!data || !selectedQuery || dryRun.isPending}
              className="action-button w-full justify-center disabled:cursor-not-allowed disabled:opacity-50"
            >
              <Play className="h-4 w-4" />
              {dryRun.isPending ? "Running preview..." : "Run Dry-Run"}
            </button>
          </section>

          <section className="space-y-4">
            <div className="grid gap-4 md:grid-cols-3">
              <div className="panel-subtle p-4">
                <div className="text-xs font-semibold uppercase tracking-[0.22em] text-slate-500">
                  Created
                </div>
                <div className="mt-3 font-mono text-3xl text-emerald-300">
                  {result?.changes.created ?? "--"}
                </div>
              </div>
              <div className="panel-subtle p-4">
                <div className="text-xs font-semibold uppercase tracking-[0.22em] text-slate-500">
                  Updated
                </div>
                <div className="mt-3 font-mono text-3xl text-sky-300">
                  {result?.changes.updated ?? "--"}
                </div>
              </div>
              <div className="panel-subtle p-4">
                <div className="text-xs font-semibold uppercase tracking-[0.22em] text-slate-500">
                  Deleted
                </div>
                <div className="mt-3 font-mono text-3xl text-rose-300">
                  {result?.changes.deleted ?? "--"}
                </div>
              </div>
            </div>

            <div className="grid gap-4 md:grid-cols-2">
              <div className="panel-subtle p-4">
                <div className="text-xs font-semibold uppercase tracking-[0.22em] text-slate-500">
                  Input sample
                </div>
                <p className="mt-2 text-sm leading-6 text-slate-400">
                  First fetched rows before diffing.
                </p>
                <pre className="mt-4 max-h-80 overflow-auto rounded-2xl border border-white/8 bg-[#08101a] px-4 py-3 font-mono text-xs leading-6 text-slate-200">
                  {result ? prettyJson(result.input_sample) : "Run a preview to inspect fetched rows."}
                </pre>
              </div>

              <div className="panel-subtle p-4">
                <div className="text-xs font-semibold uppercase tracking-[0.22em] text-slate-500">
                  After transform
                </div>
                <p className="mt-2 text-sm leading-6 text-slate-400">
                  Envelopes after diffing and transform application.
                </p>
                <pre className="mt-4 max-h-80 overflow-auto rounded-2xl border border-white/8 bg-[#08101a] px-4 py-3 font-mono text-xs leading-6 text-slate-200">
                  {result
                    ? prettyJson(result.after_transform)
                    : "Run a preview to inspect emitted envelopes."}
                </pre>
              </div>
            </div>

            <div className="panel-subtle p-4">
              <div className="text-xs font-semibold uppercase tracking-[0.22em] text-slate-500">
                Stats
              </div>
              <div className="mt-4 grid gap-3 md:grid-cols-4">
                <div className="rounded-2xl border border-white/8 bg-white/[0.03] px-4 py-3">
                  <div className="text-xs text-slate-500">Rows fetched</div>
                  <div className="mt-2 font-mono text-slate-100">
                    {result?.stats.rows_fetched ?? "--"}
                  </div>
                </div>
                <div className="rounded-2xl border border-white/8 bg-white/[0.03] px-4 py-3">
                  <div className="text-xs text-slate-500">Before transform</div>
                  <div className="mt-2 font-mono text-slate-100">
                    {result?.stats.events_before_transform ?? "--"}
                  </div>
                </div>
                <div className="rounded-2xl border border-white/8 bg-white/[0.03] px-4 py-3">
                  <div className="text-xs text-slate-500">After transform</div>
                  <div className="mt-2 font-mono text-slate-100">
                    {result?.stats.events_after_transform ?? "--"}
                  </div>
                </div>
                <div className="rounded-2xl border border-white/8 bg-white/[0.03] px-4 py-3">
                  <div className="text-xs text-slate-500">Filtered out</div>
                  <div className="mt-2 font-mono text-slate-100">
                    {result?.stats.events_filtered_out ?? "--"}
                  </div>
                </div>
              </div>
            </div>
          </section>
        </div>
      </div>
    </div>
  );
}

import {
  useCallback,
  startTransition,
  useEffect,
  useMemo,
  useState,
} from "react";
import { BookmarkPlus, Plus, Trash2, X } from "lucide-react";
import { useCreatePipe } from "@/api/pipes";
import { useCreatePipePreset, usePipePresets } from "@/api/pipePresets";
import { useSinks } from "@/api/sinks";
import { useToast } from "@/components/shared/useToast";
import type { PipePresetInfo, PipePresetSpec, PipeQueryDefinition } from "@/types/api";

type PipeMode = "manual" | "postgres_metadata" | "postgres_snapshot";
type DiffMode = "db" | "memory";

interface ManualQueryDraft {
  id: string;
  sql: string;
  key_column: string;
}

interface PipeFormProps {
  open: boolean;
  initialPresetName?: string | null;
  onClose: () => void;
}

const INPUT_CLS =
  "w-full rounded-2xl border border-white/10 bg-white/[0.04] px-4 py-3 text-sm text-slate-100 outline-none transition-colors focus:border-emerald-300/35";

function emptyManualQuery(): ManualQueryDraft {
  return {
    id: "",
    sql: "",
    key_column: "",
  };
}

function manualDraftsFromQueries(queries: PipeQueryDefinition[]): ManualQueryDraft[] {
  if (queries.length === 0) return [emptyManualQuery()];
  return queries.map((query) => ({
    id: query.id,
    sql: query.sql,
    key_column: query.key_column,
  }));
}

function normalizeMode(preset: PipePresetInfo): PipeMode {
  return preset.spec.recipe?.type ?? "manual";
}

export function PipeForm({
  open,
  initialPresetName = null,
  onClose,
}: PipeFormProps) {
  const { data: sinksData } = useSinks();
  const { data: presetsData } = usePipePresets();
  const createPipe = useCreatePipe();
  const createPreset = useCreatePipePreset();
  const { toast } = useToast();

  const [name, setName] = useState("");
  const [originDsn, setOriginDsn] = useState("");
  const [originCredential, setOriginCredential] = useState("");
  const [prefix, setPrefix] = useState("");
  const [pipeMode, setPipeMode] = useState<PipeMode>("postgres_snapshot");
  const [schemas, setSchemas] = useState("public");
  const [intervalSecs, setIntervalSecs] = useState(300);
  const [diffMode, setDiffMode] = useState<DiffMode>("db");
  const [selectedTargets, setSelectedTargets] = useState<string[]>([]);
  const [manualQueries, setManualQueries] = useState<ManualQueryDraft[]>([
    emptyManualQuery(),
  ]);
  const [selectedPresetName, setSelectedPresetName] = useState("");
  const [presetPanelOpen, setPresetPanelOpen] = useState(false);
  const [presetName, setPresetName] = useState("");
  const [presetDescription, setPresetDescription] = useState("");

  const sinks = sinksData?.sinks ?? [];
  const presets = useMemo(() => presetsData?.presets ?? [], [presetsData?.presets]);
  const normalizedQueries = manualQueries
    .map((query) => ({
      id: query.id.trim(),
      sql: query.sql.trim(),
      key_column: query.key_column.trim(),
    }))
    .filter((query) => query.id || query.sql || query.key_column);
  const hasManualQueries =
    normalizedQueries.length > 0 &&
    normalizedQueries.every((query) => query.id && query.sql && query.key_column);
  const canSavePreset = useMemo(
    () => (pipeMode === "manual" ? hasManualQueries : Boolean(prefix.trim())),
    [hasManualQueries, pipeMode, prefix],
  );
  const canSubmit = useMemo(
    () =>
      pipeMode === "manual"
        ? Boolean(
            name.trim() &&
              originDsn.trim() &&
              selectedTargets.length > 0 &&
              hasManualQueries,
          )
        : Boolean(
            name.trim() &&
              originDsn.trim() &&
              prefix.trim() &&
              selectedTargets.length > 0,
          ),
    [hasManualQueries, name, originDsn, pipeMode, prefix, selectedTargets.length],
  );

  useEffect(() => {
    if (!open) return;
    setName("");
    setOriginDsn("");
    setOriginCredential("");
    setPrefix("");
    setPipeMode("postgres_snapshot");
    setSchemas("public");
    setIntervalSecs(300);
    setDiffMode("db");
    setSelectedTargets([]);
    setManualQueries([emptyManualQuery()]);
    setSelectedPresetName("");
    setPresetPanelOpen(false);
    setPresetName("");
    setPresetDescription("");
  }, [open]);

  const applyPreset = useCallback((preset: PipePresetInfo) => {
    const mode = normalizeMode(preset);
    const recipe = preset.spec.recipe;
    const schedule = preset.spec.schedule;
    const delta = preset.spec.delta;

    setOriginDsn(preset.spec.origin_dsn ?? "");
    setOriginCredential(preset.spec.origin_credential ?? "");
    setPipeMode(mode);
    setPrefix(recipe?.prefix ?? "");
    setSchemas((recipe?.schemas ?? ["public"]).join(", "));
    setIntervalSecs(schedule?.interval_secs ?? 300);
    setDiffMode(delta?.diff_mode === "memory" ? "memory" : "db");
    setSelectedTargets(preset.spec.targets ?? []);
    setManualQueries(manualDraftsFromQueries(preset.spec.queries ?? []));
    setPresetPanelOpen(false);
  }, []);

  useEffect(() => {
    if (!open || !initialPresetName) return;
    const preset = presets.find((item) => item.name === initialPresetName);
    if (!preset) return;
    setSelectedPresetName(initialPresetName);
    startTransition(() => applyPreset(preset));
  }, [applyPreset, initialPresetName, open, presets]);

  if (!open) return null;

  function toggleTarget(target: string) {
    setSelectedTargets((current) =>
      current.includes(target)
        ? current.filter((item) => item !== target)
        : [...current, target],
    );
  }

  function updateManualQuery(index: number, patch: Partial<ManualQueryDraft>) {
    setManualQueries((current) =>
      current.map((query, queryIndex) =>
        queryIndex === index ? { ...query, ...patch } : query,
      ),
    );
  }

  function addManualQuery() {
    setManualQueries((current) => [...current, emptyManualQuery()]);
  }

  function removeManualQuery(index: number) {
    setManualQueries((current) =>
      current.length === 1
        ? [emptyManualQuery()]
        : current.filter((_, queryIndex) => queryIndex !== index),
    );
  }

  function buildRecipe() {
    if (pipeMode === "manual") return undefined;
    return {
      type: pipeMode,
      prefix,
      entity_type_id: "postgres",
      schema_id: "table",
      schemas: schemas
        .split(",")
        .map((value) => value.trim())
        .filter(Boolean),
    };
  }

  function buildPresetSpec(): PipePresetSpec {
    return {
      origin_connector: "postgres",
      origin_dsn: originDsn.trim(),
      origin_credential: originCredential.trim() || undefined,
      origin_config: {},
      targets: selectedTargets,
      queries:
        pipeMode === "manual"
          ? normalizedQueries.map((query) => ({
              id: query.id,
              sql: query.sql,
              key_column: query.key_column,
            }))
          : [],
      schedule: {
        interval_secs: intervalSecs,
        missed_tick_policy: "skip",
      },
      delta: {
        diff_mode: diffMode,
        fail_safe_threshold: 30,
      },
      retry: {
        max_retries: 3,
        retry_base_delay_secs: 5,
      },
      recipe: buildRecipe(),
      filters: [],
      transforms: [],
      links: [],
    };
  }

  function handlePresetSelection(presetNameValue: string) {
    setSelectedPresetName(presetNameValue);
    if (!presetNameValue) return;
    const preset = presets.find((item) => item.name === presetNameValue);
    if (!preset) return;
    startTransition(() => applyPreset(preset));
  }

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    const spec = buildPresetSpec();

    createPipe.mutate(
      {
        name,
        ...spec,
        origin_credential: spec.origin_credential ?? undefined,
        recipe: spec.recipe ?? undefined,
      },
      {
        onSuccess: () => {
          toast("success", `Pipe "${name}" created`);
          onClose();
        },
        onError: (err) => toast("error", err.message),
      },
    );
  }

  function handleSavePreset() {
    if (!presetName.trim()) {
      toast("error", "Preset name is required");
      return;
    }
    if (!canSavePreset) {
      toast("error", "Preset needs either a valid recipe or valid manual queries");
      return;
    }

    createPreset.mutate(
      {
        name: presetName.trim(),
        description: presetDescription.trim() || undefined,
        spec: buildPresetSpec(),
      },
      {
        onSuccess: () => {
          const savedName = presetName.trim();
          toast("success", `Preset "${savedName}" saved`);
          setSelectedPresetName(savedName);
          setPresetPanelOpen(false);
          setPresetName("");
          setPresetDescription("");
        },
        onError: (err) => toast("error", err.message),
      },
    );
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      <div className="fixed inset-0 bg-black/60" onClick={onClose} />
      <div className="panel-surface relative max-h-[92vh] w-full max-w-4xl overflow-y-auto p-6 shadow-2xl">
        <div className="mb-6 flex items-center justify-between">
          <div>
            <div className="eyebrow">Runnable Onboarding</div>
            <h2 className="mt-2 text-2xl font-semibold tracking-[-0.03em] text-white">
              Create PostgreSQL Pipe
            </h2>
          </div>
          <button
            onClick={onClose}
            className="rounded-full border border-white/10 bg-white/[0.04] p-2 text-slate-400 transition-colors hover:text-white"
          >
            <X className="h-5 w-5" />
          </button>
        </div>

        <form onSubmit={handleSubmit} className="space-y-6">
          <div className="panel-subtle p-4">
            <div className="grid gap-4 lg:grid-cols-[minmax(0,1fr)_auto] lg:items-end">
              <div>
                <label className="mb-2 block text-sm font-medium text-slate-200">
                  Preset
                </label>
                <select
                  value={selectedPresetName}
                  onChange={(e) => handlePresetSelection(e.target.value)}
                  className={INPUT_CLS}
                >
                  <option value="">custom / empty</option>
                  {presets.map((preset) => (
                    <option key={preset.name} value={preset.name}>
                      {preset.name}
                    </option>
                  ))}
                </select>
                <p className="mt-2 text-sm leading-6 text-slate-400">
                  Saved presets are reusable control-plane templates. They are included in config export/import automatically.
                </p>
              </div>
              <button
                type="button"
                onClick={() => {
                  setPresetName(selectedPresetName);
                  setPresetDescription(
                    presets.find((preset) => preset.name === selectedPresetName)?.description ??
                      "",
                  );
                  setPresetPanelOpen((current) => !current);
                }}
                className="action-button-secondary"
              >
                <BookmarkPlus className="h-4 w-4" />
                Save As Preset
              </button>
            </div>

            {presetPanelOpen ? (
              <div className="mt-4 grid gap-4 rounded-2xl border border-white/8 bg-white/[0.03] p-4 lg:grid-cols-[minmax(0,1fr)_minmax(0,1fr)_auto] lg:items-end">
                <div>
                  <label className="mb-2 block text-sm font-medium text-slate-200">
                    Preset Name
                  </label>
                  <input
                    type="text"
                    value={presetName}
                    onChange={(e) => setPresetName(e.target.value)}
                    className={INPUT_CLS}
                    placeholder="postgres-columns-aspect"
                  />
                </div>
                <div>
                  <label className="mb-2 block text-sm font-medium text-slate-200">
                    Description
                  </label>
                  <input
                    type="text"
                    value={presetDescription}
                    onChange={(e) => setPresetDescription(e.target.value)}
                    className={INPUT_CLS}
                    placeholder="Reusable custom aspect preset"
                  />
                </div>
                <button
                  type="button"
                  onClick={handleSavePreset}
                  disabled={createPreset.isPending || !canSavePreset}
                  className="action-button disabled:cursor-not-allowed disabled:opacity-50"
                >
                  {createPreset.isPending ? "Saving..." : "Save Preset"}
                </button>
              </div>
            ) : null}
          </div>

          <div className="grid gap-4 md:grid-cols-2">
            <div>
              <label className="mb-2 block text-sm font-medium text-slate-200">
                Pipe Name
              </label>
              <input
                type="text"
                value={name}
                onChange={(e) => setName(e.target.value)}
                className={INPUT_CLS}
                placeholder="some-postgresql-source-snapshot"
                required
              />
            </div>
            <div>
              <label className="mb-2 block text-sm font-medium text-slate-200">
                Mode
              </label>
              <select
                value={pipeMode}
                onChange={(e) => setPipeMode(e.target.value as PipeMode)}
                className={INPUT_CLS}
              >
                <option value="manual">manual</option>
                <option value="postgres_snapshot">postgres_snapshot</option>
                <option value="postgres_metadata">postgres_metadata</option>
              </select>
            </div>
          </div>

          <div>
            <label className="mb-2 block text-sm font-medium text-slate-200">
              PostgreSQL DSN
            </label>
            <input
              type="text"
              value={originDsn}
              onChange={(e) => setOriginDsn(e.target.value)}
              className={INPUT_CLS}
              placeholder="postgres://user:pass@host:5432/db"
              required
            />
            <p className="mt-2 text-sm leading-6 text-slate-400">
              This is the actual runtime origin DSN used by the connector. Recipe introspection and manual SQL both use it directly.
            </p>
          </div>

          {pipeMode === "manual" ? (
            <div className="panel-subtle p-4">
              <div className="mb-2 text-sm font-medium text-slate-200">
                Manual Queries
              </div>
              <p className="text-sm leading-6 text-slate-400">
                Define explicit query ids and SQL. This is the path for custom aspects and non-standard onboarding without a built-in recipe.
              </p>

              <div className="mt-4 space-y-4">
                {manualQueries.map((query, index) => (
                  <div
                    key={`${index}-${query.id}`}
                    className="rounded-2xl border border-white/10 bg-white/[0.03] p-4"
                  >
                    <div className="mb-4 flex items-center justify-between gap-3">
                      <div className="text-sm font-medium text-slate-200">
                        Query {index + 1}
                      </div>
                      <button
                        type="button"
                        onClick={() => removeManualQuery(index)}
                        className="rounded-xl border border-white/8 bg-white/[0.03] p-2 text-slate-400 transition-colors hover:border-rose-300/25 hover:text-rose-300"
                      >
                        <Trash2 className="h-4 w-4" />
                      </button>
                    </div>

                    <div className="grid gap-4 md:grid-cols-2">
                      <div>
                        <label className="mb-2 block text-sm font-medium text-slate-200">
                          Query ID
                        </label>
                        <input
                          type="text"
                          value={query.id}
                          onChange={(e) =>
                            updateManualQuery(index, { id: e.target.value })
                          }
                          className={INPUT_CLS}
                          placeholder="aspect-columns"
                        />
                      </div>
                      <div>
                        <label className="mb-2 block text-sm font-medium text-slate-200">
                          Key Column
                        </label>
                        <input
                          type="text"
                          value={query.key_column}
                          onChange={(e) =>
                            updateManualQuery(index, {
                              key_column: e.target.value,
                            })
                          }
                          className={INPUT_CLS}
                          placeholder="entityId"
                        />
                      </div>
                    </div>

                    <div className="mt-4">
                      <label className="mb-2 block text-sm font-medium text-slate-200">
                        SQL
                      </label>
                      <textarea
                        value={query.sql}
                        onChange={(e) =>
                          updateManualQuery(index, { sql: e.target.value })
                        }
                        rows={8}
                        spellCheck={false}
                        className="w-full rounded-2xl border border-white/10 bg-[#0a1320] px-4 py-3 font-mono text-sm text-slate-100 outline-none"
                        placeholder="SELECT entity_id AS entityId, payload FROM ..."
                      />
                    </div>
                  </div>
                ))}
              </div>

              <button
                type="button"
                onClick={addManualQuery}
                className="action-button-secondary mt-4"
              >
                <Plus className="h-4 w-4" />
                Add Query
              </button>
            </div>
          ) : (
            <div className="grid gap-4 md:grid-cols-3">
              <div>
                <label className="mb-2 block text-sm font-medium text-slate-200">
                  Prefix
                </label>
                <input
                  type="text"
                  value={prefix}
                  onChange={(e) => setPrefix(e.target.value)}
                  className={INPUT_CLS}
                  placeholder="some-postgresql-source"
                  required
                />
              </div>
              <div>
                <label className="mb-2 block text-sm font-medium text-slate-200">
                  Schemas
                </label>
                <input
                  type="text"
                  value={schemas}
                  onChange={(e) => setSchemas(e.target.value)}
                  className={INPUT_CLS}
                  placeholder="public, analytics"
                />
              </div>
              <div>
                <label className="mb-2 block text-sm font-medium text-slate-200">
                  Credential Name
                </label>
                <input
                  type="text"
                  value={originCredential}
                  onChange={(e) => setOriginCredential(e.target.value)}
                  className={INPUT_CLS}
                  placeholder="optional-credential-ref"
                />
              </div>
            </div>
          )}

          {pipeMode === "manual" ? (
            <div>
              <label className="mb-2 block text-sm font-medium text-slate-200">
                Credential Name
              </label>
              <input
                type="text"
                value={originCredential}
                onChange={(e) => setOriginCredential(e.target.value)}
                className={INPUT_CLS}
                placeholder="optional-credential-ref"
              />
            </div>
          ) : null}

          <div className="grid gap-4 md:grid-cols-2">
            <div>
              <label className="mb-2 block text-sm font-medium text-slate-200">
                Interval Seconds
              </label>
              <input
                type="number"
                min={1}
                value={intervalSecs}
                onChange={(e) => setIntervalSecs(Number(e.target.value))}
                className={INPUT_CLS}
              />
            </div>
            <div>
              <label className="mb-2 block text-sm font-medium text-slate-200">
                Diff Mode
              </label>
              <select
                value={diffMode}
                onChange={(e) => setDiffMode(e.target.value as DiffMode)}
                className={INPUT_CLS}
              >
                <option value="db">db</option>
                <option value="memory">memory</option>
              </select>
            </div>
          </div>

          <div className="panel-subtle p-4">
            <div className="mb-3 text-sm font-medium text-slate-200">Target Sinks</div>
            {sinks.length === 0 ? (
              <p className="text-sm leading-6 text-slate-400">
                No sinks configured yet. Create a sink first, then return here to bind targets.
              </p>
            ) : (
              <div className="grid gap-3 md:grid-cols-2">
                {sinks.map((sink) => (
                  <label
                    key={sink.name}
                    className="flex items-center gap-3 rounded-2xl border border-white/10 bg-white/[0.03] px-4 py-3"
                  >
                    <input
                      type="checkbox"
                      checked={selectedTargets.includes(sink.name)}
                      onChange={() => toggleTarget(sink.name)}
                      className="h-4 w-4 rounded border-white/20 bg-transparent"
                    />
                    <div>
                      <div className="text-sm font-medium text-white">{sink.name}</div>
                      <div className="text-xs font-mono text-slate-500">
                        {sink.sink_type}
                      </div>
                    </div>
                  </label>
                ))}
              </div>
            )}
          </div>

          <div className="flex justify-end gap-3 pt-2">
            <button
              type="button"
              onClick={onClose}
              className="action-button-secondary"
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={createPipe.isPending || !canSubmit}
              className="action-button disabled:cursor-not-allowed disabled:opacity-50"
            >
              {createPipe.isPending ? "Creating..." : "Create Pipe"}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

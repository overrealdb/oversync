import { Plus, Trash2 } from "lucide-react";
import type { SinkInfo } from "@/types/api";
import type { RecipeDraftState, DiffMode, ManualQueryDraft, PipeMode } from "./recipeDraft";
import { emptyManualQuery } from "./recipeDraft";

export const INPUT_CLS =
  "w-full rounded-2xl border border-white/10 bg-white/[0.04] px-4 py-3 text-sm text-slate-100 outline-none transition-colors focus:border-emerald-300/35";

interface RecipeSpecEditorProps {
  draft: RecipeDraftState;
  sinks: SinkInfo[];
  onDraftChange: (updater: (current: RecipeDraftState) => RecipeDraftState) => void;
}

export function RecipeSpecEditor({
  draft,
  sinks,
  onDraftChange,
}: RecipeSpecEditorProps) {
  function patchDraft(patch: Partial<RecipeDraftState>) {
    onDraftChange((current) => ({ ...current, ...patch }));
  }

  function toggleTarget(target: string) {
    onDraftChange((current) => ({
      ...current,
      selectedTargets: current.selectedTargets.includes(target)
        ? current.selectedTargets.filter((item) => item !== target)
        : [...current.selectedTargets, target],
    }));
  }

  function updateManualQuery(index: number, patch: Partial<ManualQueryDraft>) {
    onDraftChange((current) => ({
      ...current,
      manualQueries: current.manualQueries.map((query, queryIndex) =>
        queryIndex === index ? { ...query, ...patch } : query,
      ),
    }));
  }

  function addManualQuery() {
    onDraftChange((current) => ({
      ...current,
      manualQueries: [...current.manualQueries, emptyManualQuery()],
    }));
  }

  function removeManualQuery(index: number) {
    onDraftChange((current) => ({
      ...current,
      manualQueries:
        current.manualQueries.length === 1
          ? [emptyManualQuery()]
          : current.manualQueries.filter((_, queryIndex) => queryIndex !== index),
    }));
  }

  return (
    <>
      <div className="grid gap-4 md:grid-cols-2">
        <div>
          <label className="mb-2 block text-sm font-medium text-slate-200">Mode</label>
          <select
            value={draft.pipeMode}
            onChange={(e) => patchDraft({ pipeMode: e.target.value as PipeMode })}
            className={INPUT_CLS}
          >
            <option value="manual">manual</option>
            <option value="postgres_snapshot">postgres_snapshot</option>
            <option value="postgres_metadata">postgres_metadata</option>
          </select>
        </div>
        <div>
          <label className="mb-2 block text-sm font-medium text-slate-200">
            PostgreSQL DSN
          </label>
          <input
            type="text"
            value={draft.originDsn}
            onChange={(e) => patchDraft({ originDsn: e.target.value })}
            className={INPUT_CLS}
            placeholder="postgres://user:pass@host:5432/db"
            required
          />
        </div>
      </div>

      {draft.pipeMode === "manual" ? (
        <div className="panel-subtle p-4">
          <div className="mb-2 text-sm font-medium text-slate-200">Manual Queries</div>
          <p className="text-sm leading-6 text-slate-400">
            Define explicit query ids and SQL. This is the path for custom aspects and non-standard onboarding without a built-in recipe.
          </p>

          <div className="mt-4 space-y-4">
            {draft.manualQueries.map((query, index) => (
              <div
                key={`${index}-${query.id}`}
                className="rounded-2xl border border-white/10 bg-white/[0.03] p-4"
              >
                <div className="mb-4 flex items-center justify-between gap-3">
                  <div className="text-sm font-medium text-slate-200">Query {index + 1}</div>
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
                      onChange={(e) => updateManualQuery(index, { id: e.target.value })}
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
                        updateManualQuery(index, { key_column: e.target.value })
                      }
                      className={INPUT_CLS}
                      placeholder="entityId"
                    />
                  </div>
                </div>

                <div className="mt-4">
                  <label className="mb-2 block text-sm font-medium text-slate-200">SQL</label>
                  <textarea
                    value={query.sql}
                    onChange={(e) => updateManualQuery(index, { sql: e.target.value })}
                    rows={8}
                    spellCheck={false}
                    className="w-full rounded-2xl border border-white/10 bg-[#0a1320] px-4 py-3 font-mono text-sm text-slate-100 outline-none"
                    placeholder="SELECT entity_id AS entityId, payload FROM ..."
                  />
                </div>
              </div>
            ))}
          </div>

          <button type="button" onClick={addManualQuery} className="action-button-secondary mt-4">
            <Plus className="h-4 w-4" />
            Add Query
          </button>
        </div>
      ) : (
        <div className="grid gap-4 md:grid-cols-3">
          <div>
            <label className="mb-2 block text-sm font-medium text-slate-200">Prefix</label>
            <input
              type="text"
              value={draft.prefix}
              onChange={(e) => patchDraft({ prefix: e.target.value })}
              className={INPUT_CLS}
              placeholder="some-postgresql-source"
              required
            />
          </div>
          <div>
            <label className="mb-2 block text-sm font-medium text-slate-200">Schemas</label>
            <input
              type="text"
              value={draft.schemas}
              onChange={(e) => patchDraft({ schemas: e.target.value })}
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
              value={draft.originCredential}
              onChange={(e) => patchDraft({ originCredential: e.target.value })}
              className={INPUT_CLS}
              placeholder="optional-credential-ref"
            />
          </div>
        </div>
      )}

      {draft.pipeMode === "manual" ? (
        <div>
          <label className="mb-2 block text-sm font-medium text-slate-200">
            Credential Name
          </label>
          <input
            type="text"
            value={draft.originCredential}
            onChange={(e) => patchDraft({ originCredential: e.target.value })}
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
            value={draft.intervalSecs}
            onChange={(e) => patchDraft({ intervalSecs: Number(e.target.value) })}
            className={INPUT_CLS}
          />
        </div>
        <div>
          <label className="mb-2 block text-sm font-medium text-slate-200">Diff Mode</label>
          <select
            value={draft.diffMode}
            onChange={(e) => patchDraft({ diffMode: e.target.value as DiffMode })}
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
                  checked={draft.selectedTargets.includes(sink.name)}
                  onChange={() => toggleTarget(sink.name)}
                  className="h-4 w-4 rounded border-white/20 bg-transparent"
                />
                <div>
                  <div className="text-sm font-medium text-white">{sink.name}</div>
                  <div className="text-xs font-mono text-slate-500">{sink.sink_type}</div>
                </div>
              </label>
            ))}
          </div>
        )}
      </div>
    </>
  );
}

import {
  useCallback,
  startTransition,
  useEffect,
  useMemo,
  useState,
} from "react";
import { BookmarkPlus, X } from "lucide-react";
import { useCreatePipe, useResolvePipe, useUpdatePipe } from "@/api/pipes";
import { useCreatePipePreset, usePipePresets } from "@/api/pipePresets";
import { RecipeParameterValuesDialog } from "@/components/pipes/RecipeParameterValuesDialog";
import { useSinks } from "@/api/sinks";
import { RecipeSpecEditor, INPUT_CLS } from "@/components/pipes/RecipeSpecEditor";
import { useToast } from "@/components/shared/useToast";
import type {
  PipePresetInfo,
  RuntimePipeConfig,
} from "@/types/api";
import {
  buildPipePayload,
  buildPipePresetSpec,
  createDefaultRecipeDraft,
  draftFromPreset,
  draftFromRuntimePipe,
  hasCompleteManualQueries,
  normalizePresetParameters,
  type RecipeDraftState,
} from "./recipeDraft";

interface PipeFormProps {
  open: boolean;
  initialPresetName?: string | null;
  pipeName?: string | null;
  onClose: () => void;
}

export function PipeForm({
  open,
  initialPresetName = null,
  pipeName = null,
  onClose,
}: PipeFormProps) {
  const isEditMode = Boolean(pipeName);
  const { data: sinksData } = useSinks();
  const { data: presetsData } = usePipePresets();
  const resolvePipe = useResolvePipe(pipeName, open && isEditMode);
  const createPipe = useCreatePipe();
  const updatePipe = useUpdatePipe(pipeName ?? "");
  const createPreset = useCreatePipePreset();
  const { toast } = useToast();
  const resolvedPipeData = resolvePipe.data ?? null;

  const [name, setName] = useState("");
  const [draft, setDraft] = useState<RecipeDraftState>(createDefaultRecipeDraft);
  const [enabled, setEnabled] = useState(true);
  const [selectedPresetName, setSelectedPresetName] = useState("");
  const [presetPanelOpen, setPresetPanelOpen] = useState(false);
  const [presetName, setPresetName] = useState("");
  const [presetDescription, setPresetDescription] = useState("");
  const [parameterPreset, setParameterPreset] = useState<PipePresetInfo | null>(null);

  const sinks = sinksData?.sinks ?? [];
  const presets = useMemo(() => presetsData?.presets ?? [], [presetsData?.presets]);
  const hasManualQueries = hasCompleteManualQueries(draft);
  const canSavePreset = useMemo(
    () => (draft.pipeMode === "manual" ? hasManualQueries : Boolean(draft.prefix.trim())),
    [draft.pipeMode, draft.prefix, hasManualQueries],
  );
  const canSubmit = useMemo(
    () =>
      draft.pipeMode === "manual"
        ? Boolean(
            name.trim() &&
              draft.originDsn.trim() &&
              draft.selectedTargets.length > 0 &&
              hasManualQueries,
          )
        : Boolean(
            name.trim() &&
              draft.originDsn.trim() &&
              draft.prefix.trim() &&
              draft.selectedTargets.length > 0,
          ),
    [draft.originDsn, draft.pipeMode, draft.prefix, draft.selectedTargets.length, hasManualQueries, name],
  );

  const resetDraft = useCallback(() => {
    setName("");
    setDraft(createDefaultRecipeDraft());
    setEnabled(true);
    setSelectedPresetName("");
    setPresetPanelOpen(false);
    setPresetName("");
    setPresetDescription("");
    setParameterPreset(null);
  }, []);

  const applyResolvedPipe = useCallback((pipe: RuntimePipeConfig) => {
    setName(pipe.name);
    setDraft(draftFromRuntimePipe(pipe));
    setEnabled(pipe.enabled);
    setSelectedPresetName("");
    setPresetPanelOpen(false);
    setPresetName("");
    setPresetDescription("");
    setParameterPreset(null);
  }, []);

  useEffect(() => {
    if (!open) return;
    resetDraft();
  }, [open, resetDraft, pipeName]);

  useEffect(() => {
    if (!open || !isEditMode || !resolvedPipeData) return;
    startTransition(() => applyResolvedPipe(resolvedPipeData.pipe));
  }, [applyResolvedPipe, isEditMode, open, resolvedPipeData]);

  const applyPreset = useCallback((preset: PipePresetInfo) => {
    setDraft(draftFromPreset(preset));
    setPresetPanelOpen(false);
  }, []);

  const requestPresetApplication = useCallback(
    (preset: PipePresetInfo) => {
      const parameters = normalizePresetParameters(preset.spec.parameters);
      if (parameters.length === 0) {
        setSelectedPresetName(preset.name);
        startTransition(() => applyPreset(preset));
        return;
      }
      setParameterPreset(preset);
    },
    [applyPreset],
  );

  useEffect(() => {
    if (!open || !initialPresetName || isEditMode) return;
    const preset = presets.find((item) => item.name === initialPresetName);
    if (!preset) return;
    requestPresetApplication(preset);
  }, [initialPresetName, isEditMode, open, presets, requestPresetApplication]);

  if (!open) return null;

  const resolvedPipe = resolvedPipeData ? resolvedPipeData.pipe : null;
  const submitPending = createPipe.isPending || updatePipe.isPending;
  const loadingEditState = isEditMode && resolvePipe.isLoading && !resolvedPipe;

  function handlePresetSelection(presetNameValue: string) {
    if (!presetNameValue) {
      setSelectedPresetName("");
      return;
    }
    const preset = presets.find((item) => item.name === presetNameValue);
    if (!preset) return;
    requestPresetApplication(preset);
  }

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    const payload = buildPipePayload(draft);

    if (isEditMode && pipeName) {
      updatePipe.mutate(
        {
          ...payload,
          recipe: payload.recipe,
          enabled,
        },
        {
          onSuccess: () => {
            toast("success", `Pipe "${pipeName}" updated`);
            onClose();
          },
          onError: (err) => toast("error", err.message),
        },
      );
      return;
    }

    createPipe.mutate(
      {
        name: name.trim(),
        ...payload,
        recipe: payload.recipe ?? undefined,
      },
      {
        onSuccess: () => {
          toast("success", `Pipe "${name.trim()}" created`);
          onClose();
        },
        onError: (err) => toast("error", err.message),
      },
    );
  }

  function handleSavePreset() {
    if (!presetName.trim()) {
      toast("error", "Recipe name is required");
      return;
    }
    if (!canSavePreset) {
      toast("error", "Saved recipe needs either a valid recipe draft or valid manual queries");
      return;
    }

    createPreset.mutate(
      {
        name: presetName.trim(),
        description: presetDescription.trim() || undefined,
        spec: buildPipePresetSpec(draft),
      },
      {
        onSuccess: () => {
          const savedName = presetName.trim();
          toast("success", `Saved recipe "${savedName}" saved`);
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
              {isEditMode ? "Edit PostgreSQL Pipe" : "Create PostgreSQL Pipe"}
            </h2>
          </div>
          <button
            onClick={onClose}
            className="rounded-full border border-white/10 bg-white/[0.04] p-2 text-slate-400 transition-colors hover:text-white"
          >
            <X className="h-5 w-5" />
          </button>
        </div>

        {loadingEditState ? (
          <div className="panel-subtle flex items-center justify-center p-10 text-sm text-slate-400">
            Loading persisted pipe config...
          </div>
        ) : null}

        {!loadingEditState && resolvePipe.isError ? (
          <div className="rounded-2xl border border-rose-300/20 bg-rose-400/10 px-4 py-3 text-sm text-rose-100">
            Failed to load pipe details: {resolvePipe.error.message}
          </div>
        ) : null}

        <form onSubmit={handleSubmit} className={`space-y-6 ${loadingEditState ? "hidden" : ""}`}>
          <div className="panel-subtle p-4">
            <div className="grid gap-4 lg:grid-cols-[minmax(0,1fr)_auto] lg:items-end">
              <div>
                <label className="mb-2 block text-sm font-medium text-slate-200">
                  Saved Recipe
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
                  Saved recipes are reusable control-plane templates. They are included in config export/import automatically.
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
                Save As Recipe
              </button>
            </div>

            {presetPanelOpen ? (
              <div className="mt-4 grid gap-4 rounded-2xl border border-white/8 bg-white/[0.03] p-4 lg:grid-cols-[minmax(0,1fr)_minmax(0,1fr)_auto] lg:items-end">
                <div>
                  <label className="mb-2 block text-sm font-medium text-slate-200">
                    Recipe Name
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
                    placeholder="Reusable custom aspect recipe"
                  />
                </div>
                <button
                  type="button"
                  onClick={handleSavePreset}
                  disabled={createPreset.isPending || !canSavePreset}
                  className="action-button disabled:cursor-not-allowed disabled:opacity-50"
                >
                  {createPreset.isPending ? "Saving..." : "Save Recipe"}
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
                disabled={isEditMode}
                required
              />
              {isEditMode ? (
                <p className="mt-2 text-sm leading-6 text-slate-400">
                  Pipe renaming is not supported through the control plane yet. Edit the runtime config in place instead.
                </p>
              ) : null}
            </div>
            <div className="rounded-2xl border border-white/8 bg-white/[0.03] px-4 py-3 text-sm leading-6 text-slate-400">
              Pipes are runnable sync units. Saved recipes fill defaults; pipe edits control actual scheduler state, enablement, and runtime expansion.
            </div>
          </div>

          <RecipeSpecEditor draft={draft} sinks={sinks} onDraftChange={setDraft} />

          <label className="flex items-center gap-3 rounded-2xl border border-white/10 bg-white/[0.03] px-4 py-3">
            <input
              type="checkbox"
              checked={enabled}
              onChange={(e) => setEnabled(e.target.checked)}
              className="h-4 w-4 rounded border-white/20 bg-transparent"
            />
            <div>
              <div className="text-sm font-medium text-white">Pipe enabled</div>
              <div className="text-xs text-slate-500">
                Disabled pipes stay persisted and exportable, but scheduler expansion skips them.
              </div>
            </div>
          </label>

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
              disabled={submitPending || !canSubmit}
              className="action-button disabled:cursor-not-allowed disabled:opacity-50"
            >
              {submitPending
                ? isEditMode
                  ? "Saving..."
                  : "Creating..."
                : isEditMode
                  ? "Save Pipe"
                  : "Create Pipe"}
            </button>
          </div>
        </form>
      </div>
      <RecipeParameterValuesDialog
        open={parameterPreset !== null}
        preset={parameterPreset}
        onCancel={() => setParameterPreset(null)}
        onApply={(materializedPreset) => {
          setSelectedPresetName(materializedPreset.name);
          setParameterPreset(null);
          startTransition(() => applyPreset(materializedPreset));
        }}
      />
    </div>
  );
}

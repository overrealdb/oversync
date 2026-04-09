import { startTransition, useEffect, useMemo, useState } from "react";
import { CopyPlus, X } from "lucide-react";
import {
  useCreatePipePreset,
  useUpdatePipePreset,
} from "@/api/pipePresets";
import { RecipeParametersEditor } from "@/components/pipes/RecipeParametersEditor";
import { useSinks } from "@/api/sinks";
import { RecipeSpecEditor, INPUT_CLS } from "@/components/pipes/RecipeSpecEditor";
import { useToast } from "@/components/shared/useToast";
import type { PipePresetInfo, PipePresetParameter } from "@/types/api";
import {
  buildPipePresetSpecWithParameters,
  createDefaultRecipeDraft,
  draftFromPreset,
  hasCompleteManualQueries,
  normalizePresetParameters,
  type RecipeDraftState,
} from "./recipeDraft";

type SavedRecipeEditorMode = "create" | "edit" | "duplicate";

interface SavedRecipeFormProps {
  open: boolean;
  mode: SavedRecipeEditorMode;
  preset?: PipePresetInfo | null;
  onClose: () => void;
}

function canSubmitDraft(draft: RecipeDraftState): boolean {
  return draft.pipeMode === "manual"
    ? Boolean(
        draft.originDsn.trim() &&
          draft.selectedTargets.length > 0 &&
          hasCompleteManualQueries(draft),
      )
    : Boolean(
        draft.originDsn.trim() &&
          draft.prefix.trim() &&
          draft.selectedTargets.length > 0,
      );
}

function duplicateName(name: string): string {
  if (!name.trim()) return "";
  return `${name.trim()}-copy`;
}

export function SavedRecipeForm({
  open,
  mode,
  preset = null,
  onClose,
}: SavedRecipeFormProps) {
  const { data: sinksData } = useSinks();
  const createPreset = useCreatePipePreset();
  const updatePreset = useUpdatePipePreset(mode === "edit" ? preset?.name ?? "" : "");
  const { toast } = useToast();

  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [draft, setDraft] = useState<RecipeDraftState>(createDefaultRecipeDraft);
  const [parameters, setParameters] = useState<PipePresetParameter[]>([]);

  const sinks = sinksData?.sinks ?? [];
  const isEditMode = mode === "edit";
  const submitPending = createPreset.isPending || updatePreset.isPending;
  const canSubmit = useMemo(
    () => Boolean(name.trim()) && canSubmitDraft(draft),
    [draft, name],
  );

  useEffect(() => {
    if (!open) return;

    if (!preset) {
      setName("");
      setDescription("");
      setDraft(createDefaultRecipeDraft());
      setParameters([]);
      return;
    }

    startTransition(() => {
      setName(mode === "duplicate" ? duplicateName(preset.name) : preset.name);
      setDescription(preset.description?.trim() ?? "");
      setDraft(draftFromPreset(preset));
      setParameters(preset.spec.parameters ?? []);
    });
  }, [mode, open, preset]);

  if (!open) return null;

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    const payload = {
      description: description.trim() || undefined,
      spec: buildPipePresetSpecWithParameters(draft, normalizePresetParameters(parameters)),
    };

    if (isEditMode && preset) {
      updatePreset.mutate(payload, {
        onSuccess: () => {
          toast("success", `Saved recipe "${preset.name}" updated`);
          onClose();
        },
        onError: (err) => toast("error", err.message),
      });
      return;
    }

    createPreset.mutate(
      {
        name: name.trim(),
        ...payload,
      },
      {
        onSuccess: () => {
          toast("success", `Saved recipe "${name.trim()}" created`);
          onClose();
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
            <div className="eyebrow">Saved Recipe</div>
            <h2 className="mt-2 text-2xl font-semibold tracking-[-0.03em] text-white">
              {mode === "edit"
                ? "Edit Saved Recipe"
                : mode === "duplicate"
                  ? "Duplicate Saved Recipe"
                  : "Create Saved Recipe"}
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
            <div className="grid gap-4 md:grid-cols-2">
              <div>
                <label className="mb-2 block text-sm font-medium text-slate-200">
                  Recipe Name
                </label>
                <input
                  type="text"
                  value={name}
                  onChange={(e) => setName(e.target.value)}
                  className={INPUT_CLS}
                  placeholder="some-postgresql-source-columns"
                  disabled={isEditMode}
                  required
                />
                {isEditMode ? (
                  <p className="mt-2 text-sm leading-6 text-slate-400">
                    Saved recipe renaming is not supported yet. Update the persisted template in place.
                  </p>
                ) : null}
              </div>
              <div>
                <label className="mb-2 block text-sm font-medium text-slate-200">
                  Description
                </label>
                <input
                  type="text"
                  value={description}
                  onChange={(e) => setDescription(e.target.value)}
                  className={INPUT_CLS}
                  placeholder="Reusable onboarding defaults for a custom aspect"
                />
              </div>
            </div>
            <div className="mt-4 rounded-2xl border border-white/8 bg-white/[0.03] px-4 py-3 text-sm leading-6 text-slate-400">
              Saved recipes persist in the control plane and round-trip through config export/import. Treat them as reusable onboarding defaults that can later be materialized into concrete pipe drafts, not live runtime state.
            </div>
          </div>

          <RecipeSpecEditor draft={draft} sinks={sinks} onDraftChange={setDraft} />
          <RecipeParametersEditor parameters={parameters} onChange={setParameters} />

          <div className="flex justify-end gap-3 pt-2">
            {mode === "duplicate" ? (
              <div className="mr-auto inline-flex items-center gap-2 rounded-2xl border border-white/8 bg-white/[0.03] px-4 py-3 text-sm text-slate-400">
                <CopyPlus className="h-4 w-4 text-slate-300" />
                Editing a cloned copy of <span className="font-mono text-slate-200">{preset?.name}</span>
              </div>
            ) : null}
            <button type="button" onClick={onClose} className="action-button-secondary">
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
                  ? "Save Recipe"
                  : "Create Recipe"}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

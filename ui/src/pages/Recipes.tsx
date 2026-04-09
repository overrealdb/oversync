import { useState } from "react";
import { Plus, Workflow } from "lucide-react";
import { usePipePresets } from "@/api/pipePresets";
import { PipeForm } from "@/components/pipes/PipeForm";
import { PipePresetLibrary } from "@/components/pipes/PipePresetLibrary";
import { SavedRecipeForm } from "@/components/pipes/SavedRecipeForm";

type SavedRecipeEditorState =
  | { mode: "create"; presetName: null }
  | { mode: "edit" | "duplicate"; presetName: string };

export function Recipes() {
  const { data } = usePipePresets();
  const [editorState, setEditorState] = useState<SavedRecipeEditorState | null>(null);
  const [pipePresetName, setPipePresetName] = useState<string | null>(null);

  const presets = data?.presets ?? [];
  const selectedPreset =
    editorState && editorState.presetName !== null
      ? presets.find((preset) => preset.name === editorState.presetName) ?? null
      : null;

  function closeEditor() {
    setEditorState(null);
  }

  function closePipeDraft() {
    setPipePresetName(null);
  }

  return (
    <div className="section-stack">
      <section className="panel-surface px-6 py-7 sm:px-8 sm:py-8">
        <div className="flex flex-col gap-6 xl:flex-row xl:items-end xl:justify-between">
          <div>
            <div className="eyebrow">Saved Recipes</div>
            <h1 className="page-title mt-3">Recipe Library</h1>
            <p className="page-copy mt-4">
              Store reusable onboarding defaults as first-class control-plane objects. Saved recipes survive export/import, seed new pipes, and can be materialized into previewable startup-ready drafts before creation.
            </p>
          </div>
          <div className="flex flex-wrap items-center gap-3">
            <div className="rounded-full border border-white/10 bg-white/[0.04] px-4 py-2 text-sm text-slate-300">
              {presets.length} saved
            </div>
            <button
              onClick={() => setEditorState({ mode: "create", presetName: null })}
              className="action-button"
            >
              <Plus className="h-4 w-4" /> Add Recipe
            </button>
          </div>
        </div>
      </section>

      <section className="grid gap-6 xl:grid-cols-[minmax(0,1.1fr)_360px]">
        <PipePresetLibrary
          onUsePreset={setPipePresetName}
          onEditPreset={(presetName) => setEditorState({ mode: "edit", presetName })}
          onDuplicatePreset={(presetName) =>
            setEditorState({ mode: "duplicate", presetName })
          }
          primaryActionLabel="Create Pipe"
        />

        <aside className="panel-surface h-fit p-6">
          <div className="flex h-12 w-12 items-center justify-center rounded-2xl border border-emerald-300/20 bg-emerald-400/10 text-emerald-300">
            <Workflow className="h-5 w-5" />
          </div>
          <div className="mt-5 text-xs font-semibold uppercase tracking-[0.22em] text-slate-500">
            Product model
          </div>
          <div className="mt-3 space-y-4 text-sm leading-6 text-slate-400">
            <p>
              Saved recipes are reusable templates. They describe origin defaults, SQL or built-in recipe expansion, schedule, diff policy, and target sinks.
            </p>
            <p>
              Pipes are live runnable units. Create a pipe from a saved recipe when you want scheduler-owned state, history, dry-run, and enable/disable control.
            </p>
            <p>
              Parameterized recipes can also be materialized into TOML or JSON previews before pipe creation. Config export still includes both, so you can shape onboarding in the UI, then ship the resulting control-plane file into Kubernetes at startup.
            </p>
          </div>
        </aside>
      </section>

      <SavedRecipeForm
        open={editorState !== null}
        mode={editorState?.mode ?? "create"}
        preset={selectedPreset}
        onClose={closeEditor}
      />
      <PipeForm
        open={pipePresetName !== null}
        initialPresetName={pipePresetName}
        onClose={closePipeDraft}
      />
    </div>
  );
}

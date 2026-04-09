import { useState } from "react";
import { Link } from "@tanstack/react-router";
import { CopyPlus, Layers3, Pencil, Sparkles, Trash2 } from "lucide-react";
import { useDeletePipePreset, usePipePresets } from "@/api/pipePresets";
import { ConfirmDialog } from "@/components/shared/ConfirmDialog";
import { EmptyState } from "@/components/shared/EmptyState";
import { LoadingSkeleton } from "@/components/shared/LoadingSkeleton";
import { useToast } from "@/components/shared/useToast";

interface PipePresetLibraryProps {
  onUsePreset?: (presetName: string) => void;
  onEditPreset?: (presetName: string) => void;
  onDuplicatePreset?: (presetName: string) => void;
  showManageLink?: boolean;
  primaryActionLabel?: string;
}

export function PipePresetLibrary({
  onUsePreset,
  onEditPreset,
  onDuplicatePreset,
  showManageLink = false,
  primaryActionLabel = "Use Recipe",
}: PipePresetLibraryProps) {
  const { data, isLoading } = usePipePresets();
  const deletePreset = useDeletePipePreset();
  const { toast } = useToast();
  const [deleteTarget, setDeleteTarget] = useState<string | null>(null);

  if (isLoading) {
    return <LoadingSkeleton rows={3} />;
  }

  const presets = data?.presets ?? [];

  function handleDelete() {
    if (!deleteTarget) return;
    deletePreset.mutate(deleteTarget, {
      onSuccess: () => {
        toast("success", `Saved recipe "${deleteTarget}" deleted`);
        setDeleteTarget(null);
      },
      onError: (err) => toast("error", err.message),
    });
  }

  if (presets.length === 0) {
    return (
      <EmptyState
        icon={<Sparkles className="h-9 w-9" />}
        title="No saved recipes yet"
        description="Save a manual pipe or recipe draft as a reusable recipe so future onboarding starts from persisted runtime defaults instead of retyping SQL and schedule settings."
      />
    );
  }

  return (
    <>
    <div className="panel-surface overflow-hidden">
      <div className="flex flex-col gap-3 border-b border-white/8 px-6 py-5 sm:flex-row sm:items-end sm:justify-between">
          <div>
            <div className="text-xs font-semibold uppercase tracking-[0.22em] text-slate-500">
              Saved recipe library
            </div>
            <p className="mt-2 text-sm leading-6 text-slate-400">
            Saved recipes persist in the control plane, survive config export/import, prefill new pipes, and can be materialized into previewable drafts before creation.
            </p>
          </div>
        <div className="flex items-center gap-3">
          <div className="rounded-full border border-white/10 bg-white/[0.04] px-4 py-2 text-sm text-slate-300">
            {presets.length} saved
          </div>
          {showManageLink ? (
            <Link to="/recipes" className="action-button-secondary">
              Manage Recipes
            </Link>
          ) : null}
        </div>
      </div>

      <div className="grid gap-4 px-6 py-6 lg:grid-cols-2">
        {presets.map((preset) => {
          const queryCount = preset.spec.queries?.length ?? 0;
          const modeLabel = preset.spec.recipe?.type ?? "manual";
          const parameterCount = preset.spec.parameters?.length ?? 0;

          return (
            <article
              key={preset.name}
              className="rounded-[28px] border border-white/10 bg-white/[0.03] p-5"
            >
              <div className="flex items-start justify-between gap-4">
                <div>
                  <div className="flex items-center gap-3">
                    <div className="rounded-2xl border border-white/10 bg-white/[0.04] p-2 text-slate-300">
                      <Layers3 className="h-4 w-4" />
                    </div>
                    <div>
                      <h3 className="text-lg font-semibold tracking-[-0.03em] text-white">
                        {preset.name}
                      </h3>
                      <div className="mt-1 text-xs uppercase tracking-[0.22em] text-slate-500">
                        {modeLabel}
                      </div>
                    </div>
                  </div>
                  <p className="mt-3 text-sm leading-6 text-slate-400">
                    {preset.description?.trim() ||
                      "Saved onboarding defaults for future pipes."}
                  </p>
                </div>
                <div className="flex items-center gap-2">
                  {onEditPreset ? (
                    <button
                      type="button"
                      onClick={() => onEditPreset(preset.name)}
                      className="rounded-xl border border-white/8 bg-white/[0.03] p-2 text-slate-400 transition-colors hover:border-blue-300/25 hover:text-blue-300"
                      title="Edit recipe"
                    >
                      <Pencil className="h-4 w-4" />
                    </button>
                  ) : null}
                  {onDuplicatePreset ? (
                    <button
                      type="button"
                      onClick={() => onDuplicatePreset(preset.name)}
                      className="rounded-xl border border-white/8 bg-white/[0.03] p-2 text-slate-400 transition-colors hover:border-emerald-300/25 hover:text-emerald-200"
                      title="Duplicate recipe"
                    >
                      <CopyPlus className="h-4 w-4" />
                    </button>
                  ) : null}
                  <button
                    type="button"
                    onClick={() => setDeleteTarget(preset.name)}
                    className="rounded-xl border border-white/8 bg-white/[0.03] p-2 text-slate-400 transition-colors hover:border-rose-300/25 hover:text-rose-300"
                    title="Delete recipe"
                  >
                    <Trash2 className="h-4 w-4" />
                  </button>
                </div>
              </div>

              <div className="mt-5 grid gap-3 sm:grid-cols-3">
                <div className="rounded-2xl border border-white/8 bg-white/[0.03] px-4 py-3">
                  <div className="text-xs uppercase tracking-[0.2em] text-slate-500">
                    Connector
                  </div>
                  <div className="mt-2 font-mono text-sm text-slate-100">
                    {preset.spec.origin_connector}
                  </div>
                </div>
                <div className="rounded-2xl border border-white/8 bg-white/[0.03] px-4 py-3">
                  <div className="text-xs uppercase tracking-[0.2em] text-slate-500">
                    Queries / Params
                  </div>
                  <div className="mt-2 font-mono text-sm text-slate-100">
                    {queryCount} / {parameterCount}
                  </div>
                </div>
                <div className="rounded-2xl border border-white/8 bg-white/[0.03] px-4 py-3">
                  <div className="text-xs uppercase tracking-[0.2em] text-slate-500">
                    Interval
                  </div>
                  <div className="mt-2 font-mono text-sm text-slate-100">
                    {preset.spec.schedule?.interval_secs ?? 300}s
                  </div>
                </div>
              </div>

              {onUsePreset ? (
                <div className="mt-5 flex justify-end">
                  <button
                    type="button"
                    onClick={() => onUsePreset(preset.name)}
                    className="action-button-secondary"
                  >
                    {primaryActionLabel}
                  </button>
                </div>
              ) : null}
            </article>
          );
        })}
      </div>
    </div>
      <ConfirmDialog
        open={deleteTarget !== null}
        title="Delete Saved Recipe"
        message={`Are you sure you want to delete "${deleteTarget}"? This removes the reusable onboarding template from the control plane.`}
        onConfirm={handleDelete}
        onCancel={() => setDeleteTarget(null)}
        loading={deletePreset.isPending}
      />
    </>
  );
}

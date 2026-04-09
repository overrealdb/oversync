import { Layers3, Sparkles, Trash2 } from "lucide-react";
import { useDeletePipePreset, usePipePresets } from "@/api/pipePresets";
import { EmptyState } from "@/components/shared/EmptyState";
import { LoadingSkeleton } from "@/components/shared/LoadingSkeleton";
import { useToast } from "@/components/shared/useToast";

interface PipePresetLibraryProps {
  onUsePreset: (presetName: string) => void;
}

export function PipePresetLibrary({ onUsePreset }: PipePresetLibraryProps) {
  const { data, isLoading } = usePipePresets();
  const deletePreset = useDeletePipePreset();
  const { toast } = useToast();

  if (isLoading) {
    return <LoadingSkeleton rows={3} />;
  }

  const presets = data?.presets ?? [];

  if (presets.length === 0) {
    return (
      <EmptyState
        icon={<Sparkles className="h-9 w-9" />}
        title="No saved presets yet"
        description="Save a manual pipe or recipe draft as a preset to reuse onboarding defaults without rewriting SQL and schedule settings every time."
      />
    );
  }

  return (
    <div className="panel-surface overflow-hidden">
      <div className="flex flex-col gap-3 border-b border-white/8 px-6 py-5 sm:flex-row sm:items-end sm:justify-between">
        <div>
          <div className="text-xs font-semibold uppercase tracking-[0.22em] text-slate-500">
            Preset library
          </div>
          <p className="mt-2 text-sm leading-6 text-slate-400">
            Presets persist in the control plane, survive config export/import, and prefill new pipes.
          </p>
        </div>
        <div className="rounded-full border border-white/10 bg-white/[0.04] px-4 py-2 text-sm text-slate-300">
          {presets.length} saved
        </div>
      </div>

      <div className="grid gap-4 px-6 py-6 lg:grid-cols-2">
        {presets.map((preset) => {
          const queryCount = preset.spec.queries?.length ?? 0;
          const modeLabel = preset.spec.recipe?.type ?? "manual";

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
                <button
                  type="button"
                  onClick={() =>
                    deletePreset.mutate(preset.name, {
                      onSuccess: () =>
                        toast("success", `Preset "${preset.name}" deleted`),
                      onError: (err) => toast("error", err.message),
                    })
                  }
                  className="rounded-xl border border-white/8 bg-white/[0.03] p-2 text-slate-400 transition-colors hover:border-rose-300/25 hover:text-rose-300"
                  title="Delete preset"
                >
                  <Trash2 className="h-4 w-4" />
                </button>
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
                    Queries
                  </div>
                  <div className="mt-2 font-mono text-sm text-slate-100">{queryCount}</div>
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

              <div className="mt-5 flex justify-end">
                <button
                  type="button"
                  onClick={() => onUsePreset(preset.name)}
                  className="action-button-secondary"
                >
                  Use Preset
                </button>
              </div>
            </article>
          );
        })}
      </div>
    </div>
  );
}

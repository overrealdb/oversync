import { useState } from "react";
import { Play, Plus, Trash2 } from "lucide-react";
import { useDeletePipe, usePipes } from "@/api/pipes";
import { ConfirmDialog } from "@/components/shared/ConfirmDialog";
import { EmptyState } from "@/components/shared/EmptyState";
import { LoadingSkeleton } from "@/components/shared/LoadingSkeleton";
import { PipeDryRunDialog } from "@/components/pipes/PipeDryRunDialog";
import { useToast } from "@/components/shared/useToast";

interface PipesTableProps {
  onCreate: () => void;
}

export function PipesTable({ onCreate }: PipesTableProps) {
  const { data, isLoading } = usePipes();
  const deletePipe = useDeletePipe();
  const { toast } = useToast();
  const [deleteTarget, setDeleteTarget] = useState<string | null>(null);
  const [dryRunTarget, setDryRunTarget] = useState<string | null>(null);

  if (isLoading) return <LoadingSkeleton rows={5} />;

  const pipes = data?.pipes ?? [];

  function handleDelete() {
    if (!deleteTarget) return;
    deletePipe.mutate(deleteTarget, {
      onSuccess: () => {
        toast("success", `Pipe "${deleteTarget}" deleted`);
        setDeleteTarget(null);
      },
      onError: (err) => toast("error", err.message),
    });
  }

  if (pipes.length === 0) {
    return (
      <EmptyState
        title="No pipes configured"
        description="Create a PostgreSQL pipe to onboard a runnable sync path backed by recipes, targets, schedule, and diff settings."
        action={
          <button onClick={onCreate} className="action-button">
            <Plus className="h-4 w-4" /> Add Pipe
          </button>
        }
      />
    );
  }

  return (
    <>
      <div className="panel-surface overflow-hidden">
        <div className="flex flex-col gap-3 border-b border-white/8 px-6 py-5 sm:flex-row sm:items-end sm:justify-between">
          <div>
            <div className="text-xs font-semibold uppercase tracking-[0.22em] text-slate-500">
              Runnable pipelines
            </div>
            <p className="mt-2 text-sm leading-6 text-slate-400">
              These are the real onboarding units for PostgreSQL recipes, targets, schedules, and delta processing.
            </p>
          </div>
          <div className="rounded-full border border-white/10 bg-white/[0.04] px-4 py-2 text-sm text-slate-300">
            {pipes.length} total
          </div>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-left text-sm">
            <thead>
              <tr className="border-b border-white/8">
                <th className="px-6 py-3 text-xs font-medium uppercase tracking-[0.22em] text-slate-500">Name</th>
                <th className="px-6 py-3 text-xs font-medium uppercase tracking-[0.22em] text-slate-500">Origin</th>
                <th className="px-6 py-3 text-xs font-medium uppercase tracking-[0.22em] text-slate-500">Recipe</th>
                <th className="px-6 py-3 text-xs font-medium uppercase tracking-[0.22em] text-slate-500">Targets</th>
                <th className="px-6 py-3 text-xs font-medium uppercase tracking-[0.22em] text-slate-500">Interval</th>
                <th className="px-6 py-3 text-right text-xs font-medium uppercase tracking-[0.22em] text-slate-500">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-white/6">
              {pipes.map((pipe) => (
                <tr key={pipe.name} className="transition-colors hover:bg-white/[0.03]">
                  <td className="px-6 py-4 font-medium text-white">
                    <div className="flex items-center gap-3">
                      <span>{pipe.name}</span>
                      {pipe.enabled ? null : (
                        <span className="rounded-full border border-amber-300/20 bg-amber-400/10 px-2 py-1 text-[10px] font-semibold uppercase tracking-[0.18em] text-amber-200">
                          disabled
                        </span>
                      )}
                    </div>
                  </td>
                  <td className="px-6 py-4">
                    <span className="rounded-full border border-white/8 bg-white/[0.03] px-3 py-1 font-mono text-xs text-slate-300">
                      {pipe.origin_connector}
                    </span>
                  </td>
                  <td className="px-6 py-4 text-slate-300">
                    {pipe.recipe ? (
                      <div>
                        <div className="font-mono text-xs text-slate-200">{pipe.recipe.type}</div>
                        <div className="text-xs text-slate-500">{pipe.recipe.prefix}</div>
                      </div>
                    ) : (
                      <span className="text-slate-500">manual</span>
                    )}
                  </td>
                  <td className="px-6 py-4 text-slate-300">
                    {pipe.targets.length > 0 ? pipe.targets.join(", ") : "--"}
                  </td>
                  <td className="px-6 py-4 font-mono text-slate-300">
                    {pipe.interval_secs}s
                  </td>
                  <td className="px-6 py-4">
                    <div className="flex items-center justify-end gap-2">
                      <button
                        onClick={() => setDryRunTarget(pipe.name)}
                        className="rounded-xl border border-white/8 bg-white/[0.03] p-2 text-slate-400 transition-colors hover:border-emerald-300/25 hover:text-emerald-200"
                        title="Dry Run"
                      >
                        <Play className="h-4 w-4" />
                      </button>
                      <button
                        onClick={() => setDeleteTarget(pipe.name)}
                        className="rounded-xl border border-white/8 bg-white/[0.03] p-2 text-slate-400 transition-colors hover:border-rose-300/25 hover:text-rose-300"
                        title="Delete"
                      >
                        <Trash2 className="h-4 w-4" />
                      </button>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      <ConfirmDialog
        open={deleteTarget !== null}
        title="Delete Pipe"
        message={`Are you sure you want to delete "${deleteTarget}"? This removes the runnable pipeline definition.`}
        onConfirm={handleDelete}
        onCancel={() => setDeleteTarget(null)}
        loading={deletePipe.isPending}
      />
      <PipeDryRunDialog
        pipeName={dryRunTarget}
        open={dryRunTarget !== null}
        onClose={() => setDryRunTarget(null)}
      />
    </>
  );
}

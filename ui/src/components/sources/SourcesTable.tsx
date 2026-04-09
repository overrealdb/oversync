import { useState, useMemo } from "react";
import { Link } from "@tanstack/react-router";
import { Plus, Trash2, Pencil, Play, ExternalLink } from "lucide-react";
import { useSources, useDeleteSource, useTriggerSource } from "@/api/sources";
import { useHistory } from "@/api/history";
import { StatusBadge } from "@/components/shared/StatusBadge";
import { EmptyState } from "@/components/shared/EmptyState";
import { LoadingSkeleton } from "@/components/shared/LoadingSkeleton";
import { ConfirmDialog } from "@/components/shared/ConfirmDialog";
import { useToast } from "@/components/shared/useToast";
import { formatDuration, cycleDurationMs } from "@/utils/format";
import type { SourceInfo } from "@/types/api";

interface SourcesTableProps {
  onEdit: (name: string) => void;
  onCreate: () => void;
}

export function SourcesTable({ onEdit, onCreate }: SourcesTableProps) {
  const { data, isLoading } = useSources();
  const history = useHistory();
  const deleteSource = useDeleteSource();
  const triggerSource = useTriggerSource();
  const { toast } = useToast();
  const [deleteTarget, setDeleteTarget] = useState<string | null>(null);

  const avgDurations = useMemo(() => {
    const map = new Map<string, { total: number; count: number }>();
    for (const c of history.data?.cycles ?? []) {
      const dur = c.duration_ms ?? cycleDurationMs(c.started_at, c.finished_at);
      if (dur == null) continue;
      const entry = map.get(c.source) ?? { total: 0, count: 0 };
      entry.total += dur;
      entry.count++;
      map.set(c.source, entry);
    }
    const result = new Map<string, number>();
    for (const [name, { total, count }] of map) {
      result.set(name, Math.round(total / count));
    }
    return result;
  }, [history.data]);

  function handleDelete() {
    if (!deleteTarget) return;
    deleteSource.mutate(deleteTarget, {
      onSuccess: () => {
        toast("success", `Source "${deleteTarget}" deleted`);
        setDeleteTarget(null);
      },
      onError: (e) => toast("error", e.message),
    });
  }

  function handleTrigger(source: SourceInfo) {
    triggerSource.mutate(source.name, {
      onSuccess: (res) => toast("success", res.message),
      onError: (e) => toast("error", e.message),
    });
  }

  if (isLoading) return <LoadingSkeleton rows={5} />;

  const sources = data?.sources ?? [];

  if (sources.length === 0) {
    return (
      <EmptyState
        title="No sources configured"
        description="Create a source to start syncing data. Once connected, queries, execution history, and runtime metrics will show up here."
        action={
          <button
            onClick={onCreate}
            className="action-button"
          >
            <Plus className="h-4 w-4" /> Add Source
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
              Registered sources
            </div>
            <p className="mt-2 text-sm leading-6 text-slate-400">
              Scan connector type, runtime history, and quick actions without leaving the workspace.
            </p>
          </div>
          <div className="rounded-full border border-white/10 bg-white/[0.04] px-4 py-2 text-sm text-slate-300">
            {sources.length} total
          </div>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-left text-sm">
            <thead>
              <tr className="border-b border-white/8">
                <th className="px-6 py-3 text-xs font-medium uppercase tracking-[0.22em] text-slate-500">Name</th>
                <th className="px-6 py-3 text-xs font-medium uppercase tracking-[0.22em] text-slate-500">Connector</th>
                <th className="px-6 py-3 text-xs font-medium uppercase tracking-[0.22em] text-slate-500">Queries</th>
                <th className="px-6 py-3 text-xs font-medium uppercase tracking-[0.22em] text-slate-500">Last Cycle</th>
                <th className="px-6 py-3 text-xs font-medium uppercase tracking-[0.22em] text-slate-500">Total Cycles</th>
                <th className="px-6 py-3 text-xs font-medium uppercase tracking-[0.22em] text-slate-500">Avg Duration</th>
                <th className="px-6 py-3 text-right text-xs font-medium uppercase tracking-[0.22em] text-slate-500">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-white/6">
              {sources.map((source) => (
                <tr key={source.name} className="transition-colors hover:bg-white/[0.03]">
                  <td className="px-6 py-4">
                    <Link
                      to="/sources/$name"
                      params={{ name: source.name }}
                      className="flex items-center gap-1 font-medium text-slate-100 hover:text-emerald-300"
                    >
                      {source.name}
                      <ExternalLink className="h-3 w-3" />
                    </Link>
                  </td>
                  <td className="px-6 py-4">
                    <span className="rounded-full border border-white/8 bg-white/[0.03] px-3 py-1 font-mono text-xs text-slate-300">
                      {source.connector}
                    </span>
                  </td>
                  <td className="px-6 py-4">
                    <span className="font-mono text-slate-300">{source.queries.length}</span>
                  </td>
                  <td className="px-6 py-4">
                    {source.status.last_cycle ? (
                      <StatusBadge status={source.status.last_cycle.status} />
                    ) : (
                      <span className="text-slate-500">--</span>
                    )}
                  </td>
                  <td className="px-6 py-4">
                    <span className="font-mono text-slate-300">{source.status.total_cycles}</span>
                  </td>
                  <td className="px-6 py-4">
                    <span className="font-mono text-slate-300">
                      {avgDurations.has(source.name)
                        ? formatDuration(avgDurations.get(source.name)!)
                        : "--"}
                    </span>
                  </td>
                  <td className="px-6 py-4">
                    <div className="flex items-center justify-end gap-2">
                      <button
                        onClick={() => handleTrigger(source)}
                        className="rounded-xl border border-white/8 bg-white/[0.03] p-2 text-slate-400 transition-colors hover:border-emerald-300/25 hover:text-emerald-300"
                        title="Trigger sync"
                      >
                        <Play className="h-4 w-4" />
                      </button>
                      <button
                        onClick={() => onEdit(source.name)}
                        className="rounded-xl border border-white/8 bg-white/[0.03] p-2 text-slate-400 transition-colors hover:border-blue-300/25 hover:text-blue-300"
                        title="Edit"
                      >
                        <Pencil className="h-4 w-4" />
                      </button>
                      <button
                        onClick={() => setDeleteTarget(source.name)}
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
        title="Delete Source"
        message={`Are you sure you want to delete "${deleteTarget}"? This action cannot be undone.`}
        onConfirm={handleDelete}
        onCancel={() => setDeleteTarget(null)}
        loading={deleteSource.isPending}
      />
    </>
  );
}

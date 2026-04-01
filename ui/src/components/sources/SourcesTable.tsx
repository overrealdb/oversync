import { useState, useMemo } from "react";
import { Link } from "@tanstack/react-router";
import { Plus, Trash2, Pencil, Play, ExternalLink } from "lucide-react";
import { useSources, useDeleteSource, useTriggerSource } from "@/api/sources";
import { useHistory } from "@/api/history";
import { StatusBadge } from "@/components/shared/StatusBadge";
import { EmptyState } from "@/components/shared/EmptyState";
import { LoadingSkeleton } from "@/components/shared/LoadingSkeleton";
import { ConfirmDialog } from "@/components/shared/ConfirmDialog";
import { useToast } from "@/components/shared/Toast";
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
        description="Create a source to start syncing data"
        action={
          <button
            onClick={onCreate}
            className="flex items-center gap-2 px-4 py-2 bg-blue-600 hover:bg-blue-500 text-white text-sm font-medium rounded-lg transition-colors"
          >
            <Plus className="h-4 w-4" /> Add Source
          </button>
        }
      />
    );
  }

  return (
    <>
      <div className="bg-gray-900 border border-gray-800 rounded-xl overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full text-left text-sm">
            <thead>
              <tr className="border-b border-gray-800">
                <th className="px-6 py-3 text-xs font-medium text-gray-400 uppercase tracking-wider">Name</th>
                <th className="px-6 py-3 text-xs font-medium text-gray-400 uppercase tracking-wider">Connector</th>
                <th className="px-6 py-3 text-xs font-medium text-gray-400 uppercase tracking-wider">Queries</th>
                <th className="px-6 py-3 text-xs font-medium text-gray-400 uppercase tracking-wider">Last Cycle</th>
                <th className="px-6 py-3 text-xs font-medium text-gray-400 uppercase tracking-wider">Total Cycles</th>
                <th className="px-6 py-3 text-xs font-medium text-gray-400 uppercase tracking-wider">Avg Duration</th>
                <th className="px-6 py-3 text-xs font-medium text-gray-400 uppercase tracking-wider text-right">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-800/50">
              {sources.map((source) => (
                <tr key={source.name} className="hover:bg-gray-800 transition-colors">
                  <td className="px-6 py-4">
                    <Link
                      to="/sources/$name"
                      params={{ name: source.name }}
                      className="font-medium text-blue-400 hover:text-blue-300 flex items-center gap-1"
                    >
                      {source.name}
                      <ExternalLink className="h-3 w-3" />
                    </Link>
                  </td>
                  <td className="px-6 py-4">
                    <span className="font-mono text-gray-300">{source.connector}</span>
                  </td>
                  <td className="px-6 py-4">
                    <span className="font-mono text-gray-300">{source.queries.length}</span>
                  </td>
                  <td className="px-6 py-4">
                    {source.status.last_cycle ? (
                      <StatusBadge status={source.status.last_cycle.status} />
                    ) : (
                      <span className="text-gray-500">--</span>
                    )}
                  </td>
                  <td className="px-6 py-4">
                    <span className="font-mono text-gray-300">{source.status.total_cycles}</span>
                  </td>
                  <td className="px-6 py-4">
                    <span className="font-mono text-gray-300">
                      {avgDurations.has(source.name)
                        ? formatDuration(avgDurations.get(source.name)!)
                        : "--"}
                    </span>
                  </td>
                  <td className="px-6 py-4">
                    <div className="flex items-center justify-end gap-2">
                      <button
                        onClick={() => handleTrigger(source)}
                        className="p-1.5 rounded-md text-gray-400 hover:text-emerald-400 hover:bg-gray-700 transition-colors"
                        title="Trigger sync"
                      >
                        <Play className="h-4 w-4" />
                      </button>
                      <button
                        onClick={() => onEdit(source.name)}
                        className="p-1.5 rounded-md text-gray-400 hover:text-blue-400 hover:bg-gray-700 transition-colors"
                        title="Edit"
                      >
                        <Pencil className="h-4 w-4" />
                      </button>
                      <button
                        onClick={() => setDeleteTarget(source.name)}
                        className="p-1.5 rounded-md text-gray-400 hover:text-rose-400 hover:bg-gray-700 transition-colors"
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

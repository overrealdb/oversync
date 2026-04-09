import { useState } from "react";
import { Plus, Trash2, Pencil } from "lucide-react";
import { useSinks, useDeleteSink } from "@/api/sinks";
import { EmptyState } from "@/components/shared/EmptyState";
import { LoadingSkeleton } from "@/components/shared/LoadingSkeleton";
import { ConfirmDialog } from "@/components/shared/ConfirmDialog";
import { useToast } from "@/components/shared/useToast";
import type { SinkInfo } from "@/types/api";

interface SinksTableProps {
  onEdit: (sink: SinkInfo) => void;
  onCreate: () => void;
}

export function SinksTable({ onEdit, onCreate }: SinksTableProps) {
  const { data, isLoading } = useSinks();
  const deleteSink = useDeleteSink();
  const { toast } = useToast();
  const [deleteTarget, setDeleteTarget] = useState<string | null>(null);

  function handleDelete() {
    if (!deleteTarget) return;
    deleteSink.mutate(deleteTarget, {
      onSuccess: () => {
        toast("success", `Sink "${deleteTarget}" deleted`);
        setDeleteTarget(null);
      },
      onError: (e) => toast("error", e.message),
    });
  }

  if (isLoading) return <LoadingSkeleton rows={5} />;

  const sinks = data?.sinks ?? [];

  if (sinks.length === 0) {
    return (
      <EmptyState
        title="No sinks configured"
        description="Create a sink to deliver sync events. Kafka, logging, and downstream delivery surfaces will appear here once configured."
        action={
          <button
            onClick={onCreate}
            className="action-button"
          >
            <Plus className="h-4 w-4" /> Add Sink
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
              Delivery endpoints
            </div>
            <p className="mt-2 text-sm leading-6 text-slate-400">
              Keep transport types and primary config details visible before routing changes.
            </p>
          </div>
          <div className="rounded-full border border-white/10 bg-white/[0.04] px-4 py-2 text-sm text-slate-300">
            {sinks.length} total
          </div>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-left text-sm">
            <thead>
              <tr className="border-b border-white/8">
                <th className="px-6 py-3 text-xs font-medium uppercase tracking-[0.22em] text-slate-500">Name</th>
                <th className="px-6 py-3 text-xs font-medium uppercase tracking-[0.22em] text-slate-500">Type</th>
                <th className="px-6 py-3 text-xs font-medium uppercase tracking-[0.22em] text-slate-500">Config</th>
                <th className="px-6 py-3 text-right text-xs font-medium uppercase tracking-[0.22em] text-slate-500">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-white/6">
              {sinks.map((sink) => (
                <tr key={sink.name} className="transition-colors hover:bg-white/[0.03]">
                  <td className="px-6 py-4 font-medium text-white">{sink.name}</td>
                  <td className="px-6 py-4">
                    <span className="rounded-full border border-white/8 bg-white/[0.03] px-3 py-1 font-mono text-xs text-slate-300">
                      {sink.sink_type}
                    </span>
                  </td>
                  <td className="max-w-xs px-6 py-4 font-mono text-xs text-slate-400 truncate">
                    {sink.config
                      ? Object.entries(sink.config)
                          .slice(0, 3)
                          .map(([k, v]) => `${k}: ${v}`)
                          .join(", ")
                      : "--"}
                  </td>
                  <td className="px-6 py-4">
                    <div className="flex items-center justify-end gap-2">
                      <button
                        onClick={() => onEdit(sink)}
                        className="rounded-xl border border-white/8 bg-white/[0.03] p-2 text-slate-400 transition-colors hover:border-blue-300/25 hover:text-blue-300"
                        title="Edit"
                      >
                        <Pencil className="h-4 w-4" />
                      </button>
                      <button
                        onClick={() => setDeleteTarget(sink.name)}
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
        title="Delete Sink"
        message={`Are you sure you want to delete "${deleteTarget}"? This action cannot be undone.`}
        onConfirm={handleDelete}
        onCancel={() => setDeleteTarget(null)}
        loading={deleteSink.isPending}
      />
    </>
  );
}

import { useState } from "react";
import { Plus, Trash2, Pencil } from "lucide-react";
import { useSinks, useDeleteSink } from "@/api/sinks";
import { EmptyState } from "@/components/shared/EmptyState";
import { LoadingSkeleton } from "@/components/shared/LoadingSkeleton";
import { ConfirmDialog } from "@/components/shared/ConfirmDialog";
import { useToast } from "@/components/shared/Toast";
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
        description="Create a sink to deliver sync events"
        action={
          <button
            onClick={onCreate}
            className="flex items-center gap-2 px-4 py-2 bg-blue-600 hover:bg-blue-500 text-white text-sm font-medium rounded-lg transition-colors"
          >
            <Plus className="h-4 w-4" /> Add Sink
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
                <th className="px-6 py-3 text-xs font-medium text-gray-400 uppercase tracking-wider">Type</th>
                <th className="px-6 py-3 text-xs font-medium text-gray-400 uppercase tracking-wider">Config</th>
                <th className="px-6 py-3 text-xs font-medium text-gray-400 uppercase tracking-wider text-right">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-800/50">
              {sinks.map((sink) => (
                <tr key={sink.name} className="hover:bg-gray-800 transition-colors">
                  <td className="px-6 py-4 font-medium text-white">{sink.name}</td>
                  <td className="px-6 py-4 font-mono text-gray-300">{sink.sink_type}</td>
                  <td className="px-6 py-4 font-mono text-gray-400 text-xs max-w-xs truncate">
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
                        className="p-1.5 rounded-md text-gray-400 hover:text-blue-400 hover:bg-gray-700 transition-colors"
                        title="Edit"
                      >
                        <Pencil className="h-4 w-4" />
                      </button>
                      <button
                        onClick={() => setDeleteTarget(sink.name)}
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
        title="Delete Sink"
        message={`Are you sure you want to delete "${deleteTarget}"? This action cannot be undone.`}
        onConfirm={handleDelete}
        onCancel={() => setDeleteTarget(null)}
        loading={deleteSink.isPending}
      />
    </>
  );
}

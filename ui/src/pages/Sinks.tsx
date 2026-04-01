import { useState } from "react";
import { Plus } from "lucide-react";
import { SinksTable } from "@/components/sinks/SinksTable";
import { SinkForm } from "@/components/sinks/SinkForm";
import type { SinkInfo } from "@/types/api";

export function Sinks() {
  const [formOpen, setFormOpen] = useState(false);
  const [editSink, setEditSink] = useState<SinkInfo | undefined>();

  function handleCreate() {
    setEditSink(undefined);
    setFormOpen(true);
  }

  function handleEdit(sink: SinkInfo) {
    setEditSink(sink);
    setFormOpen(true);
  }

  function handleClose() {
    setFormOpen(false);
    setEditSink(undefined);
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-white">Sinks</h1>
          <p className="text-sm text-gray-400 mt-1">Manage event delivery destinations</p>
        </div>
        <button
          onClick={handleCreate}
          className="flex items-center gap-2 px-4 py-2 bg-blue-600 hover:bg-blue-500 text-white text-sm font-medium rounded-lg transition-colors"
        >
          <Plus className="h-4 w-4" /> Add Sink
        </button>
      </div>

      <SinksTable onEdit={handleEdit} onCreate={handleCreate} />
      <SinkForm open={formOpen} onClose={handleClose} sink={editSink} />
    </div>
  );
}

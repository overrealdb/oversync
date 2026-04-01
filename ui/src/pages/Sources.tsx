import { useState } from "react";
import { Plus } from "lucide-react";
import { useSources } from "@/api/sources";
import { SourcesTable } from "@/components/sources/SourcesTable";
import { SourceForm } from "@/components/sources/SourceForm";
import type { SourceInfo } from "@/types/api";

export function Sources() {
  const { data } = useSources();
  const [formOpen, setFormOpen] = useState(false);
  const [editSource, setEditSource] = useState<SourceInfo | undefined>();

  function handleCreate() {
    setEditSource(undefined);
    setFormOpen(true);
  }

  function handleEdit(name: string) {
    const source = data?.sources.find((s) => s.name === name);
    setEditSource(source);
    setFormOpen(true);
  }

  function handleClose() {
    setFormOpen(false);
    setEditSource(undefined);
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-white">Sources</h1>
          <p className="text-sm text-gray-400 mt-1">Manage data source connections</p>
        </div>
        <button
          onClick={handleCreate}
          className="flex items-center gap-2 px-4 py-2 bg-blue-600 hover:bg-blue-500 text-white text-sm font-medium rounded-lg transition-colors"
        >
          <Plus className="h-4 w-4" /> Add Source
        </button>
      </div>

      <SourcesTable onEdit={handleEdit} onCreate={handleCreate} />
      <SourceForm open={formOpen} onClose={handleClose} source={editSource} />
    </div>
  );
}

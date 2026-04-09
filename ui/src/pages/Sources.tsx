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
    <div className="section-stack">
      <section className="panel-surface px-6 py-7 sm:px-8 sm:py-8">
        <div className="flex flex-col gap-6 lg:flex-row lg:items-end lg:justify-between">
          <div>
            <div className="eyebrow">Origins</div>
            <h1 className="page-title mt-3">Sources</h1>
            <p className="page-copy mt-4">
              Register upstream systems, shape polling cadence, and keep query lanes readable as the source catalog grows.
            </p>
          </div>
          <div className="flex flex-wrap items-center gap-3">
            <div className="rounded-full border border-white/10 bg-white/[0.04] px-4 py-2 text-sm text-slate-300">
              {data?.sources.length ?? 0} configured
            </div>
            <button
              onClick={handleCreate}
              className="action-button"
            >
              <Plus className="h-4 w-4" /> Add Source
            </button>
          </div>
        </div>
      </section>

      <SourcesTable onEdit={handleEdit} onCreate={handleCreate} />
      <SourceForm open={formOpen} onClose={handleClose} source={editSource} />
    </div>
  );
}

import { useState } from "react";
import { Link } from "@tanstack/react-router";
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
            <div className="eyebrow">Legacy Origins</div>
            <h1 className="page-title mt-3">Sources</h1>
            <p className="page-copy mt-4">
              Keep an eye on older source-based configs, trigger them manually, and inspect historical runtime behavior. For new runnable PostgreSQL onboarding, use pipes instead.
            </p>
          </div>
          <div className="flex flex-wrap items-center gap-3">
            <div className="rounded-full border border-white/10 bg-white/[0.04] px-4 py-2 text-sm text-slate-300">
              {data?.sources.length ?? 0} configured
            </div>
            <Link to="/pipes" className="rounded-full border border-emerald-300/20 bg-emerald-400/10 px-4 py-2 text-sm font-medium text-emerald-200 transition-colors hover:border-emerald-300/35 hover:bg-emerald-400/15 hover:text-white">
              Use Pipes for New Syncs
            </Link>
            <button
              onClick={handleCreate}
              className="rounded-full border border-white/10 bg-white/[0.04] px-4 py-2 text-sm font-medium text-slate-200 transition-colors hover:border-white/20 hover:bg-white/[0.08] hover:text-white"
            >
              <Plus className="h-4 w-4 inline-block mr-2" /> Add Legacy Source
            </button>
          </div>
        </div>
        <div className="mt-6 rounded-3xl border border-amber-300/15 bg-amber-400/[0.08] px-5 py-4 text-sm leading-6 text-amber-100/85">
          Recipe-backed onboarding, dry-run, presets, and runnable PostgreSQL sync creation now live under <span className="font-semibold text-white">Pipes</span>. Keep this page for legacy source configs and operational inspection.
        </div>
      </section>

      <SourcesTable onEdit={handleEdit} onCreate={handleCreate} />
      <SourceForm open={formOpen} onClose={handleClose} source={editSource} />
    </div>
  );
}

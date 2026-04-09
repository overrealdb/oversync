import { useState } from "react";
import { Plus } from "lucide-react";
import { PipeForm } from "@/components/pipes/PipeForm";
import { PipePresetLibrary } from "@/components/pipes/PipePresetLibrary";
import { PipesTable } from "@/components/pipes/PipesTable";
import { usePipes } from "@/api/pipes";

export function Pipes() {
  const { data } = usePipes();
  const [formOpen, setFormOpen] = useState(false);
  const [presetName, setPresetName] = useState<string | null>(null);

  function openBlankForm() {
    setPresetName(null);
    setFormOpen(true);
  }

  function openFromPreset(name: string) {
    setPresetName(name);
    setFormOpen(true);
  }

  return (
    <div className="section-stack">
      <section className="panel-surface px-6 py-7 sm:px-8 sm:py-8">
        <div className="flex flex-col gap-6 lg:flex-row lg:items-end lg:justify-between">
          <div>
            <div className="eyebrow">Pipelines</div>
            <h1 className="page-title mt-3">Pipes</h1>
            <p className="page-copy mt-4">
              Use pipes for real PostgreSQL onboarding. This is the path that binds origin DSN, recipe expansion, targets, schedule, and diff mode into a runnable sync.
            </p>
          </div>
          <div className="flex flex-wrap items-center gap-3">
            <div className="rounded-full border border-white/10 bg-white/[0.04] px-4 py-2 text-sm text-slate-300">
              {data?.pipes.length ?? 0} configured
            </div>
            <button onClick={openBlankForm} className="action-button">
              <Plus className="h-4 w-4" /> Add Pipe
            </button>
          </div>
        </div>
      </section>

      <PipePresetLibrary onUsePreset={openFromPreset} />
      <PipesTable onCreate={openBlankForm} />
      <PipeForm
        open={formOpen}
        initialPresetName={presetName}
        onClose={() => setFormOpen(false)}
      />
    </div>
  );
}

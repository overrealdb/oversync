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
    <div className="section-stack">
      <section className="panel-surface px-6 py-7 sm:px-8 sm:py-8">
        <div className="flex flex-col gap-6 lg:flex-row lg:items-end lg:justify-between">
          <div>
            <div className="eyebrow">Destinations</div>
            <h1 className="page-title mt-3">Sinks</h1>
            <p className="page-copy mt-4">
              Control where state changes land, from Kafka topics to local diagnostics and internal transport surfaces.
            </p>
          </div>
          <button
            onClick={handleCreate}
            className="action-button"
          >
            <Plus className="h-4 w-4" /> Add Sink
          </button>
        </div>
      </section>

      <SinksTable onEdit={handleEdit} onCreate={handleCreate} />
      <SinkForm open={formOpen} onClose={handleClose} sink={editSink} />
    </div>
  );
}

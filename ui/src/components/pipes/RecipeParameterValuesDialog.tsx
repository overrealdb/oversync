import { useEffect, useMemo, useState } from "react";
import { Copy, Download, WandSparkles, X } from "lucide-react";
import type { PipePresetInfo } from "@/types/api";
import { useToast } from "@/components/shared/useToast";
import {
  buildRuntimePipeFromPresetSpec,
  materializePipePresetSpec,
  normalizePresetParameters,
  parameterInitialValues,
  serializeRuntimePipeToml,
  suggestMaterializedPipeName,
} from "./recipeDraft";

interface RecipeParameterValuesDialogProps {
  open: boolean;
  preset: PipePresetInfo | null;
  onCancel: () => void;
  onApply: (materialized: PipePresetInfo) => void;
}

export function RecipeParameterValuesDialog({
  open,
  preset,
  onCancel,
  onApply,
}: RecipeParameterValuesDialogProps) {
  const { toast } = useToast();
  const parameters = useMemo(
    () => normalizePresetParameters(preset?.spec.parameters),
    [preset],
  );
  const [values, setValues] = useState<Record<string, string>>({});
  const [pipeName, setPipeName] = useState("");
  const [previewFormat, setPreviewFormat] = useState<"toml" | "json">("toml");

  useEffect(() => {
    if (!open || !preset) return;
    const initialValues = parameterInitialValues(preset.spec.parameters);
    setValues(initialValues);
    setPipeName(
      suggestMaterializedPipeName(
        preset.name,
        materializePipePresetSpec(preset.spec, initialValues),
      ),
    );
    setPreviewFormat("toml");
  }, [open, preset]);

  const materializedSpec = useMemo(
    () =>
      preset
        ? materializePipePresetSpec(preset.spec, values)
        : null,
    [preset, values],
  );
  const effectivePipeName = useMemo(() => {
    if (!preset || !materializedSpec) return pipeName.trim();
    return pipeName.trim() || suggestMaterializedPipeName(preset.name, materializedSpec);
  }, [materializedSpec, pipeName, preset]);
  const materializedPipe = useMemo(
    () =>
      materializedSpec
        ? buildRuntimePipeFromPresetSpec(effectivePipeName, materializedSpec)
        : null,
    [effectivePipeName, materializedSpec],
  );
  const tomlPreview = useMemo(
    () => (materializedPipe ? serializeRuntimePipeToml(materializedPipe) : ""),
    [materializedPipe],
  );
  const jsonPreview = useMemo(
    () => (materializedPipe ? JSON.stringify({ pipes: [materializedPipe] }, null, 2) : ""),
    [materializedPipe],
  );
  const previewContent = previewFormat === "toml" ? tomlPreview : jsonPreview;

  if (!open || !preset || !materializedSpec || !materializedPipe) return null;
  const currentPreset = preset;

  const canApply = parameters.every((parameter) => {
    if (parameter.required === false) return true;
    return Boolean(values[parameter.name]?.trim());
  });

  function handleApply() {
    onApply({
      ...currentPreset,
      spec: materializedSpec!,
    });
  }

  async function handleCopyPreview() {
    await navigator.clipboard.writeText(previewContent);
    toast("success", `Materialized ${previewFormat.toUpperCase()} copied`);
  }

  function handleDownloadPreview() {
    const blob = new Blob([previewContent], {
      type: previewFormat === "toml" ? "text/plain" : "application/json",
    });
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = `${effectivePipeName || "materialized-pipe"}.${previewFormat}`;
    anchor.click();
    URL.revokeObjectURL(url);
    toast("success", `Downloaded ${anchor.download}`);
  }

  return (
    <div className="fixed inset-0 z-[60] flex items-center justify-center">
      <div className="fixed inset-0 bg-black/60" onClick={onCancel} />
      <div className="panel-surface relative max-h-[92vh] w-full max-w-5xl overflow-y-auto p-6 shadow-2xl">
        <div className="mb-6 flex items-center justify-between">
          <div>
            <div className="eyebrow">Recipe Parameters</div>
            <h2 className="mt-2 text-2xl font-semibold tracking-[-0.03em] text-white">
              Materialize {currentPreset.name}
            </h2>
          </div>
          <button
            onClick={onCancel}
            className="rounded-full border border-white/10 bg-white/[0.04] p-2 text-slate-400 transition-colors hover:text-white"
          >
            <X className="h-5 w-5" />
          </button>
        </div>

        <div className="rounded-2xl border border-white/8 bg-white/[0.03] px-4 py-4 text-sm leading-6 text-slate-400">
          Fill the values for placeholders used inside this saved recipe. OverSync will expand them into a concrete pipe draft without mutating the saved template itself.
        </div>

        <div className="mt-6 grid gap-6 xl:grid-cols-[minmax(0,0.9fr)_minmax(0,1.1fr)]">
          <div className="space-y-4">
            <div className="panel-subtle p-4">
              <label className="mb-2 block text-sm font-medium text-slate-200">
                Materialized Pipe Name
              </label>
              <input
                type="text"
                value={pipeName}
                onChange={(e) => setPipeName(e.target.value)}
                className="w-full rounded-2xl border border-white/10 bg-white/[0.04] px-4 py-3 text-sm text-slate-100 outline-none transition-colors focus:border-emerald-300/35"
                placeholder={suggestMaterializedPipeName(currentPreset.name, materializedSpec)}
              />
              <p className="mt-3 text-sm leading-6 text-slate-400">
                Used only for preview and export. The saved recipe itself stays unchanged, and you can still rename the pipe during creation.
              </p>
            </div>

            {parameters.map((parameter) => (
              <div key={parameter.name} className="panel-subtle p-4">
                <div className="flex flex-col gap-2 md:flex-row md:items-end md:justify-between">
                  <div>
                    <div className="text-sm font-medium text-white">
                      {parameter.label?.trim() || parameter.name}
                    </div>
                    <div className="mt-1 font-mono text-xs text-slate-500">
                      {`{{${parameter.name}}}`}
                    </div>
                  </div>
                  <div className="flex items-center gap-2 text-xs uppercase tracking-[0.18em] text-slate-500">
                    {parameter.secret ? <span>secret</span> : null}
                    {parameter.required !== false ? <span>required</span> : <span>optional</span>}
                  </div>
                </div>
                {parameter.description ? (
                  <p className="mt-3 text-sm leading-6 text-slate-400">
                    {parameter.description}
                  </p>
                ) : null}
                <input
                  type={parameter.secret ? "password" : "text"}
                  value={values[parameter.name] ?? ""}
                  onChange={(e) =>
                    setValues((current) => ({
                      ...current,
                      [parameter.name]: e.target.value,
                    }))
                  }
                  className="mt-4 w-full rounded-2xl border border-white/10 bg-white/[0.04] px-4 py-3 text-sm text-slate-100 outline-none transition-colors focus:border-emerald-300/35"
                  placeholder={parameter.default ?? ""}
                />
              </div>
            ))}
          </div>

          <div className="panel-subtle p-4">
            <div className="flex flex-col gap-4 border-b border-white/8 pb-4 sm:flex-row sm:items-end sm:justify-between">
              <div>
                <div className="text-xs font-semibold uppercase tracking-[0.22em] text-slate-500">
                  Materialized Draft
                </div>
                <p className="mt-2 text-sm leading-6 text-slate-400">
                  Preview or export a concrete pipe draft for startup config workflows. Referenced sinks and credentials must still exist in the target environment.
                </p>
              </div>
              <div className="flex flex-wrap items-center gap-2">
                <button
                  type="button"
                  onClick={() => setPreviewFormat("toml")}
                  className={`rounded-full px-4 py-2 text-sm transition-colors ${
                    previewFormat === "toml"
                      ? "bg-emerald-400 text-slate-950"
                      : "border border-white/10 bg-white/[0.04] text-slate-300"
                  }`}
                >
                  TOML
                </button>
                <button
                  type="button"
                  onClick={() => setPreviewFormat("json")}
                  className={`rounded-full px-4 py-2 text-sm transition-colors ${
                    previewFormat === "json"
                      ? "bg-emerald-400 text-slate-950"
                      : "border border-white/10 bg-white/[0.04] text-slate-300"
                  }`}
                >
                  JSON
                </button>
                <button type="button" onClick={handleCopyPreview} className="action-button-secondary">
                  <Copy className="h-4 w-4" />
                  Copy
                </button>
                <button
                  type="button"
                  onClick={handleDownloadPreview}
                  className="action-button-secondary"
                >
                  <Download className="h-4 w-4" />
                  Download
                </button>
              </div>
            </div>

            <textarea
              value={previewContent}
              readOnly
              spellCheck={false}
              rows={26}
              className="mt-4 w-full rounded-2xl border border-white/10 bg-[#0a1320] px-4 py-3 font-mono text-sm text-slate-100 outline-none"
            />
          </div>
        </div>

        <div className="mt-6 flex justify-end gap-3">
          <button type="button" onClick={onCancel} className="action-button-secondary">
            Cancel
          </button>
          <button
            type="button"
            onClick={handleApply}
            disabled={!canApply}
            className="action-button disabled:cursor-not-allowed disabled:opacity-50"
          >
            <WandSparkles className="h-4 w-4" />
            Apply Parameters
          </button>
        </div>
      </div>
    </div>
  );
}

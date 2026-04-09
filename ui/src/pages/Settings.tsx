import { useState } from "react";
import { Copy, Download } from "lucide-react";
import { useExportConfig, useImportConfig } from "@/api/export";
import { useToast } from "@/components/shared/useToast";
import { useSettingsStore } from "@/stores/settings";
import type { ExportConfigFormat } from "@/types/api";

const INTERVAL_OPTIONS = [
  { label: "1 second", value: 1000 },
  { label: "2 seconds", value: 2000 },
  { label: "5 seconds", value: 5000 },
  { label: "10 seconds", value: 10000 },
  { label: "30 seconds", value: 30000 },
];

export function Settings() {
  const { apiBaseUrl, refreshInterval, theme, setApiBaseUrl, setRefreshInterval, setTheme } =
    useSettingsStore();
  const exportConfig = useExportConfig();
  const importConfig = useImportConfig();
  const { toast } = useToast();
  const [exportFormat, setExportFormat] = useState<ExportConfigFormat>("toml");
  const [importFormat, setImportFormat] = useState<ExportConfigFormat>("toml");
  const [importContent, setImportContent] = useState("");

  const exportContent = exportConfig.data?.content ?? "";

  async function copyExport() {
    if (!exportContent) return;
    await navigator.clipboard.writeText(exportContent);
    toast("success", "Export copied to clipboard");
  }

  function downloadExport() {
    if (!exportContent) return;
    const blob = new Blob([exportContent], {
      type: exportFormat === "toml" ? "text/plain" : "application/json",
    });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `oversync-export.${exportFormat}`;
    a.click();
    URL.revokeObjectURL(url);
    toast("success", `Downloaded oversync-export.${exportFormat}`);
  }

  function handleImport() {
    importConfig.mutate(
      {
        format: importFormat,
        content: importContent,
      },
      {
        onSuccess: (result) => {
          toast("success", result.message);
          if (result.warnings.length > 0) {
            toast("error", `Imported with warnings: ${result.warnings.join(" | ")}`);
          }
        },
        onError: (err) => toast("error", err.message),
      },
    );
  }

  return (
    <div className="grid gap-6 lg:grid-cols-[320px_minmax(0,1fr)]">
      <section className="panel-surface h-fit px-6 py-7 sm:px-8">
        <div className="eyebrow">Local preferences</div>
        <h1 className="page-title mt-3">Settings</h1>
        <p className="page-copy mt-4">
          Tune how the dashboard reaches the API, how often it refreshes, and which visual mode it stores on this machine.
        </p>
        <div className="mt-8 space-y-4">
          <div className="panel-subtle p-4">
            <div className="text-xs font-semibold uppercase tracking-[0.22em] text-slate-500">
              API mode
            </div>
            <p className="mt-2 text-sm leading-6 text-slate-400">
              Use <span className="font-mono text-slate-200">/api</span> behind Vite, or point directly at a remote OverSync server.
            </p>
          </div>
          <div className="panel-subtle p-4">
            <div className="text-xs font-semibold uppercase tracking-[0.22em] text-slate-500">
              Refresh cadence
            </div>
            <p className="mt-2 text-sm leading-6 text-slate-400">
              Short intervals are great for active debugging. Longer intervals reduce dashboard chatter.
            </p>
          </div>
        </div>
      </section>

      <div className="space-y-6">
        <div className="panel-surface p-6">
          <label className="mb-2 block text-sm font-medium text-slate-200">
            API Endpoint
          </label>
          <input
            type="text"
            value={apiBaseUrl}
            onChange={(e) => setApiBaseUrl(e.target.value)}
            className="w-full rounded-2xl border border-white/10 bg-white/[0.04] px-4 py-3 text-sm font-mono text-slate-100 outline-none transition-colors focus:border-emerald-300/35"
          />
          <p className="mt-3 text-sm leading-6 text-slate-400">
            The OverSync API endpoint. Use "/api" for proxied dev server.
          </p>
        </div>

        <div className="panel-surface p-6">
          <label className="mb-2 block text-sm font-medium text-slate-200">
            Auto-Refresh Interval
          </label>
          <select
            value={refreshInterval}
            onChange={(e) => setRefreshInterval(Number(e.target.value))}
            className="w-full rounded-2xl border border-white/10 bg-white/[0.04] px-4 py-3 text-sm text-slate-100 outline-none transition-colors focus:border-emerald-300/35"
          >
            {INTERVAL_OPTIONS.map((opt) => (
              <option key={opt.value} value={opt.value}>
                {opt.label}
              </option>
            ))}
          </select>
          <p className="mt-3 text-sm leading-6 text-slate-400">
            This controls polling for the UI itself, not the schedule used by sync pipelines.
          </p>
        </div>

        <div className="panel-surface p-6">
          <label className="mb-4 block text-sm font-medium text-slate-200">Theme</label>
          <div className="flex flex-wrap gap-3">
            <label
              className={`cursor-pointer rounded-2xl border px-5 py-3 text-sm font-medium transition-colors ${
                theme === "dark"
                  ? "border-emerald-300/25 bg-emerald-400/10 text-white"
                  : "border-white/10 bg-white/[0.04] text-slate-400 hover:text-white"
              }`}
            >
              <input
                type="radio"
                name="theme"
                value="dark"
                checked={theme === "dark"}
                onChange={() => setTheme("dark")}
                className="sr-only"
              />
              Dark
            </label>
            <label
              className={`cursor-pointer rounded-2xl border px-5 py-3 text-sm font-medium transition-colors ${
                theme === "light"
                  ? "border-emerald-300/25 bg-emerald-400/10 text-white"
                  : "border-white/10 bg-white/[0.04] text-slate-400 hover:text-white"
              }`}
            >
              <input
                type="radio"
                name="theme"
                value="light"
                checked={theme === "light"}
                onChange={() => setTheme("light")}
                className="sr-only"
              />
              Light
            </label>
          </div>
          <p className="mt-3 text-sm leading-6 text-slate-400">
            Preference is stored locally in the browser and applied on the next render.
          </p>
        </div>

        <div className="panel-surface p-6">
          <div className="flex flex-wrap items-start justify-between gap-4">
            <div>
              <label className="mb-2 block text-sm font-medium text-slate-200">
                Config Export
              </label>
              <p className="text-sm leading-6 text-slate-400">
                Export the persisted control-plane config as a real startup file for Kubernetes,
                Docker, or GitOps flows.
              </p>
            </div>
            <div className="flex gap-3">
              <select
                value={exportFormat}
                onChange={(e) => setExportFormat(e.target.value as ExportConfigFormat)}
                className="rounded-2xl border border-white/10 bg-white/[0.04] px-4 py-3 text-sm text-slate-100 outline-none transition-colors focus:border-emerald-300/35"
              >
                <option value="toml">toml</option>
                <option value="json">json</option>
              </select>
              <button
                type="button"
                onClick={() =>
                  exportConfig.mutate(exportFormat, {
                    onError: (err) => toast("error", err.message),
                  })
                }
                className="rounded-2xl border border-emerald-300/30 bg-emerald-400/10 px-4 py-3 text-sm font-medium text-white transition-colors hover:bg-emerald-400/15"
              >
                {exportConfig.isPending ? "Generating..." : "Generate Export"}
              </button>
            </div>
          </div>

          <div className="mt-5 flex flex-wrap gap-3">
            <button
              type="button"
              onClick={copyExport}
              disabled={!exportContent}
              className="inline-flex items-center gap-2 rounded-2xl border border-white/10 bg-white/[0.04] px-4 py-3 text-sm text-slate-200 transition-colors hover:text-white disabled:cursor-not-allowed disabled:opacity-40"
            >
              <Copy className="h-4 w-4" />
              Copy
            </button>
            <button
              type="button"
              onClick={downloadExport}
              disabled={!exportContent}
              className="inline-flex items-center gap-2 rounded-2xl border border-white/10 bg-white/[0.04] px-4 py-3 text-sm text-slate-200 transition-colors hover:text-white disabled:cursor-not-allowed disabled:opacity-40"
            >
              <Download className="h-4 w-4" />
              Download
            </button>
          </div>

          <textarea
            value={exportContent}
            readOnly
            spellCheck={false}
            placeholder="Generate an export to see the current persisted config here."
            rows={16}
            className="mt-5 w-full rounded-2xl border border-white/10 bg-[#0a1320] px-4 py-3 font-mono text-sm text-slate-100 outline-none"
          />
        </div>

        <div className="panel-surface p-6">
          <div className="flex flex-wrap items-start justify-between gap-4">
            <div>
              <label className="mb-2 block text-sm font-medium text-slate-200">
                Config Import
              </label>
              <p className="text-sm leading-6 text-slate-400">
                Replace the current persisted control-plane config from TOML or JSON. This writes
                into the active config DB and restarts the runtime with the imported setup.
              </p>
            </div>
            <div className="flex gap-3">
              <select
                value={importFormat}
                onChange={(e) => setImportFormat(e.target.value as ExportConfigFormat)}
                className="rounded-2xl border border-white/10 bg-white/[0.04] px-4 py-3 text-sm text-slate-100 outline-none transition-colors focus:border-emerald-300/35"
              >
                <option value="toml">toml</option>
                <option value="json">json</option>
              </select>
              <button
                type="button"
                onClick={handleImport}
                disabled={!importContent.trim() || importConfig.isPending}
                className="rounded-2xl border border-amber-300/30 bg-amber-400/10 px-4 py-3 text-sm font-medium text-white transition-colors hover:bg-amber-400/15 disabled:cursor-not-allowed disabled:opacity-40"
              >
                {importConfig.isPending ? "Importing..." : "Import Config"}
              </button>
            </div>
          </div>

          <textarea
            value={importContent}
            onChange={(e) => setImportContent(e.target.value)}
            spellCheck={false}
            placeholder="Paste a full oversync TOML or JSON config here."
            rows={16}
            className="mt-5 w-full rounded-2xl border border-white/10 bg-[#0a1320] px-4 py-3 font-mono text-sm text-slate-100 outline-none"
          />
        </div>
      </div>
    </div>
  );
}

import { useSettingsStore } from "@/stores/settings";

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
      </div>
    </div>
  );
}

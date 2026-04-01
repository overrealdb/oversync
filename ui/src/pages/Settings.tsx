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
    <div className="space-y-6 max-w-xl">
      <div>
        <h1 className="text-2xl font-bold text-white">Settings</h1>
        <p className="text-sm text-gray-400 mt-1">Configure dashboard preferences</p>
      </div>

      <div className="space-y-6">
        <div className="bg-gray-900 border border-gray-800 rounded-xl p-6">
          <label className="block text-sm font-medium text-gray-300 mb-2">
            API Endpoint
          </label>
          <input
            type="text"
            value={apiBaseUrl}
            onChange={(e) => setApiBaseUrl(e.target.value)}
            className="w-full bg-gray-800 border border-gray-700 rounded-md px-3 py-2 text-sm font-mono text-gray-200 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
          />
          <p className="mt-2 text-xs text-gray-500">
            The OverSync API endpoint. Use "/api" for proxied dev server.
          </p>
        </div>

        <div className="bg-gray-900 border border-gray-800 rounded-xl p-6">
          <label className="block text-sm font-medium text-gray-300 mb-2">
            Auto-Refresh Interval
          </label>
          <select
            value={refreshInterval}
            onChange={(e) => setRefreshInterval(Number(e.target.value))}
            className="w-full bg-gray-800 border border-gray-700 rounded-md px-3 py-2 text-sm text-gray-200 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
          >
            {INTERVAL_OPTIONS.map((opt) => (
              <option key={opt.value} value={opt.value}>
                {opt.label}
              </option>
            ))}
          </select>
        </div>

        <div className="bg-gray-900 border border-gray-800 rounded-xl p-6">
          <label className="block text-sm font-medium text-gray-300 mb-3">Theme</label>
          <div className="flex gap-3">
            <label
              className={`flex items-center gap-2 px-4 py-2 text-sm font-medium rounded-lg cursor-pointer transition-colors ${
                theme === "dark"
                  ? "bg-gray-700 text-white ring-2 ring-blue-500"
                  : "bg-gray-800 text-gray-400 hover:text-white"
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
              className={`flex items-center gap-2 px-4 py-2 text-sm font-medium rounded-lg cursor-pointer transition-colors ${
                theme === "light"
                  ? "bg-gray-700 text-white ring-2 ring-blue-500"
                  : "bg-gray-800 text-gray-400 hover:text-white"
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
        </div>
      </div>
    </div>
  );
}

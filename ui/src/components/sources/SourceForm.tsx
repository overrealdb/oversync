import { useState, useEffect } from "react";
import { X, Plus, Trash2 } from "lucide-react";
import { useCreateSource, useUpdateSource } from "@/api/sources";
import { JsonEditor } from "@/components/shared/JsonEditor";
import { useToast } from "@/components/shared/Toast";
import type { SourceInfo, ConnectorType } from "@/types/api";

const CONNECTOR_TYPES: ConnectorType[] = [
  "postgres",
  "mysql",
  "http",
  "graphql",
  "trino",
  "flight_sql",
];

interface QueryDraft {
  id: string;
  key_column: string;
  sql: string;
  interval_secs: number;
  diff_mode: "memory" | "db";
  failsafe_threshold: number;
}

function emptyQuery(): QueryDraft {
  return { id: "", key_column: "", sql: "", interval_secs: 60, diff_mode: "memory", failsafe_threshold: 30 };
}

interface SourceFormProps {
  open: boolean;
  onClose: () => void;
  source?: SourceInfo;
}

const INPUT_CLS =
  "w-full bg-gray-800 border border-gray-700 rounded-md px-3 py-2 text-sm text-gray-200 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent";

function ConnectorConfigFields({
  connector,
  config,
  onChange,
}: {
  connector: string;
  config: Record<string, unknown>;
  onChange: (c: Record<string, unknown>) => void;
}) {
  function set(key: string, value: unknown) {
    onChange({ ...config, [key]: value });
  }

  function textField(label: string, key: string, placeholder?: string, type?: string) {
    return (
      <div key={key}>
        <label className="block text-xs font-medium text-gray-400 mb-1">{label}</label>
        <input
          type={type ?? "text"}
          value={(config[key] as string) ?? ""}
          onChange={(e) => set(key, type === "number" ? Number(e.target.value) : e.target.value)}
          placeholder={placeholder}
          className={INPUT_CLS}
        />
      </div>
    );
  }

  function boolField(label: string, key: string) {
    return (
      <div key={key} className="flex items-center gap-2">
        <input
          type="checkbox"
          checked={!!config[key]}
          onChange={(e) => set(key, e.target.checked)}
          className="h-4 w-4 rounded border-gray-600 bg-gray-800 text-blue-500 focus:ring-blue-500"
        />
        <label className="text-sm text-gray-300">{label}</label>
      </div>
    );
  }

  switch (connector) {
    case "postgres":
    case "mysql":
      return (
        <div className="space-y-3">
          <p className="text-xs font-medium text-gray-400 uppercase tracking-wider">Connection</p>
          <div className="grid grid-cols-2 gap-3">
            {textField("Host", "host", "localhost")}
            {textField("Port", "port", connector === "postgres" ? "5432" : "3306", "number")}
          </div>
          {textField("Database", "database", "mydb")}
          {textField("User", "user", "admin")}
          {textField("Password", "password", "••••••", "password")}
          {boolField("SSL", "ssl")}
        </div>
      );
    case "http":
      return (
        <div className="space-y-3">
          <p className="text-xs font-medium text-gray-400 uppercase tracking-wider">HTTP Config</p>
          {textField("URL", "url", "https://api.example.com/data")}
          <div>
            <label className="block text-xs font-medium text-gray-400 mb-1">Auth Type</label>
            <select
              value={(config["auth_type"] as string) ?? "none"}
              onChange={(e) => set("auth_type", e.target.value)}
              className={INPUT_CLS}
            >
              <option value="none">None</option>
              <option value="bearer">Bearer Token</option>
              <option value="basic">Basic Auth</option>
              <option value="header">Custom Header</option>
            </select>
          </div>
          {config["auth_type"] === "bearer" && textField("Token", "token", "eyJ...")}
          {config["auth_type"] === "basic" && (
            <div className="grid grid-cols-2 gap-3">
              {textField("Username", "username")}
              {textField("Password", "password", "", "password")}
            </div>
          )}
          {config["auth_type"] === "header" && (
            <div className="grid grid-cols-2 gap-3">
              {textField("Header Name", "header_name", "X-API-Key")}
              {textField("Header Value", "header_value")}
            </div>
          )}
        </div>
      );
    case "graphql":
      return (
        <div className="space-y-3">
          <p className="text-xs font-medium text-gray-400 uppercase tracking-wider">GraphQL Config</p>
          {textField("URL", "url", "https://api.example.com/graphql")}
          <div>
            <label className="block text-xs font-medium text-gray-400 mb-1">Auth Type</label>
            <select
              value={(config["auth_type"] as string) ?? "none"}
              onChange={(e) => set("auth_type", e.target.value)}
              className={INPUT_CLS}
            >
              <option value="none">None</option>
              <option value="bearer">Bearer Token</option>
              <option value="basic">Basic Auth</option>
            </select>
          </div>
          {config["auth_type"] === "bearer" && textField("Token", "token")}
          {config["auth_type"] === "basic" && (
            <div className="grid grid-cols-2 gap-3">
              {textField("Username", "username")}
              {textField("Password", "password", "", "password")}
            </div>
          )}
        </div>
      );
    case "trino":
      return (
        <div className="space-y-3">
          <p className="text-xs font-medium text-gray-400 uppercase tracking-wider">Trino Config</p>
          <div className="grid grid-cols-2 gap-3">
            {textField("Host", "host", "localhost")}
            {textField("Port", "port", "8080", "number")}
          </div>
          {textField("Catalog", "catalog", "hive")}
          {textField("Schema", "schema", "default")}
          {textField("User", "user", "trino")}
        </div>
      );
    case "flight_sql":
      return (
        <div className="space-y-3">
          <p className="text-xs font-medium text-gray-400 uppercase tracking-wider">Flight SQL Config</p>
          <div className="grid grid-cols-2 gap-3">
            {textField("Host", "host", "localhost")}
            {textField("Port", "port", "31337", "number")}
          </div>
          {textField("Auth Token", "auth", "token")}
        </div>
      );
    default:
      return null;
  }
}

export function SourceForm({ open, onClose, source }: SourceFormProps) {
  const [name, setName] = useState("");
  const [connector, setConnector] = useState<string>(CONNECTOR_TYPES[0]);
  const [config, setConfig] = useState<Record<string, unknown>>({});
  const [useJsonEditor, setUseJsonEditor] = useState(false);
  const [enabled, setEnabled] = useState(true);
  const [queries, setQueries] = useState<QueryDraft[]>([]);
  const createSource = useCreateSource();
  const updateSource = useUpdateSource(source?.name ?? "");
  const { toast } = useToast();

  const isEdit = !!source;

  useEffect(() => {
    if (source) {
      setName(source.name);
      setConnector(source.connector);
      setConfig({});
      setEnabled(true);
      setQueries(
        source.queries.map((q) => ({
          id: q.id,
          key_column: q.key_column,
          sql: "",
          interval_secs: source.interval_secs ?? 60,
          diff_mode: "memory" as const,
          failsafe_threshold: 30,
        })),
      );
    } else {
      setName("");
      setConnector(CONNECTOR_TYPES[0]);
      setConfig({});
      setEnabled(true);
      setQueries([]);
    }
    setUseJsonEditor(false);
  }, [source, open]);

  if (!open) return null;

  function addQuery() {
    setQueries([...queries, emptyQuery()]);
  }

  function removeQuery(idx: number) {
    setQueries(queries.filter((_, i) => i !== idx));
  }

  function updateQuery(idx: number, patch: Partial<QueryDraft>) {
    setQueries(queries.map((q, i) => (i === idx ? { ...q, ...patch } : q)));
  }

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    const payload = {
      connector,
      config,
      enabled,
      queries: queries.map((q) => ({
        id: q.id,
        key_column: q.key_column,
        sql: q.sql,
        interval_secs: q.interval_secs,
        diff_mode: q.diff_mode,
        failsafe_threshold: q.failsafe_threshold,
      })),
    };

    if (isEdit) {
      updateSource.mutate(payload, {
        onSuccess: () => {
          toast("success", `Source "${source.name}" updated`);
          onClose();
        },
        onError: (err) => toast("error", err.message),
      });
    } else {
      createSource.mutate(
        { name, ...payload },
        {
          onSuccess: () => {
            toast("success", `Source "${name}" created`);
            onClose();
          },
          onError: (err) => toast("error", err.message),
        },
      );
    }
  }

  const isPending = createSource.isPending || updateSource.isPending;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      <div className="fixed inset-0 bg-black/60" onClick={onClose} />
      <div className="relative bg-gray-900 border border-gray-800 rounded-xl p-6 max-w-2xl w-full mx-4 shadow-xl max-h-[90vh] overflow-y-auto">
        <div className="flex items-center justify-between mb-6">
          <h2 className="text-lg font-semibold text-white">
            {isEdit ? `Edit Source: ${source.name}` : "Create Source"}
          </h2>
          <button onClick={onClose} className="text-gray-400 hover:text-white transition-colors">
            <X className="h-5 w-5" />
          </button>
        </div>

        <form onSubmit={handleSubmit} className="space-y-5">
          {/* Name */}
          {!isEdit && (
            <div>
              <label className="block text-sm font-medium text-gray-300 mb-1">Name</label>
              <input
                type="text"
                value={name}
                onChange={(e) => setName(e.target.value)}
                required
                className={INPUT_CLS}
                placeholder="my-postgres-source"
              />
            </div>
          )}

          {/* Connector type */}
          <div>
            <label className="block text-sm font-medium text-gray-300 mb-1">Connector Type</label>
            <select
              value={connector}
              onChange={(e) => {
                setConnector(e.target.value);
                setConfig({});
              }}
              className={INPUT_CLS}
            >
              {CONNECTOR_TYPES.map((t) => (
                <option key={t} value={t}>{t}</option>
              ))}
            </select>
          </div>

          {/* Enabled toggle */}
          <div className="flex items-center gap-3">
            <button
              type="button"
              onClick={() => setEnabled(!enabled)}
              className={`relative inline-flex h-6 w-11 items-center rounded-full transition-colors ${enabled ? "bg-emerald-600" : "bg-gray-700"}`}
            >
              <span className={`inline-block h-4 w-4 transform rounded-full bg-white transition-transform ${enabled ? "translate-x-6" : "translate-x-1"}`} />
            </button>
            <span className="text-sm text-gray-300">Enabled</span>
          </div>

          {/* Configuration */}
          <div>
            <div className="flex items-center justify-between mb-2">
              <label className="text-sm font-medium text-gray-300">Configuration</label>
              <button
                type="button"
                onClick={() => setUseJsonEditor(!useJsonEditor)}
                className="text-xs text-blue-400 hover:text-blue-300"
              >
                {useJsonEditor ? "Use form fields" : "Edit as JSON"}
              </button>
            </div>
            {useJsonEditor ? (
              <JsonEditor value={config} onChange={setConfig} label="" />
            ) : (
              <ConnectorConfigFields connector={connector} config={config} onChange={setConfig} />
            )}
          </div>

          {/* Queries */}
          <div>
            <div className="flex items-center justify-between mb-3">
              <label className="text-sm font-medium text-gray-300">Queries</label>
              <button
                type="button"
                onClick={addQuery}
                className="flex items-center gap-1 text-xs text-blue-400 hover:text-blue-300"
              >
                <Plus className="h-3.5 w-3.5" /> Add Query
              </button>
            </div>

            {queries.length === 0 ? (
              <p className="text-xs text-gray-500 py-3 text-center border border-dashed border-gray-700 rounded-lg">
                No queries yet. Click "Add Query" to define one.
              </p>
            ) : (
              <div className="space-y-4">
                {queries.map((q, idx) => (
                  <div key={idx} className="border border-gray-700 rounded-lg p-4 space-y-3 relative">
                    <button
                      type="button"
                      onClick={() => removeQuery(idx)}
                      className="absolute top-3 right-3 text-gray-500 hover:text-rose-400 transition-colors"
                    >
                      <Trash2 className="h-4 w-4" />
                    </button>

                    <div className="grid grid-cols-2 gap-3">
                      <div>
                        <label className="block text-xs font-medium text-gray-400 mb-1">Query Name</label>
                        <input
                          type="text"
                          value={q.id}
                          onChange={(e) => updateQuery(idx, { id: e.target.value })}
                          placeholder="users_sync"
                          className={INPUT_CLS}
                        />
                      </div>
                      <div>
                        <label className="block text-xs font-medium text-gray-400 mb-1">Key Column</label>
                        <input
                          type="text"
                          value={q.key_column}
                          onChange={(e) => updateQuery(idx, { key_column: e.target.value })}
                          placeholder="id"
                          className={INPUT_CLS}
                        />
                      </div>
                    </div>

                    <div>
                      <label className="block text-xs font-medium text-gray-400 mb-1">SQL / Query</label>
                      <textarea
                        value={q.sql}
                        onChange={(e) => updateQuery(idx, { sql: e.target.value })}
                        placeholder="SELECT * FROM users"
                        rows={2}
                        className={INPUT_CLS + " font-mono resize-y"}
                      />
                    </div>

                    <div className="grid grid-cols-3 gap-3">
                      <div>
                        <label className="block text-xs font-medium text-gray-400 mb-1">Interval (s)</label>
                        <input
                          type="number"
                          value={q.interval_secs}
                          onChange={(e) => updateQuery(idx, { interval_secs: Number(e.target.value) })}
                          min={1}
                          className={INPUT_CLS}
                        />
                      </div>
                      <div>
                        <label className="block text-xs font-medium text-gray-400 mb-1">Diff Mode</label>
                        <select
                          value={q.diff_mode}
                          onChange={(e) => updateQuery(idx, { diff_mode: e.target.value as "memory" | "db" })}
                          className={INPUT_CLS}
                        >
                          <option value="memory">Memory</option>
                          <option value="db">Database</option>
                        </select>
                      </div>
                      <div>
                        <label className="block text-xs font-medium text-gray-400 mb-1">
                          Fail-safe ({q.failsafe_threshold}%)
                        </label>
                        <input
                          type="range"
                          min={0}
                          max={100}
                          value={q.failsafe_threshold}
                          onChange={(e) => updateQuery(idx, { failsafe_threshold: Number(e.target.value) })}
                          className="w-full accent-amber-500"
                        />
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>

          {/* Actions */}
          <div className="flex justify-end gap-3 pt-2">
            <button
              type="button"
              onClick={onClose}
              className="px-4 py-2 text-sm font-medium text-gray-300 bg-gray-800 rounded-md hover:bg-gray-700 transition-colors"
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={isPending || (!isEdit && !name.trim())}
              className="px-4 py-2 text-sm font-medium text-white bg-blue-600 rounded-md hover:bg-blue-500 disabled:opacity-50 transition-colors"
            >
              {isPending ? "Saving..." : isEdit ? "Update" : "Create"}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

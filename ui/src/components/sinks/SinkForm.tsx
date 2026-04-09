import { useState, useEffect } from "react";
import { X } from "lucide-react";
import { useCreateSink, useUpdateSink } from "@/api/sinks";
import { JsonEditor } from "@/components/shared/JsonEditor";
import { useToast } from "@/components/shared/useToast";
import type { SinkInfo, SinkType } from "@/types/api";

const SINK_TYPES: SinkType[] = ["stdout", "kafka", "surrealdb", "http"];

interface SinkFormProps {
  open: boolean;
  onClose: () => void;
  sink?: SinkInfo;
}

const INPUT_CLS =
  "w-full bg-gray-800 border border-gray-700 rounded-md px-3 py-2 text-sm text-gray-200 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent";

function SinkConfigFields({
  sinkType,
  config,
  onChange,
}: {
  sinkType: string;
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

  switch (sinkType) {
    case "http":
      return (
        <div className="space-y-3">
          <p className="text-xs font-medium text-gray-400 uppercase tracking-wider">HTTP Config</p>
          {textField("URL", "url", "https://api.example.com/ingest")}
          <div>
            <label className="block text-xs font-medium text-gray-400 mb-1">Method</label>
            <select
              value={(config["method"] as string) ?? "POST"}
              onChange={(e) => set("method", e.target.value)}
              className={INPUT_CLS}
            >
              <option value="POST">POST</option>
              <option value="PUT">PUT</option>
              <option value="PATCH">PATCH</option>
            </select>
          </div>
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
          {config["auth_type"] === "bearer" && textField("Token", "token")}
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
          <div className="grid grid-cols-2 gap-3">
            {textField("Batch Size", "batch_size", "100", "number")}
            {textField("Timeout (ms)", "timeout", "5000", "number")}
          </div>
        </div>
      );
    case "kafka":
      return (
        <div className="space-y-3">
          <p className="text-xs font-medium text-gray-400 uppercase tracking-wider">Kafka Config</p>
          {textField("Brokers", "brokers", "localhost:9092")}
          {textField("Topic", "topic", "oversync-events")}
          <div>
            <label className="block text-xs font-medium text-gray-400 mb-1">Key Format</label>
            <select
              value={(config["key_format"] as string) ?? "json"}
              onChange={(e) => set("key_format", e.target.value)}
              className={INPUT_CLS}
            >
              <option value="json">JSON</option>
              <option value="string">String</option>
              <option value="avro">Avro</option>
            </select>
          </div>
        </div>
      );
    case "surrealdb":
      return (
        <div className="space-y-3">
          <p className="text-xs font-medium text-gray-400 uppercase tracking-wider">SurrealDB Config</p>
          {textField("URL", "url", "ws://localhost:8000")}
          {textField("Namespace", "namespace", "default")}
          {textField("Database", "database", "oversync")}
          {textField("Username", "username", "root")}
          {textField("Password", "password", "", "password")}
        </div>
      );
    case "stdout":
      return (
        <div className="space-y-3">
          <p className="text-xs font-medium text-gray-400 uppercase tracking-wider">Stdout Config</p>
          {boolField("Pretty Print", "pretty")}
        </div>
      );
    default:
      return null;
  }
}

export function SinkForm({ open, onClose, sink }: SinkFormProps) {
  const [name, setName] = useState("");
  const [sinkType, setSinkType] = useState<string>(SINK_TYPES[0]);
  const [config, setConfig] = useState<Record<string, unknown>>({});
  const [useJsonEditor, setUseJsonEditor] = useState(false);
  const [enabled, setEnabled] = useState(true);
  const createSink = useCreateSink();
  const updateSink = useUpdateSink(sink?.name ?? "");
  const { toast } = useToast();

  const isEdit = !!sink;

  useEffect(() => {
    if (sink) {
      setName(sink.name);
      setSinkType(sink.sink_type);
      setConfig({});
      setEnabled(true);
    } else {
      setName("");
      setSinkType(SINK_TYPES[0]);
      setConfig({});
      setEnabled(true);
    }
    setUseJsonEditor(false);
  }, [sink, open]);

  if (!open) return null;

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    const payload = { sink_type: sinkType, config, enabled };

    if (isEdit) {
      updateSink.mutate(payload, {
        onSuccess: () => {
          toast("success", `Sink "${sink.name}" updated`);
          onClose();
        },
        onError: (err) => toast("error", err.message),
      });
    } else {
      createSink.mutate(
        { name, ...payload },
        {
          onSuccess: () => {
            toast("success", `Sink "${name}" created`);
            onClose();
          },
          onError: (err) => toast("error", err.message),
        },
      );
    }
  }

  const isPending = createSink.isPending || updateSink.isPending;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      <div className="fixed inset-0 bg-black/60" onClick={onClose} />
      <div className="relative bg-gray-900 border border-gray-800 rounded-xl p-6 max-w-lg w-full mx-4 shadow-xl max-h-[90vh] overflow-y-auto">
        <div className="flex items-center justify-between mb-6">
          <h2 className="text-lg font-semibold text-white">
            {isEdit ? `Edit Sink: ${sink.name}` : "Create Sink"}
          </h2>
          <button onClick={onClose} className="text-gray-400 hover:text-white transition-colors">
            <X className="h-5 w-5" />
          </button>
        </div>

        <form onSubmit={handleSubmit} className="space-y-5">
          {!isEdit && (
            <div>
              <label className="block text-sm font-medium text-gray-300 mb-1">Name</label>
              <input
                type="text"
                value={name}
                onChange={(e) => setName(e.target.value)}
                required
                className={INPUT_CLS}
                placeholder="my-kafka-sink"
              />
            </div>
          )}

          <div>
            <label className="block text-sm font-medium text-gray-300 mb-1">Sink Type</label>
            <select
              value={sinkType}
              onChange={(e) => {
                setSinkType(e.target.value);
                setConfig({});
              }}
              className={INPUT_CLS}
            >
              {SINK_TYPES.map((t) => (
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
              <SinkConfigFields sinkType={sinkType} config={config} onChange={setConfig} />
            )}
          </div>

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

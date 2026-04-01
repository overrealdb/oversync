import { useState, useCallback } from "react";

interface JsonEditorProps {
  value: Record<string, unknown>;
  onChange: (value: Record<string, unknown>) => void;
  label?: string;
  placeholder?: string;
}

export function JsonEditor({
  value,
  onChange,
  label,
  placeholder,
}: JsonEditorProps) {
  const [text, setText] = useState(() => JSON.stringify(value, null, 2));
  const [error, setError] = useState<string | null>(null);

  const handleChange = useCallback(
    (raw: string) => {
      setText(raw);
    },
    [],
  );

  const handleBlur = useCallback(() => {
    try {
      const parsed = JSON.parse(text) as Record<string, unknown>;
      setError(null);
      onChange(parsed);
    } catch {
      setError("Invalid JSON");
    }
  }, [text, onChange]);

  return (
    <div>
      {label && (
        <label className="mb-1 block text-sm font-medium text-gray-300">
          {label}
        </label>
      )}
      <textarea
        value={text}
        onChange={(e) => handleChange(e.target.value)}
        onBlur={handleBlur}
        placeholder={placeholder}
        rows={10}
        spellCheck={false}
        className={`
          w-full rounded-md border bg-gray-900 px-3 py-2 font-mono text-sm text-gray-200
          placeholder:text-gray-600 focus:outline-none focus:ring-2 resize-y
          ${
            error
              ? "border-rose-500 focus:ring-rose-500"
              : "border-gray-700 focus:ring-emerald-500"
          }
        `}
      />
      {error && (
        <p className="mt-1 text-xs text-rose-400">{error}</p>
      )}
    </div>
  );
}

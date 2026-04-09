import { Plus, Trash2 } from "lucide-react";
import type { PipePresetParameter } from "@/types/api";
import {
  createDefaultPresetParameter,
  normalizePresetParameters,
} from "./recipeDraft";
import { INPUT_CLS } from "./RecipeSpecEditor";

interface RecipeParametersEditorProps {
  parameters: PipePresetParameter[];
  onChange: (parameters: PipePresetParameter[]) => void;
}

export function RecipeParametersEditor({
  parameters,
  onChange,
}: RecipeParametersEditorProps) {
  function updateParameter(index: number, patch: Partial<PipePresetParameter>) {
    onChange(
      parameters.map((parameter, parameterIndex) =>
        parameterIndex === index ? { ...parameter, ...patch } : parameter,
      ),
    );
  }

  function addParameter() {
    onChange([...parameters, createDefaultPresetParameter()]);
  }

  function removeParameter(index: number) {
    onChange(parameters.filter((_, parameterIndex) => parameterIndex !== index));
  }

  const normalized = normalizePresetParameters(parameters);

  return (
    <div className="panel-subtle p-4">
      <div className="flex flex-col gap-3 md:flex-row md:items-end md:justify-between">
        <div>
          <div className="text-sm font-medium text-slate-200">Recipe Parameters</div>
          <p className="mt-2 text-sm leading-6 text-slate-400">
            Define placeholders like <span className="font-mono text-slate-200">{`{{source_name}}`}</span> in DSN, prefix, schemas, targets, or manual SQL. These fields are requested when someone creates a pipe from the saved recipe.
          </p>
        </div>
        <div className="rounded-full border border-white/10 bg-white/[0.04] px-4 py-2 text-sm text-slate-300">
          {normalized.length} parameters
        </div>
      </div>

      {parameters.length === 0 ? (
        <div className="mt-4 rounded-2xl border border-dashed border-white/10 bg-white/[0.03] px-4 py-4 text-sm leading-6 text-slate-400">
          No parameters yet. Add them only when this recipe should be reusable across multiple sources or datasets.
        </div>
      ) : (
        <div className="mt-4 space-y-4">
          {parameters.map((parameter, index) => (
            <div
              key={`${index}-${parameter.name}`}
              className="rounded-2xl border border-white/10 bg-white/[0.03] p-4"
            >
              <div className="mb-4 flex items-center justify-between gap-3">
                <div className="text-sm font-medium text-slate-200">
                  Parameter {index + 1}
                </div>
                <button
                  type="button"
                  onClick={() => removeParameter(index)}
                  className="rounded-xl border border-white/8 bg-white/[0.03] p-2 text-slate-400 transition-colors hover:border-rose-300/25 hover:text-rose-300"
                  title="Remove parameter"
                >
                  <Trash2 className="h-4 w-4" />
                </button>
              </div>

              <div className="grid gap-4 md:grid-cols-2">
                <div>
                  <label className="mb-2 block text-sm font-medium text-slate-200">
                    Parameter Name
                  </label>
                  <input
                    type="text"
                    value={parameter.name}
                    onChange={(e) => updateParameter(index, { name: e.target.value })}
                    className={INPUT_CLS}
                    placeholder="source_name"
                  />
                </div>
                <div>
                  <label className="mb-2 block text-sm font-medium text-slate-200">
                    Label
                  </label>
                  <input
                    type="text"
                    value={parameter.label ?? ""}
                    onChange={(e) => updateParameter(index, { label: e.target.value })}
                    className={INPUT_CLS}
                    placeholder="Source Name"
                  />
                </div>
              </div>

              <div className="mt-4 grid gap-4 md:grid-cols-2">
                <div>
                  <label className="mb-2 block text-sm font-medium text-slate-200">
                    Default Value
                  </label>
                  <input
                    type={parameter.secret ? "password" : "text"}
                    value={parameter.default ?? ""}
                    onChange={(e) => updateParameter(index, { default: e.target.value })}
                    className={INPUT_CLS}
                    placeholder="some-postgresql-source"
                  />
                </div>
                <div>
                  <label className="mb-2 block text-sm font-medium text-slate-200">
                    Description
                  </label>
                  <input
                    type="text"
                    value={parameter.description ?? ""}
                    onChange={(e) =>
                      updateParameter(index, { description: e.target.value })
                    }
                    className={INPUT_CLS}
                    placeholder="Used in pipe name, prefix, and SQL placeholders"
                  />
                </div>
              </div>

              <div className="mt-4 flex flex-wrap gap-4">
                <label className="flex items-center gap-3 rounded-2xl border border-white/10 bg-white/[0.03] px-4 py-3">
                  <input
                    type="checkbox"
                    checked={parameter.required !== false}
                    onChange={(e) => updateParameter(index, { required: e.target.checked })}
                    className="h-4 w-4 rounded border-white/20 bg-transparent"
                  />
                  <span className="text-sm text-slate-300">Required</span>
                </label>
                <label className="flex items-center gap-3 rounded-2xl border border-white/10 bg-white/[0.03] px-4 py-3">
                  <input
                    type="checkbox"
                    checked={parameter.secret === true}
                    onChange={(e) => updateParameter(index, { secret: e.target.checked })}
                    className="h-4 w-4 rounded border-white/20 bg-transparent"
                  />
                  <span className="text-sm text-slate-300">Secret input</span>
                </label>
              </div>
            </div>
          ))}
        </div>
      )}

      <button type="button" onClick={addParameter} className="action-button-secondary mt-4">
        <Plus className="h-4 w-4" />
        Add Parameter
      </button>
    </div>
  );
}

import { useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "./client";
import type {
  ExportConfigFormat,
  ExportConfigResponse,
  ImportConfigResponse,
} from "@/types/api";

export function useExportConfig() {
  return useMutation({
    mutationFn: (format: ExportConfigFormat) =>
      api.get<ExportConfigResponse>(`/config/export?format=${format}`),
  });
}

export function useImportConfig() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (payload: { format: ExportConfigFormat; content: string }) =>
      api.post<ImportConfigResponse>("/config/import", payload),
    onSuccess: async () => {
      await Promise.all([
        qc.invalidateQueries({ queryKey: ["pipes"] }),
        qc.invalidateQueries({ queryKey: ["pipe-presets"] }),
        qc.invalidateQueries({ queryKey: ["sinks"] }),
        qc.invalidateQueries({ queryKey: ["sources"] }),
        qc.invalidateQueries({ queryKey: ["history"] }),
      ]);
    },
  });
}

import { useMutation, useQueryClient } from "@tanstack/react-query";
import { generatedRequestOptions, unwrapGeneratedResult } from "./client";
import {
  exportConfig as exportConfigSdk,
  importConfig as importConfigSdk,
} from "./generated/sdk.gen";
import type {
  ExportConfigFormat,
  ExportConfigResponse,
  ImportConfigResponse,
} from "@/types/api";

export function useExportConfig() {
  return useMutation({
    mutationFn: (format: ExportConfigFormat) =>
      unwrapGeneratedResult<ExportConfigResponse>(
        exportConfigSdk({ ...generatedRequestOptions(), query: { format } }),
      ),
  });
}

export function useImportConfig() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (payload: { format: ExportConfigFormat; content: string }) =>
      unwrapGeneratedResult<ImportConfigResponse>(
        importConfigSdk({ ...generatedRequestOptions(), body: payload }),
      ),
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

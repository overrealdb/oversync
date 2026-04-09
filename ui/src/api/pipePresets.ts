import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "./client";
import { useSettingsStore } from "@/stores/settings";
import type {
  CreatePipePresetRequest,
  MutationResponse,
  PipePresetListResponse,
  UpdatePipePresetRequest,
} from "@/types/api";

export function usePipePresets() {
  const interval = useSettingsStore((s) => s.refreshInterval);
  return useQuery({
    queryKey: ["pipe-presets"],
    queryFn: () => api.get<PipePresetListResponse>("/pipe-presets"),
    refetchInterval: interval,
  });
}

export function useCreatePipePreset() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (payload: CreatePipePresetRequest) =>
      api.post<MutationResponse>("/pipe-presets", payload),
    onSuccess: async () => {
      await qc.invalidateQueries({ queryKey: ["pipe-presets"] });
    },
  });
}

export function useUpdatePipePreset(name: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (payload: UpdatePipePresetRequest) =>
      api.put<MutationResponse>(`/pipe-presets/${encodeURIComponent(name)}`, payload),
    onSuccess: async () => {
      await qc.invalidateQueries({ queryKey: ["pipe-presets"] });
    },
  });
}

export function useDeletePipePreset() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (name: string) =>
      api.del<MutationResponse>(`/pipe-presets/${encodeURIComponent(name)}`),
    onSuccess: async () => {
      await qc.invalidateQueries({ queryKey: ["pipe-presets"] });
    },
  });
}

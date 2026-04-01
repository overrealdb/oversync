import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "./client";
import { useSettingsStore } from "@/stores/settings";
import type {
  SourceListResponse,
  SourceInfo,
  CreateSourceRequest,
  UpdateSourceRequest,
  MutationResponse,
  TriggerResponse,
} from "@/types/api";

export function useSources() {
  const interval = useSettingsStore((s) => s.refreshInterval);
  return useQuery({
    queryKey: ["sources"],
    queryFn: () => api.get<SourceListResponse>("/sources"),
    refetchInterval: interval,
  });
}

export function useSource(name: string) {
  const interval = useSettingsStore((s) => s.refreshInterval);
  return useQuery({
    queryKey: ["sources", name],
    queryFn: () => api.get<SourceInfo>(`/sources/${encodeURIComponent(name)}`),
    refetchInterval: interval,
    enabled: !!name,
  });
}

export function useCreateSource() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (data: CreateSourceRequest) =>
      api.post<MutationResponse>("/sources", data),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["sources"] }),
  });
}

export function useUpdateSource(name: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (data: UpdateSourceRequest) =>
      api.put<MutationResponse>(`/sources/${encodeURIComponent(name)}`, data),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["sources"] }),
  });
}

export function useDeleteSource() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (name: string) =>
      api.del<MutationResponse>(`/sources/${encodeURIComponent(name)}`),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["sources"] }),
  });
}

export function useTriggerSource() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (name: string) =>
      api.post<TriggerResponse>(`/sources/${encodeURIComponent(name)}/trigger`),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["sources"] });
      qc.invalidateQueries({ queryKey: ["history"] });
    },
  });
}

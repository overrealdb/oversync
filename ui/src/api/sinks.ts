import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "./client";
import { useSettingsStore } from "@/stores/settings";
import type {
  SinkListResponse,
  CreateSinkRequest,
  UpdateSinkRequest,
  MutationResponse,
} from "@/types/api";

export function useSinks() {
  const interval = useSettingsStore((s) => s.refreshInterval);
  return useQuery({
    queryKey: ["sinks"],
    queryFn: () => api.get<SinkListResponse>("/sinks"),
    refetchInterval: interval,
  });
}

export function useCreateSink() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (data: CreateSinkRequest) =>
      api.post<MutationResponse>("/sinks", data),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["sinks"] }),
  });
}

export function useUpdateSink(name: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (data: UpdateSinkRequest) =>
      api.put<MutationResponse>(`/sinks/${encodeURIComponent(name)}`, data),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["sinks"] }),
  });
}

export function useDeleteSink() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (name: string) =>
      api.del<MutationResponse>(`/sinks/${encodeURIComponent(name)}`),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["sinks"] }),
  });
}

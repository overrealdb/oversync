import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "./client";
import { useSettingsStore } from "@/stores/settings";
import type {
  CreatePipeRequest,
  DryRunRequest,
  DryRunResult,
  MutationResponse,
  PipeListResponse,
  ResolvePipeResponse,
  UpdatePipeRequest,
} from "@/types/api";

export function usePipes() {
  const interval = useSettingsStore((s) => s.refreshInterval);
  return useQuery({
    queryKey: ["pipes"],
    queryFn: () => api.get<PipeListResponse>("/pipes"),
    refetchInterval: interval,
  });
}

export function useResolvePipe(name: string | null, enabled = true) {
  return useQuery({
    queryKey: ["pipe-resolve", name],
    queryFn: () =>
      api.get<ResolvePipeResponse>(`/pipes/${encodeURIComponent(name ?? "")}/resolve`),
    enabled: enabled && Boolean(name),
  });
}

export function useCreatePipe() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (data: CreatePipeRequest) =>
      api.post<MutationResponse>("/pipes", data),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["pipes"] }),
  });
}

export function useUpdatePipe(name: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (data: UpdatePipeRequest) =>
      api.put<MutationResponse>(`/pipes/${encodeURIComponent(name)}`, data),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["pipes"] }),
  });
}

export function useDeletePipe() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (name: string) =>
      api.del<MutationResponse>(`/pipes/${encodeURIComponent(name)}`),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["pipes"] }),
  });
}

export function useDryRunPipe() {
  return useMutation({
    mutationFn: (payload: DryRunRequest) =>
      api.post<DryRunResult>("/pipes/dry-run", payload),
  });
}

import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "./client";
import { useSettingsStore } from "@/stores/settings";
import type { StatusResponse, MutationResponse } from "@/types/api";

export function useSyncStatus() {
  const interval = useSettingsStore((s) => s.refreshInterval);
  return useQuery({
    queryKey: ["sync-status"],
    queryFn: () => api.get<StatusResponse>("/sync/status"),
    refetchInterval: interval,
  });
}

export function usePauseSync() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: () => api.post<MutationResponse>("/sync/pause"),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["sync-status"] }),
  });
}

export function useResumeSync() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: () => api.post<MutationResponse>("/sync/resume"),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["sync-status"] }),
  });
}

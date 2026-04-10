import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { generatedRequestOptions, unwrapGeneratedResult } from "./client";
import {
  pauseSync as pauseSyncSdk,
  resumeSync as resumeSyncSdk,
  syncStatus as syncStatusSdk,
} from "./generated/sdk.gen";
import { useSettingsStore } from "@/stores/settings";
import type { StatusResponse, MutationResponse } from "@/types/api";

export function useSyncStatus() {
  const interval = useSettingsStore((s) => s.refreshInterval);
  return useQuery({
    queryKey: ["sync-status"],
    queryFn: () =>
      unwrapGeneratedResult<StatusResponse>(
        syncStatusSdk({ ...generatedRequestOptions() }),
      ),
    refetchInterval: interval,
  });
}

export function usePauseSync() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: () =>
      unwrapGeneratedResult<MutationResponse>(
        pauseSyncSdk({ ...generatedRequestOptions() }),
      ),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["sync-status"] }),
  });
}

export function useResumeSync() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: () =>
      unwrapGeneratedResult<MutationResponse>(
        resumeSyncSdk({ ...generatedRequestOptions() }),
      ),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["sync-status"] }),
  });
}

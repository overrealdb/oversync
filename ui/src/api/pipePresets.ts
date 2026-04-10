import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { generatedRequestOptions, unwrapGeneratedResult } from "./client";
import {
  createPipePreset as createPipePresetSdk,
  deletePipePreset as deletePipePresetSdk,
  listPipePresets as listPipePresetsSdk,
  updatePipePreset as updatePipePresetSdk,
} from "./generated/sdk.gen";
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
    queryFn: () =>
      unwrapGeneratedResult<PipePresetListResponse>(
        listPipePresetsSdk({ ...generatedRequestOptions() }),
      ),
    refetchInterval: interval,
  });
}

export function useCreatePipePreset() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (payload: CreatePipePresetRequest) =>
      unwrapGeneratedResult<MutationResponse>(
        createPipePresetSdk({ ...generatedRequestOptions(), body: payload }),
      ),
    onSuccess: async () => {
      await qc.invalidateQueries({ queryKey: ["pipe-presets"] });
    },
  });
}

export function useUpdatePipePreset(name: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (payload: UpdatePipePresetRequest) =>
      unwrapGeneratedResult<MutationResponse>(
        updatePipePresetSdk({
          ...generatedRequestOptions(),
          path: { name },
          body: payload,
        }),
      ),
    onSuccess: async () => {
      await qc.invalidateQueries({ queryKey: ["pipe-presets"] });
    },
  });
}

export function useDeletePipePreset() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (name: string) =>
      unwrapGeneratedResult<MutationResponse>(
        deletePipePresetSdk({ ...generatedRequestOptions(), path: { name } }),
      ),
    onSuccess: async () => {
      await qc.invalidateQueries({ queryKey: ["pipe-presets"] });
    },
  });
}

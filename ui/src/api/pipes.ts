import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { generatedRequestOptions, unwrapGeneratedResult } from "./client";
import {
  createPipe as createPipeSdk,
  deletePipe as deletePipeSdk,
  dryRunHandler as dryRunPipeSdk,
  listPipes as listPipesSdk,
  resolvePipeHandler as resolvePipeSdk,
  updatePipe as updatePipeSdk,
} from "./generated/sdk.gen";
import type { DryRunRequestDoc } from "./generated/types.gen";
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
    queryFn: () =>
      unwrapGeneratedResult<PipeListResponse>(
        listPipesSdk({ ...generatedRequestOptions() }),
      ),
    refetchInterval: interval,
  });
}

export function useResolvePipe(name: string | null, enabled = true) {
  return useQuery({
    queryKey: ["pipe-resolve", name],
    queryFn: () =>
      unwrapGeneratedResult<ResolvePipeResponse>(
        resolvePipeSdk({
          ...generatedRequestOptions(),
          path: { name: name ?? "" },
        }),
      ),
    enabled: enabled && Boolean(name),
  });
}

export function useCreatePipe() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (data: CreatePipeRequest) =>
      unwrapGeneratedResult<MutationResponse>(
        createPipeSdk({ ...generatedRequestOptions(), body: data }),
      ),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["pipes"] });
      qc.invalidateQueries({ queryKey: ["history"] });
    },
  });
}

export function useUpdatePipe(name: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (data: UpdatePipeRequest) =>
      unwrapGeneratedResult<MutationResponse>(
        updatePipeSdk({ ...generatedRequestOptions(), path: { name }, body: data }),
      ),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["pipes"] });
      qc.invalidateQueries({ queryKey: ["pipe-resolve", name] });
      qc.invalidateQueries({ queryKey: ["history"] });
    },
  });
}

export function useDeletePipe() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (name: string) =>
      unwrapGeneratedResult<MutationResponse>(
        deletePipeSdk({ ...generatedRequestOptions(), path: { name } }),
      ),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["pipes"] });
      qc.invalidateQueries({ queryKey: ["history"] });
      qc.invalidateQueries({ queryKey: ["pipe-resolve"] });
    },
  });
}

export function useDryRunPipe() {
  return useMutation({
    mutationFn: (payload: DryRunRequest) =>
      unwrapGeneratedResult<DryRunResult>(
        dryRunPipeSdk({
          ...generatedRequestOptions(),
          body: {
            ...payload,
            mock_data: payload.mock_data ?? [],
            transforms: (payload.transforms ?? []) as Array<Record<string, unknown>>,
          } as unknown as DryRunRequestDoc,
        }),
      ),
  });
}

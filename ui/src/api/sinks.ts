import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { generatedRequestOptions, unwrapGeneratedResult } from "./client";
import {
  createSink as createSinkSdk,
  deleteSink as deleteSinkSdk,
  listSinks as listSinksSdk,
  updateSink as updateSinkSdk,
} from "./generated/sdk.gen";
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
    queryFn: () =>
      unwrapGeneratedResult<SinkListResponse>(
        listSinksSdk({ ...generatedRequestOptions() }),
      ),
    refetchInterval: interval,
  });
}

export function useCreateSink() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (data: CreateSinkRequest) =>
      unwrapGeneratedResult<MutationResponse>(
        createSinkSdk({ ...generatedRequestOptions(), body: data }),
      ),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["sinks"] }),
  });
}

export function useUpdateSink(name: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (data: UpdateSinkRequest) =>
      unwrapGeneratedResult<MutationResponse>(
        updateSinkSdk({ ...generatedRequestOptions(), path: { name }, body: data }),
      ),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["sinks"] }),
  });
}

export function useDeleteSink() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (name: string) =>
      unwrapGeneratedResult<MutationResponse>(
        deleteSinkSdk({ ...generatedRequestOptions(), path: { name } }),
      ),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["sinks"] }),
  });
}

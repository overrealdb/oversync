import { useQuery } from "@tanstack/react-query";
import { generatedRequestOptions, unwrapGeneratedResult } from "./client";
import { getHistory as getHistorySdk } from "./generated/sdk.gen";
import { useSettingsStore } from "@/stores/settings";
import type { HistoryResponse } from "@/types/api";

export function useHistory() {
  const interval = useSettingsStore((s) => s.refreshInterval);
  return useQuery({
    queryKey: ["history"],
    queryFn: () =>
      unwrapGeneratedResult<HistoryResponse>(
        getHistorySdk({ ...generatedRequestOptions() }),
      ),
    refetchInterval: interval,
  });
}

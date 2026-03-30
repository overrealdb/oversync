import { useQuery } from "@tanstack/react-query";
import { api } from "./client";
import { useSettingsStore } from "@/stores/settings";
import type { HistoryResponse } from "@/types/api";

export function useHistory() {
  const interval = useSettingsStore((s) => s.refreshInterval);
  return useQuery({
    queryKey: ["history"],
    queryFn: () => api.get<HistoryResponse>("/history"),
    refetchInterval: interval,
  });
}

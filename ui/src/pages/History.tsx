import { useState, useMemo } from "react";
import { useHistory } from "@/api/history";
import { HistoryTable } from "@/components/history/HistoryTable";
import { HistoryFilters } from "@/components/history/HistoryFilters";
import { HistoryCharts } from "@/components/history/HistoryCharts";
import { LoadingSkeleton } from "@/components/shared/LoadingSkeleton";
import { EmptyState } from "@/components/shared/EmptyState";
import type { CycleStatus } from "@/types/api";

const PAGE_SIZE = 25;

export function History() {
  const { data, isLoading } = useHistory();
  const [statusFilter, setStatusFilter] = useState<CycleStatus | "all">("all");
  const [search, setSearch] = useState("");
  const [dateFrom, setDateFrom] = useState("");
  const [dateTo, setDateTo] = useState("");
  const [visibleCount, setVisibleCount] = useState(PAGE_SIZE);

  const allCycles = useMemo(() => data?.cycles ?? [], [data?.cycles]);

  const filteredCycles = useMemo(() => {
    let result = allCycles;
    if (statusFilter !== "all") {
      result = result.filter((c) => c.status === statusFilter);
    }
    if (search.trim()) {
      const q = search.trim().toLowerCase();
      result = result.filter((c) => c.source.toLowerCase().includes(q));
    }
    if (dateFrom) {
      const from = new Date(dateFrom).getTime();
      result = result.filter((c) => new Date(c.started_at).getTime() >= from);
    }
    if (dateTo) {
      const to = new Date(dateTo).getTime();
      result = result.filter((c) => new Date(c.started_at).getTime() <= to);
    }
    return result;
  }, [allCycles, statusFilter, search, dateFrom, dateTo]);

  const paginatedCycles = filteredCycles.slice(0, visibleCount);
  const hasMore = visibleCount < filteredCycles.length;

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-white">History</h1>
        <p className="text-sm text-gray-400 mt-1">Cycle execution log</p>
      </div>

      {isLoading ? (
        <LoadingSkeleton rows={8} />
      ) : allCycles.length === 0 ? (
        <EmptyState
          title="No cycle history"
          description="Cycles will appear here after syncs run"
        />
      ) : (
        <>
          <HistoryCharts cycles={allCycles} />
          <HistoryFilters
            statusFilter={statusFilter}
            onStatusChange={setStatusFilter}
            search={search}
            onSearchChange={setSearch}
            dateFrom={dateFrom}
            onDateFromChange={setDateFrom}
            dateTo={dateTo}
            onDateToChange={setDateTo}
          />
          <div className="space-y-2">
            <HistoryTable cycles={paginatedCycles} />
            <div className="flex items-center justify-between px-1">
              <p className="text-xs text-gray-500">
                Showing {paginatedCycles.length} of {filteredCycles.length}{" "}
                cycles
              </p>
              {hasMore && (
                <button
                  onClick={() => setVisibleCount((c) => c + PAGE_SIZE)}
                  className="text-sm text-blue-400 hover:text-blue-300 font-medium"
                >
                  Load more
                </button>
              )}
            </div>
          </div>
        </>
      )}
    </div>
  );
}

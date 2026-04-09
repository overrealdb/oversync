import { Search, Calendar } from "lucide-react";
import type { CycleStatus } from "@/types/api";

interface HistoryFiltersProps {
  statusFilter: CycleStatus | "all";
  onStatusChange: (status: CycleStatus | "all") => void;
  search: string;
  onSearchChange: (value: string) => void;
  dateFrom: string;
  onDateFromChange: (value: string) => void;
  dateTo: string;
  onDateToChange: (value: string) => void;
}

const STATUSES: Array<CycleStatus | "all"> = ["all", "success", "failed", "aborted", "running"];

export function HistoryFilters({
  statusFilter,
  onStatusChange,
  search,
  onSearchChange,
  dateFrom,
  onDateFromChange,
  dateTo,
  onDateToChange,
}: HistoryFiltersProps) {
  return (
    <div className="flex flex-col gap-3">
      <div className="flex flex-col sm:flex-row items-start sm:items-center gap-3">
        <div className="flex items-center gap-2">
          <label className="text-xs font-medium text-gray-400 uppercase">Status:</label>
          <select
            value={statusFilter}
            onChange={(e) => onStatusChange(e.target.value as CycleStatus | "all")}
            className="bg-gray-800 border border-gray-700 rounded-md px-3 py-1.5 text-sm text-gray-200 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
          >
            {STATUSES.map((s) => (
              <option key={s} value={s}>
                {s === "all" ? "All" : s.charAt(0).toUpperCase() + s.slice(1)}
              </option>
            ))}
          </select>
        </div>

        <div className="relative flex-1 max-w-sm">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-gray-500" />
          <input
            type="text"
            value={search}
            onChange={(e) => onSearchChange(e.target.value)}
            placeholder="Search by pipe or source name..."
            className="w-full bg-gray-800 border border-gray-700 rounded-md pl-9 pr-3 py-1.5 text-sm text-gray-200 placeholder-gray-500 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
          />
        </div>
      </div>

      <div className="flex flex-col sm:flex-row items-start sm:items-center gap-3">
        <div className="flex items-center gap-2">
          <Calendar className="h-4 w-4 text-gray-500" />
          <label className="text-xs font-medium text-gray-400 uppercase">From:</label>
          <input
            type="datetime-local"
            value={dateFrom}
            onChange={(e) => onDateFromChange(e.target.value)}
            className="bg-gray-800 border border-gray-700 rounded-md px-3 py-1.5 text-sm text-gray-200 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
          />
        </div>
        <div className="flex items-center gap-2">
          <label className="text-xs font-medium text-gray-400 uppercase">To:</label>
          <input
            type="datetime-local"
            value={dateTo}
            onChange={(e) => onDateToChange(e.target.value)}
            className="bg-gray-800 border border-gray-700 rounded-md px-3 py-1.5 text-sm text-gray-200 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
          />
        </div>
        {(dateFrom || dateTo) && (
          <button
            type="button"
            onClick={() => { onDateFromChange(""); onDateToChange(""); }}
            className="text-xs text-blue-400 hover:text-blue-300"
          >
            Clear dates
          </button>
        )}
      </div>
    </div>
  );
}

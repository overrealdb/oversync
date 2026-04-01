import type { CycleInfo } from "@/types/api";
import { StatusBadge } from "@/components/shared/StatusBadge";
import { formatDate, formatNumber, formatDuration, cycleDurationMs } from "@/utils/format";

interface HistoryTableProps {
  cycles: CycleInfo[];
}

export function HistoryTable({ cycles }: HistoryTableProps) {
  return (
    <div className="bg-gray-900 border border-gray-800 rounded-xl overflow-hidden">
      <div className="overflow-x-auto">
        <table className="w-full text-left text-sm">
          <thead>
            <tr className="border-b border-gray-800">
              <th className="px-4 py-3 text-xs font-medium text-gray-400 uppercase tracking-wider">Cycle</th>
              <th className="px-4 py-3 text-xs font-medium text-gray-400 uppercase tracking-wider">Source</th>
              <th className="px-4 py-3 text-xs font-medium text-gray-400 uppercase tracking-wider">Query</th>
              <th className="px-4 py-3 text-xs font-medium text-gray-400 uppercase tracking-wider">Status</th>
              <th className="px-4 py-3 text-xs font-medium text-gray-400 uppercase tracking-wider">Created</th>
              <th className="px-4 py-3 text-xs font-medium text-gray-400 uppercase tracking-wider">Updated</th>
              <th className="px-4 py-3 text-xs font-medium text-gray-400 uppercase tracking-wider">Deleted</th>
              <th className="px-4 py-3 text-xs font-medium text-gray-400 uppercase tracking-wider">Duration</th>
              <th className="px-4 py-3 text-xs font-medium text-gray-400 uppercase tracking-wider">Started</th>
              <th className="px-4 py-3 text-xs font-medium text-gray-400 uppercase tracking-wider">Error</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-800/50">
            {cycles.map((c) => (
              <tr key={c.cycle_id} className="hover:bg-gray-800 transition-colors">
                <td className="px-4 py-3 font-mono text-gray-300">#{c.cycle_id}</td>
                <td className="px-4 py-3 font-medium text-gray-200">{c.source}</td>
                <td className="px-4 py-3 font-mono text-gray-300">{c.query}</td>
                <td className="px-4 py-3">
                  <StatusBadge status={c.status} />
                </td>
                <td className="px-4 py-3 font-mono text-emerald-400">{formatNumber(c.rows_created)}</td>
                <td className="px-4 py-3 font-mono text-blue-400">{formatNumber(c.rows_updated)}</td>
                <td className="px-4 py-3 font-mono text-rose-400">{formatNumber(c.rows_deleted)}</td>
                <td className="px-4 py-3 font-mono text-gray-300">
                  {formatDuration(c.duration_ms ?? cycleDurationMs(c.started_at, c.finished_at))}
                </td>
                <td className="px-4 py-3 font-mono text-gray-400 text-xs whitespace-nowrap">
                  {formatDate(c.started_at)}
                </td>
                <td className="px-4 py-3 text-xs text-rose-400 max-w-48 truncate" title={c.error ?? undefined}>
                  {c.error ?? "--"}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

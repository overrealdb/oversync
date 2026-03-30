import { Play } from "lucide-react";
import { useSource, useTriggerSource } from "@/api/sources";
import { useHistory } from "@/api/history";
import { StatusBadge } from "@/components/shared/StatusBadge";
import { LoadingSkeleton } from "@/components/shared/LoadingSkeleton";
import { useToast } from "@/components/shared/Toast";
import { formatDate, formatNumber, cycleDurationMs, formatDuration } from "@/utils/format";

interface SourceDetailProps {
  name: string;
}

export function SourceDetail({ name }: SourceDetailProps) {
  const { data: source, isLoading } = useSource(name);
  const history = useHistory();
  const triggerSource = useTriggerSource();
  const { toast } = useToast();

  if (isLoading) return <LoadingSkeleton rows={8} />;
  if (!source) return <p className="text-gray-400">Source not found</p>;

  function handleTrigger() {
    triggerSource.mutate(name, {
      onSuccess: (res) => toast("success", res.message),
      onError: (e) => toast("error", e.message),
    });
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-xl font-bold text-white">{source.name}</h2>
          <p className="text-sm text-gray-400 font-mono">{source.connector}</p>
        </div>
        <button
          onClick={handleTrigger}
          disabled={triggerSource.isPending}
          className="flex items-center gap-2 px-4 py-2 bg-emerald-600 hover:bg-emerald-500 text-white text-sm font-medium rounded-md disabled:opacity-50 transition-colors"
        >
          <Play className="h-4 w-4" />
          {triggerSource.isPending ? "Triggering..." : "Trigger Sync"}
        </button>
      </div>

      {/* Queries */}
      <div className="bg-gray-900 border border-gray-800 rounded-lg p-5">
        <h3 className="text-sm font-medium text-gray-300 mb-3">
          Queries ({source.queries.length})
        </h3>
        {source.queries.length === 0 ? (
          <p className="text-sm text-gray-500">No queries configured</p>
        ) : (
          <div className="space-y-2">
            {source.queries.map((q) => (
              <div key={q.id} className="flex items-center justify-between bg-gray-800/50 rounded-md px-3 py-2">
                <span className="text-sm font-mono text-gray-200">{q.id}</span>
                <span className="text-xs text-gray-400">key: {q.key_column}</span>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Status */}
      <div className="bg-gray-900 border border-gray-800 rounded-lg p-5">
        <h3 className="text-sm font-medium text-gray-300 mb-3">Status</h3>
        <div className="grid grid-cols-2 gap-4 text-sm">
          <div>
            <span className="text-gray-400">Total Cycles</span>
            <p className="font-mono text-white">{formatNumber(source.status.total_cycles)}</p>
          </div>
          {source.status.last_cycle && (
            <>
              <div>
                <span className="text-gray-400">Last Status</span>
                <div className="mt-1">
                  <StatusBadge status={source.status.last_cycle.status} />
                </div>
              </div>
              <div>
                <span className="text-gray-400">Started At</span>
                <p className="font-mono text-white">{formatDate(source.status.last_cycle.started_at)}</p>
              </div>
              <div>
                <span className="text-gray-400">Duration</span>
                <p className="font-mono text-white">
                  {formatDuration(
                    cycleDurationMs(
                      source.status.last_cycle.started_at,
                      source.status.last_cycle.finished_at,
                    ),
                  )}
                </p>
              </div>
              <div>
                <span className="text-gray-400">Rows Created</span>
                <p className="font-mono text-white">{formatNumber(source.status.last_cycle.rows_created)}</p>
              </div>
              <div>
                <span className="text-gray-400">Rows Updated</span>
                <p className="font-mono text-white">{formatNumber(source.status.last_cycle.rows_updated)}</p>
              </div>
              <div>
                <span className="text-gray-400">Rows Deleted</span>
                <p className="font-mono text-white">{formatNumber(source.status.last_cycle.rows_deleted)}</p>
              </div>
            </>
          )}
        </div>
      </div>

      {/* Recent Cycle History */}
      <div className="bg-gray-900 border border-gray-800 rounded-lg p-5">
        <h3 className="text-sm font-medium text-gray-300 mb-3">Recent Cycles</h3>
        {(() => {
          const cycles = history.data?.cycles ?? [];
          if (cycles.length === 0)
            return <p className="text-sm text-gray-500">No cycle history</p>;

          return (
            <div className="overflow-x-auto">
              <table className="w-full text-left text-sm">
                <thead>
                  <tr className="border-b border-gray-800">
                    <th className="px-3 py-2 text-xs font-medium text-gray-400 uppercase">Cycle</th>
                    <th className="px-3 py-2 text-xs font-medium text-gray-400 uppercase">Status</th>
                    <th className="px-3 py-2 text-xs font-medium text-gray-400 uppercase">Rows</th>
                    <th className="px-3 py-2 text-xs font-medium text-gray-400 uppercase">Duration</th>
                    <th className="px-3 py-2 text-xs font-medium text-gray-400 uppercase">Started</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-800/50">
                  {cycles.slice(0, 20).map((c) => (
                    <tr key={c.cycle_id} className="hover:bg-gray-800/30">
                      <td className="px-3 py-2 font-mono text-gray-300">{c.cycle_id}</td>
                      <td className="px-3 py-2"><StatusBadge status={c.status} /></td>
                      <td className="px-3 py-2 font-mono text-gray-300">
                        +{c.rows_created} ~{c.rows_updated} -{c.rows_deleted}
                      </td>
                      <td className="px-3 py-2 font-mono text-gray-300">
                        {formatDuration(cycleDurationMs(c.started_at, c.finished_at))}
                      </td>
                      <td className="px-3 py-2 font-mono text-gray-300">{formatDate(c.started_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          );
        })()}
      </div>
    </div>
  );
}

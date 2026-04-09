import { useMemo } from "react";
import { Link } from "@tanstack/react-router";
import { ArrowLeft, Play } from "lucide-react";
import { AreaChart, BarChart, LineChart } from "@tremor/react";
import { useSource, useTriggerSource } from "@/api/sources";
import { useHistory } from "@/api/history";
import { StatusBadge } from "@/components/shared/StatusBadge";
import { LoadingSkeleton } from "@/components/shared/LoadingSkeleton";
import { useToast } from "@/components/shared/useToast";
import { formatDate, formatNumber, formatDuration, cycleDurationMs } from "@/utils/format";
import { Route } from "@/routes/source-detail";

export function SourceDetailPage() {
  const { name } = Route.useParams();
  const { data: source, isLoading } = useSource(name);
  const history = useHistory();
  const triggerSource = useTriggerSource();
  const { toast } = useToast();

  const sourceCycles = useMemo(() => {
    const all = history.data?.cycles ?? [];
    return all.filter((c) => c.source === name);
  }, [history.data, name]);

  const rowsChartData = useMemo(() => {
    const map = new Map<string, number>();
    for (const c of sourceCycles) {
      const hour = new Date(c.started_at).toLocaleTimeString([], {
        hour: "2-digit",
        minute: "2-digit",
      });
      const rows = c.rows_created + c.rows_updated + c.rows_deleted;
      map.set(hour, (map.get(hour) ?? 0) + rows);
    }
    return Array.from(map.entries()).map(([time, total]) => ({
      time,
      "Rows Synced": total,
    }));
  }, [sourceCycles]);

  const durationChartData = useMemo(() => {
    return sourceCycles
      .filter((c) => c.finished_at !== null)
      .map((c) => ({
        cycle: `#${c.cycle_id}`,
        "Duration (ms)": c.duration_ms ?? cycleDurationMs(c.started_at, c.finished_at) ?? 0,
      }))
      .slice(-20);
  }, [sourceCycles]);

  const errorTimelineData = useMemo(() => {
    const hourMap = new Map<string, { total: number; errors: number }>();
    for (const c of sourceCycles) {
      const hour = new Date(c.started_at).toLocaleTimeString([], {
        hour: "2-digit",
        minute: "2-digit",
      });
      const entry = hourMap.get(hour) ?? { total: 0, errors: 0 };
      entry.total++;
      if (c.status === "failed" || c.status === "aborted") entry.errors++;
      hourMap.set(hour, entry);
    }
    return Array.from(hourMap.entries()).map(([time, { total, errors }]) => ({
      time,
      "Error Rate %": total > 0 ? Math.round((errors / total) * 100) : 0,
      Errors: errors,
    }));
  }, [sourceCycles]);

  function handleTrigger() {
    triggerSource.mutate(name, {
      onSuccess: (res) => toast("success", res.message),
      onError: (e) => toast("error", e.message),
    });
  }

  if (isLoading) {
    return (
      <div className="space-y-4">
        <Link
          to="/sources"
          className="inline-flex items-center gap-1 text-sm text-gray-400 hover:text-white transition-colors"
        >
          <ArrowLeft className="h-4 w-4" /> Back to Sources
        </Link>
        <LoadingSkeleton rows={8} />
      </div>
    );
  }

  if (!source) {
    return (
      <div className="space-y-4">
        <Link
          to="/sources"
          className="inline-flex items-center gap-1 text-sm text-gray-400 hover:text-white transition-colors"
        >
          <ArrowLeft className="h-4 w-4" /> Back to Sources
        </Link>
        <p className="text-gray-400">Source not found</p>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <Link
        to="/sources"
        className="inline-flex items-center gap-1 text-sm text-gray-400 hover:text-white transition-colors"
      >
        <ArrowLeft className="h-4 w-4" /> Back to Sources
      </Link>

      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-white">{source.name}</h1>
          <p className="text-sm text-gray-400 font-mono mt-1">{source.connector}</p>
        </div>
        <button
          onClick={handleTrigger}
          disabled={triggerSource.isPending}
          className="flex items-center gap-2 px-4 py-2 bg-emerald-600 hover:bg-emerald-500 text-white text-sm font-medium rounded-lg disabled:opacity-50 transition-colors"
        >
          <Play className="h-4 w-4" />
          {triggerSource.isPending ? "Triggering..." : "Trigger Sync"}
        </button>
      </div>

      {/* Config summary */}
      <div className="bg-gray-900 border border-gray-800 rounded-xl p-6">
        <h3 className="text-sm font-medium text-gray-300 mb-3">Configuration</h3>
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-4 text-sm">
          <div>
            <span className="text-gray-400">Connector</span>
            <p className="font-mono text-white mt-1">{source.connector}</p>
          </div>
          <div>
            <span className="text-gray-400">Interval</span>
            <p className="font-mono text-white mt-1">{source.interval_secs}s</p>
          </div>
          <div>
            <span className="text-gray-400">Total Cycles</span>
            <p className="font-mono text-white mt-1">{formatNumber(source.status.total_cycles)}</p>
          </div>
          <div>
            <span className="text-gray-400">Last Status</span>
            <div className="mt-1">
              {source.status.last_cycle ? (
                <StatusBadge status={source.status.last_cycle.status} />
              ) : (
                <span className="text-gray-500">--</span>
              )}
            </div>
          </div>
        </div>
      </div>

      {/* Queries */}
      <div className="bg-gray-900 border border-gray-800 rounded-xl p-6">
        <h3 className="text-sm font-medium text-gray-300 mb-3">
          Queries ({source.queries.length})
        </h3>
        {source.queries.length === 0 ? (
          <p className="text-sm text-gray-500">No queries configured</p>
        ) : (
          <div className="space-y-2">
            {source.queries.map((q) => (
              <div
                key={q.id}
                className="flex items-center justify-between bg-gray-800/50 rounded-lg px-4 py-2.5"
              >
                <span className="text-sm font-mono text-gray-200">{q.id}</span>
                <span className="text-xs text-gray-400">key: {q.key_column}</span>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Charts */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        <div className="bg-gray-900 border border-gray-800 rounded-xl p-6">
          <h3 className="text-sm font-medium text-gray-300 mb-4">Rows Synced Over Time</h3>
          {rowsChartData.length === 0 ? (
            <p className="text-sm text-gray-500 py-8 text-center">No data yet</p>
          ) : (
            <AreaChart
              className="h-48"
              data={rowsChartData}
              index="time"
              categories={["Rows Synced"]}
              colors={["emerald"]}
              showLegend={false}
              showGridLines={false}
              curveType="monotone"
            />
          )}
        </div>
        <div className="bg-gray-900 border border-gray-800 rounded-xl p-6">
          <h3 className="text-sm font-medium text-gray-300 mb-4">Cycle Duration Trend</h3>
          {durationChartData.length === 0 ? (
            <p className="text-sm text-gray-500 py-8 text-center">No data yet</p>
          ) : (
            <BarChart
              className="h-48"
              data={durationChartData}
              index="cycle"
              categories={["Duration (ms)"]}
              colors={["blue"]}
              showLegend={false}
              showGridLines={false}
            />
          )}
        </div>
        <div className="bg-gray-900 border border-gray-800 rounded-xl p-6">
          <h3 className="text-sm font-medium text-gray-300 mb-4">Error Timeline</h3>
          {errorTimelineData.length === 0 ? (
            <p className="text-sm text-gray-500 py-8 text-center">No data yet</p>
          ) : (
            <LineChart
              className="h-48"
              data={errorTimelineData}
              index="time"
              categories={["Error Rate %", "Errors"]}
              colors={["rose", "amber"]}
              showLegend={true}
              showGridLines={false}
              curveType="monotone"
            />
          )}
        </div>
      </div>

      {/* Cycle History */}
      <div className="bg-gray-900 border border-gray-800 rounded-xl p-6">
        <h3 className="text-sm font-medium text-gray-300 mb-3">Cycle History</h3>
        {sourceCycles.length === 0 ? (
          <p className="text-sm text-gray-500">No cycle history</p>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-left text-sm">
              <thead>
                <tr className="border-b border-gray-800">
                  <th className="px-3 py-2 text-xs font-medium text-gray-400 uppercase">Cycle</th>
                  <th className="px-3 py-2 text-xs font-medium text-gray-400 uppercase">Query</th>
                  <th className="px-3 py-2 text-xs font-medium text-gray-400 uppercase">Status</th>
                  <th className="px-3 py-2 text-xs font-medium text-gray-400 uppercase">Rows</th>
                  <th className="px-3 py-2 text-xs font-medium text-gray-400 uppercase">Duration</th>
                  <th className="px-3 py-2 text-xs font-medium text-gray-400 uppercase">Started</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-800/50">
                {sourceCycles.slice(0, 50).map((c) => (
                  <tr key={c.cycle_id} className="hover:bg-gray-800/30 transition-colors">
                    <td className="px-3 py-2 font-mono text-gray-300">#{c.cycle_id}</td>
                    <td className="px-3 py-2 font-mono text-gray-300">{c.query}</td>
                    <td className="px-3 py-2">
                      <StatusBadge status={c.status} />
                    </td>
                    <td className="px-3 py-2 font-mono text-gray-300">
                      +{c.rows_created} ~{c.rows_updated} -{c.rows_deleted}
                    </td>
                    <td className="px-3 py-2 font-mono text-gray-300">
                      {formatDuration(c.duration_ms ?? cycleDurationMs(c.started_at, c.finished_at))}
                    </td>
                    <td className="px-3 py-2 font-mono text-gray-300">
                      {formatDate(c.started_at)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}

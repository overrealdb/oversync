import { DonutChart, AreaChart, LineChart } from "@tremor/react";
import type { CycleInfo } from "@/types/api";

interface HistoryChartsProps {
  cycles: CycleInfo[];
}

function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0;
  const idx = Math.ceil((p / 100) * sorted.length) - 1;
  return sorted[Math.max(0, idx)];
}

export function HistoryCharts({ cycles }: HistoryChartsProps) {
  // --- Success rate donut ---
  const statusCounts = { success: 0, failed: 0, aborted: 0, running: 0 };
  for (const c of cycles) {
    if (c.status in statusCounts) {
      statusCounts[c.status as keyof typeof statusCounts]++;
    }
  }
  const donutData = Object.entries(statusCounts)
    .filter(([, v]) => v > 0)
    .map(([name, value]) => ({ name, value }));

  // --- Throughput area chart ---
  const hourlyRows = new Map<string, number>();
  for (const c of cycles) {
    const hour = new Date(c.started_at).toLocaleTimeString([], {
      hour: "2-digit",
      minute: "2-digit",
    });
    const rows = c.rows_created + c.rows_updated + c.rows_deleted;
    hourlyRows.set(hour, (hourlyRows.get(hour) ?? 0) + rows);
  }
  const throughputData = Array.from(hourlyRows.entries()).map(([time, rows]) => ({
    time,
    "Rows Synced": rows,
  }));

  // --- P50/P95/P99 duration chart ---
  const byHour = new Map<string, number[]>();
  for (const c of cycles) {
    if (c.duration_ms == null) continue;
    const hour = new Date(c.started_at).toLocaleTimeString([], {
      hour: "2-digit",
      minute: "2-digit",
    });
    const arr = byHour.get(hour) ?? [];
    arr.push(c.duration_ms);
    byHour.set(hour, arr);
  }
  const percentileData = Array.from(byHour.entries()).map(([time, durations]) => {
    const sorted = durations.sort((a, b) => a - b);
    return {
      time,
      P50: Math.round(percentile(sorted, 50)),
      P95: Math.round(percentile(sorted, 95)),
      P99: Math.round(percentile(sorted, 99)),
    };
  });

  return (
    <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
      <div className="bg-gray-900 border border-gray-800 rounded-xl p-6">
        <h3 className="text-sm font-medium text-gray-300 mb-4">Success Rate</h3>
        {donutData.length === 0 ? (
          <p className="text-sm text-gray-500 py-8 text-center">No data</p>
        ) : (
          <DonutChart
            className="h-44"
            data={donutData}
            category="value"
            index="name"
            colors={["emerald", "rose", "amber", "blue"]}
            showLabel={true}
            showAnimation={true}
          />
        )}
      </div>

      <div className="bg-gray-900 border border-gray-800 rounded-xl p-6">
        <h3 className="text-sm font-medium text-gray-300 mb-4">Throughput</h3>
        {throughputData.length === 0 ? (
          <p className="text-sm text-gray-500 py-8 text-center">No data</p>
        ) : (
          <AreaChart
            className="h-44"
            data={throughputData}
            index="time"
            categories={["Rows Synced"]}
            colors={["blue"]}
            showLegend={false}
            showGridLines={false}
            curveType="monotone"
          />
        )}
      </div>

      <div className="bg-gray-900 border border-gray-800 rounded-xl p-6">
        <h3 className="text-sm font-medium text-gray-300 mb-4">Cycle Duration Percentiles</h3>
        {percentileData.length === 0 ? (
          <p className="text-sm text-gray-500 py-8 text-center">No data</p>
        ) : (
          <LineChart
            className="h-44"
            data={percentileData}
            index="time"
            categories={["P50", "P95", "P99"]}
            colors={["emerald", "amber", "rose"]}
            showLegend={true}
            showGridLines={false}
            curveType="monotone"
            valueFormatter={(v) => v >= 1000 ? `${(v / 1000).toFixed(1)}s` : `${v}ms`}
          />
        )}
      </div>
    </div>
  );
}

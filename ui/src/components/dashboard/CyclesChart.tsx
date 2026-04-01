import { AreaChart } from "@tremor/react";
import type { CycleInfo } from "@/types/api";

interface CyclesChartProps {
  cycles: CycleInfo[];
}

export function CyclesChart({ cycles }: CyclesChartProps) {
  const now = Date.now();
  const cutoff = now - 24 * 60 * 60 * 1000;
  const hourlyMap = new Map<
    string,
    { Success: number; Failed: number; Aborted: number }
  >();

  for (const c of cycles) {
    const t = new Date(c.started_at).getTime();
    if (t < cutoff) continue;
    const hour = new Date(t).toLocaleTimeString([], {
      hour: "2-digit",
      minute: "2-digit",
    });
    const rows = c.rows_created + c.rows_updated + c.rows_deleted;
    const entry = hourlyMap.get(hour) ?? { Success: 0, Failed: 0, Aborted: 0 };
    if (c.status === "failed") {
      entry.Failed += rows;
    } else if (c.status === "aborted") {
      entry.Aborted += rows;
    } else {
      entry.Success += rows;
    }
    hourlyMap.set(hour, entry);
  }

  const data = Array.from(hourlyMap.entries()).map(([time, counts]) => ({
    time,
    ...counts,
  }));

  return (
    <div className="bg-gray-900 border border-gray-800 rounded-xl p-6">
      <h3 className="text-sm font-medium text-gray-300 mb-4">
        Rows Synced (Last 24h)
      </h3>
      {data.length === 0 ? (
        <p className="text-sm text-gray-500 py-8 text-center">
          No cycle data yet
        </p>
      ) : (
        <AreaChart
          className="h-56"
          data={data}
          index="time"
          categories={["Success", "Failed", "Aborted"]}
          colors={["emerald", "rose", "amber"]}
          showLegend={true}
          showGridLines={false}
          curveType="monotone"
          stack={true}
        />
      )}
    </div>
  );
}

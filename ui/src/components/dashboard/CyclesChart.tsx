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
    <div className="panel-surface p-6">
      <div className="mb-5 flex items-start justify-between gap-4">
        <div>
          <div className="eyebrow">Throughput</div>
          <h3 className="mt-2 text-lg font-semibold tracking-[-0.03em] text-white">
            Rows synced in the last 24 hours
          </h3>
        </div>
      </div>
      {data.length === 0 ? (
        <div className="panel-subtle flex h-56 items-center justify-center px-6 text-center text-sm leading-6 text-slate-500">
          No cycle data yet. Once sources start running, this panel turns into a rolling throughput view.
        </div>
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

import { BarChart } from "@tremor/react";
import type { CycleInfo } from "@/types/api";
import { cycleDurationMs } from "@/utils/format";

interface DurationChartProps {
  cycles: CycleInfo[];
}

export function DurationChart({ cycles }: DurationChartProps) {
  const sourceMap = new Map<string, { total: number; count: number }>();

  for (const c of cycles) {
    const dur = c.duration_ms ?? cycleDurationMs(c.started_at, c.finished_at);
    if (dur == null) continue;
    const source = c.source;
    const entry = sourceMap.get(source) ?? { total: 0, count: 0 };
    entry.total += dur;
    entry.count += 1;
    sourceMap.set(source, entry);
  }

  const data = Array.from(sourceMap.entries())
    .map(([source, { total, count }]) => ({
      source,
      "Avg Duration (ms)": Math.round(total / count),
    }))
    .slice(0, 10);

  return (
    <div className="bg-gray-900 border border-gray-800 rounded-xl p-6">
      <h3 className="text-sm font-medium text-gray-300 mb-4">
        Avg Cycle Duration per Source
      </h3>
      {data.length === 0 ? (
        <p className="text-sm text-gray-500 py-8 text-center">
          No cycle data yet
        </p>
      ) : (
        <BarChart
          className="h-56"
          data={data}
          index="source"
          categories={["Avg Duration (ms)"]}
          colors={["blue"]}
          showLegend={false}
          showGridLines={false}
        />
      )}
    </div>
  );
}

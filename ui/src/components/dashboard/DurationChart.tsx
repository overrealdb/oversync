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
    <div className="panel-surface p-6">
      <div className="mb-5">
        <div className="eyebrow">Latency</div>
        <h3 className="mt-2 text-lg font-semibold tracking-[-0.03em] text-white">
          Average cycle duration by pipe
        </h3>
      </div>
      {data.length === 0 ? (
        <div className="panel-subtle flex h-56 items-center justify-center px-6 text-center text-sm leading-6 text-slate-500">
          Duration history appears here after the first completed cycles.
        </div>
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

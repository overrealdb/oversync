import { SparkAreaChart } from "@tremor/react";
import type { CycleInfo, SourceInfo } from "@/types/api";

interface ErrorSparklinesProps {
  sources: SourceInfo[];
  cycles: CycleInfo[];
}

export function ErrorSparklines({ sources, cycles }: ErrorSparklinesProps) {
  if (sources.length === 0) return null;

  const cyclesBySource = new Map<string, CycleInfo[]>();
  for (const c of cycles) {
    const arr = cyclesBySource.get(c.source) ?? [];
    arr.push(c);
    cyclesBySource.set(c.source, arr);
  }

  const rows = sources.map((s) => {
    const srcCycles = cyclesBySource.get(s.name) ?? [];
    const total = srcCycles.length;
    const failed = srcCycles.filter(
      (c) => c.status === "failed" || c.status === "aborted",
    ).length;
    const rate = total > 0 ? Math.round((failed / total) * 100) : 0;

    // Build sparkline data: group cycles by hour, compute error % per hour
    const hourBuckets = new Map<string, { total: number; errors: number }>();
    for (const c of srcCycles) {
      const hour = new Date(c.started_at).toISOString().slice(0, 13);
      const bucket = hourBuckets.get(hour) ?? { total: 0, errors: 0 };
      bucket.total++;
      if (c.status === "failed" || c.status === "aborted") bucket.errors++;
      hourBuckets.set(hour, bucket);
    }
    const sparkData = Array.from(hourBuckets.entries())
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([hour, b]) => ({
        hour,
        "Error %": b.total > 0 ? Math.round((b.errors / b.total) * 100) : 0,
      }));

    return { name: s.name, rate, total, failed, sparkData };
  });

  return (
    <div className="bg-gray-900 border border-gray-800 rounded-xl p-6">
      <h3 className="text-sm font-medium text-gray-300 mb-4">
        Error Rate by Source
      </h3>
      {rows.every((r) => r.total === 0) ? (
        <p className="text-sm text-gray-500 py-4 text-center">
          No cycle data yet
        </p>
      ) : (
        <div className="space-y-3">
          {rows.map((r) => (
            <div
              key={r.name}
              className="flex items-center gap-4 py-1.5 border-b border-gray-800 last:border-0"
            >
              <span className="text-sm text-gray-300 w-32 truncate font-mono">
                {r.name}
              </span>
              <div className="flex-1 min-w-0">
                {r.sparkData.length > 1 ? (
                  <SparkAreaChart
                    className="h-8 w-full"
                    data={r.sparkData}
                    categories={["Error %"]}
                    index="hour"
                    colors={[r.rate > 20 ? "rose" : r.rate > 5 ? "amber" : "emerald"]}
                    curveType="monotone"
                  />
                ) : (
                  <div className="h-8 flex items-center">
                    <div className="h-px w-full bg-gray-800" />
                  </div>
                )}
              </div>
              <span
                className={`text-sm font-mono w-12 text-right ${
                  r.rate > 20
                    ? "text-rose-400"
                    : r.rate > 5
                      ? "text-amber-400"
                      : "text-emerald-400"
                }`}
              >
                {r.rate}%
              </span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

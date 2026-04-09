import { SparkAreaChart } from "@tremor/react";
import type { CycleInfo, PipeInfo } from "@/types/api";

interface ErrorSparklinesProps {
  pipes: PipeInfo[];
  cycles: CycleInfo[];
}

export function ErrorSparklines({ pipes, cycles }: ErrorSparklinesProps) {
  if (pipes.length === 0) return null;

  const cyclesByPipe = new Map<string, CycleInfo[]>();
  for (const c of cycles) {
    const arr = cyclesByPipe.get(c.source) ?? [];
    arr.push(c);
    cyclesByPipe.set(c.source, arr);
  }

  const rows = pipes.map((pipe) => {
    const pipeCycles = cyclesByPipe.get(pipe.name) ?? [];
    const total = pipeCycles.length;
    const failed = pipeCycles.filter(
      (c) => c.status === "failed" || c.status === "aborted",
    ).length;
    const rate = total > 0 ? Math.round((failed / total) * 100) : 0;

    // Build sparkline data: group cycles by hour, compute error % per hour
    const hourBuckets = new Map<string, { total: number; errors: number }>();
    for (const c of pipeCycles) {
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

    return { name: pipe.name, rate, total, failed, sparkData };
  });

  return (
    <div className="panel-surface p-6">
      <div className="mb-5">
        <div className="eyebrow">Reliability</div>
        <h3 className="mt-2 text-lg font-semibold tracking-[-0.03em] text-white">
          Error rate by pipe
        </h3>
      </div>
      {rows.every((r) => r.total === 0) ? (
        <div className="panel-subtle flex items-center justify-center px-6 py-10 text-center text-sm leading-6 text-slate-500">
          No cycle data yet. Error profiles will show up once pipes begin producing execution history.
        </div>
      ) : (
        <div className="space-y-3">
          {rows.map((r) => (
            <div
              key={r.name}
              className="panel-subtle flex items-center gap-4 px-4 py-3"
            >
              <span className="w-40 truncate text-sm text-slate-200">
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
                    <div className="h-px w-full bg-white/10" />
                  </div>
                )}
              </div>
              <span
                className={`w-12 text-right font-mono text-sm ${
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

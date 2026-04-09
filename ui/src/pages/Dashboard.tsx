import { Pause, Play } from "lucide-react";
import { useSources } from "@/api/sources";
import { useSinks } from "@/api/sinks";
import { useHistory } from "@/api/history";
import { useSyncStatus, usePauseSync, useResumeSync } from "@/api/sync";
import { OverviewCards } from "@/components/dashboard/OverviewCards";
import { CyclesChart } from "@/components/dashboard/CyclesChart";
import { DurationChart } from "@/components/dashboard/DurationChart";
import { ErrorSparklines } from "@/components/dashboard/ErrorSparklines";
import { StatusBadge } from "@/components/shared/StatusBadge";
import { useToast } from "@/components/shared/useToast";

export function Dashboard() {
  const sources = useSources();
  const sinks = useSinks();
  const history = useHistory();
  const syncStatus = useSyncStatus();
  const pauseSync = usePauseSync();
  const resumeSync = useResumeSync();
  const { toast } = useToast();

  const isPaused = syncStatus.data?.paused ?? false;
  const isRunning = syncStatus.data?.running ?? false;

  const sourcesList = sources.data?.sources ?? [];
  const sinksList = sinks.data?.sinks ?? [];
  const cycles = history.data?.cycles ?? [];

  const totalQueries = sourcesList.reduce((sum, s) => sum + s.queries.length, 0);
  const today = new Date().toDateString();
  const cyclesToday = cycles.filter(
    (c) => new Date(c.started_at).toDateString() === today,
  ).length;

  function handleToggleSync() {
    if (isPaused) {
      resumeSync.mutate(undefined, {
        onSuccess: () => toast("success", "Syncing resumed"),
        onError: (e) => toast("error", e.message),
      });
    } else {
      pauseSync.mutate(undefined, {
        onSuccess: () => toast("success", "Syncing paused"),
        onError: (e) => toast("error", e.message),
      });
    }
  }

  return (
    <div className="section-stack">
      <section className="panel-surface relative overflow-hidden px-6 py-7 sm:px-8 sm:py-8">
        <div className="absolute inset-y-0 right-0 hidden w-1/3 bg-[radial-gradient(circle_at_center,rgba(52,211,153,0.14),transparent_65%)] lg:block" />
        <div className="relative grid gap-6 lg:grid-cols-[minmax(0,1fr)_320px]">
          <div>
            <div className="eyebrow">Control Plane</div>
            <h1 className="page-title mt-3">Dashboard</h1>
            <p className="page-copy mt-4">
              Track sync posture, watch cycle output, and keep connector changes grounded in live system state.
            </p>
            <div className="mt-6 flex flex-wrap items-center gap-3">
              <StatusBadge status={isPaused ? "paused" : isRunning ? "running" : "idle"} />
              <span className="rounded-full border border-white/10 bg-white/[0.04] px-4 py-2 text-sm text-slate-300">
                {totalQueries} query lanes configured
              </span>
              <span className="rounded-full border border-white/10 bg-white/[0.04] px-4 py-2 text-sm text-slate-300">
                {cyclesToday} cycles started today
              </span>
            </div>
          </div>
          <div className="panel-subtle p-5">
            <div className="text-xs font-semibold uppercase tracking-[0.22em] text-slate-500">
              Sync controls
            </div>
            <div className="mt-4 space-y-4">
              <div className="flex items-center justify-between text-sm text-slate-400">
                <span>Sources online</span>
                <span className="font-mono text-lg text-white">{sourcesList.length}</span>
              </div>
              <div className="flex items-center justify-between text-sm text-slate-400">
                <span>Sinks online</span>
                <span className="font-mono text-lg text-white">{sinksList.length}</span>
              </div>
              <button
                onClick={handleToggleSync}
                disabled={pauseSync.isPending || resumeSync.isPending}
                className={`mt-2 inline-flex w-full items-center justify-center gap-2 rounded-full px-5 py-3 text-sm font-semibold transition-colors ${
                  isPaused
                    ? "bg-emerald-400 text-slate-950 hover:bg-emerald-300"
                    : "bg-amber-400 text-slate-950 hover:bg-amber-300"
                } disabled:opacity-50`}
              >
                {isPaused ? <Play className="h-4 w-4" /> : <Pause className="h-4 w-4" />}
                {isPaused ? "Resume syncing" : "Pause syncing"}
              </button>
            </div>
          </div>
        </div>
      </section>

      <OverviewCards
        sourcesCount={sourcesList.length}
        sinksCount={sinksList.length}
        queriesCount={totalQueries}
        cyclesToday={cyclesToday}
      />

      <div className="grid grid-cols-1 gap-6 lg:grid-cols-2">
        <CyclesChart cycles={cycles} />
        <DurationChart cycles={cycles} />
      </div>

      <ErrorSparklines sources={sourcesList} cycles={cycles} />
    </div>
  );
}

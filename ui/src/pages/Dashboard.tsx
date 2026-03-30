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
import { useToast } from "@/components/shared/Toast";

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
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-white">Dashboard</h1>
          <p className="text-sm text-gray-400 mt-1">OverSync pipeline overview</p>
        </div>
        <div className="flex items-center gap-3">
          <StatusBadge status={isPaused ? "paused" : isRunning ? "running" : "idle"} />
          <button
            onClick={handleToggleSync}
            disabled={pauseSync.isPending || resumeSync.isPending}
            className={`flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
              isPaused
                ? "bg-emerald-600 hover:bg-emerald-500 text-white"
                : "bg-amber-600 hover:bg-amber-500 text-white"
            } disabled:opacity-50`}
          >
            {isPaused ? <Play className="h-4 w-4" /> : <Pause className="h-4 w-4" />}
            {isPaused ? "Resume" : "Pause"}
          </button>
        </div>
      </div>

      <OverviewCards
        sourcesCount={sourcesList.length}
        sinksCount={sinksList.length}
        queriesCount={totalQueries}
        cyclesToday={cyclesToday}
      />

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <CyclesChart cycles={cycles} />
        <DurationChart cycles={cycles} />
      </div>

      <ErrorSparklines sources={sourcesList} cycles={cycles} />
    </div>
  );
}

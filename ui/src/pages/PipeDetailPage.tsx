import { useMemo, useState } from "react";
import { Link } from "@tanstack/react-router";
import { ArrowLeft, FileSearch, Pencil } from "lucide-react";
import { useHistory } from "@/api/history";
import { useResolvePipe } from "@/api/pipes";
import { PipeDryRunDialog } from "@/components/pipes/PipeDryRunDialog";
import { PipeForm } from "@/components/pipes/PipeForm";
import { LoadingSkeleton } from "@/components/shared/LoadingSkeleton";
import { StatusBadge } from "@/components/shared/StatusBadge";
import { formatDate, formatDuration, formatNumber, cycleDurationMs } from "@/utils/format";
import { Route } from "@/routes/pipe-detail";

function StatCard({
  label,
  value,
  detail,
}: {
  label: string;
  value: string;
  detail: string;
}) {
  return (
    <div className="panel-subtle p-4">
      <div className="text-xs font-semibold uppercase tracking-[0.22em] text-slate-500">
        {label}
      </div>
      <div className="mt-3 text-2xl font-semibold tracking-[-0.04em] text-white">
        {value}
      </div>
      <div className="mt-2 text-sm leading-6 text-slate-400">{detail}</div>
    </div>
  );
}

export function PipeDetailPage() {
  const { name } = Route.useParams();
  const resolvePipe = useResolvePipe(name);
  const history = useHistory();
  const [editOpen, setEditOpen] = useState(false);
  const [dryRunOpen, setDryRunOpen] = useState(false);

  const pipe = resolvePipe.data?.pipe;
  const effectiveQueries = resolvePipe.data?.effective_queries ?? [];
  const pipeCycles = useMemo(() => {
    const all = history.data?.cycles ?? [];
    return all.filter((cycle) => cycle.source === name);
  }, [history.data, name]);

  const lastCycle = pipeCycles[0] ?? null;
  const totalRows = useMemo(
    () =>
      pipeCycles.reduce(
        (sum, cycle) => sum + cycle.rows_created + cycle.rows_updated + cycle.rows_deleted,
        0,
      ),
    [pipeCycles],
  );
  const averageDuration = useMemo(() => {
    const durations = pipeCycles
      .map((cycle) => cycle.duration_ms ?? cycleDurationMs(cycle.started_at, cycle.finished_at))
      .filter((value): value is number => value !== null);
    if (durations.length === 0) return null;
    return Math.round(durations.reduce((sum, value) => sum + value, 0) / durations.length);
  }, [pipeCycles]);

  if (resolvePipe.isLoading) {
    return (
      <div className="section-stack">
        <Link
          to="/pipes"
          className="inline-flex items-center gap-1 text-sm text-slate-400 transition-colors hover:text-white"
        >
          <ArrowLeft className="h-4 w-4" /> Back to Pipes
        </Link>
        <LoadingSkeleton rows={8} />
      </div>
    );
  }

  if (resolvePipe.isError || !pipe) {
    return (
      <div className="section-stack">
        <Link
          to="/pipes"
          className="inline-flex items-center gap-1 text-sm text-slate-400 transition-colors hover:text-white"
        >
          <ArrowLeft className="h-4 w-4" /> Back to Pipes
        </Link>
        <div className="rounded-3xl border border-rose-300/20 bg-rose-400/10 px-6 py-5 text-sm text-rose-100">
          {resolvePipe.isError ? resolvePipe.error.message : `Pipe not found: ${name}`}
        </div>
      </div>
    );
  }

  return (
    <div className="section-stack">
      <Link
        to="/pipes"
        className="inline-flex items-center gap-1 text-sm text-slate-400 transition-colors hover:text-white"
      >
        <ArrowLeft className="h-4 w-4" /> Back to Pipes
      </Link>

      <section className="panel-surface px-6 py-7 sm:px-8 sm:py-8">
        <div className="flex flex-col gap-6 xl:flex-row xl:items-end xl:justify-between">
          <div>
            <div className="eyebrow">Runnable Pipe</div>
            <h1 className="page-title mt-3">{pipe.name}</h1>
            <p className="page-copy mt-4">
              Inspect the persisted runtime shape, the effective queries after recipe expansion, and the recent cycle history that this pipe actually produced.
            </p>
          </div>
          <div className="flex flex-wrap items-center gap-3">
            <div className="rounded-full border border-white/10 bg-white/[0.04] px-4 py-2 text-sm text-slate-300">
              {pipe.recipe?.type ?? "manual"}
            </div>
            {pipe.enabled ? (
              <div className="rounded-full border border-emerald-300/20 bg-emerald-400/10 px-4 py-2 text-sm font-medium text-emerald-200">
                enabled
              </div>
            ) : (
              <div className="rounded-full border border-amber-300/20 bg-amber-400/10 px-4 py-2 text-sm font-medium text-amber-200">
                disabled
              </div>
            )}
            <button onClick={() => setDryRunOpen(true)} className="action-button-secondary">
              <FileSearch className="h-4 w-4" />
              Dry Run
            </button>
            <button onClick={() => setEditOpen(true)} className="action-button">
              <Pencil className="h-4 w-4" />
              Edit Pipe
            </button>
          </div>
        </div>
      </section>

      <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <StatCard
          label="Effective Queries"
          value={formatNumber(effectiveQueries.length)}
          detail={`${pipe.queries.length} declared, ${effectiveQueries.length} runnable after recipe expansion`}
        />
        <StatCard
          label="Cycle Count"
          value={formatNumber(pipeCycles.length)}
          detail={lastCycle ? `Latest cycle #${lastCycle.cycle_id}` : "No completed cycles yet"}
        />
        <StatCard
          label="Rows Processed"
          value={formatNumber(totalRows)}
          detail="Created + updated + deleted rows across visible cycle history"
        />
        <StatCard
          label="Average Duration"
          value={averageDuration === null ? "--" : formatDuration(averageDuration)}
          detail="Mean runtime across the currently loaded cycle history"
        />
      </section>

      <div className="grid gap-6 xl:grid-cols-[minmax(0,1.15fr)_minmax(320px,0.85fr)]">
        <section className="panel-surface overflow-hidden">
          <div className="border-b border-white/8 px-6 py-5">
            <div className="text-xs font-semibold uppercase tracking-[0.22em] text-slate-500">
              Effective Queries
            </div>
            <p className="mt-2 text-sm leading-6 text-slate-400">
              This is what the scheduler really executes. Recipe-backed pipes expand here even if the persisted pipe stores no explicit query list.
            </p>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-left text-sm">
              <thead>
                <tr className="border-b border-white/8">
                  <th className="px-6 py-3 text-xs font-medium uppercase tracking-[0.22em] text-slate-500">Query</th>
                  <th className="px-6 py-3 text-xs font-medium uppercase tracking-[0.22em] text-slate-500">Key</th>
                  <th className="px-6 py-3 text-xs font-medium uppercase tracking-[0.22em] text-slate-500">Shape</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-white/6">
                {effectiveQueries.length === 0 ? (
                  <tr>
                    <td colSpan={3} className="px-6 py-5 text-slate-500">
                      No effective queries are available for this pipe.
                    </td>
                  </tr>
                ) : (
                  effectiveQueries.map((query) => (
                    <tr key={query.id} className="align-top hover:bg-white/[0.03]">
                      <td className="px-6 py-4">
                        <div className="font-mono text-xs text-slate-100">{query.id}</div>
                        <div className="mt-2 line-clamp-3 max-w-2xl font-mono text-[11px] leading-5 text-slate-500">
                          {query.sql}
                        </div>
                      </td>
                      <td className="px-6 py-4 font-mono text-slate-300">{query.key_column}</td>
                      <td className="px-6 py-4 text-slate-400">
                        {pipe.recipe ? "recipe-expanded" : "manual"}
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        </section>

        <section className="space-y-6">
          <div className="panel-surface p-6">
            <div className="text-xs font-semibold uppercase tracking-[0.22em] text-slate-500">
              Runtime Shape
            </div>
            <div className="mt-4 grid gap-4 sm:grid-cols-2">
              <div>
                <div className="text-xs uppercase tracking-[0.18em] text-slate-500">Origin</div>
                <div className="mt-2 font-mono text-sm text-white">{pipe.origin.connector}</div>
                <div className="mt-1 break-all font-mono text-xs text-slate-500">{pipe.origin.dsn}</div>
              </div>
              <div>
                <div className="text-xs uppercase tracking-[0.18em] text-slate-500">Targets</div>
                <div className="mt-2 text-sm text-white">
                  {pipe.targets.length > 0 ? pipe.targets.join(", ") : "--"}
                </div>
              </div>
              <div>
                <div className="text-xs uppercase tracking-[0.18em] text-slate-500">Schedule</div>
                <div className="mt-2 text-sm text-white">
                  every {pipe.schedule?.interval_secs ?? 300}s
                </div>
                <div className="mt-1 text-xs text-slate-500">
                  missed tick policy: {pipe.schedule?.missed_tick_policy ?? "skip"}
                </div>
              </div>
              <div>
                <div className="text-xs uppercase tracking-[0.18em] text-slate-500">Delta</div>
                <div className="mt-2 text-sm text-white">
                  {pipe.delta?.diff_mode ?? "db"} mode
                </div>
                <div className="mt-1 text-xs text-slate-500">
                  fail-safe threshold: {pipe.delta?.fail_safe_threshold ?? 30}%
                </div>
              </div>
              <div>
                <div className="text-xs uppercase tracking-[0.18em] text-slate-500">Credential</div>
                <div className="mt-2 text-sm text-white">
                  {pipe.origin.credential ?? "--"}
                </div>
              </div>
              <div>
                <div className="text-xs uppercase tracking-[0.18em] text-slate-500">Recipe Prefix</div>
                <div className="mt-2 text-sm text-white">
                  {pipe.recipe?.prefix ?? "manual pipe"}
                </div>
              </div>
            </div>
            <div className="mt-5 grid gap-3 sm:grid-cols-3">
              <div className="rounded-2xl border border-white/8 bg-white/[0.03] px-4 py-3">
                <div className="text-xs uppercase tracking-[0.18em] text-slate-500">Filters</div>
                <div className="mt-2 text-lg font-semibold text-white">{pipe.filters.length}</div>
              </div>
              <div className="rounded-2xl border border-white/8 bg-white/[0.03] px-4 py-3">
                <div className="text-xs uppercase tracking-[0.18em] text-slate-500">Transforms</div>
                <div className="mt-2 text-lg font-semibold text-white">{pipe.transforms.length}</div>
              </div>
              <div className="rounded-2xl border border-white/8 bg-white/[0.03] px-4 py-3">
                <div className="text-xs uppercase tracking-[0.18em] text-slate-500">Links</div>
                <div className="mt-2 text-lg font-semibold text-white">{pipe.links.length}</div>
              </div>
            </div>
          </div>

          <div className="panel-surface p-6">
            <div className="text-xs font-semibold uppercase tracking-[0.22em] text-slate-500">
              Latest Cycle
            </div>
            {lastCycle ? (
              <div className="mt-4 space-y-3">
                <div className="flex items-center justify-between">
                  <span className="text-sm text-slate-400">Status</span>
                  <StatusBadge status={lastCycle.status} />
                </div>
                <div className="flex items-center justify-between">
                  <span className="text-sm text-slate-400">Started</span>
                  <span className="font-mono text-sm text-white">{formatDate(lastCycle.started_at)}</span>
                </div>
                <div className="flex items-center justify-between">
                  <span className="text-sm text-slate-400">Duration</span>
                  <span className="font-mono text-sm text-white">
                    {formatDuration(lastCycle.duration_ms ?? cycleDurationMs(lastCycle.started_at, lastCycle.finished_at))}
                  </span>
                </div>
                <div className="flex items-center justify-between">
                  <span className="text-sm text-slate-400">Rows</span>
                  <span className="font-mono text-sm text-white">
                    +{lastCycle.rows_created} ~{lastCycle.rows_updated} -{lastCycle.rows_deleted}
                  </span>
                </div>
                {lastCycle.error ? (
                  <div className="rounded-2xl border border-rose-300/20 bg-rose-400/10 px-4 py-3 text-sm text-rose-100">
                    {lastCycle.error}
                  </div>
                ) : null}
              </div>
            ) : (
              <p className="mt-4 text-sm leading-6 text-slate-400">
                No cycle history yet. Once this pipe starts running, this panel turns into the latest execution snapshot.
              </p>
            )}
          </div>
        </section>
      </div>

      <section className="panel-surface overflow-hidden">
        <div className="border-b border-white/8 px-6 py-5">
          <div className="text-xs font-semibold uppercase tracking-[0.22em] text-slate-500">
            Cycle History
          </div>
          <p className="mt-2 text-sm leading-6 text-slate-400">
            Recent execution rows for this pipe only. Useful for checking whether recipe expansion and delta behavior match expectations.
          </p>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-left text-sm">
            <thead>
              <tr className="border-b border-white/8">
                <th className="px-6 py-3 text-xs font-medium uppercase tracking-[0.22em] text-slate-500">Cycle</th>
                <th className="px-6 py-3 text-xs font-medium uppercase tracking-[0.22em] text-slate-500">Query</th>
                <th className="px-6 py-3 text-xs font-medium uppercase tracking-[0.22em] text-slate-500">Status</th>
                <th className="px-6 py-3 text-xs font-medium uppercase tracking-[0.22em] text-slate-500">Rows</th>
                <th className="px-6 py-3 text-xs font-medium uppercase tracking-[0.22em] text-slate-500">Duration</th>
                <th className="px-6 py-3 text-xs font-medium uppercase tracking-[0.22em] text-slate-500">Started</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-white/6">
              {pipeCycles.length === 0 ? (
                <tr>
                  <td colSpan={6} className="px-6 py-5 text-slate-500">
                    No cycle history yet.
                  </td>
                </tr>
              ) : (
                pipeCycles.slice(0, 50).map((cycle) => (
                  <tr key={`${cycle.cycle_id}-${cycle.query}`} className="hover:bg-white/[0.03]">
                    <td className="px-6 py-4 font-mono text-slate-300">#{cycle.cycle_id}</td>
                    <td className="px-6 py-4 font-mono text-slate-300">{cycle.query}</td>
                    <td className="px-6 py-4">
                      <StatusBadge status={cycle.status} />
                    </td>
                    <td className="px-6 py-4 font-mono text-slate-300">
                      +{cycle.rows_created} ~{cycle.rows_updated} -{cycle.rows_deleted}
                    </td>
                    <td className="px-6 py-4 font-mono text-slate-300">
                      {formatDuration(cycle.duration_ms ?? cycleDurationMs(cycle.started_at, cycle.finished_at))}
                    </td>
                    <td className="px-6 py-4 font-mono text-slate-300">
                      {formatDate(cycle.started_at)}
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </section>

      <PipeForm open={editOpen} pipeName={pipe.name} onClose={() => setEditOpen(false)} />
      <PipeDryRunDialog
        pipeName={dryRunOpen ? pipe.name : null}
        open={dryRunOpen}
        onClose={() => setDryRunOpen(false)}
      />
    </div>
  );
}

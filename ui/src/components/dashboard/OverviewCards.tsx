import { Database, HardDrive, Search, RefreshCw } from "lucide-react";
import { formatNumber } from "@/utils/format";

interface OverviewCardsProps {
  pipesCount: number;
  sinksCount: number;
  queriesCount: number;
  cyclesToday: number;
}

export function OverviewCards({
  pipesCount,
  sinksCount,
  queriesCount,
  cyclesToday,
}: OverviewCardsProps) {
  const cards = [
    {
      label: "Pipes",
      hint: "Runnable sync units currently registered",
      value: pipesCount,
      icon: Database,
      accent: "text-blue-300",
      ring: "border-blue-300/15 bg-blue-400/10",
    },
    {
      label: "Sinks",
      hint: "Delivery endpoints ready for events",
      value: sinksCount,
      icon: HardDrive,
      accent: "text-amber-300",
      ring: "border-amber-300/15 bg-amber-400/10",
    },
    {
      label: "Queries",
      hint: "Polling lanes tracking origin state",
      value: queriesCount,
      icon: Search,
      accent: "text-emerald-300",
      ring: "border-emerald-300/15 bg-emerald-400/10",
    },
    {
      label: "Cycles Today",
      hint: "Completed run windows in the last day",
      value: cyclesToday,
      icon: RefreshCw,
      accent: "text-slate-100",
      ring: "border-white/10 bg-white/[0.06]",
    },
  ];

  return (
    <div className="data-grid">
      {cards.map(({ label, hint, value, icon: Icon, accent, ring }) => (
        <div
          key={label}
          className="panel-surface p-5 sm:p-6"
        >
          <div className="flex items-start justify-between gap-4">
            <div>
              <div className="text-xs font-semibold uppercase tracking-[0.22em] text-slate-500">
                {label}
              </div>
              <p className="mt-3 text-4xl font-semibold tracking-[-0.06em] text-white">
                {formatNumber(value)}
              </p>
            </div>
            <span className={`flex h-12 w-12 items-center justify-center rounded-2xl border ${ring}`}>
              <Icon className={`h-5 w-5 ${accent}`} />
            </span>
          </div>
          <p className="mt-4 text-sm leading-6 text-slate-400">{hint}</p>
        </div>
      ))}
    </div>
  );
}

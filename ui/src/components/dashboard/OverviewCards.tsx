import { Database, HardDrive, Search, RefreshCw } from "lucide-react";
import { formatNumber } from "@/utils/format";

interface OverviewCardsProps {
  sourcesCount: number;
  sinksCount: number;
  queriesCount: number;
  cyclesToday: number;
}

export function OverviewCards({
  sourcesCount,
  sinksCount,
  queriesCount,
  cyclesToday,
}: OverviewCardsProps) {
  const cards = [
    { label: "Total Sources", value: sourcesCount, icon: Database, color: "text-blue-400" },
    { label: "Total Sinks", value: sinksCount, icon: HardDrive, color: "text-purple-400" },
    { label: "Active Queries", value: queriesCount, icon: Search, color: "text-emerald-400" },
    { label: "Cycles Today", value: cyclesToday, icon: RefreshCw, color: "text-amber-400" },
  ];

  return (
    <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
      {cards.map(({ label, value, icon: Icon, color }) => (
        <div
          key={label}
          className="bg-gray-900 border border-gray-800 rounded-xl p-6"
        >
          <div className="flex items-center justify-between">
            <span className="text-sm text-gray-400">{label}</span>
            <Icon className={`h-5 w-5 ${color}`} />
          </div>
          <p className="mt-3 text-3xl font-bold font-mono text-white">
            {formatNumber(value)}
          </p>
        </div>
      ))}
    </div>
  );
}

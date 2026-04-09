import { Link, useMatchRoute } from "@tanstack/react-router";
import {
  BookMarked,
  LayoutDashboard,
  GitBranch,
  HardDrive,
  History,
  Settings,
  Zap,
} from "lucide-react";

const navItems = [
  { to: "/" as const, icon: LayoutDashboard, label: "Dashboard" },
  { to: "/pipes" as const, icon: GitBranch, label: "Pipes" },
  { to: "/recipes" as const, icon: BookMarked, label: "Saved Recipes" },
  { to: "/sinks" as const, icon: HardDrive, label: "Sinks" },
  { to: "/history" as const, icon: History, label: "History" },
  { to: "/settings" as const, icon: Settings, label: "Settings" },
];

export function Sidebar() {
  const matchRoute = useMatchRoute();

  return (
    <aside className="sticky top-0 hidden h-screen w-72 shrink-0 border-r border-white/8 bg-slate-950/70 backdrop-blur-xl md:flex md:flex-col">
      <div className="border-b border-white/8 px-5 py-5">
        <div className="flex items-center gap-3">
          <div className="flex h-10 w-10 items-center justify-center rounded-2xl border border-emerald-300/20 bg-emerald-400/10">
            <Zap className="h-5 w-5 text-emerald-300" />
          </div>
          <div>
            <span className="block text-lg font-semibold tracking-[-0.03em] text-white">
              OverSync
            </span>
            <span className="block text-xs uppercase tracking-[0.24em] text-slate-500">
              Control plane
            </span>
          </div>
        </div>
        <p className="mt-4 max-w-[16rem] text-sm leading-6 text-slate-400">
          Build runnable syncs through Pipes, reuse saved recipes, and export the resulting control-plane config for runtime deployment.
        </p>
      </div>
      <nav aria-label="Main navigation" className="flex-1 px-3 py-5">
        <div className="mb-3 px-3 text-[11px] font-semibold uppercase tracking-[0.24em] text-slate-500">
          Workspace
        </div>
        <div className="space-y-1.5">
        {navItems.map(({ to, icon: Icon, label }) => {
          const isActive = to === "/"
            ? matchRoute({ to, fuzzy: false })
            : matchRoute({ to, fuzzy: true });
          return (
            <Link
              key={to}
              to={to}
              aria-current={isActive ? "page" : undefined}
              className={`group flex items-center gap-3 rounded-2xl px-3 py-3 text-sm font-medium transition-all ${
                isActive
                  ? "bg-white/[0.08] text-white shadow-[inset_0_1px_0_rgba(255,255,255,0.04)]"
                  : "text-slate-400 hover:bg-white/[0.04] hover:text-slate-100"
              }`}
            >
              <span
                className={`flex h-9 w-9 items-center justify-center rounded-xl border transition-colors ${
                  isActive
                    ? "border-emerald-300/20 bg-emerald-400/10 text-emerald-300"
                    : "border-white/5 bg-white/[0.02] text-slate-500 group-hover:border-white/10 group-hover:text-slate-200"
                }`}
              >
                <Icon className="h-4 w-4" aria-hidden="true" />
              </span>
              <span className="tracking-[-0.02em]">{label}</span>
            </Link>
          );
        })}
        </div>
      </nav>
      <div className="px-5 pb-5">
        <div className="panel-subtle p-4">
          <div className="text-[11px] font-semibold uppercase tracking-[0.22em] text-slate-500">
            Live surface
          </div>
          <p className="mt-2 text-sm leading-6 text-slate-400">
            Designed for active pipelines, not just config editing.
          </p>
        </div>
      </div>
    </aside>
  );
}

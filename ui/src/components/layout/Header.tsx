import { useState } from "react";
import { Link, useMatchRoute } from "@tanstack/react-router";
import {
  BookMarked,
  Menu,
  X,
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

export function Header() {
  const [open, setOpen] = useState(false);
  const matchRoute = useMatchRoute();

  return (
    <header className="border-b border-white/8 bg-slate-950/70 px-4 py-3 backdrop-blur-xl md:hidden">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <div className="flex h-9 w-9 items-center justify-center rounded-2xl border border-emerald-300/20 bg-emerald-400/10">
            <Zap className="h-4 w-4 text-emerald-300" />
          </div>
          <div>
            <span className="block text-sm font-semibold tracking-[-0.03em] text-white">
              OverSync
            </span>
            <span className="block text-[10px] uppercase tracking-[0.2em] text-slate-500">
              Control plane
            </span>
          </div>
        </div>
        <button
          onClick={() => setOpen(!open)}
          className="rounded-full border border-white/10 bg-white/[0.04] p-2 text-slate-400 hover:text-white"
          aria-label="Toggle menu"
        >
          {open ? <X className="h-5 w-5" /> : <Menu className="h-5 w-5" />}
        </button>
      </div>
      {open && (
        <nav className="mt-4 space-y-1.5">
          {navItems.map(({ to, icon: Icon, label }) => {
            const isActive = to === "/" ? matchRoute({ to, fuzzy: false }) : matchRoute({ to, fuzzy: true });
            return (
              <Link
                key={to}
                to={to}
                onClick={() => setOpen(false)}
                className={`flex items-center gap-3 rounded-2xl px-3 py-3 text-sm ${
                  isActive
                    ? "bg-white/[0.08] text-white"
                    : "text-slate-400 hover:bg-white/[0.04]"
                }`}
              >
                <span className="flex h-9 w-9 items-center justify-center rounded-xl border border-white/8 bg-white/[0.03]">
                  <Icon className="h-4 w-4" />
                </span>
                {label}
              </Link>
            );
          })}
        </nav>
      )}
    </header>
  );
}

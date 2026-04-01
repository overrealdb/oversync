import { useState } from "react";
import { Link, useMatchRoute } from "@tanstack/react-router";
import {
  Menu,
  X,
  LayoutDashboard,
  Database,
  HardDrive,
  History,
  Settings,
  Zap,
} from "lucide-react";

const navItems = [
  { to: "/" as const, icon: LayoutDashboard, label: "Dashboard" },
  { to: "/sources" as const, icon: Database, label: "Sources" },
  { to: "/sinks" as const, icon: HardDrive, label: "Sinks" },
  { to: "/history" as const, icon: History, label: "History" },
  { to: "/settings" as const, icon: Settings, label: "Settings" },
];

export function Header() {
  const [open, setOpen] = useState(false);
  const matchRoute = useMatchRoute();

  return (
    <header className="md:hidden bg-gray-900 border-b border-gray-800 px-4 py-3">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Zap className="h-5 w-5 text-emerald-400" />
          <span className="font-bold text-white">OverSync</span>
        </div>
        <button
          onClick={() => setOpen(!open)}
          className="text-gray-400 hover:text-white"
          aria-label="Toggle menu"
        >
          {open ? <X className="h-5 w-5" /> : <Menu className="h-5 w-5" />}
        </button>
      </div>
      {open && (
        <nav className="mt-3 space-y-1">
          {navItems.map(({ to, icon: Icon, label }) => {
            const isActive = to === "/" ? matchRoute({ to, fuzzy: false }) : matchRoute({ to, fuzzy: true });
            return (
              <Link
                key={to}
                to={to}
                onClick={() => setOpen(false)}
                className={`flex items-center gap-3 px-3 py-2 rounded-md text-sm ${
                  isActive
                    ? "bg-gray-800 text-white"
                    : "text-gray-400 hover:bg-gray-800/50"
                }`}
              >
                <Icon className="h-4 w-4" />
                {label}
              </Link>
            );
          })}
        </nav>
      )}
    </header>
  );
}

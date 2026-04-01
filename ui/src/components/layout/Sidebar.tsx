import { Link, useMatchRoute } from "@tanstack/react-router";
import {
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

export function Sidebar() {
  const matchRoute = useMatchRoute();

  return (
    <aside className="hidden md:flex md:w-56 flex-col bg-gray-900 border-r border-gray-800 h-screen sticky top-0">
      <div className="flex items-center gap-2 px-4 py-4 border-b border-gray-800">
        <Zap className="h-6 w-6 text-emerald-400" />
        <span className="text-lg font-bold text-white">OverSync</span>
      </div>
      <nav aria-label="Main navigation" className="flex-1 px-2 py-4 space-y-1">
        {navItems.map(({ to, icon: Icon, label }) => {
          const isActive = to === "/"
            ? matchRoute({ to, fuzzy: false })
            : matchRoute({ to, fuzzy: true });
          return (
            <Link
              key={to}
              to={to}
              aria-current={isActive ? "page" : undefined}
              className={`flex items-center gap-3 px-3 py-2 rounded-md text-sm font-medium transition-colors ${
                isActive
                  ? "bg-gray-800 text-white"
                  : "text-gray-400 hover:bg-gray-800/50 hover:text-gray-200"
              }`}
            >
              <Icon className="h-4 w-4" aria-hidden="true" />
              {label}
            </Link>
          );
        })}
      </nav>
    </aside>
  );
}

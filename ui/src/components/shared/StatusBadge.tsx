type Size = "sm" | "md";

interface StatusBadgeProps {
  status: string;
  size?: Size;
}

const colorMap: Record<string, { dot: string; badge: string }> = {
  success: {
    dot: "bg-emerald-400",
    badge: "bg-emerald-500/10 text-emerald-400",
  },
  failed: {
    dot: "bg-rose-400",
    badge: "bg-rose-500/10 text-rose-400",
  },
  error: {
    dot: "bg-rose-400",
    badge: "bg-rose-500/10 text-rose-400",
  },
  aborted: {
    dot: "bg-amber-400",
    badge: "bg-amber-500/10 text-amber-400",
  },
  running: {
    dot: "bg-blue-400 animate-pulse",
    badge: "bg-blue-500/10 text-blue-400",
  },
  paused: {
    dot: "bg-gray-400",
    badge: "bg-gray-500/10 text-gray-400",
  },
};

const defaultColor = {
  dot: "bg-gray-400",
  badge: "bg-gray-500/10 text-gray-400",
};

const sizeClasses: Record<Size, { wrapper: string; dot: string }> = {
  sm: { wrapper: "px-2 py-0.5 text-xs", dot: "h-1.5 w-1.5" },
  md: { wrapper: "px-2.5 py-1 text-sm", dot: "h-2 w-2" },
};

export function StatusBadge({ status, size = "sm" }: StatusBadgeProps) {
  const colors = colorMap[status] ?? defaultColor;
  const sizes = sizeClasses[size];

  return (
    <span
      className={`inline-flex items-center gap-1.5 rounded-full font-medium ${colors.badge} ${sizes.wrapper}`}
    >
      <span className={`shrink-0 rounded-full ${colors.dot} ${sizes.dot}`} />
      {status}
    </span>
  );
}

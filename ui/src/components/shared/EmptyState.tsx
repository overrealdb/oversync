import type { ReactNode } from "react";
import { Inbox } from "lucide-react";

interface EmptyStateProps {
  icon?: ReactNode;
  title: string;
  description?: string;
  action?: ReactNode;
}

export function EmptyState({ icon, title, description, action }: EmptyStateProps) {
  return (
    <div className="panel-surface flex flex-col items-center justify-center px-6 py-16 text-center sm:px-10 sm:py-20">
      <div className="mb-5 flex h-[4.5rem] w-[4.5rem] items-center justify-center rounded-[28px] border border-white/10 bg-white/[0.04] text-slate-500">
        {icon ?? <Inbox className="h-9 w-9" />}
      </div>
      <div className="eyebrow mb-3">Nothing here yet</div>
      <h3 className="text-2xl font-semibold tracking-[-0.03em] text-white">{title}</h3>
      {description && (
        <p className="mt-3 max-w-md text-sm leading-6 text-slate-400">{description}</p>
      )}
      {action && <div className="mt-6">{action}</div>}
    </div>
  );
}

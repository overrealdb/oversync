export function LoadingSkeleton({ rows = 5 }: { rows?: number }) {
  return (
    <div className="space-y-3 animate-pulse">
      {Array.from({ length: rows }, (_, i) => (
        <div key={i} className="h-10 bg-gray-800/50 rounded-md" />
      ))}
    </div>
  );
}

export function CardSkeleton() {
  return (
    <div className="animate-pulse bg-gray-900 border border-gray-800 rounded-lg p-6">
      <div className="h-4 w-24 bg-gray-800 rounded mb-3" />
      <div className="h-8 w-16 bg-gray-800 rounded" />
    </div>
  );
}

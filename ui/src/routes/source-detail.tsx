import { createRoute } from "@tanstack/react-router";
import { Route as rootRoute } from "./__root";
import { SourceDetailPage } from "@/pages/SourceDetailPage";

export const Route = createRoute({
  getParentRoute: () => rootRoute,
  path: "/sources/$name",
  component: SourceDetailPage,
});

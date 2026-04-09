import { createRoute } from "@tanstack/react-router";
import { Route as rootRoute } from "./__root";
import { PipeDetailPage } from "@/pages/PipeDetailPage";

export const Route = createRoute({
  getParentRoute: () => rootRoute,
  path: "/pipes/$name",
  component: PipeDetailPage,
});

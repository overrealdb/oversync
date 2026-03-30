import { createRoute } from "@tanstack/react-router";
import { Route as rootRoute } from "./__root";
import { Sources } from "@/pages/Sources";

export const Route = createRoute({
  getParentRoute: () => rootRoute,
  path: "/sources",
  component: Sources,
});

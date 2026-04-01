import { createRoute } from "@tanstack/react-router";
import { Route as rootRoute } from "./__root";
import { History } from "@/pages/History";

export const Route = createRoute({
  getParentRoute: () => rootRoute,
  path: "/history",
  component: History,
});

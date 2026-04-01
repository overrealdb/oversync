import { createRoute } from "@tanstack/react-router";
import { Route as rootRoute } from "./__root";
import { Dashboard } from "@/pages/Dashboard";

export const Route = createRoute({
  getParentRoute: () => rootRoute,
  path: "/",
  component: Dashboard,
});

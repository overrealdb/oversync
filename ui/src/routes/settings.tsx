import { createRoute } from "@tanstack/react-router";
import { Route as rootRoute } from "./__root";
import { Settings } from "@/pages/Settings";

export const Route = createRoute({
  getParentRoute: () => rootRoute,
  path: "/settings",
  component: Settings,
});

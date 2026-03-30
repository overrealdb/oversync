import { createRoute } from "@tanstack/react-router";
import { Route as rootRoute } from "./__root";
import { Sinks } from "@/pages/Sinks";

export const Route = createRoute({
  getParentRoute: () => rootRoute,
  path: "/sinks",
  component: Sinks,
});

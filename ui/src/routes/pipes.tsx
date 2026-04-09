import { createRoute } from "@tanstack/react-router";
import { Route as rootRoute } from "./__root";
import { Pipes } from "@/pages/Pipes";

export const Route = createRoute({
  getParentRoute: () => rootRoute,
  path: "/pipes",
  component: Pipes,
});

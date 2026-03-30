import { createRootRoute, Outlet } from "@tanstack/react-router";
import { Layout } from "@/components/layout/Layout";
import { ErrorBoundary } from "@/components/shared/ErrorBoundary";

export const Route = createRootRoute({
  component: () => (
    <Layout>
      <ErrorBoundary>
        <Outlet />
      </ErrorBoundary>
    </Layout>
  ),
});

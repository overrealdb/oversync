import { Route as rootRoute } from "./routes/__root";
import { Route as indexRoute } from "./routes/index";
import { Route as sourcesRoute } from "./routes/sources";
import { Route as sourceDetailRoute } from "./routes/source-detail";
import { Route as sinksRoute } from "./routes/sinks";
import { Route as historyRoute } from "./routes/history";
import { Route as settingsRoute } from "./routes/settings";

const routeTree = rootRoute.addChildren([
  indexRoute,
  sourcesRoute,
  sourceDetailRoute,
  sinksRoute,
  historyRoute,
  settingsRoute,
]);

export { routeTree };

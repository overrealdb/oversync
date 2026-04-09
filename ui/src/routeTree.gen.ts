import { Route as rootRoute } from "./routes/__root";
import { Route as indexRoute } from "./routes/index";
import { Route as pipesRoute } from "./routes/pipes";
import { Route as pipeDetailRoute } from "./routes/pipe-detail";
import { Route as recipesRoute } from "./routes/recipes";
import { Route as sinksRoute } from "./routes/sinks";
import { Route as historyRoute } from "./routes/history";
import { Route as settingsRoute } from "./routes/settings";

const routeTree = rootRoute.addChildren([
  indexRoute,
  pipesRoute,
  pipeDetailRoute,
  recipesRoute,
  sinksRoute,
  historyRoute,
  settingsRoute,
]);

export { routeTree };

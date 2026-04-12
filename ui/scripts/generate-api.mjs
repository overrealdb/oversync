import { execFileSync } from "node:child_process";
import { copyFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const uiDir = path.resolve(scriptDir, "..");
const repoRoot = path.resolve(uiDir, "..");
const openapiPath = path.join(uiDir, "openapi.json");
const rustClientOpenapiPath = path.join(
  repoRoot,
  "crates",
  "oversync-client",
  "openapi.json",
);

execFileSync(
  "cargo",
  [
    "run",
    "--features",
    "api cli",
    "--bin",
    "oversync",
    "--",
    "openapi",
    "--file",
    openapiPath,
  ],
  {
    cwd: repoRoot,
    stdio: "inherit",
    env: {
      ...process.env,
      ...(process.env.OVERSYNC_CARGO_TARGET_DIR && !process.env.CARGO_TARGET_DIR
        ? { CARGO_TARGET_DIR: process.env.OVERSYNC_CARGO_TARGET_DIR }
        : {}),
    },
  },
);

copyFileSync(openapiPath, rustClientOpenapiPath);

execFileSync("npx", ["openapi-ts"], {
  cwd: uiDir,
  stdio: "inherit",
  env: process.env,
});

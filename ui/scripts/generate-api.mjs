import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import path from "node:path";

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const uiDir = path.resolve(scriptDir, "..");
const repoRoot = path.resolve(uiDir, "..");
const openapiPath = path.join(uiDir, "openapi.json");

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
      CARGO_TARGET_DIR:
        process.env.OVERSYNC_CARGO_TARGET_DIR ?? "/Volumes/storage/rust-targets/oversync",
    },
  },
);

execFileSync("npx", ["openapi-ts"], {
  cwd: uiDir,
  stdio: "inherit",
  env: process.env,
});

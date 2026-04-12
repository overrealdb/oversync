import { execFileSync } from "node:child_process";
import { readFileSync, writeFileSync, mkdtempSync, rmSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const uiDir = path.resolve(scriptDir, "..");
const repoRoot = path.resolve(uiDir, "..");
const openapiPath = path.join(uiDir, "openapi.json");

const normalizeOpenApi = (raw) => {
  const spec = JSON.parse(raw);
  if (spec?.info && typeof spec.info === "object") {
    delete spec.info.version;
  }
  return JSON.stringify(spec, null, 2) + "\n";
};

execFileSync("node", [path.join(scriptDir, "generate-api.mjs")], {
  cwd: uiDir,
  stdio: "inherit",
  env: process.env,
});

try {
  execFileSync("git", ["diff", "--exit-code", "--", "ui/src/api/generated"], {
    cwd: repoRoot,
    stdio: "inherit",
    env: process.env,
  });
} catch {
  console.error("Generated TypeScript SDK is out of date.");
  process.exit(1);
}

const currentSpec = normalizeOpenApi(readFileSync(openapiPath, "utf8"));
const trackedSpec = normalizeOpenApi(
  execFileSync("git", ["show", "HEAD:ui/openapi.json"], {
    cwd: repoRoot,
    encoding: "utf8",
    env: process.env,
  }),
);

if (currentSpec !== trackedSpec) {
  const tempDir = mkdtempSync(path.join(os.tmpdir(), "oversync-openapi-"));
  const expectedPath = path.join(tempDir, "expected.json");
  const actualPath = path.join(tempDir, "actual.json");
  writeFileSync(expectedPath, trackedSpec);
  writeFileSync(actualPath, currentSpec);

  try {
    execFileSync("diff", ["-u", expectedPath, actualPath], {
      cwd: repoRoot,
      stdio: "inherit",
      env: process.env,
    });
  } catch {
    console.error(
      "OpenAPI schema changed beyond info.version. Regenerate and commit ui/openapi.json.",
    );
    rmSync(tempDir, { recursive: true, force: true });
    process.exit(1);
  }

  rmSync(tempDir, { recursive: true, force: true });
}

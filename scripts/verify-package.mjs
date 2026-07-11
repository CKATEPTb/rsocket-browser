import { execFileSync } from "node:child_process";
import assert from "node:assert/strict";
import { existsSync, readdirSync, readFileSync, statSync } from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";

const packageJson = JSON.parse(readFileSync("package.json", "utf8"));

assert.deepStrictEqual(Object.keys(packageJson.exports), ["."], "package must expose only the root entry");
assert.deepStrictEqual(
  Object.keys(packageJson.scripts ?? {}).sort(),
  ["build", "test"],
  "package scripts must stay limited to test and build"
);
assert.equal(packageJson.sideEffects, false, "package must be tree-shakeable");
assert.equal(packageJson.publishConfig?.access, "public", "package must publish publicly");

for (const dependency of ["bebyte", "reactor-core-ts", "rsocket-frames-ts"]) {
  assert.ok(packageJson.dependencies?.[dependency], `missing runtime dependency: ${dependency}`);
}

assertFile("dist/index.js");
assertFile("dist/index.d.ts");
assert.ok(!existsSync("dist/_virtual"), "dist must not contain Vite helper chunks");

const rootModule = await import(pathToFileURL(path.resolve("dist/index.js")).href);
assert.deepStrictEqual(
  Object.keys(rootModule).sort(),
  [
    "FireAndForgetController",
    "RSocket",
    "RequestChannelController",
    "RequestResponseController",
    "RequestStreamController"
  ].sort(),
  "runtime root exports must stay limited to RSocket and abstract controller classes"
);

const distSourceFiles = sourceFiles("dist");
assertNoAliasSpecifiers(distSourceFiles.filter((file) => file.endsWith(".d.ts")), "declaration files");
assertNoAliasSpecifiers(distSourceFiles.filter((file) => file.endsWith(".js")), "runtime js files");
assertSourceLayout();
assertWorkflowLayout();
assertPackContents();

console.log("Package verification passed.");

function assertFile(file) {
  assert.ok(existsSync(file) && statSync(file).isFile(), `missing file: ${file}`);
}

function assertSourceLayout() {
  const entries = readdirSync("src", { withFileTypes: true });
  for (const entry of entries) {
    if (entry.isFile()) {
      assert.equal(entry.name, "index.ts", "src root may contain only index.ts files");
    }
  }

  let internalRelativeImports = 0;
  for (const file of tsFiles("src")) {
    const source = readFileSync(file, "utf8");
    if (/from\s+["']\.\.?\//.test(source) || /export\s+.+from\s+["']\.\.?\//.test(source)) {
      internalRelativeImports += 1;
    }
  }
  assert.equal(internalRelativeImports, 0, "src imports must use @ aliases instead of internal relative paths");
}

function assertWorkflowLayout() {
  const workflowDirectory = path.join(".github", "workflows");
  assert.ok(existsSync(workflowDirectory), "project must have a CI/CD workflow directory");
  const workflows = readdirSync(workflowDirectory).filter((file) => file.endsWith(".yml") || file.endsWith(".yaml"));
  assert.deepStrictEqual(workflows, ["ci-cd.yml"], "project must keep a single CI/CD workflow");
}

function assertPackContents() {
  const output = execFileSync(process.execPath, [npmCli(), "pack", "--dry-run", "--json"], { encoding: "utf8" });
  const [pack] = JSON.parse(output);
  const files = new Set(pack.files.map((file) => file.path));

  for (const required of ["package.json", "README.md", "LICENSE.md", "dist/index.js", "dist/index.d.ts"]) {
    assert.ok(files.has(required), `npm package is missing ${required}`);
  }

  for (const file of files) {
    assert.ok(!file.startsWith("src/"), `npm package must not include source file ${file}`);
    assert.ok(!file.startsWith("test/"), `npm package must not include test file ${file}`);
    assert.ok(!file.startsWith("scripts/"), `npm package must not include script file ${file}`);
    assert.ok(!file.startsWith(".github/"), `npm package must not include workflow file ${file}`);
    assert.notEqual(file, "package-lock.json", "npm package must not include package-lock.json");
  }
}

function npmCli() {
  const cli = process.env.npm_execpath;
  assert.ok(cli, "npm_execpath is required to run package verification");
  return cli;
}

function assertNoAliasSpecifiers(files, label) {
  const offenders = files.filter((file) => {
    const source = readFileSync(file, "utf8");
    return source.includes("\"@") || source.includes("'@");
  });
  assert.deepStrictEqual(offenders, [], `${label} must not contain local @ aliases`);
}

function tsFiles(directory) {
  return walk(directory, (file) => file.endsWith(".ts"));
}

function sourceFiles(directory) {
  return walk(directory, (file) => file.endsWith(".d.ts") || file.endsWith(".js"));
}

function walk(directory, predicate) {
  const files = [];
  for (const entry of readdirSync(directory, { withFileTypes: true })) {
    const current = path.join(directory, entry.name);
    if (entry.isDirectory()) files.push(...walk(current, predicate));
    else if (entry.isFile() && predicate(current)) files.push(current);
  }
  return files;
}

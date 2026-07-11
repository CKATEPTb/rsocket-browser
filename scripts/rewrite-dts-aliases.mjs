import { readdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";

const distRoot = path.resolve("dist");

for (const file of await declarationFiles(distRoot)) {
  const source = await readFile(file, "utf8");
  const next = source.replace(/(["'])@(?:\/([^"']+))?\1/g, (match, quote, target = "index.js") => {
    const absoluteTarget = path.join(distRoot, target);
    let relative = path.relative(path.dirname(file), absoluteTarget).replaceAll(path.sep, "/");
    if (!relative.startsWith(".")) relative = `./${relative}`;
    return `${quote}${relative}${quote}`;
  });
  if (next !== source) await writeFile(file, next);
}

async function declarationFiles(directory) {
  const entries = await readdir(directory, { withFileTypes: true });
  const files = await Promise.all(entries.map(async (entry) => {
    const current = path.join(directory, entry.name);
    if (entry.isDirectory()) return declarationFiles(current);
    return entry.isFile() && entry.name.endsWith(".d.ts") ? [current] : [];
  }));
  return files.flat();
}

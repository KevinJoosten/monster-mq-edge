// Cross-checks the mapping schema testdata with ajv (draft 2020-12, strict,
// allErrors). It adds no dependency: ajv 8 is taken from the dashboard
// checkout next to the edge repository, or from AJV_DIR. Without ajv 8 the
// check prints "skipped" and exits 0.
//
// Usage (from the edge repository root):
//
//	UNSMODELER_ISSUES_OUT=/tmp/issues.json go test ./internal/unsmodeler -run TestSchemaTestdata
//	node internal/unsmodeler/schema/ajvcheck.mjs [/tmp/issues.json]
//
// With the issues file, the issue paths of every file are compared with the
// Go evaluator's, normalised to the Go convention: additionalProperties at
// the property, ajv's if entries and contains sub-errors dropped, paths
// compared as sets. The Go-only pre-checks (size, nesting, duplicate keys,
// tree depth) have no testdata ajv could see.
import fs from "node:fs";
import path from "node:path";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";

const dir = path.dirname(fileURLToPath(import.meta.url));
const ajvDir = process.env.AJV_DIR || path.resolve(dir, "../../../../dashboard/node_modules/ajv");

let Ajv2020, version;
try {
  const require = createRequire(path.join(ajvDir, "package.json"));
  version = require(path.join(ajvDir, "package.json")).version;
  if (!version.startsWith("8.")) throw new Error(`ajv ${version} is not version 8`);
  Ajv2020 = require(path.join(ajvDir, "dist/2020")).default;
} catch (err) {
  console.log(`skipped: no ajv 8 at ${ajvDir} (${err.message})`);
  process.exit(0);
}

const goIssues = process.argv[2] ? JSON.parse(fs.readFileSync(process.argv[2], "utf8")) : null;
const schema = JSON.parse(fs.readFileSync(path.join(dir, "mapping.schema.json"), "utf8"));
const validate = new Ajv2020({ strict: true, allErrors: true }).compile(schema);
console.log(`ajv ${version} strict: schema compiled`);

const esc = (k) => k.replace(/~/g, "~0").replace(/\//g, "~1");
function ajvPaths(errors) {
  const out = new Set();
  for (const e of errors) {
    if (e.keyword === "if") continue;
    if (/\/contains\//.test(e.schemaPath)) continue;
    let p = e.instancePath;
    if (e.keyword === "additionalProperties") p += "/" + esc(e.params.additionalProperty);
    out.add(p);
  }
  return [...out].sort();
}

let failed = 0;
let compared = 0;
let pathDiffs = 0;
for (const kind of ["valid", "invalid"]) {
  const d = path.join(dir, "testdata", kind);
  for (const f of fs.readdirSync(d).filter((f) => f.endsWith(".json")).sort()) {
    const key = `${kind}/${f}`;
    const ok = validate(JSON.parse(fs.readFileSync(path.join(d, f), "utf8")));
    if ((kind === "valid") !== ok) {
      failed++;
      console.log(`FAIL ${key}: ajv says ${ok ? "valid" : "invalid"}`);
    }
    if (!goIssues) continue;
    if (!(key in goIssues)) {
      pathDiffs++;
      console.log(`MISSING ${key} in the Go issues file`);
      continue;
    }
    const a = ok ? [] : ajvPaths(validate.errors);
    const g = [...new Set((goIssues[key] || []).map((i) => i.path))].sort();
    compared++;
    if (JSON.stringify(a) !== JSON.stringify(g)) {
      pathDiffs++;
      console.log(`PATHS ${key}: ajv ${JSON.stringify(a)} go ${JSON.stringify(g)}`);
    }
  }
}
console.log(failed === 0 ? "ajv: all files as expected" : `ajv: ${failed} unexpected result(s)`);
if (goIssues) {
  console.log(pathDiffs === 0 ? `paths: ajv and Go agree on all ${compared} files` : `paths: ${pathDiffs} file(s) differ`);
}
process.exit(failed === 0 && pathDiffs === 0 ? 0 : 1);

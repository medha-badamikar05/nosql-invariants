// Path A glue: the kind of Node.js transform that sits between an export and mongoimport.
// Reads one JSON object per line (from PostgreSQL row_to_json) and writes relaxed Extended JSON.
// Nothing here is "wrong" by local standards: JSON.parse / JSON.stringify and new Date()
// are the idiomatic calls. The losses come from the platform, which is the point.
const readline = require("readline");
const rl = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
rl.on("line", (line) => {
  if (!line.trim()) return;
  const row = JSON.parse(line);                    // numbers -> binary64 (2^53, decimals)
  for (const k of Object.keys(row)) {
    if (k.endsWith("_at") && typeof row[k] === "string") {
      row[k] = { $date: new Date(row[k]).toISOString() };   // JS Date: millisecond resolution
    }
  }
  process.stdout.write(JSON.stringify(row) + "\n");
});

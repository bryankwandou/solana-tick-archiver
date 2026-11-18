// Find minutes inside the RPC node's retention window where the ticks table is empty or thin
// (< 30 % of the median minute), delete those partial minutes, and emit their slot ranges
// (oldest first) for backfill.mjs. Runs once before the backfill matrix.
import { DuckDBInstance } from '@duckdb/node-api';
import fs from 'fs';
const KEY = process.env.APERTURE_KEY, MD = process.env.MOTHERDUCK_TOKEN;
const RPC = `https://solana-rpc.rpcfast.com/?api_key=${KEY}`;
const rpc = async (method, params) => {
  for (let i = 0; ; i++) {
    try { const j = await (await fetch(RPC, { method: 'POST', headers: { 'content-type': 'application/json' }, signal: AbortSignal.timeout(30000), body: JSON.stringify({ jsonrpc: '2.0', id: 1, method, params }) })).json();
      if (j.error) { if (/skipped|not available|cleaned/i.test(j.error.message) || j.error.code === -32007 || j.error.code === -32009) return null; throw new Error(j.error.message); } return j.result; }
    catch (e) { if (i >= 4) throw e; await new Promise((s) => setTimeout(s, 1000 * (i + 1))); }
  }
};
const timeAt = async (slot) => { for (let k = 0; k < 40; k++) { const t = await rpc('getBlockTime', [slot + k]); if (t != null) return t; } return null; };
const MARGIN = Number(process.env.MARGIN || 300);
const first = (await rpc('getFirstAvailableBlock', [])) + MARGIN;
const tip = await rpc('getSlot', [{ commitment: 'finalized' }]);
const t0 = await timeAt(first);
const db = await (await DuckDBInstance.create(`md:?motherduck_token=${MD}`)).connect();
const rows = (await db.runAndReadAll(`SELECT (ts//60000)::BIGINT m, count(*)::BIGINT n FROM solana_ticks.main.ticks WHERE ts >= ${t0 * 1000} GROUP BY 1`)).getRowObjects();
const map = new Map(rows.map((r) => [Number(r.m), Number(r.n)]));
const ns = [...map.values()].sort((a, b) => a - b); const med = ns.length ? ns[Math.floor(ns.length / 2)] : 0;
const m0 = Math.ceil(t0 / 60), m1 = Math.floor(Date.now() / 60000) - 15;
const gaps = []; let cur = null;
for (let m = m0; m <= m1; m++) if ((map.get(m) || 0) < med * 0.3) { if (cur && cur[1] === m - 1) cur[1] = m; else gaps.push(cur = [m, m]); }
// slot for a unix second: binary search over [first, tip]
const slotAt = async (sec) => { let lo = first, hi = tip; while (hi - lo > 2) { const mid = Math.floor((lo + hi) / 2); const t = await timeAt(mid); if (t == null || t >= sec) hi = mid; else lo = mid; } return lo; };
const ranges = []; let gm = 0;
for (const [a, b] of gaps) {
  gm += b - a + 1;
  await db.run(`DELETE FROM solana_ticks.main.ticks WHERE ts >= ${a * 60000} AND ts < ${(b + 1) * 60000}`);
  ranges.push([Math.max(first, (await slotAt(a * 60)) - 2), (await slotAt((b + 1) * 60)) + 2]);
}
const slots = ranges.reduce((s, [a, b]) => s + b - a, 0);
console.log(`retention from ${new Date(t0 * 1000).toISOString()}, median ${med}/min, gap minutes ${gm} in ${gaps.length} windows, ${slots} slots`);
for (const [i, g] of gaps.entries()) console.log(` ${new Date(g[0] * 60000).toISOString().slice(0, 16)} +${g[1] - g[0] + 1} min -> slots ${ranges[i][0]}..${ranges[i][1]}`);
if (process.env.GITHUB_OUTPUT) fs.appendFileSync(process.env.GITHUB_OUTPUT, `ranges=${JSON.stringify(ranges)}\n`);

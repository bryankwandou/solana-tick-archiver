// Backfill price ticks from every block the RPC node still holds (≈ last 39 h), up to where
// the live archiver's ticks begin. Job PART of PARTS takes one slice of that slot range.
import { DuckDBInstance } from '@duckdb/node-api';
import fs from 'fs'; import zlib from 'zlib';
import { decodeEvent, decodeTx, Registry, Candles } from './decode.mjs';
const KEY = process.env.APERTURE_KEY, MD = process.env.MOTHERDUCK_TOKEN;
const PART = Number(process.env.PART || 0), PARTS = Number(process.env.PARTS || 1), CONC = Number(process.env.CONC || 24);
const RPC = `https://solana-rpc.rpcfast.com/?api_key=${KEY}`;
const log = (...a) => console.log(new Date().toISOString().slice(11, 19), `[${PART}]`, ...a);
const rpc = async (method, params, tries = 4) => {
  for (let i = 0; ; i++) {
    try { const r = await fetch(RPC, { method: 'POST', headers: { 'content-type': 'application/json' }, signal: AbortSignal.timeout(30000), body: JSON.stringify({ jsonrpc: '2.0', id: 1, method, params }) });
      const j = await r.json(); if (j.error) { if (j.error.code === -32007 || j.error.code === -32009 || /skipped|not available|cleaned/i.test(j.error.message)) return null; throw new Error(j.error.message); } return j.result; }
    catch (e) { if (i >= tries) throw e; await new Promise((s) => setTimeout(s, 1000 * (i + 1))); }
  }
};
const db = await (await DuckDBInstance.create(`md:?motherduck_token=${MD}`)).connect();
await db.run(`CREATE TABLE IF NOT EXISTS solana_ticks.main.ticks (mint VARCHAR, ts BIGINT, p FLOAT)`);
// RANGES (from plan.mjs) = gap slot ranges, oldest first. Slots are cut into 50-slot chunks dealt
// round-robin to the parts, so every part starts at the oldest blocks before the node prunes them.
const RANGES = JSON.parse(process.env.RANGES || '[]');
const mine = []; let ci = 0;
for (const [ra, rb] of RANGES) for (let s = ra; s < rb; s += 50, ci++) if (ci % PARTS === PART) for (let x = s; x < Math.min(s + 50, rb); x++) mine.push(x);
const a = 0, b = mine.length;
log(`${RANGES.length} gap ranges, my ${b} slots${b ? ` from ${mine[0]} to ${mine[b - 1]}` : ''}`);
const reg = new Registry('registry.json', RPC); const C = new Candles(1000);
let pending = [], total = 0, blocks = 0, bytes = 0; const t0 = Date.now();
async function flush() {
  const tr = pending; pending = []; if (!tr.length) return;
  tr.sort((x, y) => x[2] - y[2]);
  for (let i = 0; i < tr.length; i += 20000) { try { await reg.resolve(tr.slice(i, i + 20000).map((x) => x[0])); } catch (e) { log('registry', e.message); } }
  for (const [t, ts] of tr) C.add(reg, t, ts);
  const ticks = C.takeTicks(); if (!ticks.length) return;
  const f = `bf-${PART}-${Date.now()}.csv.gz`;
  fs.writeFileSync(f, zlib.gzipSync(ticks.map((r) => `${r.mint},${r.ts},${r.p}`).join(String.fromCharCode(10))));
  await db.run(`INSERT INTO solana_ticks.main.ticks SELECT * FROM read_csv('${f}', header=false, columns={'mint':'VARCHAR','ts':'BIGINT','p':'FLOAT'})`);
  fs.unlinkSync(f); total += ticks.length;
  const el = (Date.now() - t0) / 1000;
  log(`blocks ${blocks}/${b - a}, ticks ${total}, ${(blocks / el).toFixed(1)} blk/s, ${(bytes / 1e9).toFixed(1)} GB downloaded, SOL/USD ${C.solUsd.toFixed(2)}`);
}
let next = a;
async function worker() {
  while (next < b) {
    const slot = mine[next++];
    let blk; try { blk = await rpc('getBlock', [slot, { encoding: 'json', transactionDetails: 'full', rewards: false, maxSupportedTransactionVersion: 1 }]); } catch (e) { log('block', slot, e.message); continue; }
    blocks++; if (!blk) continue;
    const ts = (blk.blockTime || 0) * 1000;
    blk.transactions.forEach((tx, i) => {
      if (!tx.meta || tx.meta.err) return;
      bytes += 6000;
      for (const l of tx.meta.logMessages || []) if (l.startsWith('Program data: ')) {
        const t = decodeEvent(Buffer.from(l.slice(14), 'base64')); if (t) pending.push([t, ts, slot * 10000 + i]);
      }
      for (const t of decodeTx(tx)) pending.push([t, ts, slot * 10000 + i]); // Meteora DLMM/DBC/DAMM, Raydium AMM v4
    });
  }
}
const timer = setInterval(() => { flush().catch((e) => log('flush', e.message)); }, 60000);
await Promise.all(Array.from({ length: CONC }, worker));
clearInterval(timer); await flush(); log('done');

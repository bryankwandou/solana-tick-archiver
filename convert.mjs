// One-off: turn the stored raw events (dex_events) into price ticks, then drop the raw table.
// Raw holds signatures/wallet-bearing payloads; the user wants price data only.
import { DuckDBInstance } from '@duckdb/node-api';
import fs from 'fs'; import zlib from 'zlib';
import { decodeEvent, Registry, Candles } from './decode.mjs';
const KEY = process.env.APERTURE_KEY, MD = process.env.MOTHERDUCK_TOKEN;
const log = (...a) => console.log(new Date().toISOString().slice(11, 19), ...a);
const db = await (await DuckDBInstance.create(`md:?motherduck_token=${MD}`)).connect();
const exists = (await db.runAndReadAll(`SELECT count(*)::INT n FROM duckdb_tables() WHERE database_name='solana_ticks' AND table_name='dex_events'`)).getRowObjects()[0].n;
if (!exists) { log('dex_events already gone'); process.exit(0); }
await db.run(`CREATE TABLE IF NOT EXISTS solana_ticks.main.ticks (mint VARCHAR, ts BIGINT, p FLOAT)`);
const reg = new Registry('registry.json', `https://solana-rpc.rpcfast.com/?api_key=${KEY}`);
const C = new Candles(1000);
const { t0, t1 } = (await db.runAndReadAll(`SELECT min(ts)::BIGINT t0, max(ts)::BIGINT t1 FROM solana_ticks.main.dex_events`)).getRowObjects()[0];
const STEP = 10 * 60000; let total = 0;
const seen = new Set();
for (let a = Number(t0); a <= Number(t1); a += STEP) {
  const rows = (await db.runAndReadAll(`SELECT ts, sig, data FROM solana_ticks.main.dex_events WHERE ts >= ${a} AND ts < ${a + STEP} ORDER BY ts`)).getRowObjects();
  const trades = [];
  for (const r of rows) { if (seen.has(r.sig)) continue; seen.add(r.sig);
    for (const d of r.data.split(' ')) { const t = decodeEvent(Buffer.from(d, 'base64')); if (t) trades.push([t, Number(r.ts)]); } }
  if (seen.size > 3e6) seen.clear();
  for (let i = 0; i < trades.length; i += 20000) { try { await reg.resolve(trades.slice(i, i + 20000).map((x) => x[0])); } catch (e) { log('registry', e.message); } }
  for (const [t, ts] of trades) C.add(reg, t, ts);
  const ticks = C.takeTicks(); if (!ticks.length) continue;
  const f = `conv-${a}.csv.gz`;
  fs.writeFileSync(f, zlib.gzipSync(ticks.map((r) => `${r.mint},${r.ts},${r.p}`).join(String.fromCharCode(10))));
  await db.run(`INSERT INTO solana_ticks.main.ticks SELECT * FROM read_csv('${f}', header=false, columns={'mint':'VARCHAR','ts':'BIGINT','p':'FLOAT'})`);
  fs.unlinkSync(f); total += ticks.length;
  log(`${new Date(a).toISOString().slice(11, 16)} raw ${rows.length} → ticks ${ticks.length} (total ${total}, pools ${Object.keys(reg.pools).length})`);
}
const n = (await db.runAndReadAll(`SELECT count(*)::BIGINT n FROM solana_ticks.main.ticks WHERE ts <= ${t1}`)).getRowObjects()[0].n;
log(`converted ticks in raw window: ${n}`);
if (Number(n) > 0) { await db.run(`DROP TABLE solana_ticks.main.dex_events`); log('raw table dropped'); }

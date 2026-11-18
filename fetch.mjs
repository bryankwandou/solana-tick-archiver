// Read-only: subscribes to DEX program logs over RPC Fast WSS, keeps the
// decodable event payloads ("Program data:") per transaction, and appends
// compressed batches to MotherDuck every FLUSH_SEC. Stops cleanly after RUN_MIN.
import { DuckDBInstance } from '@duckdb/node-api';
import fs from 'fs'; import zlib from 'zlib';
import { decodeEvent, decodeTx, Registry, Candles } from './decode.mjs';
const KEY = process.env.APERTURE_KEY, MD = process.env.MOTHERDUCK_TOKEN;
const RUN_MIN = Number(process.env.RUN_MIN || 340), FLUSH_SEC = Number(process.env.FLUSH_SEC || 300);
const CAP_GB = Number(process.env.CAP_GB || 9);
const RAW = process.env.RAW === '1'; // raw events ~7 GB/day; off: only 1s candles are stored
const PROGRAMS = {
  pumpfun: '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P', pumpswap: 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA',
  raydium_amm: '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8', raydium_clmm: 'CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK',
  raydium_cpmm: 'CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C', orca_whirlpool: 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc',
  meteora_dlmm: 'LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo', jupiter_v6: 'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4',
  raydium_launchlab: 'LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj',
};
let buf = [], total = 0, stopped = false;
// per-token 1-minute USD candles decoded from swap events (tiny: ~5 MB/day for all tokens)
const reg = new Registry('registry.json', `https://solana-rpc.rpcfast.com/?api_key=${KEY}`);
const candles = new Candles(1000); /* 1-second buckets; 1m/5m/1h derive from these */ let pendingTrades = [], seenSig = new Map(), nCandles = 0;
const t0 = Date.now(), log = (...a) => console.log(new Date().toISOString().slice(11, 19), ...a);

function connect() {
  const ws = new WebSocket(`wss://solana-rpc.rpcfast.com/?api_key=${KEY}`);
  const subs = {};
  ws.onopen = () => { let id = 1; for (const [name, p] of Object.entries(PROGRAMS)) {
    subs[id] = name; ws.send(JSON.stringify({ jsonrpc: '2.0', id: id++, method: 'logsSubscribe', params: [{ mentions: [p] }, { commitment: 'processed' }] })); }
    log('ws open'); };
  const subName = {};
  ws.onmessage = (ev) => {
    const s = ev.data;
    // cheap pre-filter: ~75% of notifications are failed txs or carry no event data
    if (s.indexOf('"err":null') < 0 && s.indexOf('"result":') > 0 && s.indexOf('"subscription"') > 0) return;
    if (s.indexOf('"subscription"') > 0 && s.indexOf('Program data: ') < 0) return;
    const m = JSON.parse(s);
    if (m.id && m.result !== undefined) { subName[m.result] = subs[m.id]; return; }
    if (m.error) { log('rpc error', JSON.stringify(m.error).slice(0, 200)); return; }
    const v = m.params?.result?.value; if (!v || v.err) return;
    const data = v.logs.filter((l) => l.startsWith('Program data: ')).map((l) => l.slice(14));
    if (!data.length) return;
    if (!seenSig.has(v.signature)) { seenSig.set(v.signature, Date.now());
      for (const d of data) { const t = decodeEvent(Buffer.from(d, 'base64')); if (t) pendingTrades.push([t, Date.now()]); } }
    if (RAW) buf.push({ ts: Date.now(), slot: m.params.result.context.slot, sig: v.signature, prog: subName[m.params.subscription] || '?', data: data.join(' ') });
  };
  ws.onclose = () => { if (!stopped) { log('ws closed, reconnect in 2s'); setTimeout(connect, 2000); } };
  ws.onerror = (e) => log('ws error', e.message || '');
  return ws;
}

// DRY=1: decode and log only, never write to MotherDuck (for tests next to the live run). DUMP=file: also save ticks as JSON lines
const DRY = process.env.DRY === '1', DUMP = process.env.DUMP;
const db = DRY ? { run: async () => {}, runAndReadAll: async () => ({ getRowObjects: () => [] }) } : await (await DuckDBInstance.create(`md:?motherduck_token=${MD}`)).connect();
await db.run(`CREATE DATABASE IF NOT EXISTS solana_ticks`);
if (RAW) await db.run(`CREATE TABLE IF NOT EXISTS solana_ticks.main.dex_events (ts BIGINT, slot BIGINT, sig VARCHAR, prog VARCHAR, data VARCHAR)`);
await db.run(`CREATE TABLE IF NOT EXISTS solana_ticks.main.ticks (mint VARCHAR, ts BIGINT, p FLOAT)`);
// every 15s: resolve pools/mints for new trades and fold them into candles
setInterval(async () => {
  const tr = pendingTrades; pendingTrades = []; if (!tr.length) return;
  try { await reg.resolve(tr.map((x) => x[0])); } catch (e) { log('registry error', e.message); }
  for (const [t, ts] of tr) candles.add(reg, t, ts);
  const cut = Date.now() - 120000; for (const [k, v] of seenSig) { if (v < cut) seenSig.delete(k); else break; }
}, 15000);
async function flushCandles() { // price ticks only: token, time (ms), USD price — no sender, no signature
  const rows = candles.takeTicks(); if (!rows.length) return;
  if (DRY) { if (DUMP) fs.appendFileSync(DUMP, rows.map((r) => JSON.stringify(r)).join(String.fromCharCode(10)) + String.fromCharCode(10)); nCandles += rows.length; log(`DRY ticks +${rows.length} (total ${nCandles}, tokens ${new Set(rows.map((r) => r.mint)).size}, SOL/USD ${candles.solUsd.toFixed(2)}, skipped ${candles.skipped}) by source ${JSON.stringify(candles.byKind)}`); return; }
  const f = `ticks-${Date.now()}.csv.gz`;
  fs.writeFileSync(f, zlib.gzipSync(rows.map((r) => `${r.mint},${r.ts},${r.p}`).join(String.fromCharCode(10))));
  await db.run(`INSERT INTO solana_ticks.main.ticks SELECT * FROM read_csv('${f}', header=false, columns={'mint':'VARCHAR','ts':'BIGINT','p':'FLOAT'})`);
  fs.unlinkSync(f); nCandles += rows.length;
  log(`ticks +${rows.length} (total ${nCandles}, tokens ${new Set(rows.map((r) => r.mint)).size}, SOL/USD ${candles.solUsd.toFixed(2)}, skipped ${candles.skipped}) by source ${JSON.stringify(candles.byKind)}`);
}
async function sizeGB() {
  // real stored bytes (estimated_size in duckdb_tables() is a ROW count, not bytes)
  const r = await db.runAndReadAll(`SELECT (active_bytes + historical_bytes)::DOUBLE b FROM md_information_schema.storage_info WHERE database_name='solana_ticks'`);
  return Number(r.getRowObjects()[0]?.b || 0) / 1e9;
}
let capped = (await sizeGB()) >= CAP_GB;
async function flush() {
  await flushCandles().catch((e) => log('candle flush error', e.message));
  const rows = buf; buf = [];
  if (!rows.length || capped) return;
  const f = `batch-${Date.now()}.json.gz`;
  fs.writeFileSync(f, zlib.gzipSync(rows.map((r) => JSON.stringify(r)).join('\n')));
  await db.run(`INSERT INTO solana_ticks.main.dex_events SELECT ts, slot, sig, prog, data FROM read_json('${f}', format='newline_delimited', columns={ts:'BIGINT',slot:'BIGINT',sig:'VARCHAR',prog:'VARCHAR',data:'VARCHAR'})`);
  fs.unlinkSync(f); total += rows.length;
  const gb = await sizeGB(); capped = gb >= CAP_GB;
  const mins = (Date.now() - t0) / 60000;
  log(`flushed ${rows.length} rows (total ${total}, ${(total / mins / 60).toFixed(1)}/s), md size ${gb.toFixed(3)} GB${capped ? ' CAP REACHED' : ''}`);
}
const ws = connect();
// ---------- block path: Meteora DLMM / DBC / DAMM v1 / DAMM v2 and Raydium AMM v4 ----------
// Their swaps are not decodable from logsSubscribe (emit_cpi events, or ray_log without the pool), so
// every confirmed block is read once with getBlock (~2.5 requests/s) and decoded with decodeTx.
const RPC = `https://solana-rpc.rpcfast.com/?api_key=${KEY}`, BLOCKS = process.env.BLOCKS !== '0';
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function call(method, params) {
  for (let i = 0; ; i++) {
    try { const j = await (await fetch(RPC, { method: 'POST', headers: { 'content-type': 'application/json' }, signal: AbortSignal.timeout(30000), body: JSON.stringify({ jsonrpc: '2.0', id: 1, method, params }) })).json();
      if (!j.error) return j.result;
      if (j.error.code === -32007 || j.error.code === -32009 || /skipped|cleaned/i.test(j.error.message)) return null; // slot without a block
      if (i >= 8) throw new Error(j.error.message); }
    catch (e) { if (i >= 8) throw e; }
    await sleep(500 * (i + 1)); // also covers "block not available yet"
  }
}
const bs = { blocks: 0, trades: {}, skipped: 0, lag: 0, ms: 0 };
async function blockLoop() {
  let last = 0;
  while (!stopped) {
    let tip; try { tip = await call('getSlot', [{ commitment: 'confirmed' }]); } catch (e) { log('getSlot', e.message); await sleep(2000); continue; }
    if (!last) last = tip - 1;
    if (tip - last > 300) { bs.skipped += tip - last - 300; last = tip - 300; } // never more than ~2 min behind
    const slots = []; for (let x = last + 1; x <= tip; x++) slots.push(x);
    let i = 0;
    await Promise.all(Array.from({ length: Math.min(8, slots.length) }, async () => {
      while (i < slots.length) { const slot = slots[i++]; const t1 = Date.now();
        let blk; try { blk = await call('getBlock', [slot, { encoding: 'json', transactionDetails: 'full', rewards: false, maxSupportedTransactionVersion: 1, commitment: 'confirmed' }]); } catch (e) { log('getBlock', slot, e.message); continue; }
        bs.ms += Date.now() - t1; if (!blk) continue; bs.blocks++;
        const ts = (blk.blockTime || Math.floor(Date.now() / 1000)) * 1000; bs.lag = Date.now() - ts;
        for (const tx of blk.transactions) for (const t of decodeTx(tx)) { pendingTrades.push([t, ts]); bs.trades[t.kind] = (bs.trades[t.kind] || 0) + 1; }
      }
    }));
    last = tip; await sleep(400);
  }
}
if (BLOCKS) { blockLoop().catch((e) => log('block loop died', e.message));
  setInterval(() => { log(`blocks ${bs.blocks} (avg ${(bs.ms / Math.max(1, bs.blocks)).toFixed(0)} ms, lag ${(bs.lag / 1000).toFixed(1)} s, skipped ${bs.skipped}) trades ${JSON.stringify(bs.trades)}`); }, 60000); }
const timer = setInterval(() => flush().catch((e) => log('flush error', e.message)), FLUSH_SEC * 1000);
setTimeout(async () => { stopped = true; clearInterval(timer); ws.close(); await flush().catch((e) => log('final flush error', e.message)); await flushCandles().catch(() => {}); log('done'); process.exit(0); }, RUN_MIN * 60000);

// Read-only: subscribes to DEX program logs over RPC Fast WSS, keeps the
// decodable event payloads ("Program data:") per transaction, and appends
// compressed batches to MotherDuck every FLUSH_SEC. Stops cleanly after RUN_MIN.
import { DuckDBInstance } from '@duckdb/node-api';
import fs from 'fs'; import zlib from 'zlib';
const KEY = process.env.APERTURE_KEY, MD = process.env.MOTHERDUCK_TOKEN;
const RUN_MIN = Number(process.env.RUN_MIN || 340), FLUSH_SEC = Number(process.env.FLUSH_SEC || 300);
const CAP_GB = Number(process.env.CAP_GB || 9);
const PROGRAMS = {
  pumpfun: '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P', pumpswap: 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA',
  raydium_amm: '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8', raydium_clmm: 'CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK',
  raydium_cpmm: 'CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C', orca_whirlpool: 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc',
  meteora_dlmm: 'LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo', jupiter_v6: 'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4',
};
let buf = [], total = 0, stopped = false;
const t0 = Date.now(), log = (...a) => console.log(new Date().toISOString().slice(11, 19), ...a);

function connect() {
  const ws = new WebSocket(`wss://solana-rpc.rpcfast.com/?api_key=${KEY}`);
  const subs = {};
  ws.onopen = () => { let id = 1; for (const [name, p] of Object.entries(PROGRAMS)) {
    subs[id] = name; ws.send(JSON.stringify({ jsonrpc: '2.0', id: id++, method: 'logsSubscribe', params: [{ mentions: [p] }, { commitment: 'confirmed' }] })); }
    log('ws open'); };
  const subName = {};
  ws.onmessage = (ev) => {
    const m = JSON.parse(ev.data);
    if (m.id && m.result !== undefined) { subName[m.result] = subs[m.id]; return; }
    if (m.error) { log('rpc error', JSON.stringify(m.error).slice(0, 200)); return; }
    const v = m.params?.result?.value; if (!v || v.err) return;
    const data = v.logs.filter((l) => l.startsWith('Program data: ')).map((l) => l.slice(14));
    if (!data.length) return;
    buf.push({ ts: Date.now(), slot: m.params.result.context.slot, sig: v.signature, prog: subName[m.params.subscription] || '?', data: data.join(' ') });
  };
  ws.onclose = () => { if (!stopped) { log('ws closed, reconnect in 2s'); setTimeout(connect, 2000); } };
  ws.onerror = (e) => log('ws error', e.message || '');
  return ws;
}

const db = await (await DuckDBInstance.create(`md:?motherduck_token=${MD}`)).connect();
await db.run(`CREATE DATABASE IF NOT EXISTS solana_ticks`);
await db.run(`CREATE TABLE IF NOT EXISTS solana_ticks.main.dex_events (ts BIGINT, slot BIGINT, sig VARCHAR, prog VARCHAR, data VARCHAR)`);
async function sizeGB() {
  const r = await db.runAndReadAll(`SELECT sum(estimated_size)::DOUBLE b FROM duckdb_tables() WHERE database_name='solana_ticks'`);
  return Number(r.getRowObjects()[0]?.b || 0) / 1e9;
}
let capped = (await sizeGB()) >= CAP_GB;
async function flush() {
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
const timer = setInterval(() => flush().catch((e) => log('flush error', e.message)), FLUSH_SEC * 1000);
setTimeout(async () => { stopped = true; clearInterval(timer); ws.close(); await flush().catch((e) => log('final flush error', e.message)); log('done'); process.exit(0); }, RUN_MIN * 60000);

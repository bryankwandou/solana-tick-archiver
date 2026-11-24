// Single-runner multi-worker batch archiver (Jenis 2: 1 run per 2 hours).
// Fetches complete Solana blocks via RPC Fast Aperture getBlock across 4 worker threads,
// decodes all 14 DEXes (log events + full tx inner CPI), writes to MotherDuck in 1 batch,
// ensures live SQL candle views exist, and supports BENCH_MINUTES side-by-side verification.
import { Worker } from 'worker_threads';
import { DuckDBInstance } from '@duckdb/node-api';
import fs from 'fs';
import path from 'path';
import zlib from 'zlib';
import { Registry, Candles, WSOL, encodeTicksV2, encodeCandlesV2, MintDict } from './decode.mjs';

const KEY = process.env.APERTURE_KEY;
const MD = process.env.MOTHERDUCK_TOKEN;
const RPC = `https://solana-rpc.rpcfast.com/?api_key=${KEY}`;
const WORKERS = Math.max(1, Number(process.env.WORKERS || 4));
const CONC_PER_WORKER = Math.max(1, Number(process.env.CONC_PER_WORKER || 20));
const BENCH_MINUTES = Number(process.env.BENCH_MINUTES || 0);
const BENCH_LAG_MIN = Number(process.env.BENCH_LAG_MIN || 15);
const MAX_GAP_HOURS = Number(process.env.MAX_GAP_HOURS || 12);

if (!KEY || !MD) {
  console.error('Missing APERTURE_KEY or MOTHERDUCK_TOKEN');
  process.exit(1);
}

const tStart = Date.now();
const log = (...a) => console.log(new Date().toISOString().slice(11, 19), ...a);

async function rpc(method, params) {
  for (let i = 0; ; i++) {
    try {
      const res = await fetch(RPC, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        signal: AbortSignal.timeout(30000),
        body: JSON.stringify({ jsonrpc: '2.0', id: 1, method, params }),
      });
      const j = await res.json();
      if (j.error) {
        if (/skipped|not available|cleaned/i.test(j.error.message) || j.error.code === -32007 || j.error.code === -32009) return null;
        throw new Error(j.error.message);
      }
      return j.result;
    } catch (e) {
      if (i >= 5) throw e;
      await new Promise((r) => setTimeout(r, 1000 * (i + 1)));
    }
  }
}

async function timeAt(slot) {
  for (let k = 0; k < 50; k++) {
    const t = await rpc('getBlockTime', [slot + k]);
    if (t != null) return t;
  }
  return null;
}

async function slotAt(first, tip, targetSec) {
  let lo = first;
  let hi = tip;
  while (hi - lo > 2) {
    const mid = Math.floor((lo + hi) / 2);
    const t = await timeAt(mid);
    if (t == null || t >= targetSec) hi = mid;
    else lo = mid;
  }
  return lo;
}

const db = await (await DuckDBInstance.create(`md:?motherduck_token=${MD}`)).connect();
await db.run(`CREATE DATABASE IF NOT EXISTS solana_ticks`);
await db.run(`CREATE TABLE IF NOT EXISTS solana_ticks.main.ticks (mint VARCHAR, ts BIGINT, p FLOAT)`);

// Seed SOL/USD from MotherDuck so early swaps before first SOL/USDC trade are never skipped
let seedSolUsd = 0;
try {
  const sr = (await db.runAndReadAll(`SELECT p::DOUBLE p FROM solana_ticks.main.ticks WHERE mint='${WSOL}' ORDER BY ts DESC LIMIT 1`)).getRowObjects()[0];
  if (sr && Number(sr.p) > 1) seedSolUsd = Number(sr.p);
} catch {}

const first = (await rpc('getFirstAvailableBlock', [])) + 300;
const tip = await rpc('getSlot', [{ commitment: 'finalized' }]);
const firstTime = await timeAt(first);
const tipTime = await timeAt(tip);
log(`RPC retention: slot ${first} (${new Date(firstTime * 1000).toISOString()}) -> tip ${tip} (${new Date(tipTime * 1000).toISOString()}) | seed SOL/USD=${seedSolUsd.toFixed(2)}`);

let targetRanges = []; // [[startSlot, endSlot], ...]
let windowStartMs = 0;
let windowEndMs = 0;

if (BENCH_MINUTES > 0) {
  // Exact completed window ending BENCH_LAG_MIN minutes ago so we can compare directly vs Jenis 1 (archive.yml)
  const endSec = Math.floor((Date.now() - BENCH_LAG_MIN * 60000) / 60000) * 60;
  const startSec = Math.max(firstTime + 60, endSec - BENCH_MINUTES * 60);
  windowStartMs = startSec * 1000;
  windowEndMs = endSec * 1000;
  const s0 = await slotAt(first, tip, startSec);
  const s1 = await slotAt(first, tip, endSec);
  targetRanges.push([s0, s1]);
  log(`BENCH_MODE (${BENCH_MINUTES} min): ${new Date(windowStartMs).toISOString()} -> ${new Date(windowEndMs).toISOString()} | slots ${s0}..${s1} (${s1 - s0 + 1} slots)`);
} else {
  // Production mode: find all missing or thin (<30% median) minutes in the last MAX_GAP_HOURS (capped by RPC retention)
  const lookbackSec = Math.max(firstTime + 60, Math.floor(Date.now() / 1000) - MAX_GAP_HOURS * 3600);
  const rows = (await db.runAndReadAll(`SELECT (ts // 60000)::BIGINT m, count(*)::BIGINT n FROM solana_ticks.main.ticks WHERE ts >= ${lookbackSec * 1000} GROUP BY 1`)).getRowObjects();
  const map = new Map(rows.map((r) => [Number(r.m), Number(r.n)]));
  const ns = [...map.values()].sort((a, b) => a - b);
  const med = ns.length ? ns[Math.floor(ns.length / 2)] : 5000;
  const m0 = Math.ceil(lookbackSec / 60);
  const m1 = Math.floor((tipTime - 30) / 60);
  const gaps = [];
  let cur = null;
  for (let m = m0; m <= m1; m++) {
    if ((map.get(m) || 0) < med * 0.3) {
      if (cur && cur[1] === m - 1) cur[1] = m;
      else gaps.push((cur = [m, m]));
    }
  }
  log(`PROD_MODE: lookback ${new Date(lookbackSec * 1000).toISOString()} -> ${new Date(m1 * 60000).toISOString()}, median=${med}/min, gap windows=${gaps.length}`);
  for (const [a, b] of gaps) {
    await db.run(`DELETE FROM solana_ticks.main.ticks WHERE ts >= ${a * 60000} AND ts < ${(b + 1) * 60000}`);
    const s0 = Math.max(first, (await slotAt(first, tip, a * 60)) - 2);
    const s1 = Math.min(tip, (await slotAt(first, tip, (b + 1) * 60)) + 2);
    targetRanges.push([s0, s1]);
    log(`  gap ${new Date(a * 60000).toISOString().slice(0, 16)} (+${b - a + 1} min) -> slots ${s0}..${s1} (${s1 - s0 + 1} slots)`);
  }
}

const allSlots = [];
for (const [a, b] of targetRanges) {
  for (let s = a; s <= b; s++) allSlots.push(s);
}

if (!allSlots.length) {
  log('No missing slots to fetch. Ensuring candle views exist...');
} else {
  // Deal slots in 50-slot interleaved chunks across workers for uniform load balance
  const workerSlots = Array.from({ length: WORKERS }, () => []);
  let chunkIdx = 0;
  for (let i = 0; i < allSlots.length; i += 50, chunkIdx++) {
    const target = workerSlots[chunkIdx % WORKERS];
    for (let j = i; j < Math.min(i + 50, allSlots.length); j++) target.push(allSlots[j]);
  }

  const reg = new Registry('registry.json', RPC);
  const candles = new Candles(1000);
  if (seedSolUsd > 0) candles.solUsd = seedSolUsd;

  const collected = [];
  const stats = Array.from({ length: WORKERS }, () => ({ doneBlocks: 0, skippedSlots: 0, errorSlots: 0, rpcCalls: 0 }));

  const tFetch0 = Date.now();
  log(`Spawning ${WORKERS} workers x ${CONC_PER_WORKER} conc (${WORKERS * CONC_PER_WORKER} total concurrent getBlock) for ${allSlots.length} slots...`);

  await Promise.all(
    workerSlots.map(
      (slots, wi) =>
        new Promise((resolve, reject) => {
          const w = new Worker(new URL('./batch-worker.mjs', import.meta.url), {
            workerData: { workerId: wi, slots, rpcUrl: RPC, conc: CONC_PER_WORKER },
          });
          w.on('message', (msg) => {
            if (msg.type === 'trades') {
              for (let k = 0; k < msg.trades.length; k++) collected.push(msg.trades[k]);
            } else if (msg.type === 'progress' || msg.type === 'done') {
              stats[wi] = { doneBlocks: msg.doneBlocks, skippedSlots: msg.skippedSlots, errorSlots: msg.errorSlots, rpcCalls: msg.rpcCalls };
            }
          });
          w.on('error', reject);
          w.on('exit', (code) => (code === 0 ? resolve() : reject(new Error(`worker ${wi} exited ${code}`))));
        })
    )
  );

  const fetchSec = (Date.now() - tFetch0) / 1000;
  const totBlocks = stats.reduce((s, x) => s + x.doneBlocks, 0);
  const totSkipped = stats.reduce((s, x) => s + x.skippedSlots, 0);
  const totErr = stats.reduce((s, x) => s + x.errorSlots, 0);
  const totRpc = stats.reduce((s, x) => s + x.rpcCalls, 0);
  log(`Fetch complete in ${fetchSec.toFixed(1)}s: ${totBlocks} blocks (${totSkipped} skipped slots, ${totErr} errs) | ${(allSlots.length / fetchSec).toFixed(1)} slots/s | ${(totBlocks / fetchSec).toFixed(1)} blk/s | ${(totRpc / fetchSec).toFixed(1)} rpc/s | raw trades=${collected.length}`);

  const tDecode0 = Date.now();
  collected.sort((a, b) => a[2] - b[2]);
  // Resolve all unknown pools & token decimals in chunks
  for (let i = 0; i < collected.length; i += 25000) {
    try {
      await reg.resolve(collected.slice(i, i + 25000).map((x) => x[0]));
    } catch (e) {
      log('registry chunk warn:', e.message);
    }
  }
  reg.save();

  for (let i = 0; i < collected.length; i++) {
    const [trade, ts] = collected[i];
    if (BENCH_MINUTES > 0 && (ts < windowStartMs || ts >= windowEndMs)) continue;
    candles.add(reg, trade, ts);
  }
  const ticks = candles.takeTicks();
  const decodeSec = (Date.now() - tDecode0) / 1000;
  const uniqueTokens = new Set(ticks.map((r) => r.mint)).size;
  log(`Decode + registry in ${decodeSec.toFixed(1)}s: ticks=${ticks.length}, uniqueTokens=${uniqueTokens}, skipped=${candles.skipped}, SOL/USD=${candles.solUsd.toFixed(2)}, bySource=${JSON.stringify(candles.byKind)}`);

  if (BENCH_MINUTES > 0) {
    // 1. Encode compressed ticks + 1m candles locally to measure exact output size
    const benchDir = 'bench-out';
    fs.mkdirSync(benchDir, { recursive: true });
    const dict = new MintDict(path.join(benchDir, 'mints.txt'));
    const cdict = new MintDict(path.join(benchDir, 'mints-1m.txt'));
    const tEnc0 = Date.now();
    const tickBuf = encodeTicksV2(ticks.map((r) => ({ ...r })), dict);
    const cm = new Map();
    for (const r of ticks) {
      const m = Math.floor(r.ts / 60000);
      const k = r.mint + '|' + m;
      const c = cm.get(k);
      if (!c) cm.set(k, { mint: r.mint, minute: m, o: r.p, h: r.p, l: r.p, c: r.p, v: 0, n: 1 });
      else {
        if (r.p > c.h) c.h = r.p;
        if (r.p < c.l) c.l = r.p;
        c.c = r.p;
        c.n++;
      }
    }
    const candleRows = [...cm.values()];
    const candleBuf = encodeCandlesV2(candleRows, cdict);
    const encSec = (Date.now() - tEnc0) / 1000;

    // 2. Compare directly against MotherDuck (Jenis 1: archive.yml) for the EXACT same time window
    const mdRow = (
      await db.runAndReadAll(`
        SELECT count(*)::BIGINT AS n, count(DISTINCT mint)::BIGINT AS tokens
        FROM solana_ticks.main.ticks
        WHERE ts >= ${windowStartMs} AND ts < ${windowEndMs}
      `)
    ).getRowObjects()[0];
    const mdTicks = Number(mdRow.n);
    const mdTokens = Number(mdRow.tokens);
    const totalSec = (Date.now() - tStart) / 1000;
    const summary = {
      mode: 'BENCH_2H_SIDE_BY_SIDE',
      windowStart: new Date(windowStartMs).toISOString(),
      windowEnd: new Date(windowEndMs).toISOString(),
      windowMinutes: BENCH_MINUTES,
      workers: WORKERS,
      concTotal: WORKERS * CONC_PER_WORKER,
      slotsTotal: allSlots.length,
      blocksFetched: totBlocks,
      skippedSlots: totSkipped,
      errorSlots: totErr,
      fetchSec: +fetchSec.toFixed(1),
      decodeSec: +decodeSec.toFixed(1),
      encodeSec: +encSec.toFixed(1),
      totalWallSec: +totalSec.toFixed(1),
      slotsPerSec: +(allSlots.length / fetchSec).toFixed(1),
      blocksPerSec: +(totBlocks / fetchSec).toFixed(1),
      jenis2_batch_ticks: ticks.length,
      jenis2_batch_tokens: uniqueTokens,
      jenis2_candles_1m: candleRows.length,
      jenis2_tick_file_MB: +(tickBuf.length / 1e6).toFixed(2),
      jenis2_candle1m_file_MB: +(candleBuf.length / 1e6).toFixed(2),
      jenis2_bytes_per_tick: +(tickBuf.length / Math.max(1, ticks.length)).toFixed(2),
      jenis1_archive_md_ticks: mdTicks,
      jenis1_archive_md_tokens: mdTokens,
      completeness_ratio_pct: +((ticks.length / Math.max(1, mdTicks)) * 100).toFixed(2),
      token_coverage_ratio_pct: +((uniqueTokens / Math.max(1, mdTokens)) * 100).toFixed(2),
      cicd_savings_pct: +((1 - totalSec / (BENCH_MINUTES * 60)) * 100).toFixed(2),
      bySource: candles.byKind,
    };
    console.log('=== BENCHMARK_SUMMARY_JSON ===');
    console.log(JSON.stringify(summary, null, 2));
    fs.rmSync(benchDir, { recursive: true, force: true });
  } else if (ticks.length) {
    // Production: single atomic MotherDuck commit (keeps historical_bytes minimal!)
    const f = `batch2h-${Date.now()}.csv.gz`;
    fs.writeFileSync(f, zlib.gzipSync(ticks.map((r) => `${r.mint},${r.ts},${r.p}`).join('\n')));
    const tMd0 = Date.now();
    await db.run(`INSERT INTO solana_ticks.main.ticks SELECT * FROM read_csv('${f}', header=false, columns={'mint':'VARCHAR','ts':'BIGINT','p':'FLOAT'})`);
    fs.unlinkSync(f);
    log(`Inserted ${ticks.length} ticks (${uniqueTokens} tokens) into MotherDuck in ${((Date.now() - tMd0) / 1000).toFixed(1)}s`);
  }
}

// Always ensure zero-storage live candle views exist in MotherDuck
await db.run(`
  CREATE OR REPLACE VIEW solana_ticks.main.candles_1m AS
  SELECT mint, (ts // 60000) * 60000 AS t_ms, arg_min(p, ts) AS o, max(p) AS h, min(p) AS l, arg_max(p, ts) AS c, count(*)::BIGINT AS trades
  FROM solana_ticks.main.ticks WHERE p > 0 GROUP BY mint, (ts // 60000)
`);
await db.run(`
  CREATE OR REPLACE VIEW solana_ticks.main.candles_5m AS
  SELECT mint, (ts // 300000) * 300000 AS t_ms, arg_min(p, ts) AS o, max(p) AS h, min(p) AS l, arg_max(p, ts) AS c, count(*)::BIGINT AS trades
  FROM solana_ticks.main.ticks WHERE p > 0 GROUP BY mint, (ts // 300000)
`);
await db.run(`
  CREATE OR REPLACE VIEW solana_ticks.main.candles_1h AS
  SELECT mint, (ts // 3600000) * 3600000 AS t_ms, arg_min(p, ts) AS o, max(p) AS h, min(p) AS l, arg_max(p, ts) AS c, count(*)::BIGINT AS trades
  FROM solana_ticks.main.ticks WHERE p > 0 GROUP BY mint, (ts // 3600000)
`);
log(`Done in ${((Date.now() - tStart) / 1000).toFixed(1)}s total`);
process.exit(0);


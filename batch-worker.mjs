// Worker thread for parallel getBlock fetch + JSON parse + swap decode across vCPUs.
import { parentPort, workerData } from 'worker_threads';
import { decodeEvent, decodeTx, decodeUniversalTx } from './decode.mjs';

const { workerId, slots, rpcUrl, conc, maxRps } = workerData;
const MAX_RPS = Math.max(1, Number(maxRps || 8)); // 4 workers x 8 = 32 RPS hard cap (keeps total <=499 RPS with hft-stream)
let secWin = Math.floor(performance.now() / 1000);
let secCount = 0;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function waitRateSlot() {
  while (true) {
    const now = performance.now();
    const s = Math.floor(now / 1000);
    if (s !== secWin) { secWin = s; secCount = 0; }
    if (secCount < MAX_RPS) { secCount++; return; }
    await sleep(Math.max(10, (s + 1) * 1000 - now));
  }
}

async function fetchBlock(slot) {
  for (let attempt = 0; ; attempt++) {
    try {
      const res = await fetch(rpcUrl, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        signal: AbortSignal.timeout(30000),
        body: JSON.stringify({
          jsonrpc: '2.0',
          id: 1,
          method: 'getBlock',
          params: [slot, { encoding: 'json', transactionDetails: 'full', rewards: false, maxSupportedTransactionVersion: 1 }],
        }),
      });
      if (res.status === 429 || res.status >= 500) {
        if (attempt >= 5) return { err: `http ${res.status}` };
        await sleep(400 * (attempt + 1));
        continue;
      }
      const j = await res.json();
      if (j.error) {
        if (j.error.code === -32007 || j.error.code === -32009 || /skipped|not available|cleaned/i.test(j.error.message)) {
          return { skipped: true };
        }
        if (attempt >= 4) return { err: j.error.message };
        await sleep(500 * (attempt + 1));
        continue;
      }
      return { blk: j.result };
    } catch (e) {
      if (attempt >= 4) return { err: e.message || 'fetch failed' };
      await sleep(500 * (attempt + 1));
    }
  }
}

let idx = 0;
let doneBlocks = 0;
let skippedSlots = 0;
let errorSlots = 0;
let rpcCalls = 0;
let batch = [];

function flushToParent() {
  if (!batch.length) return;
  parentPort.postMessage({ type: 'trades', workerId, trades: batch });
  batch = [];
}

async function runSlotLoop() {
  while (idx < slots.length) {
    const slot = slots[idx++];
    await waitRateSlot();
    rpcCalls++;
    const r = await fetchBlock(slot);
    if (r.skipped) {
      skippedSlots++;
      continue;
    }
    if (r.err || !r.blk) {
      errorSlots++;
      continue;
    }
    doneBlocks++;
    const blk = r.blk;
    const baseMs = (blk.blockTime || 0) * 1000;
    const txs = blk.transactions || [];
    const txTotal = txs.length || 1;
    for (let i = 0; i < txs.length; i++) {
      const tx = txs[i];
      if (!tx.meta || tx.meta.err) continue;
      // Preserve intra-block chronological order within the ~400ms slot window
      const ts = baseMs + Math.min(999, Math.floor((i * 400) / txTotal));
      const order = slot * 10000 + i;
      let txHit = 0;
      const logs = tx.meta.logMessages;
      if (logs) {
        for (let li = 0; li < logs.length; li++) {
          const l = logs[li];
          if (l.startsWith('Program data: ')) {
            const t = decodeEvent(Buffer.from(l.slice(14), 'base64'));
            if (t) { batch.push([t, ts, order]); txHit++; }
          }
        }
      }
      const extra = decodeTx(tx);
      for (let ei = 0; ei < extra.length; ei++) {
        batch.push([extra[ei], ts, order]);
        txHit++;
      }
      // Universal fallback: captures SolFi, HumidiFi, ZeroFi, Phoenix, OpenBook, Manifest,
      // Lifinity, Stabble, Sanctum, Heaven, Boop buys, and ANY other Solana DEX!
      if (txHit === 0) {
        const u = decodeUniversalTx(tx);
        if (u) batch.push([u, ts, order]);
      }
    }
    if (batch.length >= 2000) flushToParent();
  }
}

const progressTimer = setInterval(() => {
  flushToParent();
  parentPort.postMessage({ type: 'progress', workerId, doneBlocks, skippedSlots, errorSlots, rpcCalls });
}, 10000);

await Promise.all(Array.from({ length: Math.max(1, conc) }, runSlotLoop));
clearInterval(progressTimer);
flushToParent();
parentPort.postMessage({ type: 'done', workerId, doneBlocks, skippedSlots, errorSlots, rpcCalls });


// Nonstop high-frequency Solana DEX live vault streamer on GitHub Actions.
// - Zero external npm dependencies (boots in <10s, zero laptop Wi-Fi usage).
// - Zero MotherDuck wakeup/storage impact (batch-2h.yml handles MotherDuck archiving).
// - Sustains >=400 req/sec (default target 445 req/sec, adaptive 429 guard below 500 RPS limit).
// - Overlapping two-lane self-handoff (A <-> B) for zero-gap continuity until STOP_AT_ISO.
import { Worker } from 'node:worker_threads';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const KEY = process.env.APERTURE_KEY;
const GH_TOKEN = process.env.GH_TOKEN || process.env.GITHUB_TOKEN || '';
const REPO = process.env.GITHUB_REPOSITORY || 'bryankwandou/solana-tick-archiver';
const LANE = (process.env.LANE || 'A').toUpperCase() === 'B' ? 'B' : 'A';
const NEXT_LANE = LANE === 'A' ? 'B' : 'A';
const WORKERS = Math.max(1, Math.min(8, Number(process.env.WORKERS || 4)));
const HARD_CAP_RPS = Math.max(50, Math.min(460, Number(process.env.HARD_CAP_RPS || 452)));
const BASE_TARGET_RPS = Math.max(50, Math.min(HARD_CAP_RPS, Number(process.env.TARGET_RPS || 444)));
const START_RPS = Math.max(50, Math.min(BASE_TARGET_RPS, Number(process.env.START_RPS || BASE_TARGET_RPS)));
const MIN_FLOOR_RPS = Math.max(50, Math.min(BASE_TARGET_RPS, Number(process.env.MIN_FLOOR_RPS || 412)));
const MAX_CEIL_RPS = Math.max(BASE_TARGET_RPS, Math.min(HARD_CAP_RPS, Number(process.env.MAX_CEIL_RPS || 452)));
const RUN_MIN = Math.max(1, Math.min(345, Number(process.env.RUN_MIN || 330)));
const HANDOFF_LEAD_SEC = Math.max(30, Number(process.env.HANDOFF_LEAD_SEC || 180));
const ENABLE_CHAIN = process.env.ENABLE_CHAIN !== '0';
const STOP_AT_ISO = process.env.STOP_AT_ISO || '2026-12-16T09:50:00Z';
const STOP_AT_MS = Date.parse(STOP_AT_ISO);

if (!KEY) {
  console.error('Missing APERTURE_KEY');
  process.exit(1);
}

const log = (...args) => console.log(new Date().toISOString().slice(11, 19), `[lane=${LANE}]`, ...args);

if (Number.isFinite(STOP_AT_MS) && Date.now() >= STOP_AT_MS) {
  log(`Cutoff ${STOP_AT_ISO} reached; exiting without RPC calls.`);
  process.exit(0);
}

const RPC_URL = `https://solana-rpc.rpcfast.com/?api_key=${KEY}`;
const parsedUrl = new URL(RPC_URL);
const WORKER_FILE = path.join(path.dirname(fileURLToPath(import.meta.url)), 'hft-worker.mjs');

const TOKEN_PROGRAM = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA';
const TOKEN_2022 = 'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb';
const WSOL = 'So11111111111111111111111111111111111111112';
const USDC = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v';
const USDT = 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB';

const B58 = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz';
function encodeBase58(bytes) {
  let zeros = 0;
  while (zeros < bytes.length && bytes[zeros] === 0) zeros++;
  const digits = [0];
  for (let i = zeros; i < bytes.length; i++) {
    let carry = bytes[i];
    for (let j = 0; j < digits.length; j++) {
      carry += digits[j] << 8;
      digits[j] = carry % 58;
      carry = (carry / 58) | 0;
    }
    while (carry > 0) {
      digits.push(carry % 58);
      carry = (carry / 58) | 0;
    }
  }
  let out = '1'.repeat(zeros);
  for (let i = digits.length - 1; i >= 0; i--) out += B58[digits[i]];
  return out;
}

// Verified canonical flagship Solana DEX vaults (always tracked as core baseline)
const CORE_VAULTS = [
  { addr: 'DQyrAcCrDXQ7NeoqGgDCZwBvWDcYmFCjSb9JtteuvPpz', pool: '58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUWBkwMihLYQo2', dex: 'raydium_v4', kind: 'sol_ref_base' },
  { addr: 'HLmqeL62xR1QoZ1HKKbXRrdN1p3phKpxRMb2VVopvBBz', pool: '58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUWBkwMihLYQo2', dex: 'raydium_v4', kind: 'sol_ref_quote' },
];

// Direct pool account vault byte offsets: [vaultA, vaultB, mintA, mintB]
const DIRECT_VAULT_OFFSETS = {
  pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA: [139, 171, 43, 75],
  '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8': [336, 368, 400, 432],
  CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C: [72, 104, 168, 200],
};

async function rpcJson(method, params) {
  const res = await fetch(RPC_URL, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    signal: AbortSignal.timeout(12000),
    body: JSON.stringify({ jsonrpc: '2.0', id: 1, method, params }),
  });
  if (!res.ok) return null;
  const j = await res.json();
  return j.error ? null : j.result;
}

async function discoverActiveVaults() {
  const poolSet = new Map();
  const gtUrls = [
    'https://api.geckoterminal.com/api/v2/networks/solana/trending_pools?page=1',
    'https://api.geckoterminal.com/api/v2/networks/solana/trending_pools?page=2',
    'https://api.geckoterminal.com/api/v2/networks/solana/pools?sort=h24_tx_count_desc&page=1',
    'https://api.geckoterminal.com/api/v2/networks/solana/pools?sort=h24_volume_usd_desc&page=1',
  ];

  for (const url of gtUrls) {
    try {
      const r = await fetch(url, { headers: { accept: 'application/json' }, signal: AbortSignal.timeout(10000) });
      if (!r.ok) continue;
      const j = await r.json();
      for (const item of j.data || []) {
        const addr = item.attributes?.address;
        const dexId = item.relationships?.dex?.data?.id || 'solana_dex';
        if (addr && !poolSet.has(addr)) poolSet.set(addr, dexId);
      }
    } catch { /* continue with remaining sources */ }
  }

  // Always include flagship SOL/USDC pools across Raydium, Orca, Meteora
  const canonicalPools = [
    ['58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUWBkwMihLYQo2', 'raydium_v4'],
    ['Czfq3xZZDmsdGdUyrNLtRhGc47cXcZtLG4crryfu44zE', 'orca_whirlpool'],
    ['3ucNos4NbumPLZNWztqGHNFFgkHeRMBQAVemeeomsUxv', 'raydium_clmm'],
    ['BGm1tav58oGcsQJehL9WXBFXF7D27vZsKefj4xJKD5Y', 'meteora_dlmm'],
    ['5rCf1DM8LjKTw4YqhnoLcngyZYeNnQqztScTogYHAS6', 'meteora_dlmm'],
  ];
  for (const [addr, dex] of canonicalPools) {
    if (!poolSet.has(addr)) poolSet.set(addr, dex);
  }

  const pools = [...poolSet.entries()].slice(0, 80);
  const vaults = new Map();
  for (const cv of CORE_VAULTS) vaults.set(cv.addr, cv);

  // Batch fetch pool accounts via getMultipleAccounts to decode direct offsets first
  const fallbackPools = [];
  for (let i = 0; i < pools.length; i += 40) {
    const chunk = pools.slice(i, i + 40);
    const res = await rpcJson('getMultipleAccounts', [
      chunk.map((x) => x[0]),
      { encoding: 'base64', commitment: 'processed' },
    ]).catch(() => null);
    const vals = res?.value || [];
    for (let k = 0; k < chunk.length; k++) {
      const [poolAddr, dex] = chunk[k];
      const acc = vals[k];
      if (!acc?.data?.[0]) continue;
      const owner = acc.owner || '';
      const raw = Buffer.from(acc.data[0], 'base64');
      if (owner === '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P' && raw.length >= 49) {
        vaults.set(poolAddr, { addr: poolAddr, pool: poolAddr, dex: 'pumpfun', kind: 'pump_curve' });
        continue;
      }
      const offs = DIRECT_VAULT_OFFSETS[owner];
      if (offs && raw.length >= Math.max(...offs) + 32) {
        const vA = encodeBase58(raw.subarray(offs[0], offs[0] + 32));
        const vB = encodeBase58(raw.subarray(offs[1], offs[1] + 32));
        const mA = encodeBase58(raw.subarray(offs[2], offs[2] + 32));
        const mB = encodeBase58(raw.subarray(offs[3], offs[3] + 32));
        if (mA === WSOL || mA === USDC || mA === USDT || mB === WSOL || mB === USDC || mB === USDT) {
          if (!vaults.has(vA)) vaults.set(vA, { addr: vA, pool: poolAddr, dex, kind: 'vault' });
          if (!vaults.has(vB)) vaults.set(vB, { addr: vB, pool: poolAddr, dex, kind: 'vault' });
          continue;
        }
      }
      fallbackPools.push([poolAddr, dex]);
    }
  }

  // Resolve remaining DEX pools (Orca Whirlpool, Meteora DLMM, Raydium CLMM, etc.) via getTokenAccountsByOwner
  const resolveSlice = fallbackPools.slice(0, 30);
  await Promise.all(
    resolveSlice.map(async ([poolAddr, dex]) => {
      try {
        const [t1, t2] = await Promise.all([
          rpcJson('getTokenAccountsByOwner', [poolAddr, { programId: TOKEN_PROGRAM }, { encoding: 'jsonParsed', commitment: 'processed' }]),
          rpcJson('getTokenAccountsByOwner', [poolAddr, { programId: TOKEN_2022 }, { encoding: 'jsonParsed', commitment: 'processed' }]),
        ]);
        const list = [...(t1?.value || []), ...(t2?.value || [])];
        for (const item of list.slice(0, 2)) {
          const pk = item.pubkey;
          if (pk && !vaults.has(pk)) vaults.set(pk, { addr: pk, pool: poolAddr, dex, kind: 'vault' });
        }
      } catch { /* ignore individual pool failure */ }
    }),
  );

  // Verify accounts exist (non-null) before entering high-frequency ring
  const cand = [...vaults.values()];
  const verified = [];
  for (let i = 0; i < cand.length; i += 50) {
    const chunk = cand.slice(i, i + 50);
    const res = await rpcJson('getMultipleAccounts', [
      chunk.map((x) => x.addr),
      { encoding: 'base64', commitment: 'processed', dataSlice: { offset: 0, length: 16 } },
    ]).catch(() => null);
    const vals = res?.value || [];
    for (let k = 0; k < chunk.length; k++) {
      if (vals[k]?.data?.[0]) verified.push(chunk[k]);
    }
  }
  return verified.length ? verified : CORE_VAULTS;
}

async function ghApi(method, apiPath, body) {
  if (!GH_TOKEN) return null;
  const res = await fetch(`https://api.github.com${apiPath}`, {
    method,
    headers: {
      authorization: `Bearer ${GH_TOKEN}`,
      accept: 'application/vnd.github+json',
      'user-agent': 'solana-tick-archiver-hft',
      ...(body ? { 'content-type': 'application/json' } : {}),
    },
    body: body ? JSON.stringify(body) : undefined,
    signal: AbortSignal.timeout(15000),
  });
  if (!res.ok) return { status: res.status, text: await res.text().catch(() => '') };
  if (res.status === 204) return { status: 204 };
  return { status: res.status, data: await res.json().catch(() => null) };
}

async function dispatchNextLane(startRps) {
  const r = await ghApi('POST', `/repos/${REPO}/actions/workflows/hft-stream.yml/dispatches`, {
    ref: 'main',
    inputs: {
      lane: NEXT_LANE,
      target_rps: String(BASE_TARGET_RPS),
      start_rps: String(startRps),
      run_min: String(RUN_MIN),
      workers: String(WORKERS),
    },
  });
  log(`Handoff dispatch -> lane=${NEXT_LANE} start_rps=${startRps} status=${r?.status ?? 'no-token'}`);
  return r?.status === 204;
}

async function countActiveHftRuns() {
  const r = await ghApi('GET', `/repos/${REPO}/actions/workflows/hft-stream.yml/runs?status=in_progress&per_page=10`);
  return r?.data?.workflow_runs?.length || 0;
}

const tStart = Date.now();
const initialTargets = await discoverActiveVaults();
const uniquePools = new Set(initialTargets.map((t) => t.pool)).size;
const dexCounts = {};
for (const t of initialTargets) dexCounts[t.dex] = (dexCounts[t.dex] || 0) + 1;
log(`Starting nonstop HFT DEX stream: target=${BASE_TARGET_RPS} RPS, workers=${WORKERS}, runMin=${RUN_MIN}, cutoff=${STOP_AT_ISO}`);
log(`Verified ${initialTargets.length} live vault accounts across ${uniquePools} pools: ${JSON.stringify(dexCounts)}`);

let currentTotalRps = START_RPS;
let softStartActive = START_RPS < BASE_TARGET_RPS;
let calmWindows = 0;
let totalOk = 0;
let total429 = 0;
let totalErr = 0;
let totalSwaps = 0;
let latestSlot = 0;
let latestSolUsd = 0;
let solBase = 0n;
let solQuote = 0n;

// Aggregate 5s worker reports into 10s coordinator windows
const win = { ok: 0, r429: 0, err: 0, swaps: 0, bytesIn: 0, inFlight: 0, p50s: [], p95s: [] };

const workers = [];
for (let w = 0; w < WORKERS; w++) {
  const slice = initialTargets.filter((_, idx) => idx % WORKERS === w);
  const wTargets = slice.length ? slice : initialTargets;
  const worker = new Worker(WORKER_FILE, {
    workerData: {
      workerId: w,
      rpcHost: parsedUrl.hostname,
      rpcPath: parsedUrl.pathname + parsedUrl.search,
      initialRps: currentTotalRps / WORKERS,
      maxInFlight: 96,
      targets: wTargets,
    },
  });
  worker.on('message', (m) => {
    if (m.type !== 'stats') return;
    win.ok += m.ok;
    win.r429 += m.r429;
    win.err += m.err;
    win.swaps += m.swaps;
    win.bytesIn += m.bytesIn;
    win.inFlight += m.inFlight;
    if (m.p50 > 0) win.p50s.push(m.p50);
    if (m.p95 > 0) win.p95s.push(m.p95);
    if (m.maxSlot > latestSlot) latestSlot = m.maxSlot;
    if (m.solReserve && m.solReserve !== '0') solBase = BigInt(m.solReserve);
    if (m.usdcReserve && m.usdcReserve !== '0') solQuote = BigInt(m.usdcReserve);
    if (solBase > 0n && solQuote > 0n) {
      latestSolUsd = (Number(solQuote) / 1e6) / (Number(solBase) / 1e9);
    }
  });
  worker.on('error', (e) => log(`worker ${w} error:`, e.message));
  workers.push(worker);
}

function broadcastRps(newTotal) {
  currentTotalRps = newTotal;
  const perWorker = newTotal / WORKERS;
  for (const w of workers) w.postMessage({ type: 'set_rps', rps: perWorker });
}

let handoffDispatched = false;
let shuttingDown = false;
let nextActiveSince = 0;
let lastReportAt = Date.now();

async function gracefulShutdown(reason) {
  if (shuttingDown) return;
  shuttingDown = true;
  log(`Shutting down (${reason}): ramping RPS down cleanly...`);
  broadcastRps(Math.floor(currentTotalRps * 0.5));
  await new Promise((r) => setTimeout(r, 4000));
  broadcastRps(0);
  for (const w of workers) w.postMessage({ type: 'stop' });
  const elapsedSec = (Date.now() - tStart) / 1000;
  log(
    `FINAL SUMMARY: elapsed=${(elapsedSec / 60).toFixed(2)}m avg_rps=${(totalOk / Math.max(1, elapsedSec)).toFixed(1)} ` +
    `total_ok=${totalOk} total_429=${total429} total_err=${totalErr} swaps_detected=${totalSwaps} last_slot=${latestSlot}`,
  );
  setTimeout(() => process.exit(0), 2000);
}

// Every 10 seconds: print telemetry, run adaptive 429 guard, check cutoff & overlapping handoff
setInterval(async () => {
  if (shuttingDown) return;
  const now = Date.now();
  const dtSec = Math.max(1, (now - lastReportAt) / 1000);
  lastReportAt = now;

  const rps = win.ok / dtSec;
  totalOk += win.ok;
  total429 += win.r429;
  totalErr += win.err;
  totalSwaps += win.swaps;

  const p50 = win.p50s.length ? Math.round(win.p50s.reduce((a, b) => a + b, 0) / win.p50s.length) : 0;
  const p95 = win.p95s.length ? Math.round(Math.max(...win.p95s)) : 0;
  const kbSec = (win.bytesIn / 1024 / dtSec).toFixed(1);
  const elapsedMin = ((now - tStart) / 60000).toFixed(1);

  log(
    `rps=${rps.toFixed(1)} target=${currentTotalRps} ok=${win.ok} r429=${win.r429} err=${win.err} ` +
    `p50=${p50}ms p95=${p95}ms swaps=${win.swaps} sol=${latestSolUsd ? latestSolUsd.toFixed(2) : '-'} ` +
    `slot=${latestSlot} bw=${kbSec}KB/s t=${elapsedMin}m`,
  );

  // Soft-start ramp: if launched as overlapping handoff lane at 222 RPS, wait until old lane exits
  // before ramping up to BASE_TARGET_RPS so combined traffic NEVER crosses 499 RPS.
  if (softStartActive && !handoffDispatched) {
    const active = await countActiveHftRuns().catch(() => 1);
    if (active <= 1) {
      softStartActive = false;
      log(`Solo lane confirmed (active=${active}); ramping ${currentTotalRps} -> ${BASE_TARGET_RPS} RPS`);
      broadcastRps(BASE_TARGET_RPS);
    }
  } else if (win.r429 > 3) {
    // Adaptive rate control: stay >=412 RPS while hard-capped <=452 RPS (48+ RPS buffer below 500 ceiling)
    calmWindows = 0;
    const stepped = Math.max(MIN_FLOOR_RPS, currentTotalRps - 12);
    if (stepped !== currentTotalRps) {
      log(`Adaptive backoff: ${currentTotalRps} -> ${stepped} RPS (r429=${win.r429})`);
      broadcastRps(stepped);
    }
  } else if (win.r429 === 0 && !handoffDispatched) {
    calmWindows++;
    if (calmWindows >= 2 && currentTotalRps < BASE_TARGET_RPS) {
      const raised = Math.min(BASE_TARGET_RPS, currentTotalRps + 8);
      log(`Adaptive recovery: ${currentTotalRps} -> ${raised} RPS`);
      broadcastRps(raised);
    } else if (calmWindows >= 3 && rps < 408 && currentTotalRps < MAX_CEIL_RPS) {
      const nudged = Math.min(MAX_CEIL_RPS, currentTotalRps + 8);
      log(`Floor boost: observed ${rps.toFixed(1)} RPS -> target ${nudged} RPS`);
      broadcastRps(nudged);
    }
  }

  win.ok = 0;
  win.r429 = 0;
  win.err = 0;
  win.swaps = 0;
  win.bytesIn = 0;
  win.inFlight = 0;
  win.p50s = [];
  win.p95s = [];

  // Hard cutoff guard before RPC Fast Aperture expires (16 Dec 18:02 UTC+8)
  if (Number.isFinite(STOP_AT_MS) && now >= STOP_AT_MS) {
    await gracefulShutdown(`cutoff ${STOP_AT_ISO}`);
    return;
  }

  const runElapsedSec = (now - tStart) / 1000;
  const maxRunSec = RUN_MIN * 60;

  // Coordinated zero-spike handoff: drop current lane to half (222 RPS) BEFORE dispatching next lane at 222 RPS
  // so 222 + 222 = 444 <= 499 RPS (zero 500+ spikes, zero seconds below 400 RPS).
  if (ENABLE_CHAIN && !handoffDispatched && runElapsedSec >= maxRunSec - HANDOFF_LEAD_SEC) {
    if (!Number.isFinite(STOP_AT_MS) || now + 300_000 < STOP_AT_MS) {
      handoffDispatched = true;
      const halfRps = Math.floor(BASE_TARGET_RPS / 2);
      log(`Preparing zero-spike handoff: halving lane=${LANE} to ${halfRps} RPS before starting lane=${NEXT_LANE}`);
      broadcastRps(halfRps);
      await dispatchNextLane(halfRps).catch((e) => log('dispatch error:', e.message));
    }
  }

  // Once opposite lane is confirmed in_progress for 15s (both running 222 + 222 = 444 RPS), exit cleanly
  if (handoffDispatched) {
    const active = await countActiveHftRuns().catch(() => 1);
    if (active >= 2) {
      if (!nextActiveSince) {
        nextActiveSince = now;
        log(`Next lane=${NEXT_LANE} active alongside lane=${LANE} (combined <=${BASE_TARGET_RPS} RPS); exiting in 12s`);
      } else if (now - nextActiveSince >= 12000) {
        await gracefulShutdown(`handoff complete to lane=${NEXT_LANE}`);
        return;
      }
    }
  }

  if (runElapsedSec >= maxRunSec) {
    await gracefulShutdown(`RUN_MIN=${RUN_MIN} reached`);
  }
}, 10000);

// Refresh hottest trending Solana DEX pools every 15 minutes without pausing the stream
setInterval(async () => {
  if (shuttingDown) return;
  try {
    const fresh = await discoverActiveVaults();
    if (fresh.length >= CORE_VAULTS.length) {
      for (let w = 0; w < WORKERS; w++) {
        const slice = fresh.filter((_, idx) => idx % WORKERS === w);
        workers[w].postMessage({ type: 'set_targets', targets: slice.length ? slice : fresh });
      }
      log(`Refreshed active DEX target set: ${fresh.length} verified vaults`);
    }
  } catch (e) {
    log('target refresh skipped:', e.message);
  }
}, 15 * 60 * 1000);

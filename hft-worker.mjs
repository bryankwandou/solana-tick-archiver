// Worker thread for sustained, paced high-frequency Solana DEX vault polling
// over persistent HTTP/1.1 Keep-Alive connections (zero external dependencies).
import https from 'node:https';
import { parentPort, workerData } from 'node:worker_threads';

const { workerId, rpcHost, rpcPath, initialRps, maxInFlight } = workerData;

const agent = new https.Agent({
  keepAlive: true,
  keepAliveMsecs: 30000,
  maxSockets: Math.max(64, maxInFlight),
  maxFreeSockets: Math.max(32, Math.floor(maxInFlight / 2)),
  scheduling: 'fifo',
});

let targets = workerData.targets || [];
let targetRps = Number(initialRps) || 112;
let stopped = false;
let inFlight = 0;
let cursor = 0;

// Pre-serialized JSON-RPC bodies per target to minimize CPU/GC overhead at 450+ RPS
let payloads = [];
function rebuildPayloads(list) {
  targets = list;
  payloads = list.map((t, i) => {
    const slice = t.kind === 'pump_curve'
      ? { offset: 8, length: 16 }
      : { offset: 64, length: 8 };
    const body = JSON.stringify({
      jsonrpc: '2.0',
      id: i + 1,
      method: 'getAccountInfo',
      params: [t.addr, { encoding: 'base64', commitment: 'processed', dataSlice: slice }],
    });
    return {
      addr: t.addr,
      pool: t.pool,
      dex: t.dex,
      kind: t.kind,
      body,
      byteLen: Buffer.byteLength(body),
    };
  });
}
rebuildPayloads(targets);

const lastVal = new Map();
let ok = 0;
let r429 = 0;
let err = 0;
let swaps = 0;
let maxSlot = 0;
let bytesIn = 0;
let latencies = [];
let solReserve = 0n;
let usdcReserve = 0n;

function fireOne() {
  if (!payloads.length || stopped || inFlight >= maxInFlight) return;
  const item = payloads[cursor++ % payloads.length];
  inFlight++;
  const t0 = performance.now();

  const req = https.request(
    {
      hostname: rpcHost,
      port: 443,
      path: rpcPath,
      method: 'POST',
      agent,
      headers: {
        'content-type': 'application/json',
        'content-length': item.byteLen,
      },
      timeout: 8000,
    },
    (res) => {
      let data = '';
      res.setEncoding('utf8');
      res.on('data', (chunk) => {
        data += chunk;
      });
      res.on('end', () => {
        inFlight--;
        const ms = performance.now() - t0;
        bytesIn += data.length;
        if (res.statusCode === 429) {
          r429++;
          return;
        }
        if (res.statusCode !== 200) {
          err++;
          return;
        }
        ok++;
        if (latencies.length < 4000) latencies.push(ms);
        try {
          const parsed = JSON.parse(data);
          if (parsed.error) {
            err++;
            ok--;
            return;
          }
          const slot = parsed.result?.context?.slot;
          if (slot && slot > maxSlot) maxSlot = slot;
          const b64 = parsed.result?.value?.data?.[0];
          if (b64) {
            const buf = Buffer.from(b64, 'base64');
            if (buf.length >= 8) {
              const amount = buf.readBigUInt64LE(0);
              const prev = lastVal.get(item.addr);
              if (prev !== undefined && prev !== amount) {
                swaps++;
              }
              lastVal.set(item.addr, amount);
              if (item.kind === 'sol_ref_base') solReserve = amount;
              else if (item.kind === 'sol_ref_quote') usdcReserve = amount;
            }
          }
        } catch {
          err++;
          ok--;
        }
      });
    },
  );

  req.on('error', () => {
    inFlight--;
    err++;
  });
  req.on('timeout', () => {
    req.destroy();
  });
  req.write(item.body);
  req.end();
}

// Strict phase-staggered per-second token-bucket pacer (spreads 4 workers cleanly to prevent micro-burst 429s)
const phaseOffsetMs = workerId * 250;
let tokens = 0;
let lastTick = performance.now();
let secWindowStart = Math.floor((lastTick + phaseOffsetMs) / 1000);
let secDispatched = 0;
const pacer = setInterval(() => {
  if (stopped) return;
  const now = performance.now();
  const curSec = Math.floor((now + phaseOffsetMs) / 1000);
  if (curSec !== secWindowStart) {
    secWindowStart = curSec;
    secDispatched = 0;
  }
  const hardSecCap = Math.floor(targetRps);
  if (hardSecCap <= 0 || secDispatched >= hardSecCap) {
    lastTick = now;
    return;
  }
  const dt = Math.min(40, now - lastTick);
  lastTick = now;
  tokens = Math.min(1.0, tokens + (targetRps * dt) / 1000);
  while (tokens >= 1 && inFlight < maxInFlight && secDispatched < hardSecCap) {
    tokens -= 1;
    secDispatched++;
    fireOne();
  }
}, 5);

// Flush telemetry snapshot to coordinator every 1s for smooth 10s RPS aggregation
const reporter = setInterval(() => {
  const lats = latencies;
  latencies = [];
  let p50 = 0;
  let p95 = 0;
  if (lats.length) {
    lats.sort((a, b) => a - b);
    p50 = lats[Math.floor(lats.length * 0.5)] || 0;
    p95 = lats[Math.floor(lats.length * 0.95)] || 0;
  }
  parentPort.postMessage({
    type: 'stats',
    workerId,
    ok,
    r429,
    err,
    swaps,
    maxSlot,
    bytesIn,
    inFlight,
    p50,
    p95,
    solReserve: solReserve.toString(),
    usdcReserve: usdcReserve.toString(),
  });
  ok = 0;
  r429 = 0;
  err = 0;
  swaps = 0;
  bytesIn = 0;
}, 1000);

parentPort.on('message', (msg) => {
  if (msg.type === 'set_rps') {
    targetRps = Math.max(0, Number(msg.rps) || 0);
  } else if (msg.type === 'set_targets' && Array.isArray(msg.targets) && msg.targets.length) {
    rebuildPayloads(msg.targets);
  } else if (msg.type === 'stop') {
    stopped = true;
    clearInterval(pacer);
    clearInterval(reporter);
    setTimeout(() => {
      agent.destroy();
      process.exit(0);
    }, 1500);
  }
});

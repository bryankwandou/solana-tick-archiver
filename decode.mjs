// Swap decoder + 1-minute USD candles per token.
// Decodes the "Program data:" events of: Pump.fun TradeEvent, PumpSwap Buy/SellEvent,
// Raydium CLMM SwapEvent, Raydium CPMM SwapEvent, Orca Whirlpool Traded, Raydium LaunchLab TradeEvent.
// decodeTx() covers what logs alone cannot (needs a full transaction from getBlock/getTransaction):
//   Meteora DLMM Swap, DBC EvtSwap, DAMM v2 EvtSwap2 (all emit_cpi inner instructions),
//   Raydium AMM v4 ray_log (pool = instruction account 1), Meteora DAMM v1 Swap (direction from vault deltas).
// Moonshot TradeEvent (154 bytes) and Boop TokenSoldEvent (128) are decoded from logs too. Boop buys
// (TokenBoughtEvent) are skipped: no buy tx in the RPC history on 2026-11-18 to check the layout against.
// Pool → mints and mint → decimals are looked up once via RPC (getMultipleAccounts)
// and cached on disk in registry.json.
import crypto from 'crypto'; import fs from 'fs';

const ev = (n) => crypto.createHash('sha256').update('event:' + n).digest().subarray(0, 8).toString('hex');
const D_BOOP_SOLD = ev('TokenSoldEvent'), D_TRADE = ev('TradeEvent'), D_BUY = ev('BuyEvent'), D_SELL = ev('SellEvent'), D_SWAP = ev('SwapEvent'), D_TRADED = ev('Traded');

const B58 = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz';
export function b58(buf) { let n = 0n; for (const x of buf) n = n * 256n + BigInt(x); let s = ''; while (n > 0n) { s = B58[Number(n % 58n)] + s; n /= 58n; }
  for (const x of buf) { if (x === 0) s = '1' + s; else break; } return s; }
const pk = (b, o) => b58(b.subarray(o, o + 32));
const u64 = (b, o) => Number(b.readBigUInt64LE(o));

const TOKEN_PROGS = new Set(['TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA', 'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb']);
export const WSOL = 'So11111111111111111111111111111111111111112';
const USD = new Set(['EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB']);
export const PROG = {
  dlmm: 'LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo', dbc: 'dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN',
  damm2: 'cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG', damm1: 'Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB',
  launchlab: 'LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj', ammv4: '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8',
  moonshot: 'MoonCVVNZFSYkqNXP6bxHLPL6QQJiMagDL3qcqUQTrG', boop: 'boop8hVGQGqehUK2iVEMEnMrL5RbjywRzHKBmBE7ry4',
};
const OWNERS = { clmm: 'CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK', orca: 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc', pumpswap: 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA',
  dlmm: PROG.dlmm, damm2: PROG.damm2, launchlab: PROG.launchlab, ammv4: PROG.ammv4, dbc: PROG.dbc, moonshot: PROG.moonshot };
// pool layouts: offsets of the two mints inside the pool account (checked against live pool accounts)
// dbc: [base_mint, config]; the quote mint is read from the config account (offset 8), see resolve()
const POOL_MINTS = { clmm: [73, 105], orca: [101, 181], pumpswap: [43, 75], dlmm: [88, 120], damm2: [168, 200], launchlab: [205, 237], ammv4: [400, 432], dbc: [136, 72], moonshot: [24, -1] }; // moonshot: curve.mint, quote = WSOL (-1)
const DBC_CONFIG_QUOTE = 8;

// Returns a raw trade { pool?, kind, mintA?, mintB?, amtA, amtB } or null.
// amtA/amtB are raw token amounts of the two sides (A/B as stored in the pool).
export function decodeEvent(buf) {
  if (buf.length < 16) return null;
  const d = buf.subarray(0, 8).toString('hex');
  try {
    if (d === D_TRADE && buf.length === 147) { // raydium launchlab (bonk.fun) TradeEvent: spot price from the curve after the trade
      const vb = u64(buf, 48), vq = u64(buf, 56), rb = u64(buf, 80), rq = u64(buf, 88);
      const base = vb - rb, quote = vq + rq; if (!(base > 0 && quote > 0)) return null;
      return { kind: 'launchlab', pool: pk(buf, 8), amtA: 1, amtB: quote / base };
    }
    if (d === D_TRADE && buf.length === 154) { // moonshot: amount(8) tokens, collateral_amount(16) lamports, curve(48), cost_token(80), type(144)
      const tok = u64(buf, 8), col = u64(buf, 16); if (!tok || col < 100000 || pk(buf, 80) !== WSOL) return null; // dust trades round the price
      return { kind: 'moonshot', pool: pk(buf, 48), amtA: tok, amtB: col };
    }
    if (d === D_BOOP_SOLD && buf.length === 128) { // boop TokenSoldEvent: mint(8), amount_in(40) tokens, amount_out(48) lamports, swap_fee(56)
      const tok = u64(buf, 40), sol = u64(buf, 48); if (!tok || sol < 100000) return null;
      return { kind: 'boop', mintA: pk(buf, 8), mintB: WSOL, amtA: tok, amtB: sol };
    }
    if (d === D_TRADE && buf.length >= 97) { // pump.fun bonding curve: base = mint (6 dec), quote = SOL
      const tok = u64(buf, 48), sol = u64(buf, 40); if (!tok || !sol) return null;
      return { kind: 'pumpfun', mintA: pk(buf, 8), mintB: WSOL, amtA: tok, amtB: sol };
    }
    if ((d === D_BUY || d === D_SELL) && buf.length >= 152) { // pumpswap: base/quote from pool
      const base = u64(buf, 16), quote = u64(buf, 64); if (!base || !quote) return null;
      return { kind: 'pumpswap', pool: pk(buf, 120), amtA: base, amtB: quote };
    }
    if (d === D_SWAP && buf.length === 170) { // raydium cpmm (mints inside the event)
      const inA = u64(buf, 56), outA = u64(buf, 64); if (!inA || !outA) return null;
      return { kind: 'cpmm', mintA: pk(buf, 89), mintB: pk(buf, 121), amtA: inA, amtB: outA };
    }
    if (d === D_SWAP && (buf.length === 205 || buf.length === 221)) { // raydium clmm
      const a0 = u64(buf, 136), a1 = u64(buf, 152); if (!a0 || !a1) return null;
      return { kind: 'clmm', pool: pk(buf, 8), amtA: a0, amtB: a1 };
    }
    if (d === D_TRADED && buf.length === 121) { // orca whirlpool
      const aToB = buf[40] === 1, inp = u64(buf, 73), out = u64(buf, 81); if (!inp || !out) return null;
      return { kind: 'orca', pool: pk(buf, 8), amtA: aToB ? inp : out, amtB: aToB ? out : inp };
    }
  } catch { return null; }
  return null;
}

// ---------- full-transaction path (programs whose swap data is not in "Program data:" logs) ----------
// Meteora DLMM / DBC / DAMM v2 emit their swap events with emit_cpi (a self-CPI whose data is
// 0xe445a52e51cb9a1d + event), which logsSubscribe never shows. Raydium AMM v4 writes "ray_log"
// (amounts, no pool) and Meteora DAMM v1 a Swap event without pool or direction; both need the
// instruction's accounts. So these are decoded from a full transaction (getBlock / getTransaction).
const EMIT_CPI = 'e445a52e51cb9a1d';
const D_DLMM_SWAP = ev('Swap'), D_DAMM1_SWAP = D_DLMM_SWAP, D_DBC_SWAP = ev('EvtSwap'), D_DAMM2_SWAP2 = ev('EvtSwap2');
const B58IDX = Object.fromEntries([...B58].map((c, i) => [c, BigInt(i)]));
function d58(s) { let n = 0n; for (const c of s) n = n * 58n + B58IDX[c]; const a = []; while (n > 0n) { a.push(Number(n & 255n)); n >>= 8n; }
  for (const c of s) { if (c === '1') a.push(0); else break; } return Buffer.from(a.reverse()); }
const Q64 = 2 ** 64;
const sqrtPx = (b, o) => { const s = Number(b.readBigUInt64LE(o)) + Number(b.readBigUInt64LE(o + 8)) * Q64; const r = s / Q64; return r * r; };
const INVOKE = /^Program ([1-9A-HJ-NP-Za-km-z]{32,44}) invoke \[/, DONE = /^Program [1-9A-HJ-NP-Za-km-z]{32,44} (success|failed)/;
export const TX_PROGS = new Set([PROG.dlmm, PROG.dbc, PROG.damm2, PROG.damm1, PROG.ammv4]);
// tx = { transaction: { message: { accountKeys, instructions } }, meta } as returned by getTransaction / getBlock (encoding json)
export function decodeTx(tx) {
  const meta = tx.meta; if (!meta || meta.err) return [];
  const msg = tx.transaction.message;
  const keys = [...msg.accountKeys, ...(meta.loadedAddresses?.writable || []), ...(meta.loadedAddresses?.readonly || [])];
  if (!keys.some((k) => TX_PROGS.has(k))) return [];
  // instructions in execution order (top level, then its inner instructions)
  const inner = new Map((meta.innerInstructions || []).map((x) => [x.index, x.instructions]));
  const flat = [];
  msg.instructions.forEach((ix, i) => { flat.push({ ix, logs: [] }); for (const x of inner.get(i) || []) flat.push({ ix: x, logs: [] }); });
  // attach "Program data:" / ray_log lines to the instruction that printed them (invoke lines follow execution order)
  let k = 0; const stack = [];
  for (const l of meta.logMessages || []) {
    const inv = INVOKE.exec(l); if (inv) { const id = inv[1];
      while (k < flat.length && keys[flat[k].ix.programIdIndex] !== id) k++; stack.push(k < flat.length ? k : -1); k++; }
    else if (DONE.test(l)) stack.pop();
    else if (stack.length && stack[stack.length - 1] >= 0 && (l.startsWith('Program data: ') || l.startsWith('Program log: ray_log: '))) flat[stack[stack.length - 1]].logs.push(l);
  }
  const bal = new Map(); // account index → { mint, pre, post }
  for (const b of meta.preTokenBalances || []) bal.set(b.accountIndex, { mint: b.mint, pre: BigInt(b.uiTokenAmount.amount), post: 0n });
  for (const b of meta.postTokenBalances || []) { const x = bal.get(b.accountIndex) || { mint: b.mint, pre: 0n, post: 0n }; x.post = BigInt(b.uiTokenAmount.amount); bal.set(b.accountIndex, x); }
  const out = [];
  for (const { ix, logs } of flat) {
    const prog = keys[ix.programIdIndex]; if (!TX_PROGS.has(prog)) continue;
    try {
      if (prog === PROG.ammv4) { // ray_log SwapBaseIn (3) / SwapBaseOut (4): [type u8, 6×u64, out|in u64]; direction 1 = pc→coin, 2 = coin→pc
        const rl = logs.find((l) => l.startsWith('Program log: ray_log: ')); if (!rl) continue;
        const b = Buffer.from(rl.slice(22), 'base64'); if (b.length < 57 || (b[0] !== 3 && b[0] !== 4)) continue;
        const dir = u64(b, 17); const inAmt = b[0] === 3 ? u64(b, 1) : u64(b, 49), outAmt = b[0] === 3 ? u64(b, 49) : u64(b, 9);
        if (!inAmt || !outAmt || (dir !== 1 && dir !== 2)) continue;
        const net = inAmt * 0.9975; // 0.25 % trade fee is taken from the input
        out.push({ kind: 'ammv4', pool: keys[ix.accounts[1]], amtA: dir === 2 ? net : outAmt, amtB: dir === 2 ? outAmt : net });
        continue;
      }
      if (prog === PROG.damm1) { // Swap { in_amount, out_amount, trade_fee, protocol_fee, host_fee }; direction from the pool's token-vault deltas
        const sw = logs.map((l) => Buffer.from(l.slice(14), 'base64')).find((b) => b.length === 48 && b.subarray(0, 8).toString('hex') === D_DAMM1_SWAP); if (!sw) continue;
        const va = bal.get(ix.accounts[5]), vb = bal.get(ix.accounts[6]); if (!va || !vb) continue;
        const inAmt = u64(sw, 8) - u64(sw, 24), outAmt = u64(sw, 16); if (!(inAmt > 0 && outAmt > 0)) continue;
        const aIn = va.post > va.pre, bIn = vb.post > vb.pre; if (aIn === bIn) continue;
        out.push({ kind: 'damm1', mintA: va.mint, mintB: vb.mint, amtA: aIn ? inAmt : outAmt, amtB: aIn ? outAmt : inAmt });
        continue;
      }
      const b = d58(ix.data); if (b.length < 24 || b.subarray(0, 8).toString('hex') !== EMIT_CPI) continue;
      const e = b.subarray(8), d = e.subarray(0, 8).toString('hex');
      if (prog === PROG.dlmm && d === D_DLMM_SWAP && e.length === 137) { // lb_pair, from, bins, amount_in(80), amount_out(88), swap_for_y(96), fee(97)
        const inAmt = u64(e, 80) - u64(e, 97), outAmt = u64(e, 88), xIn = e[96] === 1; if (!(inAmt > 0 && outAmt > 0)) continue;
        out.push({ kind: 'dlmm', pool: pk(e, 8), amtA: xIn ? inAmt : outAmt, amtB: xIn ? outAmt : inAmt });
      } else if (prog === PROG.dbc && d === D_DBC_SWAP && e.length === 162) { // pool(8), config(40), ..., next_sqrt_price u128 (106): sqrt(quote/base) Q64.64
        const px = sqrtPx(e, 106); if (!(px > 0)) continue;
        out.push({ kind: 'dbc', pool: pk(e, 8), amtA: 1, amtB: px });
      } else if (prog === PROG.damm2 && d === D_DAMM2_SWAP2 && e.length === 188) { // pool(8), ..., next_sqrt_price u128 (92): sqrt(B/A) Q64.64
        const px = sqrtPx(e, 92); if (!(px > 0)) continue;
        out.push({ kind: 'damm2', pool: pk(e, 8), amtA: 1, amtB: px });
      }
    } catch { /* malformed: skip */ }
  }
  return out;
}

// ---------- registry (pool → mints, mint → decimals) ----------
export class Registry {
  constructor(file, rpcUrl) { this.file = file; this.rpc = rpcUrl;
    try { const j = JSON.parse(fs.readFileSync(file, 'utf8')); this.pools = j.pools || {}; this.dec = j.dec || {}; } catch { this.pools = {}; this.dec = {}; }
    this.dec[WSOL] = 9; for (const m of USD) this.dec[m] = 6; this.dirty = false; }
  save() { if (!this.dirty) return; fs.writeFileSync(this.file + '.tmp', JSON.stringify({ pools: this.pools, dec: this.dec })); fs.renameSync(this.file + '.tmp', this.file); this.dirty = false; }
  async accounts(keys) {
    const out = {};
    for (let i = 0; i < keys.length; i += 100) {
      const chunk = keys.slice(i, i + 100);
      const r = await fetch(this.rpc, { method: 'POST', headers: { 'content-type': 'application/json' }, signal: AbortSignal.timeout(15000),
        body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'getMultipleAccounts', params: [chunk, { encoding: 'base64', commitment: 'confirmed' }] }) });
      const j = await r.json(); if (!j.result) throw new Error(j.error?.message || 'rpc');
      j.result.value.forEach((a, k) => { out[chunk[k]] = a ? { owner: a.owner, data: Buffer.from(a.data[0], 'base64') } : null; });
    }
    return out;
  }
  // resolve whatever pools/mints these trades need; unknown → null entries (never retried more than once a run)
  async resolve(trades) {
    const needPools = [...new Set(trades.filter((t) => t.pool && !(t.pool in this.pools)).map((t) => t.pool))];
    if (needPools.length) {
      const acc = await this.accounts(needPools);
      for (const p of needPools) {
        const a = acc[p]; const kind = trades.find((t) => t.pool === p).kind; const offs = POOL_MINTS[kind];
        this.pools[p] = a && a.owner === OWNERS[kind] && a.data.length >= Math.max(...offs) + 32 ? [pk(a.data, offs[0]), offs[1] < 0 ? WSOL : pk(a.data, offs[1])] : null;
        if (kind === 'dbc' && this.pools[p]) this.pools[p] = ['cfg', ...this.pools[p]]; // [marker, base_mint, config] until the config is read
      }
      this.dirty = true;
    }
    // dbc: second hop, config account → quote mint
    const cfgPools = Object.entries(this.pools).filter(([, v]) => v && v[0] === 'cfg');
    if (cfgPools.length) {
      const acc = await this.accounts([...new Set(cfgPools.map(([, v]) => v[2]))]);
      for (const [p, v] of cfgPools) { const a = acc[v[2]];
        this.pools[p] = a && a.owner === OWNERS.dbc && a.data.length >= DBC_CONFIG_QUOTE + 32 ? [v[1], pk(a.data, DBC_CONFIG_QUOTE)] : null; }
      this.dirty = true;
    }
    const mints = new Set();
    for (const t of trades) { const m = this.mintsOf(t); if (m) for (const x of m) if (!(x in this.dec)) mints.add(x); }
    if (mints.size) {
      const acc = await this.accounts([...mints]);
      for (const m of mints) { const a = acc[m]; this.dec[m] = a && TOKEN_PROGS.has(a.owner) && a.data.length >= 45 ? a.data[44] : null; }
      this.dirty = true;
    }
  }
  mintsOf(t) { if (t.mintA) return [t.mintA, t.mintB]; const v = this.pools[t.pool]; return v && v[0] !== 'cfg' ? v : null; }
}

// ---------- candles ----------
// Turn a raw trade into { mint, priceUsd, volUsd } using the latest SOL/USD seen in SOL↔USDC/USDT trades.
export class Candles {
  constructor(bucketMs = 60000) { this.bucketMs = bucketMs; this.ticks = []; this.solUsd = 0; this.map = new Map(); this.skipped = 0; this.byKind = {}; }
  add(reg, t, tsMs) {
    const m = reg.mintsOf(t); if (!m) { this.skipped++; return; }
    let [mA, mB] = m, aA = t.amtA, aB = t.amtB; const dA = reg.dec[mA], dB = reg.dec[mB];
    if (dA == null || dB == null) { this.skipped++; return; }
    let qA = mA === WSOL || USD.has(mA), qB = mB === WSOL || USD.has(mB);
    // orient: base = non-quote side; SOL vs USD → base SOL
    if (qA && !qB) { [mA, mB] = [mB, mA]; [aA, aB] = [aB, aA]; }
    else if (qA && qB) { if (USD.has(mA)) { [mA, mB] = [mB, mA]; [aA, aB] = [aB, aA]; } if (mA !== WSOL) { this.skipped++; return; } }
    else if (!qB) { this.skipped++; return; } // token↔token with no SOL/USD side
    if (t.kind === 'pumpfun' && reg.dec[mA] !== 6) { this.skipped++; return; } // pump.fun mints are always 6 decimals; others reuse the event name
    const base = aA / 10 ** reg.dec[mA], quote = aB / 10 ** reg.dec[mB]; if (!(base > 0 && quote > 0)) return;
    if ((mB === WSOL && quote > 50000) || (USD.has(mB) && quote > 10_000_000)) { this.skipped++; return; } // implausible single trade
    const pxQ = quote / base;
    if (mA === WSOL && USD.has(mB)) { if (pxQ > 1 && pxQ < 100000) this.solUsd = pxQ; }
    const usdPerQuote = USD.has(mB) ? 1 : this.solUsd; if (!usdPerQuote) { this.skipped++; return; }
    const px = pxQ * usdPerQuote, vol = quote * usdPerQuote; if (!isFinite(px) || px <= 0) return;
    this.ticks.push({ mint: mA, ts: tsMs, p: px, k: t.kind }); this.byKind[t.kind] = (this.byKind[t.kind] || 0) + 1; // k = source, for logs only (not stored)
    const minute = Math.floor(tsMs / this.bucketMs), key = mA + '|' + minute;
    const c = this.map.get(key);
    if (!c) this.map.set(key, { mint: mA, minute, o: px, h: px, l: px, c: px, v: vol, n: 1 });
    else { if (px > c.h) c.h = px; if (px < c.l) c.l = px; c.c = px; c.v += vol; c.n++; }
  }
  takeTicks() { const t = this.ticks; this.ticks = []; this.map.clear(); return t; }
  // pop candles whose minute is closed (older than `beforeMinute`)
  take(beforeMinute) { const out = []; for (const [k, c] of this.map) if (c.minute < beforeMinute) { out.push(c); this.map.delete(k); } return out; }
}

// ---------- compact candle file: per chunk = brotli( mint dictionary + rows ) ----------
import zlib from 'zlib';
export function encodeCandles(rows) {
  const mints = [...new Set(rows.map((r) => r.mint))]; const idx = new Map(mints.map((m, i) => [m, i]));
  rows.sort((a, b) => idx.get(a.mint) - idx.get(b.mint) || a.minute - b.minute);
  const parts = []; const vu = (x) => { const a = []; while (x >= 128) { a.push((x % 128) | 128); x = Math.floor(x / 128); } a.push(x); parts.push(Buffer.from(a)); };
  vu(mints.length); for (const m of mints) parts.push(Buffer.from(bs58dec(m)));
  vu(rows.length); let pm = -1, pmin = 0;
  for (const r of rows) {
    const mi = idx.get(r.mint); vu(mi); vu(mi === pm ? r.minute - pmin : r.minute); pm = mi; pmin = r.minute;
    const f = Buffer.alloc(20); f.writeFloatLE(r.o, 0); f.writeFloatLE(r.h, 4); f.writeFloatLE(r.l, 8); f.writeFloatLE(r.c, 12); f.writeFloatLE(r.v, 16); parts.push(f); vu(r.n);
  }
  return zlib.brotliCompressSync(Buffer.concat(parts), { params: { [zlib.constants.BROTLI_PARAM_QUALITY]: 11, [zlib.constants.BROTLI_PARAM_LGWIN]: 24 } });
}
export function decodeCandles(z) {
  const b = zlib.brotliDecompressSync(z); let p = 0;
  const vu = () => { let x = 0, s = 1; for (;;) { const c = b[p++]; x += (c & 127) * s; if (c < 128) return x; s *= 128; } };
  const nm = vu(); const mints = []; for (let i = 0; i < nm; i++) { mints.push(b58(b.subarray(p, p + 32))); p += 32; }
  const n = vu(); const rows = []; let pm = -1, pmin = 0;
  for (let i = 0; i < n; i++) { const mi = vu(); const dm = vu(); const minute = mi === pm ? pmin + dm : dm; pm = mi; pmin = minute;
    const r = { mint: mints[mi], t: minute * 60, o: b.readFloatLE(p), h: b.readFloatLE(p + 4), l: b.readFloatLE(p + 8), c: b.readFloatLE(p + 12), v: b.readFloatLE(p + 16) }; p += 20; r.n = vu(); rows.push(r); }
  return rows;
}
function bs58dec(s) { let n = 0n; for (const c of s) n = n * 58n + BigInt(B58.indexOf(c)); const out = Buffer.alloc(32); for (let i = 31; i >= 0 && n > 0n; i--) { out[i] = Number(n & 255n); n >>= 8n; } return out; }

// ---------- v2 compact format (local backup) ----------
// Mint ids come from an append-only dictionary file (one base58 mint per line, id = line index),
// so chunks never repeat 32-byte addresses. Prices: log-price ticks of 0.01%, delta-coded per mint;
// volume: log1p quantized to 1%. Decoded prices are within ±0.005% of the original.
export class MintDict {
  constructor(file) { this.file = file; this.ids = new Map(); this.list = [];
    try { for (const m of fs.readFileSync(file, 'utf8').split('\n')) if (m) { this.ids.set(m, this.list.length); this.list.push(m); } } catch {} }
  id(m) { let i = this.ids.get(m); if (i === undefined) { i = this.list.length; this.ids.set(m, i); this.list.push(m); fs.appendFileSync(this.file, m + '\n'); } return i; }
}
export function encodeCandlesV2(rows, dict) {
  for (const r of rows) r.id = dict.id(r.mint);
  rows.sort((a, b) => a.id - b.id || a.minute - b.minute);
  const out = []; const vu = (x) => { while (x >= 128) { out.push((x % 128) | 128); x = Math.floor(x / 128); } out.push(x); };
  const vs = (x) => vu(x >= 0 ? x * 2 : -x * 2 - 1);
  const q = (p) => Math.round(Math.log(p) * 10000), qv = (v) => Math.max(0, Math.round(Math.log1p(v) * 100));
  vu(rows.length); let pid = -1, pt = 0, pc = 0;
  for (const r of rows) {
    const first = r.id !== pid;
    vu(first ? r.id - pid : 0); // 0 = same mint as previous row; else positive id delta (rows sorted by id)
    if (first) { vu(r.minute); pc = 0; } else vu(r.minute - pt);
    const O = q(r.o), H = q(r.h), L = q(r.l), C = q(r.c); const flat = O === H && H === L && L === C;
    vu((flat ? 1 : 0) | (Math.min(r.n, 63) << 1)); if (r.n >= 63) vu(r.n);
    vs(O - pc); if (!flat) { vu(H - Math.max(O, C)); vu(Math.min(O, C) - L); vs(C - O); }
    vu(qv(r.v)); pid = r.id; pt = r.minute; pc = C;
  }
  return zlib.brotliCompressSync(Buffer.from(out), { params: { [zlib.constants.BROTLI_PARAM_QUALITY]: 11, [zlib.constants.BROTLI_PARAM_LGWIN]: 24 } });
}
export function decodeCandlesV2(z, mintList) {
  const b = zlib.brotliDecompressSync(z); let p = 0;
  const vu = () => { let x = 0, s = 1; for (;;) { const c = b[p++]; x += (c & 127) * s; if (c < 128) return x; s *= 128; } };
  const vs = () => { const z = vu(); return z % 2 ? -(z + 1) / 2 : z / 2; };
  const n = vu(); const rows = []; let id = -1, t = 0, pc = 0;
  for (let i = 0; i < n; i++) {
    const d = vu(); if (d) { id += d; t = vu(); pc = 0; } else t += vu();
    const f = vu(); const flat = f & 1; let cnt = f >> 1; if (cnt >= 63) cnt = vu();
    const O = pc + vs(); let H = O, L = O, C = O;
    if (!flat) { const hx = vu(), lx = vu(); C = O + vs(); H = Math.max(O, C) + hx; L = Math.min(O, C) - lx; }
    const v = Math.expm1(vu() / 100); pc = C;
    const P = (k) => Math.exp(k / 10000);
    rows.push({ mint: mintList[id], t, o: P(O), h: P(H), l: P(L), c: P(C), v, n: cnt });
  }
  return rows;
}

// ---------- tick format (price only): per chunk rows sorted by mint id, ts ----------
// id delta, ts delta in ms, log-price delta in 0.01% ticks. ~3-4 bytes per tick after brotli.
export function encodeTicksV2(rows, dict) {
  for (const r of rows) r.id = dict.id(r.mint);
  rows.sort((a, b) => a.id - b.id || a.ts - b.ts);
  const out = []; const vu = (x) => { while (x >= 128) { out.push((x % 128) | 128); x = Math.floor(x / 128); } out.push(x); };
  const vs = (x) => vu(x >= 0 ? x * 2 : -x * 2 - 1);
  vu(rows.length); let pid = -1, pt = 0, pp = 0;
  for (const r of rows) {
    const P = Math.round(Math.log(r.p) * 10000);
    if (r.id !== pid) { vu(r.id - pid); vu(r.ts); vs(P); } else { vu(0); vu(r.ts - pt); vs(P - pp); }
    pid = r.id; pt = r.ts; pp = P;
  }
  return zlib.brotliCompressSync(Buffer.from(out), { params: { [zlib.constants.BROTLI_PARAM_QUALITY]: 11, [zlib.constants.BROTLI_PARAM_LGWIN]: 24 } });
}
export function decodeTicksV2(z, mintList) {
  const b = zlib.brotliDecompressSync(z); let p = 0;
  const vu = () => { let x = 0, s = 1; for (;;) { const c = b[p++]; x += (c & 127) * s; if (c < 128) return x; s *= 128; } };
  const vs = () => { const z = vu(); return z % 2 ? -(z + 1) / 2 : z / 2; };
  const n = vu(); const rows = []; let id = -1, ts = 0, P = 0;
  for (let i = 0; i < n; i++) { const d = vu(); if (d) { id += d; ts = vu(); P = vs(); } else { ts += vu(); P += vs(); }
    rows.push({ mint: mintList[id], ts, p: Math.exp(P / 10000) }); }
  return rows;
}

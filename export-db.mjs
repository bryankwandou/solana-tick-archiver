// Pull every price tick from MotherDuck for [FROM_H hours ago, now] onto the laptop, one file per hour,
// in the same tick v2 format the live fetcher writes (own mint dictionary in local-ticks/from-db/mints.txt).
// Also writes 1-minute candles (all tokens) in candle v2 format, to compare sizes.
import fs from 'fs'; import path from 'path';
import { DuckDBInstance } from '@duckdb/node-api';
import { encodeTicksV2, encodeCandlesV2, MintDict } from './decode.mjs';
const env = process.env;
const OUT = process.env.OUT || "export"; fs.mkdirSync(OUT, { recursive: true });
const db = await (await DuckDBInstance.create(`md:?motherduck_token=${env.MOTHERDUCK_TOKEN}`)).connect();
const H = 3600000, now = Date.now(), from = Math.floor((now - Number(process.env.FROM_H || 20) * H) / H) * H;
const dict = new MintDict(path.join(OUT, 'mints.txt')), cdict = new MintDict(path.join(OUT, 'mints-1m.txt'));
let tTicks = 0, tBytes = 0, cRows = 0, cBytes = 0; const sum = [];
for (let h = from; h < now; h += H) {
  const name = new Date(h).toISOString().slice(0, 13).replace(/[-T]/g, '');
  if (fs.existsSync(path.join(OUT, `ticks-${name}.v2.br`)) && h + H < now - 1800000) { console.log(name, 'exists, skip'); continue; }
  const rows = [];
  for (let s = h; s < h + H; s += 600000) for (let k = 0; ; k++) {
    try { const r = await db.runAndReadAll(`SELECT mint, ts::BIGINT ts, p::DOUBLE p FROM solana_ticks.main.ticks WHERE ts >= ${s} AND ts < ${s + 600000} AND p > 0`);
      for (const x of r.getRowObjects()) rows.push({ mint: x.mint, ts: Number(x.ts), p: x.p }); break; }
    catch (e) { if (k >= 5) throw e; console.log('retry', name, e.message.slice(0, 80)); await new Promise((q) => setTimeout(q, 5000 * (k + 1))); }
  }
  if (!rows.length) { sum.push([name, 0, 0, 0]); continue; }
  const z = encodeTicksV2(rows, dict); fs.writeFileSync(path.join(OUT, `ticks-${name}.v2.br`), z);
  // 1-minute OHLC per token from the same ticks (rows now sorted by mint id, ts)
  const cm = new Map();
  for (const t of rows) { const k = t.mint + ' ' + Math.floor(t.ts / 60000); let c = cm.get(k);
    if (!c) cm.set(k, { mint: t.mint, minute: Math.floor(t.ts / 60000), o: t.p, h: t.p, l: t.p, c: t.p, v: 0, n: 1 });
    else { c.h = Math.max(c.h, t.p); c.l = Math.min(c.l, t.p); c.c = t.p; c.n++; } }
  const crow = [...cm.values()]; const zc = encodeCandlesV2(crow, cdict); fs.writeFileSync(path.join(OUT, `candles1m-${name}.v2.br`), zc);
  tTicks += rows.length; tBytes += z.length; cRows += crow.length; cBytes += zc.length;
  sum.push([name, rows.length, z.length, zc.length]);
  console.log(name, 'ticks', rows.length, 'tick file', (z.length / 1e6).toFixed(2), 'MB', '1m candles', crow.length, (zc.length / 1e6).toFixed(2), 'MB');
}
const dsz = fs.statSync(path.join(OUT, 'mints.txt')).size;
console.log(JSON.stringify({ from: new Date(from).toISOString(), to: new Date(now).toISOString(), ticks: tTicks, tickMB: +(tBytes / 1e6).toFixed(2), mintsTxtMB: +(dsz / 1e6).toFixed(2), candles1m: cRows, candleMB: +(cBytes / 1e6).toFixed(2), tokens: dict.list.length }));
fs.writeFileSync(path.join(OUT, 'summary.csv'), 'hour_utc,ticks,tick_bytes,candle1m_bytes\n' + sum.map((x) => x.join(',')).join('\n'));

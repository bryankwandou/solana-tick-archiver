import { DuckDBInstance } from '@duckdb/node-api';
const db = await (await DuckDBInstance.create(`md:?motherduck_token=${process.env.MOTHERDUCK_TOKEN}`)).connect();
const r = await db.runAndReadAll(`SELECT count(*)::BIGINT n, count(DISTINCT prog)::INT progs, max(ts)::BIGINT lastts FROM solana_ticks.main.dex_events`);
const o = r.getRowObjects()[0]; console.log('rows', String(o.n), 'programs', o.progs, 'last write', Math.round((Date.now()-Number(o.lastts))/1000), 's ago');

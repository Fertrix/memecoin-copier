// GRABADORA (máquina del tiempo continua, gratis): cada lanzamiento de pump.fun + su curva estado a estado durante TRACK_S.
// - Lanzamientos: WebSocket gratuito de PumpPortal (subscribeNewToken).
// - Curva: accountSubscribe en el WebSocket público oficial de Solana → reservas exactas tras cada cambio.
// Salida: data/rec/AAAA-MM-DD.jsonl (una línea por token al terminar de seguirlo). No compra nada.
import fs from 'node:fs';
import path from 'node:path';

const TRACK_S = Number(process.env.TRACK_S ?? 1200);       // seguir cada token 20 min
const IDLE_S = Number(process.env.IDLE_S ?? 300);          // o hasta 5 min sin cambios
const MAX_SUBS = Number(process.env.MAX_SUBS ?? 400);
const DIR = path.join(process.cwd(), 'data', 'rec');
fs.mkdirSync(DIR, { recursive: true });
const log = m => console.log(`[${new Date().toLocaleTimeString('es-ES')}] ${m}`);
// Al reiniciar (actualización), guardar lo que se estaba siguiendo en vez de perderlo
function flushAll(why) { for (const [c, r] of toks) { r.end = why; r.endAt = Date.now(); try { fs.appendFileSync(path.join(DIR, new Date(r.t0).toISOString().slice(0, 10) + '.jsonl'), JSON.stringify(r) + String.fromCharCode(10)); } catch {} } toks.clear(); }
process.on('disconnect', () => { flushAll('reinicio'); process.exit(0); });
process.on('SIGTERM', () => { flushAll('reinicio'); process.exit(0); });
process.on('SIGINT', () => { flushAll('reinicio'); process.exit(0); });

const toks = new Map();         // curva -> registro
const subOf = new Map(), curveOfSub = new Map(), reqCurve = new Map();
let ws = null, ready = false, rid = 1000, stats = { launches: 0, written: 0, skipped: 0, notif: 0 };

function finish(curve, why) {
  const r = toks.get(curve); if (!r) return;
  toks.delete(curve);
  const sid = subOf.get(curve); subOf.delete(curve);
  if (sid != null) { curveOfSub.delete(sid); try { ws.send(JSON.stringify({ jsonrpc: '2.0', id: ++rid, method: 'accountUnsubscribe', params: [sid] })); } catch {} }
  r.end = why; r.endAt = Date.now();
  const f = path.join(DIR, new Date(r.t0).toISOString().slice(0, 10) + '.jsonl');
  fs.appendFile(f, JSON.stringify(r) + '\n', () => {});
  stats.written++;
}
function sub(curve) {
  if (!ready) return;
  const id = ++rid; reqCurve.set(id, curve);
  ws.send(JSON.stringify({ jsonrpc: '2.0', id, method: 'accountSubscribe', params: [curve, { encoding: 'base64', commitment: 'processed' }] }));
}
function connect() {
  ws = new WebSocket(process.env.WS_URL || 'wss://api.mainnet-beta.solana.com'); ready = false;
  ws.onopen = () => { ready = true; subOf.clear(); curveOfSub.clear(); for (const c of toks.keys()) sub(c); log(`grabadora conectada (${toks.size} curvas en seguimiento)`); };
  ws.onmessage = e => {
    let j; try { j = JSON.parse(String(e.data)); } catch { return; }
    if (j.id && reqCurve.has(j.id)) { const c = reqCurve.get(j.id); reqCurve.delete(j.id); if (j.result == null) return; if (toks.has(c)) { subOf.set(c, j.result); curveOfSub.set(j.result, c); } else try { ws.send(JSON.stringify({ jsonrpc: '2.0', id: ++rid, method: 'accountUnsubscribe', params: [j.result] })); } catch {} return; }
    const c = curveOfSub.get(j?.params?.subscription); if (!c) return;
    const r = toks.get(c); const d = j.params.result?.value?.data?.[0]; if (!r || !d) return;
    const b = Buffer.from(d, 'base64'); if (b.length < 49) return;
    stats.notif++;
    r.s.push([Date.now() - r.t0, Number(b.readBigUInt64LE(16)), Number(b.readBigUInt64LE(8)), j.params.result.context?.slot ?? 0]);
    r.last = Date.now();
    if (b[48] === 1) finish(c, 'graduó');
  };
  ws.onclose = () => { ready = false; setTimeout(connect, 2000); };
  ws.onerror = () => { try { ws.close(); } catch {} };
}
connect();
setInterval(() => { try { if (ready) ws.send('{"jsonrpc":"2.0","id":9,"method":"getHealth"}'); } catch {} }, 30000);
const RUNNER_SOL = Number(process.env.RUNNER_SOL ?? 15), RUNNER_S = Number(process.env.RUNNER_S ?? 10800);
const realSol = r => { const x = r.s[r.s.length - 1]; return x ? x[1] / 1e9 - 30 : 0; };
setInterval(() => { const now = Date.now(); for (const [c, r] of toks) {
  const limit = (r.runner || realSol(r) >= RUNNER_SOL) ? RUNNER_S : TRACK_S; if (limit === RUNNER_S) r.runner = true;
  if (now - r.t0 > limit * 1000) finish(c, 'tiempo'); else if (now - r.last > IDLE_S * (r.runner ? 6 : 1) * 1000) finish(c, 'inactivo'); } }, 5000);
setInterval(() => log(`grabadora: ${stats.launches} lanzamientos, ${toks.size} en seguimiento, ${stats.written} guardados, ${stats.skipped} sin hueco, ${stats.notif} cambios de curva`), 10 * 60 * 1000);

function pp() {
  const p = new WebSocket('wss://pumpportal.fun/api/data');
  p.onopen = () => { p.send(JSON.stringify({ method: 'subscribeNewToken' })); log('grabadora: escuchando lanzamientos'); };
  p.onmessage = e => { let m; try { m = JSON.parse(String(e.data)); } catch { return; } onLaunch(m); };
  p.onclose = () => setTimeout(pp, 3000);
  p.onerror = () => { try { p.close(); } catch {} };
}
function onLaunch(m) {
  {
    if (m.txType !== 'create' || !m.bondingCurveKey) return;
    stats.launches++;
    if (toks.size >= MAX_SUBS) { stats.skipped++; return; }
    const r = { mint: m.mint, curve: m.bondingCurveKey, creator: m.traderPublicKey, sig: m.signature, t0: Date.now(), name: m.name, symbol: m.symbol, uri: m.uri,
      devTokens: Number(m.initialBuy) || 0, devSol: Number(m.solAmount) || 0, vSol0: Number(m.vSolInBondingCurve), vTok0: Number(m.vTokensInBondingCurve), mayhem: !!m.is_mayhem_mode, pool: m.pool, s: [], last: Date.now() };
    toks.set(m.bondingCurveKey, r); sub(m.bondingCurveKey);
  }
}
if (process.env.FEED === 'ipc') { process.on('message', msg => { if (msg?.type === 'create') onLaunch(msg.m); }); log('grabadora: lanzamientos compartidos'); }
else pp();

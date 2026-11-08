// GRABADORA (máquina del tiempo continua, gratis): cada lanzamiento de pump.fun + su curva estado a estado durante TRACK_S.
// - Lanzamientos: WebSocket gratuito de PumpPortal (subscribeNewToken), compartido por app.mjs (FEED=ipc).
// - Curva, dos fuentes que se unen por slot:
//   1) accountSubscribe en el WebSocket público oficial (detalle fino, pero a ratos deja de enviar sin cerrar);
//   2) sondeo HTTP getMultipleAccounts (100 curvas por consulta) cada POLL_MS → nunca hay huecos de más de ~POLL_MS.
// Salida: data/rec/AAAA-MM-DD.jsonl (una línea por token al terminar de seguirlo). No compra nada.
// Fila: [msDesdeT0, vSolLamports, vTokRaw, slot, fuente(0=ws,1=sondeo)]
import fs from 'node:fs';
import path from 'node:path';

const TRACK_S = Number(process.env.TRACK_S ?? 1200);       // seguir cada token 20 min
const IDLE_S = Number(process.env.IDLE_S ?? 300);          // o hasta 5 min sin cambios
const MAX_SUBS = Number(process.env.MAX_SUBS ?? 400);
const POLL_MS = Number(process.env.POLL_MS ?? 2500);
const RPC = process.env.POLL_RPC || 'https://api.mainnet-beta.solana.com';
const PUMP = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P';
const DIR = path.join(process.cwd(), 'data', 'rec');
fs.mkdirSync(DIR, { recursive: true });
const NL = String.fromCharCode(10);
const log = m => console.log(`[${new Date().toLocaleTimeString('es-ES')}] ${m}`);
const fileOf = r => path.join(DIR, new Date(r.t0).toISOString().slice(0, 10) + '.jsonl');
// Al reiniciar (actualización), guardar lo que se estaba siguiendo en vez de perderlo
function flushAll(why) { for (const [, r] of toks) { r.end = why; r.endAt = Date.now(); try { fs.appendFileSync(fileOf(r), JSON.stringify(r) + NL); } catch {} } toks.clear(); }
process.on('disconnect', () => { flushAll('reinicio'); process.exit(0); });
process.on('SIGTERM', () => { flushAll('reinicio'); process.exit(0); });
process.on('SIGINT', () => { flushAll('reinicio'); process.exit(0); });

const toks = new Map();         // curva -> registro
const subOf = new Map(), curveOfSub = new Map(), reqCurve = new Map();
let ws = null, ready = false, gen = 0, rid = 1000, lastWsNotif = Date.now();
const stats = { launches: 0, written: 0, skipped: 0, notif: 0, polled: 0, pollRows: 0, pollErr: 0, subErr: 0, wsStall: 0 };

// Añade un estado si es nuevo (por slot) y distinto del último
function addRow(r, vSol, vTok, slot, src) {
  const last = r.s[r.s.length - 1];
  if (last && slot && last[3] && slot < last[3]) return;              // más viejo que lo que ya tenemos
  if (last && last[1] === vSol && last[2] === vTok) return;           // sin cambios
  r.s.push([Date.now() - r.t0, vSol, vTok, slot, src]);
  r.last = Date.now();
}
function finish(curve, why) {
  const r = toks.get(curve); if (!r) return;
  toks.delete(curve);
  const sid = subOf.get(curve); subOf.delete(curve);
  if (sid != null) { curveOfSub.delete(sid); if (r.gen === gen) try { ws.send(JSON.stringify({ jsonrpc: '2.0', id: ++rid, method: 'accountUnsubscribe', params: [sid] })); } catch {} }
  r.end = why; r.endAt = Date.now(); delete r.gen;
  fs.appendFile(fileOf(r), JSON.stringify(r) + NL, () => {});
  stats.written++;
}
function sub(curve) {
  if (!ready) return;
  const id = ++rid; reqCurve.set(id, { curve, gen });
  ws.send(JSON.stringify({ jsonrpc: '2.0', id, method: 'accountSubscribe', params: [curve, { encoding: 'base64', commitment: 'processed' }] }));
}
function connect() {
  const my = ++gen;
  ws = new WebSocket(process.env.WS_URL || 'wss://api.mainnet-beta.solana.com'); ready = false;
  const w = ws;
  w.onopen = () => { if (my !== gen) return; ready = true; lastWsNotif = Date.now(); subOf.clear(); curveOfSub.clear(); reqCurve.clear(); for (const c of toks.keys()) sub(c); log(`grabadora conectada (${toks.size} curvas en seguimiento)`); };
  w.onmessage = e => {
    if (my !== gen) return;
    let j; try { j = JSON.parse(String(e.data)); } catch { return; }
    if (j.id && reqCurve.has(j.id)) { const { curve: c } = reqCurve.get(j.id); reqCurve.delete(j.id); if (j.result == null) { stats.subErr++; const r = toks.get(c); if (r) r.subErr = true; return; } if (toks.has(c)) { subOf.set(c, j.result); curveOfSub.set(j.result, c); toks.get(c).gen = gen; } else try { w.send(JSON.stringify({ jsonrpc: '2.0', id: ++rid, method: 'accountUnsubscribe', params: [j.result] })); } catch {} return; }
    const c = curveOfSub.get(j?.params?.subscription); if (!c) return;
    const r = toks.get(c); const d = j.params.result?.value?.data?.[0]; if (!r || !d) return;
    const b = Buffer.from(d, 'base64'); if (b.length < 49) return;
    stats.notif++; lastWsNotif = Date.now();
    addRow(r, Number(b.readBigUInt64LE(16)), Number(b.readBigUInt64LE(8)), j.params.result.context?.slot ?? 0, 0);
    if (b[48] === 1) finish(c, 'graduó');
  };
  w.onclose = () => { if (my !== gen) return; ready = false; setTimeout(connect, 2000); };
  w.onerror = () => { try { w.close(); } catch {} };
}
connect();
// El WebSocket público a veces se queda mudo sin cerrarse: si hay muchas curvas y ningún aviso en 60 s, reconectar
setInterval(() => { if (ready && toks.size >= 30 && Date.now() - lastWsNotif > 60000) { stats.wsStall++; log('grabadora: WebSocket mudo, reconectando'); const w = ws; gen++; ready = false; try { w.close(); } catch {} setTimeout(connect, 500); } }, 10000);

// Sondeo HTTP: estado exacto de todas las curvas seguidas, 100 por consulta
let polling = false;
async function poll() {
  if (polling || !toks.size) return; polling = true;
  try {
    const keys = [...toks.keys()];
    for (let i = 0; i < keys.length; i += 100) {
      const chunk = keys.slice(i, i + 100);
      let j = null;
      try { const res = await fetch(RPC, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'getMultipleAccounts', params: [chunk, { encoding: 'base64', commitment: 'processed', dataSlice: { offset: 0, length: 49 } }] }), signal: AbortSignal.timeout(8000) }); j = await res.json(); } catch {}
      if (!j?.result) { stats.pollErr++; continue; }
      stats.polled++;
      const slot = j.result.context?.slot ?? 0;
      j.result.value.forEach((a, k) => {
        const c = chunk[k], r = toks.get(c); if (!r || !a) return;
        if (a.owner !== PUMP) { r.badOwner = a.owner; return; }
        const b = Buffer.from(a.data[0], 'base64'); if (b.length < 49) return;
        const n = r.s.length; addRow(r, Number(b.readBigUInt64LE(16)), Number(b.readBigUInt64LE(8)), slot, 1); if (r.s.length > n) stats.pollRows++;
        r.polls = (r.polls || 0) + 1;
        if (b[48] === 1) finish(c, 'graduó');
      });
    }
  } finally { polling = false; }
}
setInterval(poll, POLL_MS);

const RUNNER_SOL = Number(process.env.RUNNER_SOL ?? 15), RUNNER_S = Number(process.env.RUNNER_S ?? 10800);
const DEV_MIN = Number(process.env.DEV_MIN ?? 8), DEV_TRACK_S = Number(process.env.DEV_TRACK_S ?? 3900);
const realSol = r => { const x = r.s[r.s.length - 1]; return x ? x[1] / 1e9 - 30 : 0; };
setInterval(() => { const now = Date.now(); for (const [c, r] of toks) {
  if (r.badOwner) { finish(c, 'no es curva pump'); continue; }
  const dev = r.devTokens / 1e7 >= DEV_MIN; // candidatos de dev fuerte: se siguen al menos DEV_TRACK_S (la salida corredor llega a 60 min)
  const limit = Math.max((r.runner || realSol(r) >= RUNNER_SOL) ? RUNNER_S : TRACK_S, dev ? DEV_TRACK_S : 0); if (realSol(r) >= RUNNER_SOL) r.runner = true;
  if (now - r.t0 > limit * 1000) finish(c, 'tiempo'); else if (now - r.last > IDLE_S * (r.runner || dev ? 6 : 1) * 1000) finish(c, 'inactivo'); } }, 5000);
setInterval(() => log(`grabadora: ${stats.launches} lanzamientos, ${toks.size} en seguimiento, ${stats.written} guardados, ${stats.skipped} sin hueco, ${stats.notif} avisos ws, ${stats.pollRows} cambios por sondeo (${stats.polled} consultas, ${stats.pollErr} fallidas), ${stats.wsStall} ws mudo, ${stats.subErr} suscripciones fallidas`), 10 * 60 * 1000);

function pp() {
  const p = new WebSocket('wss://pumpportal.fun/api/data');
  p.onopen = () => { p.send(JSON.stringify({ method: 'subscribeNewToken' })); log('grabadora: escuchando lanzamientos'); };
  p.onmessage = e => { let m; try { m = JSON.parse(String(e.data)); } catch { return; } onLaunch(m); };
  p.onclose = () => setTimeout(pp, 3000);
  p.onerror = () => { try { p.close(); } catch {} };
}
function onLaunch(m) {
  if (m.txType !== 'create' || !m.bondingCurveKey) return;
  stats.launches++;
  if (toks.size >= MAX_SUBS) { stats.skipped++; return; }
  const r = { v: 2, mint: m.mint, curve: m.bondingCurveKey, creator: m.traderPublicKey, sig: m.signature, t0: Date.now(), name: m.name, symbol: m.symbol, uri: m.uri,
    devTokens: Number(m.initialBuy) || 0, devSol: Number(m.solAmount) || 0, vSol0: Number(m.vSolInBondingCurve), vTok0: Number(m.vTokensInBondingCurve), mayhem: !!m.is_mayhem_mode, pool: m.pool, s: [], last: Date.now() };
  toks.set(m.bondingCurveKey, r); sub(m.bondingCurveKey);
}
if (process.env.FEED === 'ipc') { process.on('message', msg => { if (msg?.type === 'create') onLaunch(msg.m); }); log('grabadora: lanzamientos compartidos'); }
else pp();

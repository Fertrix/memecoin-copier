// BOT "DEV FUERTE + TOKEN TRANQUILO" (PAPER, 0 € reales). Patrón encontrado con la máquina del tiempo (9,7 h de datos exactos):
// el dev compra >= DEV_PCT % del suministro al lanzar y, a los DECIDE_S segundos, casi nadie ha operado (< MAX_TXS txs) → comprar.
// Venta: TP / SL sobre la venta REAL simulada en la curva, o a los HOLD_S segundos.
// Backtest (sin mirar el futuro, 2,5 s de retraso): positivo en las 4 ventanas, +4 % a +17 % por trade, ~3,7 trades/h.
// Gratis: lanzamientos por el WebSocket gratuito de PumpPortal; cuentas y firmas por RPC público.
import fs from 'node:fs';

const START_SOL = Number(process.env.START_SOL ?? 0.09);
const FRACTION = Number(process.env.FRACTION ?? 0.25);
const DEV_PCT = Number(process.env.DEV_PCT ?? 8);
const DECIDE_S = Number(process.env.DECIDE_S ?? 30);
const MAX_TXS = Number(process.env.MAX_TXS ?? 9);
const TP = Number(process.env.TP ?? 1.0), SL = Number(process.env.SL ?? 0.25), HOLD_S = Number(process.env.HOLD_S ?? 100);
const FEE = 0.0125, PRIO = 0.0002;
const STATE = process.env.STATE || 'data/livecopy_dev.json';
const RPCS = (process.env.RPC_URLS || 'https://solana-rpc.publicnode.com,https://api.mainnet-beta.solana.com').split(',');
const WSOL = 'So11111111111111111111111111111111111111112';
const sleep = ms => new Promise(r => setTimeout(r, ms));
const log = m => console.log(`[${new Date().toLocaleTimeString('es-ES')}] ${m}`);
const emit = m => { try { if (process.send) process.send(m); } catch {} };
process.on('disconnect', () => process.exit(0));

const st = fs.existsSync(STATE) ? JSON.parse(fs.readFileSync(STATE, 'utf8')) : { startedAt: Date.now(), balance: START_SOL, open: {}, closed: [], credits: 0, stats: {} };
st.stats ||= {};
const bump = k => st.stats[k] = (st.stats[k] || 0) + 1;
const save = () => fs.writeFileSync(STATE, JSON.stringify(st, null, 1));

let rpcIdx = 0;
async function rpc(method, params) {
  for (let i = 0; i < RPCS.length * 2; i++) {
    const url = RPCS[(rpcIdx + i) % RPCS.length];
    try {
      const r = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method, params }), signal: AbortSignal.timeout(8000) });
      if (r.status === 429 || r.status >= 500) { await sleep(300); continue; }
      const j = await r.json(); if (j.error) { await sleep(200); continue; }
      rpcIdx = (rpcIdx + i) % RPCS.length; return j.result ?? null;
    } catch { await sleep(300); }
  }
  return null;
}
// Estado de la bonding curve: reservas virtuales exactas
async function curve(key) {
  const r = await rpc('getAccountInfo', [key, { encoding: 'base64', commitment: 'processed' }]);
  const d = r?.value?.data?.[0]; if (!d) return null;
  const b = Buffer.from(d, 'base64');
  return { vTok: Number(b.readBigUInt64LE(8)), vSol: Number(b.readBigUInt64LE(16)), complete: b[48] === 1 };
}
async function jupSell(mint, raw) {
  try { const r = await fetch(`https://lite-api.jup.ag/swap/v1/quote?inputMint=${mint}&outputMint=${WSOL}&amount=${raw}&slippageBps=1000`, { signal: AbortSignal.timeout(8000) }); const j = await r.json(); return j?.outAmount ? Number(j.outAmount) / 1e9 : null; } catch { return null; }
}
// Venta simulada sobre la curva, contando con que nuestra compra (de papel) sigue dentro
function sellValue(c, pos) {
  const vSol = c.vSol + pos.shiftLam, vTok = c.vTok - pos.tokens;
  const out = vSol - (vSol * vTok) / (vTok + pos.tokens);
  return Math.max(0, out / 1e9 * (1 - FEE) - PRIO);
}

// ---------- Contador en vivo de transacciones por curva (WebSocket público, gratis) ----------
const watchers = new Map(); // curva -> Set(firmas)
const subIds = new Map(), reqToCurve = new Map(), subToCurve = new Map();
let lws = null, lwsReady = false, reqId = 100;
function lwsConnect() {
  lws = new WebSocket(process.env.WS_URL || 'wss://api.mainnet-beta.solana.com'); lwsReady = false;
  lws.onopen = () => { lwsReady = true; subIds.clear(); subToCurve.clear(); for (const c of watchers.keys()) lwsSub(c); };
  lws.onmessage = e => { try {
    const j = JSON.parse(String(e.data));
    if (j.id && reqToCurve.has(j.id)) { const c = reqToCurve.get(j.id); reqToCurve.delete(j.id); if (watchers.has(c)) { subIds.set(c, j.result); subToCurve.set(j.result, c); } else lws.send(JSON.stringify({ jsonrpc: '2.0', id: ++reqId, method: 'logsUnsubscribe', params: [j.result] })); return; }
    const v = j?.params?.result?.value; const c = subToCurve.get(j?.params?.subscription);
    if (v?.signature && c && watchers.has(c) && !v.err) watchers.get(c).add(v.signature);
  } catch {} };
  lws.onclose = () => { lwsReady = false; setTimeout(lwsConnect, 2000); };
  lws.onerror = () => { try { lws.close(); } catch {} };
}
function lwsSub(c) { if (!lwsReady) return; const id = ++reqId; reqToCurve.set(id, c); lws.send(JSON.stringify({ jsonrpc: '2.0', id, method: 'logsSubscribe', params: [{ mentions: [c] }, { commitment: 'processed' }] })); }
function watchCurve(c) { if (watchers.has(c)) return; watchers.set(c, new Set()); lwsSub(c); }
function unwatchCurve(c) { watchers.delete(c); const sid = subIds.get(c); subIds.delete(c); if (sid != null) { subToCurve.delete(sid); try { lws.send(JSON.stringify({ jsonrpc: '2.0', id: ++reqId, method: 'logsUnsubscribe', params: [sid] })); } catch {} } }
lwsConnect();
setInterval(() => { try { if (lwsReady) lws.send('{"jsonrpc":"2.0","id":9,"method":"getHealth"}'); } catch {} }, 30000);

async function meta(uri) {
  const cid = uri?.match(/ipfs\/([^/?#]+)/)?.[1];
  for (const u of cid ? [`https://ipfs.io/ipfs/${cid}`, `https://dweb.link/ipfs/${cid}`] : uri ? [uri] : []) {
    try { const r = await fetch(u, { signal: AbortSignal.timeout(6000) }); if (r.ok) { const j = await r.json(); return { twitter: j.twitter || null, telegram: j.telegram || null, website: j.website || null, image: j.image || null }; } } catch {}
  }
  return {};
}
// Radar: últimas decisiones (para el panel)
st.recent ||= [];
function radar(x) { st.recent.push({ at: Date.now(), ...x }); if (st.recent.length > 80) st.recent.splice(0, st.recent.length - 80); }

// ---------- Anti-rug: creadores y "huellas" de fábrica (cantidad exacta comprada por el dev) que ya hicieron rug ----------
st.rug ||= { creators: {}, prints: [] };
const printOf = initialBuy => String(Math.round(Number(initialBuy) / 1e5)); // misma cantidad exacta = misma fábrica
function markRug(creator, initialBuy, why) {
  if (creator) st.rug.creators[creator] = Date.now();
  st.rug.prints.push([Date.now(), printOf(initialBuy)]);
  st.rug.prints = st.rug.prints.filter(([t]) => Date.now() - t < 6 * 3600e3).slice(-500);
  log(`anti-rug: apunto ${why} (creador ${creator?.slice(0, 6)}, huella ${printOf(initialBuy)})`);
}
function rugReason(m) {
  if (st.rug.creators[m.traderPublicKey]) return 'creador con rug previo';
  const p = printOf(m.initialBuy), n = st.rug.prints.filter(([t, x]) => x === p && Date.now() - t < 3 * 3600e3).length;
  if (n >= 2) return `fábrica de rugs (misma compra exacta, ${n} rugs en 3 h)`;
  return null;
}

const pending = new Map();
async function onCreate(m) {
  const devPct = (Number(m.initialBuy) || 0) / 1e9 * 100;
  if ((m.pool && m.pool !== 'pump') || m.is_mayhem_mode) return;
  bump('lanzamientos');
  if (devPct < DEV_PCT) return;
  const total = Number(m.vTokensInBondingCurve) + Number(m.initialBuy);
  if (!(total > 1.06e9 && total < 1.08e9)) return; // suministro no estándar (Mayhem, etc.)
  if (pending.has(m.mint) || st.open[m.mint]) return;
  bump('devFuerte');
  const seenAt = Date.now(); pending.set(m.mint, seenAt);
  watchCurve(m.bondingCurveKey);
  log(`lanzamiento ${m.symbol || m.mint.slice(0, 6)}: el dev compró ${devPct.toFixed(1)} % — reviso a los ${DECIDE_S} s`);
  const socialsP = meta(m.uri).catch(() => ({}));
  const base = { mint: m.mint, symbol: m.symbol, name: m.name, devPct: +devPct.toFixed(1), creator: m.traderPublicKey };
  await sleep(Math.max(0, seenAt - 1000 + DECIDE_S * 1000 - Date.now()));
  // ¿Cuántas transacciones ha tenido la curva? (sin contar la de creación)
  // El índice de firmas de los RPC públicos va con retraso, así que se une con lo visto en vivo por WebSocket desde el lanzamiento.
  const sigs = await rpc('getSignaturesForAddress', [m.bondingCurveKey, { limit: 50, commitment: 'confirmed' }]);
  const live = watchers.get(m.bondingCurveKey); unwatchCurve(m.bondingCurveKey);
  pending.delete(m.mint);
  const all = new Set(live || []);
  for (const s of sigs || []) if (!s.err) all.add(s.signature);
  all.delete(m.signature);
  if (!sigs && !live) { bump('sinDatos'); return; }
  const txs = all.size;
  const socials = await socialsP;
  if (txs >= MAX_TXS) { radar({ ...base, ...socials, txs, decision: 'no: mucha actividad' }); bump('demasiadaActividad'); log(`${m.symbol || m.mint.slice(0, 6)}: ${txs} transacciones a los ${DECIDE_S} s (límite < ${MAX_TXS}) — no compro`); save(); return; }
  // ¿El dev sigue teniendo sus tokens? Si ya vendió, el token está "tranquilo" porque está muerto: no comprar.
  // (PublicNode no permite esta consulta: se usa el RPC oficial; si falla, no se bloquea la compra)
  let devAcc = null;
  for (let i = 0; i < 3 && !devAcc; i++) { try { const r = await fetch('https://api.mainnet-beta.solana.com', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'getTokenAccountsByOwner', params: [m.traderPublicKey, { mint: m.mint }, { encoding: 'jsonParsed', commitment: 'processed' }] }), signal: AbortSignal.timeout(6000) }); const j = await r.json(); devAcc = j.result || null; } catch {} if (!devAcc) await sleep(400); }
  const devNow = (devAcc?.value || []).reduce((a, x) => a + Number(x.account?.data?.parsed?.info?.tokenAmount?.uiAmount || 0), 0);
  if (devAcc && devNow < Number(m.initialBuy) * 0.9) { markRug(m.traderPublicKey, m.initialBuy, 'dev vendió en < 30 s'); bump('devVendio'); radar({ ...base, ...socials, txs, decision: 'no: el dev ya vendió' }); log(`${m.symbol || m.mint.slice(0, 6)}: el dev ya vendió (le queda ${(devNow / 1e7).toFixed(1)} % de ${devPct.toFixed(1)} %) — no compro`); save(); return; }
  const rr = rugReason(m);
  if (rr) { bump('antiRug'); radar({ ...base, ...socials, txs, decision: 'no: ' + rr }); log(`${m.symbol || m.mint.slice(0, 6)}: ${rr} — no compro`); save(); return; }
  const c = await curve(m.bondingCurveKey);
  if (!c || c.complete) return;
  const size = Math.min(st.balance * FRACTION, st.balance - 0.003);
  if (size < 0.005) { log('sin saldo libre'); return; }
  const sinLam = Math.floor((size * (1 - FEE) - PRIO) * 1e9);
  const tokens = c.vTok - (c.vSol * c.vTok) / (c.vSol + sinLam);
  st.balance -= size;
  radar({ ...base, ...socials, txs, decision: 'COMPRA' });
  const pos = st.open[m.mint] = { size, tokens, shiftLam: sinLam, curve: m.bondingCurveKey, openedAt: Date.now(), devPct, txs, creator: m.traderPublicKey, initialBuy: m.initialBuy, symbol: m.symbol, name: m.name, ...socials, lagMs: Date.now() - seenAt - DECIDE_S * 1000, peak: 0, path: [] };
  log(`COMPRA ${m.symbol || m.mint.slice(0, 6)} | dev ${devPct.toFixed(1)} % | ${txs} txs a los ${DECIDE_S} s | ${size.toFixed(4)} SOL`);
  emit({ type: 'buy', mint: m.mint, symbol: m.symbol, name: m.name, whaleSol: null, kind: 'dev', route: 'pump', lagMs: pos.lagMs });
  save();
  watch(m.mint);
}

async function watch(mint) {
  const pos = st.open[mint];
  let reason = 'tiempo', back = null;
  while (Date.now() < pos.openedAt + HOLD_S * 1000) {
    const c = await curve(pos.curve);
    if (c?.complete) { reason = 'graduó'; break; }
    if (c) {
      const v = sellValue(c, pos), r = v / pos.size - 1;
      pos.peak = Math.max(pos.peak, r);
      if (pos.path.length < 120) pos.path.push([Math.round((Date.now() - pos.openedAt) / 1000), +r.toFixed(4)]);
      if (r >= TP) { reason = 'TP'; back = v; break; }
      if (r <= -SL) { reason = 'SL'; back = v; if (Date.now() - pos.openedAt < 30000) markRug(pos.creator, pos.initialBuy, 'desplome en < 30 s tras comprar'); break; }
    }
    await sleep(1000);
  }
  // Ejecución: la venta aterriza ~1 s después de decidir; se usa el estado de la curva en ese momento
  await sleep(800);
  const c = await curve(pos.curve);
  if (c && !c.complete) back = sellValue(c, pos);
  else { const j = await jupSell(mint, Math.floor(pos.tokens)); if (j != null) back = Math.max(0, j - PRIO); }
  back ??= 0;
  const ret = back / pos.size - 1;
  st.balance += back;
  st.closed.push({ mint, ret, size: pos.size, out: back, holdS: (Date.now() - pos.openedAt) / 1000, lagMs: pos.lagMs, at: Date.now(), whaleSol: null, kind: 'dev', reason, rise: null, peak: pos.peak, route: 'pump', devPct: pos.devPct, txs: pos.txs, path: pos.path, symbol: pos.symbol, name: pos.name, twitter: pos.twitter, telegram: pos.telegram, website: pos.website });
  delete st.open[mint];
  emit({ type: 'sell', mint, ret, holdS: (Date.now() - pos.openedAt) / 1000, symbol: pos.symbol });
  log(`VENTA (${reason}) ${pos.symbol || mint.slice(0, 6)} ${ret >= 0 ? '+' : ''}${(ret * 100).toFixed(1)} % (máx ${(pos.peak * 100).toFixed(0)} %) | saldo ${st.balance.toFixed(4)} SOL | ${st.closed.length} cerrados, ${st.closed.filter(x => x.ret > 0).length} ganadores`);
  save();
}
for (const m of Object.keys(st.open)) watch(m);

function connect() {
  const ws = new WebSocket('wss://pumpportal.fun/api/data');
  ws.onopen = () => { ws.send(JSON.stringify({ method: 'subscribeNewToken' })); log(`Bot DEV FUERTE PAPER | saldo ${st.balance.toFixed(4)} SOL | dev >= ${DEV_PCT} % y < ${MAX_TXS} txs a los ${DECIDE_S} s | TP ${TP * 100} % SL ${SL * 100} % máx ${HOLD_S} s — escuchando lanzamientos`); };
  ws.onmessage = e => { try { const m = JSON.parse(String(e.data)); if (m.txType === 'create' && m.mint && m.bondingCurveKey) onCreate(m); } catch {} };
  ws.onclose = () => setTimeout(connect, 3000);
  ws.onerror = () => { try { ws.close(); } catch {} };
}
connect();
setInterval(save, 60000);
setInterval(() => log(`resumen: ${JSON.stringify(st.stats)}`), 30 * 60 * 1000);

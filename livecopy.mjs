// Copiador PAPER (0 € reales, nunca firma ni envía transacciones). Ver README_ES.md
// COPIADOR EN VIVO (PAPER, 0 € reales) de la wallet 649ot.
// Detecta sus compras/ventas on-chain (Helius gratis), espera la latencia de aterrizaje y lee el precio REAL del pool en ese
// instante para simular nuestra compra/venta con comisiones, priority fee e impacto propio. Nunca firma ni envía transacciones.
// Uso: HK=<helius_key> node research/livecopy_649ot.mjs
import fs from 'node:fs';

const W = process.env.WALLET || '649ot18fccD6Mc9V4aHWTdoSVCuqAEtg9aWuxBA9cRoU';
const K = process.env.HK || process.env.HELIUS_API_KEY || ''; // opcional: solo como último recurso
const START_SOL = Number(process.env.START_SOL ?? 0.09);
const FRACTION = Number(process.env.FRACTION ?? 0.25);  // % del saldo por trade
const LAT = Number(process.env.LAT ?? 800);             // ms entre detectar y aterrizar
const POLL = Number(process.env.POLL ?? 1500);
const FEE = 0.0125, PRIO = 0.0002;
const WSOL = 'So11111111111111111111111111111111111111112';
const STATE = process.env.STATE || 'data/livecopy_649ot.json';
// RPCs GRATUITOS sin créditos (PublicNode y el oficial de Solana); Helius solo como último recurso si hay clave.
const RPCS = (process.env.RPC_URLS || 'https://solana-rpc.publicnode.com,https://api.mainnet-beta.solana.com').split(',').map(x => x.trim()).filter(Boolean);
if (K && process.env.HELIUS_FALLBACK !== '0') RPCS.push(`https://mainnet.helius-rpc.com/?api-key=${K}`);
const WS_URL = process.env.WS_URL || 'wss://api.mainnet-beta.solana.com';
let rpcIdx = 0;
const sleep = ms => new Promise(r => setTimeout(r, ms));

let st = fs.existsSync(STATE) ? JSON.parse(fs.readFileSync(STATE, 'utf8')) : null;
// STATE_RESET: si cambia, se empieza de cero (p. ej. tras arreglar un fallo que dejó el saldo mal contado)
if (!st || (process.env.STATE_RESET && st.resetId !== process.env.STATE_RESET)) st = { startedAt: Date.now(), balance: START_SOL, open: {}, closed: [], seen: [], credits: 0, resetId: process.env.STATE_RESET || null };
const seen = new Set(st.seen);
// Si el proceso padre (app.mjs) se reinicia o desaparece, este copiador se cierra para no quedar duplicado
process.on('disconnect', () => process.exit(0));
const BOOT = Date.now();
const emit = m => { try { if (process.send) process.send(m); } catch {} };
const save = () => { st.seen = [...seen].slice(-3000); fs.writeFileSync(STATE, JSON.stringify(st, null, 1)); };
const log = m => console.log(`[${new Date().toLocaleTimeString('es-ES')}] ${m}`);

// Prueba el RPC actual; si falla o limita (429), pasa al siguiente de la lista. Solo cuenta créditos si usa Helius.
async function rpc(method, params) {
  for (let i = 0; i < RPCS.length * 2; i++) {
    const url = RPCS[(rpcIdx + i) % RPCS.length];
    try {
      const r = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method, params }), signal: AbortSignal.timeout(8000) });
      if (url.includes('helius')) st.credits++;
      if (r.status === 429 || r.status >= 500) { await sleep(300); continue; }
      const j = await r.json();
      if (j.error && /rate|limit|busy/i.test(j.error.message || '')) { await sleep(300); continue; }
      rpcIdx = (rpcIdx + i) % RPCS.length; // quedarse con el que funcionó
      return j.result ?? null;
    } catch { await sleep(200); }
  }
  return null;
}

// Lee el precio actual del pool: curva de pump.fun (cuenta bonding curve) o pool AMM (cuentas de token del pool)
async function poolPrice(pool) {
  if (pool.kind === 'curva') {
    const acc = await rpc('getAccountInfo', [pool.owner, { encoding: 'base64', commitment: 'processed' }]);
    const b = acc?.value?.data?.[0] ? Buffer.from(acc.value.data[0], 'base64') : null;
    if (!b || b.length < 49) return null;
    const vTok = Number(b.readBigUInt64LE(8)) / 1e6, vSol = Number(b.readBigUInt64LE(16)) / 1e9;
    return { price: vSol / vTok, solR: vSol };
  }
  const accs = await rpc('getMultipleAccounts', [[pool.tokAcc, pool.solAcc], { encoding: 'jsonParsed', commitment: 'processed' }]);
  const [t, s] = accs?.value || [];
  const tok = Number(t?.data?.parsed?.info?.tokenAmount?.uiAmount), sol = Number(s?.data?.parsed?.info?.tokenAmount?.uiAmount);
  if (!(tok > 0 && sol > 0)) return null;
  return { price: sol / tok, solR: sol };
}

// Analiza una tx de la ballena: mint, dirección (compra/venta), SOL, y localiza el pool
function parse(tx) {
  const m = tx.meta; if (!m || m.err) return null;
  const keys = [...tx.transaction.message.accountKeys, ...(m.loadedAddresses?.writable || []), ...(m.loadedAddresses?.readonly || [])].map(k => typeof k === 'string' ? k : k.pubkey);
  const wi = keys.indexOf(W);
  const pre = m.preTokenBalances || [], post = m.postTokenBalances || [];
  const amt = (arr, mint, owner) => arr.filter(b => b.mint === mint && b.owner === owner).reduce((a, b) => a + Number(b.uiTokenAmount.uiAmount || 0), 0);
  const mints = [...new Set([...pre, ...post].map(b => b.mint).filter(x => x !== WSOL))];
  const moved = mints.map(mt => ({ mt, d: amt(post, mt, W) - amt(pre, mt, W) })).filter(x => Math.abs(x.d) > 0);
  if (moved.length !== 1) return null;
  const { mt: mint, d } = moved[0];
  const solCh = (wi >= 0 ? (m.postBalances[wi] - m.preBalances[wi]) / 1e9 : 0) + (amt(post, WSOL, W) - amt(pre, WSOL, W));
  const side = d > 0 && solCh < 0 ? 'buy' : d < 0 && solCh > 0 ? 'sell' : null;
  if (!side) return null;
  const cands = post.filter(b => b.mint === mint && b.owner !== W);
  if (!cands.length) return null;
  const pb = cands.reduce((a, b) => (Number(b.uiTokenAmount.amount) > Number(a.uiTokenAmount.amount) ? b : a));
  const ws = post.find(b => b.mint === WSOL && b.owner === pb.owner);
  const pool = ws ? { kind: 'amm', owner: pb.owner, tokAcc: keys[pb.accountIndex], solAcc: keys[ws.accountIndex] } : { kind: 'curva', owner: pb.owner };
  // Precio JUSTO DESPUÉS de la tx de la ballena (solo curva de pump.fun: reservas virtuales = reales + 30 SOL / + 279,9 M tokens)
  // En la curva de pump.fun el precio es proporcional al cuadrado del SOL virtual (30 + SOL real de la cuenta de la curva)
  let refVSol = null;
  if (pool.kind === 'curva' && mint.endsWith('pump')) {
    const ci = keys.indexOf(pb.owner);
    if (ci >= 0) refVSol = 30 + m.postBalances[ci] / 1e9 - 0.00149;
  }
  return { mint, side, sol: Math.abs(solCh), pool, refVSol, remaining: amt(post, mint, W), moved: Math.abs(d), slot: tx.slot || 0 };
}

// Cotización REAL ejecutable vía Jupiter (cualquier launchpad/AMM, comisiones del pool incluidas). Devuelve outAmount (unidades raw).
async function jupQuote(inputMint, outputMint, amountRaw) {
  for (let i = 0; i < 3; i++) {
    try {
      const r = await fetch(`https://lite-api.jup.ag/swap/v1/quote?inputMint=${inputMint}&outputMint=${outputMint}&amount=${amountRaw}&slippageBps=1000`, { signal: AbortSignal.timeout(8000) });
      if (r.status === 429) { await sleep(1500); continue; }
      const j = await r.json();
      if (j?.outAmount) return { out: BigInt(j.outAmount), route: (j.routePlan || []).map(x => x.swapInfo?.label).join('>') };
    } catch { await sleep(500); }
  }
  return null;
}

const RULE_MIN = Number(process.env.RULE_MIN ?? 1), RULE_MAX = Number(process.env.RULE_MAX ?? 4);
const MAX_RISE = Number(process.env.MAX_RISE ?? 1); // 1 = sin filtro; 0.03 = solo copiar si subió <= 3 %
// Solo pump.fun (curva) si ONLY_PUMP=1: es lo único validado con datos; otros launchpads (Launchlab, Meteora...) no.
const ONLY_PUMP = process.env.ONLY_PUMP === '1';
// Evita compras duplicadas: varias compras seguidas de la ballena en el mismo token llegan a la vez; solo la primera cuenta.
const pending = new Set(), pendingSell = new Map();
// Última venta vista de la ballena por token (slot): las tx se procesan en paralelo y pueden llegar desordenadas.
const lastSellSlot = new Map();
const PUMP = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P';
// Comprueba en la cadena que la cuenta es de verdad una curva de pump.fun activa (dueño = programa pump, sin completar)
async function isPumpCurve(addr) {
  const a = await rpc('getAccountInfo', [addr, { encoding: 'base64', commitment: 'processed' }]);
  const v = a?.value; if (!v || v.owner !== PUMP) return false;
  const b = Buffer.from(v.data[0], 'base64'); return b.length >= 49 && b[48] === 0;
}
async function onBuy(ev, sig, txTime) {
  if (st.open[ev.mint] || pending.has(ev.mint)) return;
  pending.add(ev.mint);
  try {
    if (ONLY_PUMP && !(ev.pool.kind === 'curva' && ev.mint.endsWith('pump') && (ev.fast || await isPumpCurve(ev.pool.owner)))) { log(`compra ${ev.mint.slice(0, 6)} NO copiada: no es una curva de pump.fun activa (${ev.pool.kind})`); return; }
    if ((lastSellSlot.get(ev.mint) || 0) >= ev.slot) { log(`compra ${ev.mint.slice(0, 6)} NO copiada: la ballena ya vendió después de esta compra`); return; }
    await onBuyInner(ev, sig, txTime);
  } finally {
    pending.delete(ev.mint);
    // si la ballena vendió mientras comprábamos, aplicar esa venta ahora
    const ps = pendingSell.get(ev.mint); pendingSell.delete(ev.mint);
    if (ps && st.open[ev.mint]) onSell(ps);
  }
}
async function onBuyInner(ev, sig, txTime) {
  if (st.open[ev.mint]) return;
  // Regla validada fuera de muestra: copiar solo cuando la ballena compra entre 1 y 4 SOL
  if (ev.sol < RULE_MIN || ev.sol >= RULE_MAX) { log(`compra ${ev.mint.slice(0, 6)} NO copiada: ballena ${ev.sol.toFixed(2)} SOL fuera de la regla ${RULE_MIN}-${RULE_MAX} SOL`); return; }
  // Filtro "llegamos antes que los bots": solo copiar si el precio NO ha subido más de MAX_RISE desde su tx.
  // Validado en 150 trayectorias tx a tx de 649ot (positivo en las dos mitades); solo curva pump.fun.
  if (MAX_RISE < 1) {
    if (!ev.refVSol) { log(`compra ${ev.mint.slice(0, 6)} NO copiada: no es curva de pump.fun (filtro de subida solo validado ahí)`); return; }
    const now = await poolPrice(ev.pool);
    if (!now) { log(`compra ${ev.mint.slice(0, 6)} NO copiada: no pude leer la curva`); return; }
    const rise = (now.solR / ev.refVSol) ** 2 - 1; // poolPrice('curva') devuelve solR = SOL virtual actual
    if (rise > MAX_RISE) { log(`compra ${ev.mint.slice(0, 6)} NO copiada: el precio ya subió ${(rise * 100).toFixed(1)}% desde su compra (los bots llegaron antes; máx ${(MAX_RISE * 100).toFixed(0)}%)`); return; }
    log(`compra ${ev.mint.slice(0, 6)} PASA el filtro: subida desde su compra ${(rise * 100).toFixed(1)}% (máx ${(MAX_RISE * 100).toFixed(0)}%)`);
    ev.rise = rise;
  }
  await sleep(LAT);
  const size = Math.min(st.balance * FRACTION, st.balance - 0.003);
  if (size < 0.005) return log('saldo insuficiente');
  const lamports = Math.floor((size - PRIO) * 1e9);
  const q = await jupQuote(WSOL, ev.mint, lamports);
  if (!q) return onBuyPool(ev, sig, txTime); // sin ruta en Jupiter: método de lectura directa del pool
  st.balance -= size;
  const pIn = ev.pool.kind === 'curva' && ev.pool.owner ? await poolPrice(ev.pool) : null;
  st.open[ev.mint] = { size, tokensRaw: q.out.toString(), route: q.route, pool: ev.pool, whaleSig: sig, whaleSol: ev.sol, openedAt: Date.now(), lagMs: Date.now() - txTime * 1000, detMs: ev.detAt ? ev.detAt - txTime * 1000 : null, fast: !!ev.fast, vSolEntry: pIn?.solR ?? null, peak: 1, rise: ev.rise ?? null };
  emit({ type: 'buy', mint: ev.mint, whaleSol: ev.sol, kind: ev.pool.kind, route: q.route, lagMs: st.open[ev.mint].lagMs });
  log(`COMPRA copiada ${ev.mint.slice(0, 6)} vía ${q.route} | ballena ${ev.sol.toFixed(2)} SOL | nosotros ${size.toFixed(4)} SOL | retraso real ${(st.open[ev.mint].lagMs / 1000).toFixed(1)} s${ev.detAt ? ` (detección ${((ev.detAt - txTime * 1000) / 1000).toFixed(1)} s, rápido)` : ''}`);
  save();
}

async function onBuyPool(ev, sig, txTime) {
  if (st.open[ev.mint]) return; // solo la primera compra
  if (!ev.pool.owner) return log(`compra ${ev.mint.slice(0, 6)}: sin ruta en Jupiter y sin dirección de la curva, no copiada`);
  await sleep(LAT);
  let p = await poolPrice(ev.pool);
  if (!p) { await sleep(400); p = await poolPrice(ev.pool); }
  if (!p) return log(`compra ${ev.mint} (${ev.pool.kind} ${ev.pool.owner}): no pude leer el pool`);
  const size = Math.min(st.balance * FRACTION, st.balance - 0.003);
  if (size < 0.005) return log('saldo insuficiente');
  const sin = size * (1 - FEE) - PRIO, imp = sin / p.solR;
  const tokens = sin / (p.price * (1 + imp));
  st.balance -= size;
  st.open[ev.mint] = { size, tokens, pool: ev.pool, entryPrice: p.price, poolSol: p.solR, whaleSig: sig, whaleSol: ev.sol, openedAt: Date.now(), lagMs: Date.now() - txTime * 1000 };
  emit({ type: 'buy', mint: ev.mint, whaleSol: ev.sol, kind: ev.pool.kind, route: ev.pool.kind, lagMs: st.open[ev.mint].lagMs });
  log(`COMPRA copiada ${ev.mint.slice(0, 6)} (${ev.pool.kind}) | ballena ${ev.sol.toFixed(2)} SOL | nosotros ${size.toFixed(4)} SOL | retraso real ${(st.open[ev.mint].lagMs / 1000).toFixed(1)} s`);
  save();
}
// Venta PROPORCIONAL: si la ballena vende una parte (p. ej. 30 % de lo que tiene), vendemos el mismo 30 % de lo nuestro.
// Con PROPORTIONAL=0 se vuelve al modo antiguo (todo en su primera venta).
const PROPORTIONAL = process.env.PROPORTIONAL !== '0';
const MAX_HOLD_S = Number(process.env.MAX_HOLD_S ?? 1200);
// Las ventas de un mismo token se ejecutan EN ORDEN, una detrás de otra (antes se descartaban si llegaban seguidas)
const sellQ = new Map();
function onSell(ev, reason = 'espejo') {
  if (reason === 'espejo' && ev.slot) lastSellSlot.set(ev.mint, Math.max(lastSellSlot.get(ev.mint) || 0, ev.slot));
  const p = (sellQ.get(ev.mint) || Promise.resolve()).then(() => onSellInner(ev, reason)).catch(e => log(`venta ${ev.mint.slice(0, 6)}: error ${e?.message || e}`));
  sellQ.set(ev.mint, p); p.then(() => { if (sellQ.get(ev.mint) === p) sellQ.delete(ev.mint); });
  return p;
}
async function onSellInner(ev, reason = 'espejo') {
  if (pending.has(ev.mint) && reason === 'espejo') {
    // acumula ventas que llegan mientras compramos (se aplican como una sola, con la fracción total vendida)
    const prev = pendingSell.get(ev.mint);
    pendingSell.set(ev.mint, prev ? { ...ev, moved: (prev.moved || 0) + (ev.moved || 0) } : ev); return;
  }
  const pos = st.open[ev.mint]; if (!pos || pos.closing) return;
  let f = 1;
  if (PROPORTIONAL && reason === 'espejo' && ev.moved > 0 && ev.remaining != null) f = Math.min(1, ev.moved / (ev.moved + ev.remaining));
  if (f > 0.95) f = 1;
  pos.closing = true;
  await sleep(LAT);
  let out;
  if (pos.tokensRaw) {
    const part = f >= 1 ? BigInt(pos.tokensRaw) : BigInt(pos.tokensRaw) * BigInt(Math.round(f * 1e6)) / 1000000n;
    let q = await jupQuote(ev.mint, WSOL, part.toString());
    for (let i = 0; !q && i < 4; i++) { await sleep(1500); q = await jupQuote(ev.mint, WSOL, part.toString()); }
    if (!q) { log(`venta ${ev.mint.slice(0, 6)}: sin cotización de salida, ${f >= 1 ? 'posición marcada como pérdida total REAL (sin liquidez)' : 'venta parcial omitida'}`); if (f < 1) { pos.closing = false; return; } }
    out = q ? Math.max(0, Number(q.out) / 1e9 - PRIO) : 0;
    if (f < 1) pos.tokensRaw = (BigInt(pos.tokensRaw) - part).toString();
  } else {
    const p = await poolPrice(pos.pool);
    if (!p) { pos.closing = false; log(`venta ${ev.mint.slice(0, 6)}: no pude leer el pool, se reintentará`); return; }
    const price = p.price, tk = pos.tokens * f;
    out = Math.max(0, (tk * price * (1 - (p ? pos.size * f / p.solR : 0))) * (1 - FEE) - PRIO);
    if (f < 1) pos.tokens -= tk;
  }
  st.balance += out;
  if (f < 1) {
    pos.outSoFar = (pos.outSoFar || 0) + out; pos.partials = (pos.partials || 0) + 1; pos.closing = false;
    log(`VENTA PARCIAL ${ev.mint.slice(0, 6)}: la ballena vendió ${(f * 100).toFixed(0)}% de lo suyo → vendemos ${(f * 100).toFixed(0)}% (+${out.toFixed(4)} SOL)`);
    save(); return;
  }
  out += pos.outSoFar || 0;
  const ret = out / pos.size - 1;
  st.closed.push({ mint: ev.mint, ret, size: pos.size, out, holdS: (Date.now() - pos.openedAt) / 1000, lagMs: pos.lagMs, at: Date.now(), whaleSol: pos.whaleSol, kind: pos.pool.kind, poolSol: pos.poolSol, rule14: pos.whaleSol >= 1 && pos.whaleSol < 4, reason, peak: pos.peak ?? null, rise: pos.rise ?? null, route: pos.route ?? null, detMs: pos.detMs ?? null, fast: !!pos.fast });
  emit({ type: 'sell', mint: ev.mint, ret, holdS: (Date.now() - pos.openedAt) / 1000 });
  delete st.open[ev.mint];
  const eq = st.balance + Object.values(st.open).reduce((a, o) => a + o.size, 0);
  const n = st.closed.length, w = st.closed.filter(c => c.ret > 0).length;
  log(`VENTA (${reason}) ${ev.mint.slice(0, 6)} ${ret >= 0 ? '+' : ''}${(ret * 100).toFixed(1)}% | equity ${eq.toFixed(4)} SOL (x${(eq / START_SOL).toFixed(2)}) | ${n} cerrados, ${w} ganadores | créditos ${st.credits}`);
  save();
}

// Valor EN TIEMPO REAL de las posiciones abiertas cada 15 s: cotización de venta de Jupiter de lo que nos queda (+ lo ya vendido en parciales)
setInterval(async () => {
  let ch = false;
  for (const [mint, pos] of Object.entries(st.open)) {
    if (pos.closing || !pos.tokensRaw) continue;
    const q = await jupQuote(mint, WSOL, pos.tokensRaw);
    if (!q || !st.open[mint]) continue;
    pos.markSol = Math.max(0, Number(q.out) / 1e9 - PRIO); pos.markAt = Date.now(); ch = true;
  }
  if (ch) save();
}, 15000);

// Tope de tiempo: si la ballena no vende en MAX_HOLD_S, cerramos nosotros (no quedarnos atrapados en un token muerto)
setInterval(() => { for (const [mint, pos] of Object.entries(st.open)) if (!pos.closing && Date.now() - pos.openedAt > MAX_HOLD_S * 1000) { log(`tiempo máximo ${mint.slice(0, 6)}: ${MAX_HOLD_S} s sin venta de la ballena → vendemos`); onSell({ mint }, 'tiempo'); } }, 5000);

// Trailing stop: si la posición llega a +TRAIL_ARM y luego cae TRAIL desde el máximo, vendemos sin esperar a la ballena.
const TRAIL = Number(process.env.TRAIL ?? 0), TRAIL_ARM = Number(process.env.TRAIL_ARM ?? 0.4);
if (TRAIL > 0) setInterval(async () => {
  for (const [mint, pos] of Object.entries(st.open)) {
    if (pos.closing || !pos.vSolEntry || pos.pool.kind !== 'curva') continue;
    const p = await poolPrice(pos.pool); if (!p) continue;
    const r = (p.solR / pos.vSolEntry) ** 2;
    pos.peak = Math.max(pos.peak || 1, r);
    if (pos.peak >= 1 + TRAIL_ARM && r <= pos.peak * (1 - TRAIL)) { log(`trailing ${mint.slice(0, 6)}: máximo x${pos.peak.toFixed(2)}, ahora x${r.toFixed(2)} -> vendemos`); onSell({ mint }, 'trailing'); }
  }
}, 1500);

async function loop() {
  log(`Copiador PAPER de ${W.slice(0, 6)} | saldo ${st.balance.toFixed(4)} SOL | ${FRACTION * 100}% por trade | latencia ${LAT} ms`);
  // no copiar el historial anterior al arranque
  if (!st.seen.length) { const s0 = await rpc('getSignaturesForAddress', [W, { limit: 50 }]); for (const s of s0 || []) seen.add(s.signature); save(); }
  startWs();
  // Respaldo por sondeo (más lento) por si el WebSocket se corta
  for (;;) {
    const sigs = await rpc('getSignaturesForAddress', [W, { limit: 25, commitment: 'confirmed' }]);
    const fresh = (sigs || []).filter(s => !s.err && !seen.has(s.signature)).reverse();
    for (const s of fresh) await processSig(s.signature, s.blockTime, 'sondeo');
    await sleep(wsAlive ? Number(process.env.POLL_WS ?? 4000) : POLL); // el WebSocket público pierde avisos: respaldo cada 4 s
  }
}

const MAX_BUY_AGE_S = Number(process.env.MAX_BUY_AGE_S ?? 30);
async function processSig(sig, blockTime, via) {
  if (seen.has(sig)) return;
  seen.add(sig);
  let tx = null;
  // Aviso 'processed' (antes): la tx se puede leer en cuanto se confirma (~0,4-1 s); reintento rápido
  for (let i = 0; i < 30 && !tx; i++) { tx = await rpc('getTransaction', [sig, { maxSupportedTransactionVersion: 1, encoding: 'json', commitment: 'confirmed' }]); if (!tx) await sleep(150); }
  const ev = tx && parse(tx);
  if (!ev) return;
  setWhale(ev.mint, ev.remaining, tx.slot);
  const bt = blockTime || tx.blockTime || Date.now() / 1000;
  const ageS = Date.now() / 1000 - bt;
  // Compras antiguas (p. ej. hechas mientras el bot se reiniciaba por una actualización): se ignoran. Solo se avisa si llegó tarde en vivo.
  if (ev.side === 'buy' && ageS > MAX_BUY_AGE_S) { if (Date.now() - BOOT > 60000) log(`compra ${ev.mint.slice(0, 6)} ignorada: detectada con ${ageS.toFixed(0)} s de retraso (>30 s)`); return; }
  if (ev.side === 'buy') { log(`(detectada vía ${via})`); onBuy(ev, sig, bt); } else onSell(ev);
}

// ---------- Camino RÁPIDO (curva de pump.fun) ----------
// El aviso 'processed' del WebSocket ya trae los logs de la tx; pump.fun emite ahí un TradeEvent con token, SOL, tokens,
// compra/venta y cartera. Así decidimos al instante, sin esperar a la confirmación ni a getTransaction (~1-3 s menos).
const TRADE_DISC = 'bddb7fd34ee661ee'; // sha256("event:TradeEvent")[0..8]
const B58 = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz';
const b58 = b => { let x = BigInt('0x' + (Buffer.from(b).toString('hex') || '0')), s = ''; while (x > 0n) { s = B58[Number(x % 58n)] + s; x /= 58n; } for (const v of b) { if (v) break; s = '1' + s; } return s; };
function tradeEvents(logs) {
  const out = [];
  for (const l of logs || []) {
    if (!l.startsWith('Program data: ')) continue;
    let b; try { b = Buffer.from(l.slice(14), 'base64'); } catch { continue; }
    if (b.length < 153 || b.subarray(0, 8).toString('hex') !== TRADE_DISC) continue;
    let o = 8; const mint = b58(b.subarray(o, o + 32)); o += 32;
    const sol = Number(b.readBigUInt64LE(o)) / 1e9; o += 8; const tok = Number(b.readBigUInt64LE(o)) / 1e6; o += 8;
    const isBuy = b[o] === 1; o += 1; const user = b58(b.subarray(o, o + 32)); o += 32; const ts = Number(b.readBigInt64LE(o)); o += 8;
    const vSol = Number(b.readBigUInt64LE(o)) / 1e9; o += 24; const realTok = Number(b.readBigUInt64LE(o)) / 1e6;
    out.push({ mint, sol, tok, isBuy, user, ts, vSol, realTok });
  }
  return out;
}
// Tokens que tiene la ballena de cada token (para ventas proporcionales); se corrige en segundo plano con el saldo real
const whaleTok = new Map(), whaleSlot = new Map();
// Solo acepta un saldo si viene de un bloque igual o posterior al último conocido (las correcciones llegan desordenadas)
const setWhale = (mint, tok, slot) => { if ((whaleSlot.get(mint) || 0) > (slot || 0)) return; whaleTok.set(mint, tok); whaleSlot.set(mint, slot || 0); };
async function reconcile(sig, mint) {
  for (let i = 0; i < 20; i++) { const tx = await rpc('getTransaction', [sig, { maxSupportedTransactionVersion: 1, encoding: 'json', commitment: 'confirmed' }]); if (tx) { const ev = parse(tx); if (ev && ev.mint === mint) setWhale(mint, ev.remaining, tx.slot); return; } await sleep(300); }
}
function fastPath(v, slot) {
  const evs = tradeEvents(v.logs).filter(e => e.user === W);
  if (evs.length !== 1 || !evs[0].mint.endsWith('pump')) return false;
  const e = evs[0], now = Date.now();
  seen.add(v.signature);
  if (e.isBuy) {
    setWhale(e.mint, (whaleTok.get(e.mint) ?? 0) + e.tok, slot);
    reconcile(v.signature, e.mint);
    if (!(e.realTok > 0)) return true; // curva ya completada
    log(`(detectada vía websocket rápido, ${((now - e.ts * 1000) / 1000).toFixed(1)} s tras su tx)`);
    onBuy({ mint: e.mint, side: 'buy', sol: e.sol, pool: { kind: 'curva', owner: null }, refVSol: e.vSol, slot, detAt: now, fast: true }, v.signature, e.ts);
  } else {
    const have = whaleTok.get(e.mint);
    if (have == null) { seen.delete(v.signature); return false; } // no sabemos cuánto tenía: camino lento con su saldo real
    const remaining = Math.max(0, have - e.tok); setWhale(e.mint, remaining, slot);
    reconcile(v.signature, e.mint);
    onSell({ mint: e.mint, side: 'sell', moved: e.tok, remaining, slot, fast: true });
  }
  return true;
}

// Suscripción en tiempo real: Solana avisa en cuanto una tx menciona a la wallet (sin esperar al sondeo)
let wsAlive = false, wsCur = null;
setInterval(() => { try { if (wsCur?.readyState === 1) wsCur.send(JSON.stringify({ jsonrpc: '2.0', id: 99, method: 'getHealth' })); } catch {} }, 30000);
function startWs() {
  const ws = new WebSocket(WS_URL); wsCur = ws;
  ws.onopen = () => { wsAlive = true; ws.send(JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'logsSubscribe', params: [{ mentions: [W] }, { commitment: 'processed' }] })); log('WebSocket conectado: detección en tiempo real'); };
  ws.onmessage = m => {
    try {
      const j = JSON.parse(String(m.data));
      const v = j?.params?.result?.value;
      if (!v?.signature || v.err || seen.has(v.signature)) return;
      if (!fastPath(v, j.params.result.context?.slot || 0)) processSig(v.signature, null, 'websocket');
    } catch {}
  };
  const retry = () => { wsAlive = false; setTimeout(startWs, 3000); };
  ws.onclose = retry; ws.onerror = () => { try { ws.close(); } catch {} };
}
// Modo PRUEBA (REPLAY=archivo.json con [{signature, blockTime}]): mete las tx reales todas a la vez y desordenadas.
if (process.env.REPLAY) {
  const chrono = process.env.REPLAY_ORDER === 'chrono';
  const L = JSON.parse(fs.readFileSync(process.env.REPLAY, 'utf8')).sort(chrono ? (a, b) => a.blockTime - b.blockTime : () => Math.random() - 0.5);
  log(`PRUEBA: ${L.length} tx reales ${chrono ? 'en orden, como en vivo' : 'en paralelo y desordenadas'}`);
  (chrono ? (async () => { for (const x of L) { await processSig(x.signature, x.blockTime, 'prueba'); await sleep(1500); } })() : Promise.all(L.map(x => processSig(x.signature, x.blockTime, 'prueba')))).then(async () => { await sleep(8000); for (const q of sellQ.values()) await q; save(); log(`PRUEBA terminada | saldo ${st.balance.toFixed(4)} | abiertas ${Object.keys(st.open).length} | cerradas ${st.closed.length}`); process.exit(0); });
} else loop();

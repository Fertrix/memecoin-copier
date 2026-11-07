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

const st = fs.existsSync(STATE) ? JSON.parse(fs.readFileSync(STATE, 'utf8')) : { startedAt: Date.now(), balance: START_SOL, open: {}, closed: [], seen: [], credits: 0 };
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
  return { mint, side, sol: Math.abs(solCh), pool, refVSol, remaining: amt(post, mint, W), moved: Math.abs(d) };
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
async function onBuy(ev, sig, txTime) {
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
  const pIn = ev.pool.kind === 'curva' ? await poolPrice(ev.pool) : null;
  st.open[ev.mint] = { size, tokensRaw: q.out.toString(), route: q.route, pool: ev.pool, whaleSig: sig, whaleSol: ev.sol, openedAt: Date.now(), lagMs: Date.now() - txTime * 1000, vSolEntry: pIn?.solR ?? null, peak: 1, rise: ev.rise ?? null };
  emit({ type: 'buy', mint: ev.mint, whaleSol: ev.sol, kind: ev.pool.kind, route: q.route, lagMs: st.open[ev.mint].lagMs });
  log(`COMPRA copiada ${ev.mint.slice(0, 6)} vía ${q.route} | ballena ${ev.sol.toFixed(2)} SOL${ev.sol >= 1 && ev.sol < 4 ? ' (regla 1-4 ✓)' : ''} | nosotros ${size.toFixed(4)} SOL | retraso real ${(st.open[ev.mint].lagMs / 1000).toFixed(1)} s`);
  save();
}

async function onBuyPool(ev, sig, txTime) {
  if (st.open[ev.mint]) return; // solo la primera compra
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
async function onSell(ev, reason = 'espejo') {
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
  st.closed.push({ mint: ev.mint, ret, size: pos.size, out, holdS: (Date.now() - pos.openedAt) / 1000, lagMs: pos.lagMs, at: Date.now(), whaleSol: pos.whaleSol, kind: pos.pool.kind, poolSol: pos.poolSol, rule14: pos.whaleSol >= 1 && pos.whaleSol < 4, reason, peak: pos.peak ?? null, rise: pos.rise ?? null, route: pos.route ?? null });
  emit({ type: 'sell', mint: ev.mint, ret, holdS: (Date.now() - pos.openedAt) / 1000 });
  delete st.open[ev.mint];
  const eq = st.balance + Object.values(st.open).reduce((a, o) => a + o.size, 0);
  const n = st.closed.length, w = st.closed.filter(c => c.ret > 0).length;
  log(`VENTA (${reason}) ${ev.mint.slice(0, 6)} ${ret >= 0 ? '+' : ''}${(ret * 100).toFixed(1)}% | equity ${eq.toFixed(4)} SOL (x${(eq / START_SOL).toFixed(2)}) | ${n} cerrados, ${w} ganadores | créditos ${st.credits}`);
  save();
}

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
    await sleep(wsAlive ? 10000 : POLL); // con RPC públicos el respaldo no cuesta créditos
  }
}

async function processSig(sig, blockTime, via) {
  if (seen.has(sig)) return;
  seen.add(sig);
  let tx = null;
  // Aviso 'processed' (antes): la tx se puede leer en cuanto se confirma (~0,4-1 s); reintento rápido
  for (let i = 0; i < 30 && !tx; i++) { tx = await rpc('getTransaction', [sig, { maxSupportedTransactionVersion: 1, encoding: 'json', commitment: 'confirmed' }]); if (!tx) await sleep(150); }
  const ev = tx && parse(tx);
  if (!ev) return;
  const bt = blockTime || tx.blockTime || Date.now() / 1000;
  const ageS = Date.now() / 1000 - bt;
  // Compras antiguas (p. ej. hechas mientras el bot se reiniciaba por una actualización): se ignoran. Solo se avisa si llegó tarde en vivo.
  if (ev.side === 'buy' && ageS > 30) { if (Date.now() - BOOT > 60000) log(`compra ${ev.mint.slice(0, 6)} ignorada: detectada con ${ageS.toFixed(0)} s de retraso (>30 s)`); return; }
  if (ev.side === 'buy') { log(`(detectada vía ${via})`); onBuy(ev, sig, bt); } else onSell(ev);
}

// Suscripción en tiempo real: Solana avisa en cuanto una tx menciona a la wallet (sin esperar al sondeo)
let wsAlive = false;
function startWs() {
  const ws = new WebSocket(WS_URL);
  ws.onopen = () => { wsAlive = true; ws.send(JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'logsSubscribe', params: [{ mentions: [W] }, { commitment: 'processed' }] })); log('WebSocket conectado: detección en tiempo real'); };
  ws.onmessage = m => {
    try {
      const j = JSON.parse(String(m.data));
      const v = j?.params?.result?.value;
      if (v?.signature && !v.err) processSig(v.signature, null, 'websocket');
    } catch {}
  };
  const retry = () => { wsAlive = false; setTimeout(startWs, 3000); };
  ws.onclose = retry; ws.onerror = () => { try { ws.close(); } catch {} };
  setInterval(() => { try { if (ws.readyState === 1) ws.send(JSON.stringify({ jsonrpc: '2.0', id: 99, method: 'getHealth' })); } catch {} }, 30000);
}
loop();

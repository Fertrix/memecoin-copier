// BOT LISTO (PAPER, 0 € reales) en monedas ya graduadas (PumpSwap).
// - Cada 20 min saca de GeckoTerminal las pools de PumpSwap con más actividad (moneda/SOL).
// - Cada minuto lee las reservas EXACTAS de cada pool (cuentas de la pool por RPC público): precio, liquidez, flujo de SOL.
// - Cada 5 min un modelo (árboles entrenados con el día completo del 15-oct, todas las monedas aunque luego murieran)
//   mira ~15 datos del pasado de cada moneda y predice lo que ganaría quedándose 30 min.
//   Compra solo si la predicción supera el umbral del 0,1 % de mejores momentos; con la moneda en mano la revisa cada 5 min
//   y vende cuando el modelo deja de predecir ganancia (o a las 6 h).
// - Comisión real de PumpSwap según capitalización (medida en los datos), prioridad 0,0002 SOL por tx, alquiler de cuenta.
// Prueba histórica "como en vivo" (entrena con lo anterior, opera las 4 h siguientes, 4 veces): 90 ops, +15,8 % de media.
import fs from 'node:fs';

const START_SOL = Number(process.env.START_SOL ?? 0.15);
const SIZE = Number(process.env.SIZE ?? 0.025);
const MAX_OPEN = Number(process.env.MAX_OPEN ?? 6);
const MAX_HOLD_MIN = Number(process.env.MAX_HOLD_MIN ?? 360);
const BUY_DELAY_MS = Number(process.env.BUY_DELAY_MS ?? 2000);
const MODEL = process.env.MODEL || 'models/smart_novol.json';
const STATE = process.env.STATE || 'data/livecopy_smart.json';
const HIST = STATE.replace(/\.json$/, '') + '_hist.json';
const PRIO = 0.0002, RENT = 0.00204, MIN_LIQ = 30, HMAX = 240;
const WSOL = 'So11111111111111111111111111111111111111112';
const RPCS = (process.env.RPC_URLS || 'https://api.mainnet-beta.solana.com,https://solana-rpc.publicnode.com').split(',');
const sleep = ms => new Promise(r => setTimeout(r, ms));
const log = m => console.log(`[${new Date().toLocaleTimeString('es-ES')}] ${m}`);
const emit = m => { try { if (process.send) process.send(m); } catch {} };
process.on('disconnect', () => { save(); process.exit(0); });
for (const s of ['SIGTERM', 'SIGINT']) process.on(s, () => { save(); process.exit(0); });

// ---------- Modelo ----------
const MD = JSON.parse(fs.readFileSync(MODEL, 'utf8'));
function leaf(n, x) {
  while (n.leaf_value === undefined) {
    let v = x[n.split_feature]; const miss = n.missing_type;
    if (v == null || Number.isNaN(v)) { if (miss === 'NaN') { n = n.default_left ? n.left_child : n.right_child; continue; } v = 0; }
    if (miss === 'Zero' && v === 0) { n = n.default_left ? n.left_child : n.right_child; continue; }
    n = v <= n.threshold ? n.left_child : n.right_child;
  }
  return n.leaf_value;
}
const predict = x => MD.trees.reduce((s, t) => s + leaf(t, x), 0);
// Modelo ANTI-HUNDIMIENTO (monedas agrupadas): probabilidad de que la pool se hunda en los próximos 30 min.
// Máquina del tiempo corregida: saltar si >= 40 % → hundidas 9 → 4 de 59, media por compra +10 % → +14,5 %.
const RUG = fs.existsSync(process.env.MODEL_RUG || 'models/smart_rug.json') ? JSON.parse(fs.readFileSync(process.env.MODEL_RUG || 'models/smart_rug.json', 'utf8')) : null;
const RUG_MAX = Number(process.env.RUG_MAX ?? RUG?.threshold ?? 0.4);
const rugProb = x => 1 / (1 + Math.exp(-RUG.trees.reduce((s, t) => s + leaf(t, x), 0)));
if (RUG) { let mx = 0; RUG.check.forEach((x, i) => { mx = Math.max(mx, Math.abs(rugProb(x.map(v => v ?? NaN)) - RUG.check_prob[i])); }); if (mx > 1e-6) { console.log('ERROR: el modelo anti-hundimiento en JS no coincide', mx); process.exit(1); } }
// GRABACIÓN diaria de lo que ve el bot (reservas de cada pool cada minuto + datos de cada pool) para reentrenar con días reales
const REC_DIR = 'data/smart_rec'; try { fs.mkdirSync(REC_DIR, { recursive: true }); } catch {}
const recFile = () => `${REC_DIR}/${new Date().toISOString().slice(0, 10)}.jsonl`;
const rec = o => { if (process.env.REC === '0') return; try { fs.appendFileSync(recFile(), JSON.stringify(o) + String.fromCharCode(10)); } catch {} };
{ let mx = 0; MD.check.forEach((x, i) => { mx = Math.max(mx, Math.abs(predict(x.map(v => v ?? NaN)) - MD.check_pred[i])); }); if (mx > 1e-6) { console.log('ERROR: el modelo en JS no coincide con Python', mx); process.exit(1); } }

// ---------- Estado ----------
const loadJ = f => { for (const g of [f, f + '.bak']) { try { if (fs.existsSync(g)) return JSON.parse(fs.readFileSync(g, 'utf8')); } catch {} } return null; };
const st = loadJ(STATE) || { startedAt: Date.now(), balance: START_SOL, open: {}, closed: [], credits: 0, stats: { pools: 0, checks: 0 } };
st.stats ||= { pools: 0, checks: 0 };
const H = loadJ(HIST) || {}; // pool -> { mint, symbol, bv, qv, h: { minuto: [Qlam, Rraw] } }
let lastBak = 0;
function save() {
  try {
    const w = (f, o) => { fs.writeFileSync(f + '.tmp', JSON.stringify(o)); fs.renameSync(f + '.tmp', f); };
    w(STATE, st); w(HIST, H);
    if (Date.now() - lastBak > 600000) { lastBak = Date.now(); fs.copyFileSync(STATE, STATE + '.bak'); }
  } catch (e) { log('no pude guardar: ' + e.message); }
}

// ---------- RPC ----------
let rpcIdx = 0;
async function rpc(method, params) {
  for (let i = 0; i < RPCS.length * 3; i++) {
    const url = RPCS[(rpcIdx + i) % RPCS.length];
    try {
      const r = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method, params }), signal: AbortSignal.timeout(10000) });
      if (r.status === 429 || r.status >= 500) { await sleep(600); continue; }
      const j = await r.json(); if (j.error) { await sleep(400); continue; }
      rpcIdx = (rpcIdx + i) % RPCS.length; return j.result ?? null;
    } catch { await sleep(400); }
  }
  return null;
}
const B58 = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz';
function b58(b) { let n = BigInt('0x' + Buffer.from(b).toString('hex')), s = ''; while (n > 0n) { s = B58[Number(n % 58n)] + s; n /= 58n; } for (const x of b) { if (x === 0) s = '1' + s; else break; } return s; }
async function balances(accts) { // cuenta -> cantidad (bruta)
  const out = {};
  for (let i = 0; i < accts.length; i += 100) {
    const r = await rpc('getMultipleAccounts', [accts.slice(i, i + 100), { encoding: 'jsonParsed', commitment: 'confirmed' }]);
    if (!r) continue;
    r.value.forEach((a, k) => { const amt = a?.data?.parsed?.info?.tokenAmount?.amount; if (amt != null) out[accts[i + k]] = Number(amt); });
  }
  return out;
}

// ---------- Universo: pools de PumpSwap con actividad ----------
async function universe() {
  const found = new Map();
  for (const sort of ['h24_volume_usd_desc', 'h24_tx_count_desc']) for (let page = 1, tries = 0; page <= (sort === 'h24_volume_usd_desc' ? 10 : 5); page++) {
    try {
      const r = await fetch(`https://api.geckoterminal.com/api/v2/networks/solana/dexes/pumpswap/pools?page=${page}&sort=${sort}`, { signal: AbortSignal.timeout(15000) });
      if (r.status === 429) { if (++tries <= 4) { await sleep(20000); page--; } continue; }
      tries = 0;
      const j = await r.json();
      for (const p of j.data || []) found.set(p.attributes.address, p.attributes.name);
    } catch {}
    await sleep(5000);
  }
  const fresh = [...found.keys()].filter(p => !H[p]);
  for (let i = 0; i < fresh.length; i += 100) {
    const r = await rpc('getMultipleAccounts', [fresh.slice(i, i + 100), { encoding: 'base64' }]);
    r?.value?.forEach((a, k) => {
      if (!a || a.owner !== 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA') return;
      const b = Buffer.from(a.data[0], 'base64'); if (b.length < 203) return;
      const base = b58(b.subarray(43, 75)), quote = b58(b.subarray(75, 107));
      if (quote !== WSOL || !base.endsWith('pump')) return; // solo pools moneda/SOL de monedas de pump.fun (como en la prueba)
      H[fresh[i + k]] = { mint: base, symbol: (found.get(fresh[i + k]) || '').split(' /')[0], bv: b58(b.subarray(139, 171)), qv: b58(b.subarray(171, 203)), h: {} };
    });
  }
  const live = new Set(found.keys()); for (const o of Object.values(st.open)) live.add(o.pool);
  if (found.size >= 50) for (const p of Object.keys(H)) { if (live.has(p)) continue; const g = H[p].grad; if (g ? Date.now() - g > 6 * 3600000 : true) delete H[p]; } // fuera de la lista: solo se quedan graduaciones de < 6 h y posiciones abiertas
  st.stats.pools = Object.keys(H).length;
  log(`universo: ${st.stats.pools} pools de PumpSwap vigiladas (${fresh.length} nuevas revisadas)`);
}
const nowMin = () => Math.floor(Date.now() / 60000);

// ---------- Datos (idénticos a la prueba histórica) ----------
function features(p, i) {
  const h = H[p].h; const Ql = k => h[k]?.[0], Rr = k => h[k]?.[1];
  const lp = k => (Ql(k) != null ? Math.log(Ql(k) / Rr(k)) : NaN);
  const Qs = k => (Ql(k) != null ? Ql(k) / 1e9 : NaN);
  const L = []; for (let k = i - 181; k <= i; k++) L.push(lp(k)); // L[181 - k] = lp(i - k)
  const at = k => L[181 - k];
  const win = n => L.slice(182 - n); // los n últimos minutos
  const nmax = a => { let m = -Infinity, ok = false; for (const v of a) if (!Number.isNaN(v)) { ok = true; if (v > m) m = v; } return ok ? m : NaN; };
  const nmin = a => -nmax(a.map(v => -v));
  const r1 = L.map((v, k) => (k === 0 ? NaN : v - L[k - 1]));
  const nstd = a => { const b = a.filter(v => !Number.isNaN(v)); if (!b.length) return NaN; const m = b.reduce((s, v) => s + v, 0) / b.length; return Math.sqrt(b.reduce((s, v) => s + (v - m) ** 2, 0) / b.length); };
  const cur = at(0), Qi = Qs(i);
  const F = {
    sube5: cur - at(5), sube15: cur - at(15), sube30: cur - at(30), sube60: cur - at(60), sube180: cur - at(180),
    desde_max60: cur - nmax(win(61)), desde_max180: cur - nmax(win(181)), desde_min180: cur - nmin(win(181)),
    vol5: NaN, vol30: NaN, vol180: NaN,
    flujo5: (Qi - Qs(i - 5)) / Qi, flujo30: (Qi - Qs(i - 30)) / Qi,
    log_liq: Math.log(Qi), log_mc: Math.log(Math.exp(cur) * 1e6),
    volat30: nstd(r1.slice(182 - 31)), volat180: nstd(r1.slice(1)),
  };
  return MD.features.map(n => F[n]);
}
const fee = mc => mc < 420 ? 0.0125 : mc < 1500 ? 0.012 : mc < 2500 ? 0.0115 : mc < 5000 ? 0.011 : mc < 10000 ? 0.01 : mc < 20000 ? 0.0095 : mc < 50000 ? 0.0065 : mc < 100000 ? 0.0045 : 0.003;
async function reserves(p) { const b = await balances([H[p].qv, H[p].bv]); const Q = b[H[p].qv], R = b[H[p].bv]; return Q > 0 && R > 0 ? [Q, R] : null; }
function sellValue([Q, R], pos) { const out = Q - Q * R / (R + pos.tokens); return Math.max(0, out / 1e9 * (1 - fee(Q / R * 1e6)) - PRIO); }

// ---------- Compra / venta ----------
let pending = 0;
async function buy(p, pred, prug = null) {
  pending++;
  try { await buyNow(p, pred, prug); } finally { pending--; }
}
async function buyNow(p, pred, prug) {
  const size = Math.min(SIZE, st.balance - 0.003 - RENT);
  if (size < 0.005) { log('sin saldo libre'); return; }
  await sleep(BUY_DELAY_MS); // la compra real tarda unos segundos en aterrizar: precio de ese momento
  const r = await reserves(p); if (!r) return;
  const [Q, R] = r; const x = (size - PRIO) * 1e9 * (1 - fee(Q / R * 1e6)); const tokens = R - Q * R / (Q + x);
  st.balance -= size + RENT;
  const pos = st.open[H[p].mint] = { pool: p, size, rent: RENT, tokens, openedAt: Date.now(), symbol: H[p].symbol, pred, prug, peak: 0, liq: Q / 1e9, mc: Q / R * 1e6, path: [] };
  log(`COMPRA ${pos.symbol} | el modelo predice +${(pred * 100).toFixed(1)} % en 30 min${prug != null ? ` · riesgo de hundimiento ${(prug * 100).toFixed(0)} %` : ''} | liquidez ${pos.liq.toFixed(0)} SOL | cap. ${pos.mc.toFixed(0)} SOL | ${size.toFixed(4)} SOL`);
  emit({ type: 'buy', mint: H[p].mint, symbol: pos.symbol, name: pos.symbol, whaleSol: null, kind: 'smart', route: 'pumpswap', lagMs: BUY_DELAY_MS });
  save();
}
async function sell(mint, reason) {
  const pos = st.open[mint]; if (!pos || pos.selling) return; pos.selling = true;
  await sleep(1000);
  const r = await reserves(pos.pool);
  const back = r ? sellValue(r, pos) : (pos.markSol ?? 0);
  const ret = back / pos.size - 1;
  st.balance += back + (pos.rent ?? 0);
  st.closed.push({ mint, ret, size: pos.size, out: back, holdS: (Date.now() - pos.openedAt) / 1000, at: Date.now(), whaleSol: null, kind: 'smart', reason, peak: pos.peak, route: 'pumpswap', path: pos.path, symbol: pos.symbol, name: pos.symbol, pred: pos.pred, prug: pos.prug ?? null });
  delete st.open[mint];
  emit({ type: 'sell', mint, ret, size: pos.size, holdS: (Date.now() - pos.openedAt) / 1000, symbol: pos.symbol });
  log(`VENTA (${reason}) ${pos.symbol} ${ret >= 0 ? '+' : ''}${(ret * 100).toFixed(1)} % en ${((Date.now() - pos.openedAt) / 60000).toFixed(0)} min | saldo ${st.balance.toFixed(4)} SOL | ${st.closed.length} cerradas, ${st.closed.filter(x => x.ret > 0).length} ganadoras`);
  save();
}


// ---------- OPERADORES: la cartera que hace la compra grande al graduarse se repite en muchas monedas ----------
// Unos operadores dejan ganar (+40/+60 % de media en la prueba), otros hunden la moneda. El bot aprende en vivo:
// si una moneda agrupada se desploma (la pool pierde >= 85 % de sus SOL en <= 10 min), su operador queda vetado.
st.ops ||= {}; // operador -> { tokens, rugs, ok }
st.cre ||= {}; // creador -> nº de monedas suyas hundidas
// Máquina del tiempo: vetar al OPERADOR quita también las buenas (los mismos operadores inflan y luego hunden: el modelo sabe cuándo salir);
// vetar al CREADOR con un hundimiento previo sí mejora (umbral exigente: +23,4 % → +26,8 %, quita la única hundida).
const vetoed = cre => !!cre && (st.cre[cre] || 0) >= 1;
async function findOperator(pool) {
  const P = H[pool]; if (!P || P.op) return;
  let sigs = [], before;
  for (let pg = 0; pg < 6; pg++) { // ir hacia atrás hasta el principio de la pool
    const r = await rpc('getSignaturesForAddress', [pool, { limit: 1000, commitment: 'confirmed', ...(before ? { before } : {}) }]);
    if (!r?.length) break; sigs = sigs.concat(r); before = r.at(-1).signature; if (r.length < 1000) break;
  }
  if (!sigs.length) return;
  let best = null;
  for (const sg of sigs.slice(-12)) { // las más antiguas: el bloque de la graduación
    const tx = await rpc('getTransaction', [sg.signature, { maxSupportedTransactionVersion: 0, encoding: 'json', commitment: 'confirmed' }]);
    if (!tx || tx.meta?.err) continue;
    const payer = tx.transaction.message.accountKeys[0]; if (payer === P.creator) continue;
    const spent = (tx.meta.preBalances[0] - tx.meta.postBalances[0]) / 1e9;
    if (spent > 1 && (!best || spent > best.spent)) best = { payer, spent };
  }
  if (!best) return;
  P.op = best.payer; P.opSol = best.spent;
  const o = st.ops[P.op] ||= { tokens: 0, rugs: 0, ok: 0 }; o.tokens++;
  log(`operador de ${P.symbol}: ${P.op.slice(0, 6)} compró ${best.spent.toFixed(0)} SOL al graduarse | historial: ${o.tokens} monedas, ${o.rugs} hundidas, ${o.ok} aguantaron (solo informativo)`);
}
function judgeOperators(m) { // ¿se hundió alguna agrupada? ¿cuál aguantó 60 min?
  for (const [p, P] of Object.entries(H)) {
    if (!P.bund || P.judged) continue;
    const ks = Object.keys(P.h).map(Number).sort((a, b) => a - b); if (ks.length < 2) continue;
    const now = P.h[m]; if (!now) continue;
    const prev = Math.max(...ks.filter(k => k >= m - 10 && k < m).map(k => P.h[k][0]), 0);
    const o = P.op ? (st.ops[P.op] ||= { tokens: 1, rugs: 0, ok: 0 }) : { rugs: 0, ok: 0 };
    if (prev > 0 && now[0] < prev * 0.15) { P.judged = 'rug'; o.rugs++; if (P.creator) st.cre[P.creator] = (st.cre[P.creator] || 0) + 1; log(`HUNDIMIENTO en ${P.symbol}: la pool pasó de ${(prev / 1e9).toFixed(0)} a ${(now[0] / 1e9).toFixed(1)} SOL → creador ${P.creator ? P.creator.slice(0, 6) + ' VETADO' : 'desconocido'}${P.op ? ` · operador ${P.op.slice(0, 6)} (${P.opSol.toFixed(0)} SOL): ${o.rugs} hundidas` : ''}`); }
    else if (Date.now() - P.grad > 3600000) { P.judged = 'ok'; o.ok++; }
  }
}


// ---------- RUGCHECK (filtros de Clinter) ----------
// RC_SHADOW=1: audita TODAS las graduaciones a los RC_AT_MIN min y lo graba (para medir qué filtros evitan hundimientos).
// RC_FILTER=1: además no compra si falla algún filtro de Clinter (score > 500, riesgos "danger", > 16 insiders, dev > 3 %,
//              pool fuera de 8-95 %, top 10 > 60 %).
const RC_SHADOW = process.env.RC_SHADOW === '1', RC_FILTER = process.env.RC_FILTER === '1', RC_AT_MIN = Number(process.env.RC_AT_MIN ?? 8);
const rcQueue = []; let rcBusy = false;
async function rcAudit(mint) {
  try {
    const r = await fetch(`https://api.rugcheck.xyz/v1/tokens/${mint}/report`, { headers: { 'User-Agent': 'Mozilla/5.0' }, signal: AbortSignal.timeout(25000) });
    if (!r.ok) return null; const d = await r.json();
    const known = d.knownAccounts || {}; const poolOwners = new Set(Object.entries(known).filter(([, v]) => /pool|amm|raydium|meteora|orca|vault|bonding/i.test(v?.name || '')).map(([k]) => k));
    let poolPct = 0; const reg = [];
    for (const h of d.topHolders || []) { const pct = Number(h.pct || 0); if (poolOwners.has(h.owner) || pct > 35) poolPct += pct; else reg.push(pct); }
    reg.sort((a, b) => b - a);
    const supply = Number(d.token?.supply || 1e15);
    return { score: d.score || 0, insiders: d.graphInsidersDetected || 0, devPct: +((Number(d.creatorBalance || 0) / Math.max(supply, 1)) * 100).toFixed(2), poolPct: +poolPct.toFixed(1),
      top1: +(reg[0] || 0).toFixed(2), top5: +reg.slice(0, 5).reduce((a, b) => a + b, 0).toFixed(2), top10: +reg.slice(0, 10).reduce((a, b) => a + b, 0).toFixed(2),
      danger: (d.risks || []).filter(x => x.level === 'danger').map(x => x.name), warn: (d.risks || []).filter(x => x.level === 'warn').map(x => x.name), holders: d.totalHolders ?? null };
  } catch { return null; }
}
function rcFails(a) {
  if (!a) return 'sin informe de RugCheck';
  if (a.score > 500) return `RugCheck score ${a.score} > 500`;
  if (a.danger.length) return `riesgos: ${a.danger.join(', ')}`;
  if (a.insiders > 16) return `${a.insiders} insiders conectados`;
  if (a.devPct > 3) return `el dev tiene ${a.devPct} %`;
  if (a.poolPct < 8 || a.poolPct > 95) return `pool con ${a.poolPct} % del suministro`;
  if (a.top10 > 60) return `top 10 carteras con ${a.top10} %`;
  return null;
}
async function rcPump() {
  if (rcBusy) return; rcBusy = true;
  while (rcQueue.length) {
    const p = rcQueue.shift(); const P = H[p]; if (!P) continue;
    const a = await rcAudit(P.mint); P.rc = a || { fail: true }; P.rcAt = Date.now();
    rec({ rc: p, mint: P.mint, age: P.grad ? +((Date.now() - P.grad) / 60000).toFixed(1) : null, bund: P.bund ?? null, ...(a || { fail: true }) });
    await sleep(2500);
  }
  rcBusy = false;
}
function rcSchedule() { // cada minuto: encola las graduaciones que cumplen RC_AT_MIN y aún no tienen informe
  if (!RC_SHADOW && !RC_FILTER) return;
  for (const [p, P] of Object.entries(H)) if (P.grad && !P.rcQueued && Date.now() - P.grad >= RC_AT_MIN * 60000 && Date.now() - P.grad < 3 * 3600000) { P.rcQueued = 1; rcQueue.push(p); }
  rcPump();
}

// ---------- Bucle de cada minuto ----------
async function tick() {
  const m = nowMin(); const pools = Object.keys(H);
  const b = await balances(pools.flatMap(p => [H[p].qv, H[p].bv]));
  const snap = {};
  for (const p of pools) { const Q = b[H[p].qv], R = b[H[p].bv]; if (Q > 0 && R > 0) { H[p].h[m] = [Q, R]; snap[p] = [Q, R]; for (const k of Object.keys(H[p].h)) if (+k < m - HMAX) delete H[p].h[k]; } }
  for (const p of pools) if (!H[p].recMeta) { H[p].recMeta = 1; const P = H[p]; rec({ meta: p, mint: P.mint, symbol: P.symbol, bund: P.bund ?? null, secs: P.secs ?? null, creator: P.creator ?? null, grad: P.grad ?? null, op: P.op ?? null, opSol: P.opSol ?? null }); }
  rec({ m, r: snap });
  for (const [mint, pos] of Object.entries(st.open)) { // valor en tiempo real
    const e = H[pos.pool]?.h[m]; if (!e) continue;
    const v = sellValue(e, pos), r = v / pos.size - 1; pos.markSol = v; pos.markAt = Date.now(); pos.peak = Math.max(pos.peak, r);
    if (pos.path.length < 400) pos.path.push([Math.round((Date.now() - pos.openedAt) / 1000), +r.toFixed(4)]);
  }
  judgeOperators(m);
  rcSchedule();
  if (m % 5 !== 0) { save(); return; }
  // decisión cada 5 min
  let best = null, n = 0;
  for (const p of pools) {
    const e = H[p].h[m]; if (!e) continue;
    const have = Object.keys(H[p].h).length; if (have < 3) continue; // en la prueba, las buenas señales salen 9-47 min tras graduarse
    const x = features(p, m); const pr = predict(x); n++;
    const mint = H[p].mint, pos = st.open[mint];
    if (pos) {
      if (pr < MD.exit_below) sell(mint, `el modelo ya no ve ganancia (${(pr * 100).toFixed(1)} %)`);
      else if (Date.now() - pos.openedAt >= MAX_HOLD_MIN * 60000) sell(mint, 'máx 6 h');
      continue;
    }
    if (e[0] / 1e9 < MIN_LIQ) continue;
    if (BUNDLE_MODE === 'only' && !H[p].bund) continue;
    if (BUNDLE_MODE === 'skip' && (H[p].bund || !H[p].grad)) continue; // normales recién graduadas que hemos visto nacer o graduarse
    if (BUNDLE_MODE === 'grad' && !H[p].grad) continue; // cualquier graduación vista en vivo
    if (RC_FILTER) { const why = rcFails(H[p].rc?.fail ? null : H[p].rc); if (why) continue; }
    if (H[p].grad && Date.now() - H[p].grad < MIN_AGE_MIN * 60000) continue; // aún en la zona de peligro
    if (vetoed(H[p].creator)) continue; // creador que ya ha hundido una moneda
    if (!best || pr > best.pr) best = { p, pr };
    if (PAUSE_BUYS && pr > (BUY_TH ?? MD.threshold)) { st.stats.pausedSignals = (st.stats.pausedSignals || 0) + 1; continue; } // en pausa: solo mide y graba
    if (pr > (BUY_TH ?? MD.threshold) && Object.keys(st.open).length + pending < MAX_OPEN) {
      if (RUG && H[p].grad && H[p].bund) {
        const pg = rugProb([...x, (Date.now() - H[p].grad) / 60000]);
        if (pg >= RUG_MAX) { log(`${H[p].symbol}: el modelo de precio predice +${(pr * 100).toFixed(1)} % pero el anti-hundimiento da ${(pg * 100).toFixed(0)} % de riesgo → no compro`); st.stats.rugSkips = (st.stats.rugSkips || 0) + 1; continue; }
        buy(p, pr, pg);
      } else buy(p, pr);
    }
  }
  st.stats.checks++; st.stats.lastEval = { at: Date.now(), pools: n, best: best ? { symbol: H[best.p].symbol, pred: best.pr } : null };
  log(`nacimientos vistos ${created.size} | revisadas ${n} monedas (${pools.filter(p => H[p].bund).length} agrupadas) | mejor candidata (${BUNDLE_MODE === 'skip' ? 'normales' : BUNDLE_MODE === 'only' ? 'agrupadas' : 'todas'}): ${best ? H[best.p].symbol + ' ' + (best.pr * 100).toFixed(1) + ' %' : '—'} (compra si > ${((BUY_TH ?? MD.threshold) * 100).toFixed(1)} %) | abiertas ${Object.keys(st.open).length} | saldo ${st.balance.toFixed(4)} SOL`);
  save();
}

if (process.env.SMART_TEST) { // comprobación: datos calculados igual que en la prueba histórica
  const T = JSON.parse(fs.readFileSync(process.env.SMART_TEST, 'utf8')); H.test = { h: T.h };
  console.log(JSON.stringify(T.points.map(i => features('test', i)))); process.exit(0);
}
log(`Bot LISTO PAPER (graduadas PumpSwap) | saldo ${st.balance.toFixed(4)} SOL | ${SIZE} SOL por compra, máx ${MAX_OPEN} a la vez | modelo ${MODEL} (${MD.trees.length} árboles, umbral +${(MD.threshold * 100).toFixed(1)} %)`);
universe().catch(e => log('universo: ' + e.message));
// GRADUACIONES EN VIVO: cada moneda que pasa a PumpSwap entra en vigilancia al momento (de ahí salen casi todas las señales)
const MIG = '39azUYFWPz3VHgKCf3VChUwbpURdCHRxjWVowf5jUJjg';
// MONEDAS AGRUPADAS: el creador compra toda la curva y se gradúa al instante. En la prueba histórica TODA la ganancia sale de ahí
// (se gradúan <= 120 s tras crearse: 72 ops, +15,4 % de media; las demás: 12 ops, −14,1 %). Solo compramos esas.
const ONLY_BUNDLED = process.env.ONLY_BUNDLED !== '0', BUNDLE_S = Number(process.env.BUNDLE_S ?? 120);
// MODO de monedas: 'only' = solo agrupadas (octubre: ahí estaba la ganancia) · 'skip' = NUNCA agrupadas.
// Datos exactos del 9-10 nov: se hunden 132 de 137 agrupadas (96 %) frente a 42 de 89 normales (47 %).
const BUNDLE_MODE = process.env.BUNDLE_MODE || (ONLY_BUNDLED ? 'only' : 'any');
const BUY_TH = process.env.BUY_TH != null ? Number(process.env.BUY_TH) : null; // umbral de compra propio (si no, el del modelo)
// Los hundimientos los hace una cartera grande que compró al graduarse y vende en los primeros minutos: esperar 10 min.
// Máquina del tiempo (agrupadas): hundidas 7 % → 2 %, media +15,4 % → +23,4 %.
const MIN_AGE_MIN = Number(process.env.MIN_AGE_MIN ?? 10);
// PAUSA de compras: el bot sigue vigilando, grabando datos y gestionando lo abierto, pero no abre nada nuevo
const PAUSE_BUYS = process.env.PAUSE_BUYS === '1';
const created = new Map(); // moneda -> cuándo la vimos nacer (lanzamientos compartidos de pump.fun)
const creatorOf = new Map();
process.on('message', msg => { if (msg?.type === 'create' && msg.m?.mint) { created.set(msg.m.mint, Date.now()); if (msg.m.traderPublicKey) creatorOf.set(msg.m.mint, msg.m.traderPublicKey); if (created.size > 20000) { const cut = Date.now() - 900000; for (const [k, t] of created) if (t < cut) { created.delete(k); creatorOf.delete(k); } } } });
const seenSig = new Set();
async function onGraduation(sig) {
  if (seenSig.has(sig)) return; seenSig.add(sig); if (seenSig.size > 5000) seenSig.clear();
  let tx = null; for (let i = 0; i < 15 && !tx; i++) { tx = await rpc('getTransaction', [sig, { maxSupportedTransactionVersion: 0, encoding: 'json', commitment: 'confirmed' }]); if (!tx) await sleep(1000); }
  if (!tx || !(tx.meta?.logMessages || []).some(l => /Instruction: Migrate/.test(l))) return;
  const keys = [...tx.transaction.message.accountKeys, ...(tx.meta.loadedAddresses?.writable || []), ...(tx.meta.loadedAddresses?.readonly || [])];
  const post = tx.meta.postTokenBalances || [];
  const tok = post.filter(b => b.mint.endsWith('pump')).sort((a, b) => Number(b.uiTokenAmount.amount) - Number(a.uiTokenAmount.amount))[0];
  if (!tok) return;
  const sol = post.find(b => b.mint === WSOL && b.owner === tok.owner);
  if (!sol) return;
  const p0 = Number(sol.uiTokenAmount.uiAmount) / Number(tok.uiTokenAmount.uiAmount);
  if (!(p0 > 2e-7 && p0 < 6e-7)) return; // no es la pool estándar
  const pool = tok.owner;
  if (H[pool]) return;
  const born = created.get(tok.mint), gradAt = (tx.blockTime || Date.now() / 1000) * 1000;
  const secs = born ? Math.max(0, (gradAt - born) / 1000) : null;
  const bund = secs != null && secs <= BUNDLE_S;
  H[pool] = { mint: tok.mint, symbol: tok.mint.slice(0, 6), bv: keys[tok.accountIndex], qv: keys[sol.accountIndex], h: {}, grad: Date.now(), bund, secs, creator: creatorOf.get(tok.mint) };
  if (bund) setTimeout(() => findOperator(pool).catch(() => {}), 8000);
  st.stats.grads = (st.stats.grads || 0) + 1;
  log(`graduación ${tok.mint.slice(0, 6)} → ${bund ? `AGRUPADA (se graduó a los ${secs.toFixed(0)} s de nacer): candidata` : secs != null ? `normal (tardó ${(secs / 60).toFixed(0)} min): solo se vigila` : 'nacimiento no visto: solo se vigila'} (${Object.keys(H).length} pools)`);
  try { const r = await fetch(`https://api.dexscreener.com/tokens/v1/solana/${tok.mint}`, { signal: AbortSignal.timeout(8000) }); const j = await r.json(); if (j?.[0]?.baseToken?.symbol) H[pool].symbol = j[0].baseToken.symbol; } catch {}
}
function connectMig() {
  const ws = new WebSocket(process.env.WS_URL || 'wss://api.mainnet-beta.solana.com');
  ws.onopen = () => ws.send(JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'logsSubscribe', params: [{ mentions: [MIG] }, { commitment: 'confirmed' }] }));
  ws.onmessage = m => { try { const v = JSON.parse(String(m.data))?.params?.result?.value; if (v?.signature && !v.err && (v.logs || []).some(l => /Instruction: Migrate/.test(l))) onGraduation(v.signature).catch(() => {}); } catch {} };
  ws.onclose = () => setTimeout(connectMig, 3000);
  ws.onerror = () => { try { ws.close(); } catch {} };
  const hb = setInterval(() => { if (ws.readyState === 1) { try { ws.send('{"jsonrpc":"2.0","id":9,"method":"getHealth"}'); } catch {} } else if (ws.readyState > 1) clearInterval(hb); }, 30000);
}
connectMig();
setInterval(() => universe().catch(e => log('universo: ' + e.message)), 20 * 60000);
(async function loop() {
  for (;;) {
    const wait = 60000 - (Date.now() % 60000) + 1500; await sleep(wait);
    try { await tick(); } catch (e) { log('error en el minuto: ' + e.message); }
  }
})();

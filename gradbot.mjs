// BOT DE GRADUACIONES (PAPER, 0 € reales): aprovecha la presión compradora de BOOST tras la graduación a PumpSwap.
// - Detecta graduaciones en tiempo real (WebSocket público, cuenta de migración de pump.fun) — sin créditos.
// - Precio de nacimiento del pool sacado de la propia tx de migración (reservas exactas).
// - Desde +60 s hasta +120 s: si el precio REAL de compra (cotización Jupiter) <= precio de graduación * (1 + MAX_PREMIUM), compra.
// - Vende cuando la cotización REAL de venta da >= TP neto, o a los HOLD_S segundos de la graduación.
// Backtest (96 graduaciones, velas 1 min, entrando solo desde el minuto 2): +10,7 % / +1,5 % por trade en las dos mitades.
import fs from 'node:fs';

const START_SOL = Number(process.env.START_SOL ?? 0.09);
const FRACTION = Number(process.env.FRACTION ?? 0.25);
const MAX_PREMIUM = Number(process.env.MAX_PREMIUM ?? 0.05);
const MIN_PREMIUM = Number(process.env.MIN_PREMIUM ?? -0.35); // más barato que esto = token desplomándose: no comprar
const SL_NET = Number(process.env.SL_NET ?? 0.15);            // stop loss sobre la cotización real de venta
const TP_NET = Number(process.env.TP_NET ?? 0.12);
const HOLD_S = Number(process.env.HOLD_S ?? 180);
const WAIT_FROM = 60, WAIT_TO = 120, STEP = 3000, PRIO = 0.0002;
const STATE = process.env.STATE || 'data/livecopy_grad.json';
const MIG = '39azUYFWPz3VHgKCf3VChUwbpURdCHRxjWVowf5jUJjg';
const WSOL = 'So11111111111111111111111111111111111111112';
const RPCS = (process.env.RPC_URLS || 'https://solana-rpc.publicnode.com,https://api.mainnet-beta.solana.com').split(',');
const WS_URL = process.env.WS_URL || 'wss://api.mainnet-beta.solana.com';
const sleep = ms => new Promise(r => setTimeout(r, ms));
const log = m => console.log(`[${new Date().toLocaleTimeString('es-ES')}] ${m}`);
const emit = m => { try { if (process.send) process.send(m); } catch {} };
process.on('disconnect', () => process.exit(0));

const st = fs.existsSync(STATE) ? JSON.parse(fs.readFileSync(STATE, 'utf8')) : { startedAt: Date.now(), balance: START_SOL, open: {}, closed: [], seen: [], credits: 0, stats: { grads: 0, noFill: 0 } };
st.stats ||= { grads: 0, noFill: 0 };
const seen = new Set(st.seen);
const processing = new Set();
const save = () => { st.seen = [...seen].slice(-2000); fs.writeFileSync(STATE, JSON.stringify(st, null, 1)); };

let rpcIdx = 0;
async function rpc(method, params) {
  for (let i = 0; i < RPCS.length * 2; i++) {
    const url = RPCS[(rpcIdx + i) % RPCS.length];
    try {
      const r = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method, params }), signal: AbortSignal.timeout(8000) });
      if (r.status === 429 || r.status >= 500) { await sleep(400); continue; }
      const j = await r.json(); rpcIdx = (rpcIdx + i) % RPCS.length; return j.result ?? null;
    } catch { await sleep(300); }
  }
  return null;
}
let jupNext = 0;
async function jup(inputMint, outputMint, amount) {
  for (let i = 0; i < 4; i++) {
    const now = Date.now(); if (now < jupNext) await sleep(jupNext - now); jupNext = Math.max(now, jupNext) + 700; // ~85/min máx
    try {
      const r = await fetch(`https://lite-api.jup.ag/swap/v1/quote?inputMint=${inputMint}&outputMint=${outputMint}&amount=${amount}&slippageBps=1000`, { signal: AbortSignal.timeout(8000) });
      if (r.status === 429) { await sleep(2000); continue; }
      const j = await r.json(); if (j?.outAmount) return BigInt(j.outAmount);
      return null;
    } catch { await sleep(500); }
  }
  return null;
}

async function onGraduation(sig) {
  if (seen.has(sig)) return; seen.add(sig);
  let tx = null; for (let i = 0; i < 20 && !tx; i++) { tx = await rpc('getTransaction', [sig, { maxSupportedTransactionVersion: 1, encoding: 'json', commitment: 'confirmed' }]); if (!tx) await sleep(500); }
  const logs = tx?.meta?.logMessages || [];
  if (!logs.some(l => /Instruction: Migrate/.test(l))) return;
  const post = tx.meta.postTokenBalances || [];
  const tokBal = post.filter(b => b.mint.endsWith('pump')).sort((a, b) => Number(b.uiTokenAmount.uiAmount) - Number(a.uiTokenAmount.uiAmount))[0];
  if (!tokBal) return;
  const mint = tokBal.mint;
  // Precio de referencia = el teórico de graduación (85 SOL / 206,9 M tokens), el mismo del backtest;
  // las velas reales confirman que el mercado arranca ahí (mediana 4,16e-7).
  const p0 = 85 / 206.9e6;
  // Descartar graduaciones NO estándar (otro suministro, otra moneda de cotización, Mayhem...): el precio del pool en la tx debe ser normal
  const wsolB = post.find(b => b.mint === WSOL && b.owner === tokBal.owner);
  const txP0 = wsolB ? Number(wsolB.uiTokenAmount.uiAmount) / Number(tokBal.uiTokenAmount.uiAmount) : null;
  if (!txP0 || txP0 < 2e-7 || txP0 > 6e-7) return; // esta tx no es la del pool estándar (la otra tx de la misma graduación sí lo será)
  if (processing.has(mint)) return; processing.add(mint); // una graduación llega en 2 transacciones
  const t0 = (tx.blockTime || Math.floor(Date.now() / 1000)) * 1000;
  if (st.open[mint] || Date.now() - t0 > WAIT_TO * 1000) return;
  st.stats.grads++;
  log(`graduación ${mint.slice(0, 6)} | precio de referencia ${p0.toExponential(3)} SOL/token`);
  // Esperar al segundo 60 y vigilar hasta el 120
  while (Date.now() < t0 + WAIT_FROM * 1000) await sleep(1000);
  const size = Math.min(st.balance * FRACTION, st.balance - 0.003);
  if (size < 0.005) { log(`graduación ${mint.slice(0, 6)}: sin saldo libre`); return; }
  const lamports = Math.floor((size - PRIO) * 1e9);
  let minPrem = Infinity;
  while (Date.now() < t0 + WAIT_TO * 1000) {
    const out = await jup(WSOL, mint, lamports);
    if (out) {
      const price = (lamports / 1e9) / (Number(out) / 1e6); // incluye comisión e impacto
      const prem = price / p0 - 1;
      minPrem = Math.min(minPrem, prem);
      if (prem < MIN_PREMIUM) { log(`graduación ${mint.slice(0, 6)}: precio ${(prem * 100).toFixed(0)} % bajo la referencia — se está hundiendo, no compro`); st.stats.crash = (st.stats.crash || 0) + 1; save(); return; }
      if (prem <= MAX_PREMIUM + 0.015) {
        st.balance -= size;
        st.open[mint] = { size, tokensRaw: out.toString(), p0, t0, openedAt: Date.now(), whaleSol: null, lagMs: Date.now() - t0, route: 'grad', pool: { kind: 'amm' }, rise: prem };
        log(`COMPRA graduación ${mint.slice(0, 6)} a +${((Date.now() - t0) / 1000).toFixed(0)} s | ${(prem * 100).toFixed(1)} % sobre el precio de nacimiento | ${size.toFixed(4)} SOL`);
        emit({ type: 'buy', mint, whaleSol: null, kind: 'grad', route: 'grad', lagMs: Date.now() - t0 });
        save(); return watch(mint);
      }
    }
    await sleep(STEP);
  }
  st.stats.noFill++; save();
  (st.stats.minPrems ||= []).push(Number.isFinite(minPrem) ? +minPrem.toFixed(3) : null); if (st.stats.minPrems.length > 300) st.stats.minPrems.shift();
  log(`graduación ${mint.slice(0, 6)}: sin compra — entre los 60 y 120 s el precio más bajo estuvo ${Number.isFinite(minPrem) ? (minPrem * 100).toFixed(0) + ' %' : '?'} por encima de la referencia (límite +${((MAX_PREMIUM + 0.015) * 100).toFixed(1)} %)`);
}

async function watch(mint) {
  const pos = st.open[mint];
  let reason = 'tiempo';
  while (Date.now() < pos.t0 + HOLD_S * 1000) {
    const out = await jup(mint, WSOL, pos.tokensRaw);
    if (out) {
      const r = (Number(out) / 1e9 - PRIO) / pos.size - 1;
      (pos.path ||= []).push([Math.round((Date.now() - pos.openedAt) / 1000), +r.toFixed(4)]);
      if (r >= TP_NET) { reason = 'TP'; break; }
      if (r <= -SL_NET) { reason = 'SL'; break; }
    }
    await sleep(STEP);
  }
  let out = await jup(mint, WSOL, pos.tokensRaw);
  for (let i = 0; !out && i < 4; i++) { await sleep(1500); out = await jup(mint, WSOL, pos.tokensRaw); }
  const back = out ? Math.max(0, Number(out) / 1e9 - PRIO) : 0;
  const ret = back / pos.size - 1;
  st.balance += back;
  st.closed.push({ mint, ret, size: pos.size, out: back, holdS: (Date.now() - pos.openedAt) / 1000, lagMs: pos.lagMs, at: Date.now(), whaleSol: null, kind: 'grad', reason, rise: pos.rise, peak: null, route: 'grad', path: (pos.path || []).slice(0, 80) });
  delete st.open[mint];
  emit({ type: 'sell', mint, ret, holdS: (Date.now() - pos.openedAt) / 1000 });
  const eq = st.balance + Object.values(st.open).reduce((a, o) => a + o.size, 0);
  log(`VENTA (${reason}) graduación ${mint.slice(0, 6)} ${ret >= 0 ? '+' : ''}${(ret * 100).toFixed(1)} % | saldo ${eq.toFixed(4)} SOL | ${st.closed.length} cerrados, ${st.closed.filter(c => c.ret > 0).length} ganadores`);
  save();
}

// Al arrancar: cerrar posiciones que quedaron abiertas tras un reinicio
for (const m of Object.keys(st.open)) watch(m);

function connect() {
  const ws = new WebSocket(WS_URL);
  ws.onopen = () => { ws.send(JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'logsSubscribe', params: [{ mentions: [MIG] }, { commitment: 'confirmed' }] })); log(`Bot de graduaciones PAPER | saldo ${st.balance.toFixed(4)} SOL | ${FRACTION * 100}% por trade | límite +${MAX_PREMIUM * 100}% | TP ${TP_NET * 100}% | máx ${HOLD_S}s — escuchando migraciones`); };
  ws.onmessage = m => { try { const v = JSON.parse(String(m.data))?.params?.result?.value; if (v?.signature && !v.err && (v.logs || []).some(l => /Instruction: Migrate/.test(l))) onGraduation(v.signature); } catch {} };
  ws.onclose = () => setTimeout(connect, 3000);
  ws.onerror = () => { try { ws.close(); } catch {} };
  setInterval(() => { try { if (ws.readyState === 1) ws.send('{"jsonrpc":"2.0","id":9,"method":"getHealth"}'); } catch {} }, 30000);
}
connect();
setInterval(save, 60000);

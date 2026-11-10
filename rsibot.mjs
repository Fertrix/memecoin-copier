// BOT REBOTE RSI (PAPER, 0 € reales) en memecoins GRANDES de Solana (WIF, FARTCOIN, PENGU, TRUMP, MEW, BOME...).
// Velas de 4 h (GeckoTerminal). Compra cuando el RSI(14) de la última vela cerrada baja de RSI_IN (25) y vende cuando supera RSI_OUT (55).
// Coste por lado COST (0,5 %: comisión de la pool + deslizamiento) y prioridad 0,0002 SOL por transacción.
// Máquina del tiempo (15 monedas, mayo-noviembre 2026, mercado cayendo −13 % a −59 %): 36 operaciones, +3,9 % de media,
// positivo en las dos mitades, con umbrales vecinos (RSI < 20/25, salida 50-60) y con el doble de coste.
import fs from 'node:fs';
const START_SOL = Number(process.env.START_SOL ?? 0.15), SIZE = Number(process.env.SIZE ?? 0.025), MAX_OPEN = Number(process.env.MAX_OPEN ?? 6);
const RSI_IN = Number(process.env.RSI_IN ?? 25), RSI_OUT = Number(process.env.RSI_OUT ?? 55), COST = Number(process.env.COST ?? 0.005), PRIO = 0.0002;
const TF_H = Number(process.env.TF_H ?? 4);
const STATE = process.env.STATE || 'data/livecopy_rsi.json';
const POOLS = JSON.parse(fs.readFileSync(process.env.POOLS || 'models/rsi_pools.json', 'utf8'));
const sleep = ms => new Promise(r => setTimeout(r, ms));
const log = m => console.log(`[${new Date().toLocaleTimeString('es-ES')}] ${m}`);
const emit = m => { try { if (process.send) process.send(m); } catch {} };
const loadJ = f => { for (const g of [f, f + '.bak']) { try { if (fs.existsSync(g)) return JSON.parse(fs.readFileSync(g, 'utf8')); } catch {} } return null; };
const st = loadJ(STATE) || { startedAt: Date.now(), balance: START_SOL, open: {}, closed: [], credits: 0, stats: { checks: 0 } };
let lastBak = 0;
const save = () => { try { fs.writeFileSync(STATE + '.tmp', JSON.stringify(st)); fs.renameSync(STATE + '.tmp', STATE); if (Date.now() - lastBak > 600000) { lastBak = Date.now(); fs.copyFileSync(STATE, STATE + '.bak'); } } catch (e) { log('no pude guardar: ' + e.message); } };
process.on('disconnect', () => { save(); process.exit(0); });
for (const s of ['SIGTERM', 'SIGINT']) process.on(s, () => { save(); process.exit(0); });
const ema = (x, n) => { const o = [x[0]]; const a = 2 / (n + 1); for (let i = 1; i < x.length; i++) o.push(o[i - 1] + a * (x[i] - o[i - 1])); return o; };
function rsi(cl, n = 14) {
  const d = cl.map((v, i) => (i ? v - cl[i - 1] : 0));
  const u = ema(d.map(v => Math.max(v, 0)), 2 * n - 1), dn = ema(d.map(v => Math.max(-v, 0)), 2 * n - 1);
  return u.map((v, i) => 100 - 100 / (1 + v / Math.max(dn[i], 1e-30)));
}
async function candles(pool) {
  for (let i = 0; i < 5; i++) {
    try {
      const r = await fetch(`https://api.geckoterminal.com/api/v2/networks/solana/pools/${pool}/ohlcv/hour?aggregate=${TF_H}&limit=300&currency=token`, { signal: AbortSignal.timeout(20000) });
      if (r.status === 429) { await sleep(15000); continue; }
      if (!r.ok) return null;
      const j = await r.json(); return (j?.data?.attributes?.ohlcv_list || []).map(x => ({ t: x[0], c: +x[4] })).sort((a, b) => a.t - b.t);
    } catch { await sleep(3000); }
  }
  return null;
}
async function check() {
  let seen = 0;
  for (const P of POOLS) {
    const c = await candles(P.pool); await sleep(2500);
    if (!c || c.length < 60) continue; seen++;
    const now = Date.now() / 1000, cur = c.at(-1).c; // precio actual (vela en curso)
    const closed = c.filter(x => x.t + TF_H * 3600 <= now); if (closed.length < 60) continue;
    const R = rsi(closed.map(x => x.c)), r = R.at(-1), barT = closed.at(-1).t;
    const pos = st.open[P.pool];
    if (pos) {
      pos.markSol = pos.size * (cur / pos.entry) * (1 - COST) ** 2 - PRIO; pos.markAt = Date.now(); pos.rsi = r;
      if (r > RSI_OUT && barT > pos.bar) {
        const back = Math.max(0, pos.size * (cur / pos.entry) * (1 - COST) ** 2 - PRIO), ret = back / pos.size - 1;
        st.balance += back;
        st.closed.push({ mint: P.pool, ret, size: pos.size, out: back, holdS: (Date.now() - pos.openedAt) / 1000, at: Date.now(), whaleSol: null, kind: 'rsi', reason: `RSI ${r.toFixed(0)} > ${RSI_OUT}`, route: 'jup', symbol: P.name, name: P.name, rsiIn: pos.rsiIn });
        delete st.open[P.pool];
        emit({ type: 'sell', mint: P.pool, ret, size: pos.size, holdS: (Date.now() - pos.openedAt) / 1000, symbol: P.name });
        log(`VENTA ${P.name} ${ret >= 0 ? '+' : ''}${(ret * 100).toFixed(1)} % (RSI ${r.toFixed(0)}) en ${((Date.now() - pos.openedAt) / 3600000).toFixed(1)} h | saldo ${st.balance.toFixed(4)} SOL | ${st.closed.length} cerradas, ${st.closed.filter(x => x.ret > 0).length} ganadoras`);
      }
    } else if (r < RSI_IN && st.lastBar?.[P.pool] !== barT && Object.keys(st.open).length < MAX_OPEN) {
      const size = Math.min(SIZE, st.balance - 0.003); if (size < 0.005) { log('sin saldo libre'); continue; }
      st.balance -= size; (st.lastBar ||= {})[P.pool] = barT;
      st.open[P.pool] = { size, entry: cur, openedAt: Date.now(), bar: barT, rsiIn: r, symbol: P.name, name: P.name };
      emit({ type: 'buy', mint: P.pool, symbol: P.name, name: P.name, whaleSol: null, kind: 'rsi', route: 'jup', lagMs: 0 });
      log(`COMPRA ${P.name} | RSI ${r.toFixed(1)} < ${RSI_IN} | precio ${cur.toPrecision(4)} ${P.quote} | ${size.toFixed(4)} SOL`);
    }
  }
  st.stats.checks++; st.stats.lastCheck = { at: Date.now(), pools: seen };
  if (st.stats.checks % 4 === 1) log(`revisadas ${seen}/${POOLS.length} monedas | abiertas ${Object.keys(st.open).length} | saldo ${st.balance.toFixed(4)} SOL`);
  save();
}
log(`Bot REBOTE RSI PAPER | ${POOLS.length} memecoins grandes | velas de ${TF_H} h | compra RSI < ${RSI_IN}, vende RSI > ${RSI_OUT} | ${SIZE} SOL por compra | saldo ${st.balance.toFixed(4)} SOL`);
(async function loop() { for (;;) { try { await check(); } catch (e) { log('error: ' + e.message); } await sleep(15 * 60000); } })();

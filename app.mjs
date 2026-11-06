// Servidor del copiador PAPER (0 € reales): lanza un copiador por ballena, lleva una CARTERA ÚNICA que copia a todas
// con el mismo dinero (lo que harías de verdad con tu saldo), sirve el panel web y, si hay webhook, avisa por Discord.
import { fork } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import http from 'node:http';
import { fileURLToPath } from 'node:url';

const DIR = path.dirname(fileURLToPath(import.meta.url)); // código (se actualiza solo desde GitHub)
const ROOT = process.cwd();                                  // carpeta del servidor: .env y data/ (no se tocan al actualizar)
fs.mkdirSync(path.join(ROOT, 'data'), { recursive: true });

// ---------- Registro en memoria (para /api/logs) ----------
const LOG = [], ERR = [];
const keep = (arr, line, max) => { arr.push(line); if (arr.length > max) arr.splice(0, arr.length - max); };
const stamp = () => new Date().toISOString().replace('T', ' ').slice(0, 19);
const _log = console.log.bind(console), _err = console.error.bind(console);
console.log = (...a) => { const l = a.join(' '); keep(LOG, `${stamp()} ${l}`, 800); _log(...a); };
console.error = (...a) => { const l = a.map(x => x?.stack || String(x)).join(' '); keep(ERR, `${stamp()} ${l}`, 300); keep(LOG, `${stamp()} ERROR ${l}`, 800); _err(...a); };
process.on('uncaughtException', e => console.error('uncaughtException', e));
process.on('unhandledRejection', e => console.error('unhandledRejection', e));

// ---------- .env ----------
const ENVF = path.join(ROOT, '.env');
if (fs.existsSync(ENVF)) {
  for (const line of fs.readFileSync(ENVF, 'utf8').split(/\r?\n/)) {
    const m = line.match(/^\s*([A-Z0-9_]+)\s*=\s*(.*)\s*$/);
    if (m && !process.env[m[1]]) process.env[m[1]] = m[2].replace(/^["']|["']$/g, '');
  }
}
if (!process.env.HELIUS_API_KEY) { console.error('Falta HELIUS_API_KEY en el archivo .env'); process.exit(1); }

const COPIERS = JSON.parse(fs.readFileSync(path.join(DIR, 'wallets.json'), 'utf8'));
const BOOT = Date.now();
const EUR_PER_SOL = Number(process.env.EUR_PER_SOL || 82);
const GOAL_EUR = Number(process.env.GOAL_EUR || 90);

// ---------- Discord (opcional) ----------
const HOOK = process.env.DISCORD_WEBHOOK || '';
const queue = [];
async function discord(text) {
  if (!HOOK) return;
  queue.push(text);
}
setInterval(async () => {
  if (!HOOK || !queue.length) return;
  const content = queue.splice(0, 8).join('\n').slice(0, 1900);
  try { await fetch(HOOK, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ content }) }); } catch {}
}, 2500);

// ---------- Cartera única ----------
// Un solo saldo (START_SOL) que copia a todas las ballenas: cada compra mete SHARED_FRACTION del saldo LIBRE.
const CF = path.join(ROOT, 'data', 'cartera.json');
const START = Number(process.env.START_SOL || 0.09);
const SHARED_FRACTION = Number(process.env.SHARED_FRACTION || 0.25);
const MAX_TRADE_SOL = Number(process.env.MAX_TRADE_SOL || 1);
const cart = fs.existsSync(CF) ? JSON.parse(fs.readFileSync(CF, 'utf8')) : { startedAt: Date.now(), start: START, cash: START, open: {}, closed: [], curve: [[Date.now(), START]] };
const saveCart = () => fs.writeFileSync(CF, JSON.stringify(cart));
const cartEquity = () => cart.cash + Object.values(cart.open).reduce((a, o) => a + o.size, 0);
function onEvent(name, m) {
  const key = `${name}:${m.mint}`;
  if (m.type === 'buy') {
    const size = Math.min(cart.cash * SHARED_FRACTION, MAX_TRADE_SOL, cart.cash - 0.003);
    if (size < 0.005) { console.log(`[cartera] sin saldo libre para copiar ${name} ${m.mint.slice(0, 6)}`); return; }
    cart.cash -= size;
    cart.open[key] = { name, mint: m.mint, size, at: Date.now(), whaleSol: m.whaleSol, route: m.route };
    console.log(`[cartera] COMPRA ${name} ${m.mint.slice(0, 6)} con ${size.toFixed(4)} SOL`);
    discord(`🟢 **${name}** compró ${m.whaleSol?.toFixed(2)} SOL de \`${m.mint.slice(0, 6)}\` → copiamos ${size.toFixed(4)} SOL (retraso ${(m.lagMs / 1000).toFixed(1)} s)`);
  } else if (m.type === 'sell') {
    const o = cart.open[key]; if (!o) return;
    delete cart.open[key];
    const back = Math.max(0, o.size * (1 + m.ret));
    cart.cash += back;
    cart.closed.push({ name, mint: m.mint, size: o.size, ret: m.ret, pnl: back - o.size, at: Date.now(), holdS: m.holdS });
    const eq = cartEquity();
    cart.curve.push([Date.now(), eq]);
    if (cart.curve.length > 5000) cart.curve = cart.curve.filter((_, i) => i % 2 === 0);
    console.log(`[cartera] VENTA ${name} ${m.mint.slice(0, 6)} ${(m.ret * 100).toFixed(1)} % | saldo ${eq.toFixed(4)} SOL`);
    discord(`${m.ret >= 0 ? '✅' : '🔻'} **${name}** vendió \`${m.mint.slice(0, 6)}\`: **${m.ret >= 0 ? '+' : ''}${(m.ret * 100).toFixed(1)} %** | cartera ${eq.toFixed(4)} SOL (${(eq * EUR_PER_SOL).toFixed(2)} €)`);
  }
  saveCart();
}

// ---------- Copiadores ----------
const KIDS = new Set();
const shutdown = () => { for (const k of KIDS) { try { k.kill(); } catch {} } process.exit(0); };
process.on('SIGTERM', shutdown); process.on('SIGINT', shutdown); process.on('disconnect', shutdown);
function start(c) {
  const env = { ...process.env, WALLET: c.wallet, STATE: `data/livecopy_${c.name}.json`, RULE_MIN: String(c.ruleMin ?? 0), RULE_MAX: String(c.ruleMax ?? 1e9), FRACTION: String(c.fraction ?? 0.25), START_SOL: String(c.startSol ?? 0.09) };
  const ch = fork(path.join(DIR, 'livecopy.mjs'), [], { env, cwd: ROOT, stdio: ['ignore', 'pipe', 'pipe', 'ipc'] });
  ch.stdout.on('data', d => { for (const l of String(d).split('\n').filter(Boolean)) console.log(`[${c.name}] ${l}`); });
  ch.stderr.on('data', d => { for (const l of String(d).split('\n').filter(Boolean)) console.error(`[${c.name}] ${l}`); });
  KIDS.add(ch);
  ch.on('message', m => { try { onEvent(c.name, m); } catch (e) { console.error('evento', e); } });
  ch.on('exit', code => { KIDS.delete(ch); console.log(`[${c.name}] se ha parado (código ${code}); relanzando en 10 s`); setTimeout(() => start(c), 10000); });
}
for (const c of COPIERS) start(c);

// ---------- Estado ----------
function readState(c) { try { return JSON.parse(fs.readFileSync(path.join(ROOT, 'data', `livecopy_${c.name}.json`), 'utf8')); } catch { return null; } }
function status() {
  const copiers = COPIERS.map(c => {
    const s = readState(c);
    const startSol = c.startSol ?? 0.09;
    if (!s) return { name: c.name, label: c.label || c.name, wallet: c.wallet, rule: [c.ruleMin, c.ruleMax], expected: c.expected ?? null, startSol, equity: startSol, closed: [], open: [], credits: 0 };
    const open = Object.entries(s.open).map(([mint, o]) => ({ mint, size: o.size, whaleSol: o.whaleSol, route: o.route || o.pool?.kind, openedAt: o.openedAt, lagMs: o.lagMs }));
    return {
      name: c.name, label: c.label || c.name, wallet: c.wallet, rule: [c.ruleMin, c.ruleMax], expected: c.expected ?? null, startSol, startedAt: s.startedAt, credits: s.credits || 0,
      equity: s.balance + open.reduce((a, o) => a + o.size, 0),
      closed: s.closed.map(x => ({ mint: x.mint, ret: x.ret, size: x.size, out: x.out, at: x.at, holdS: x.holdS, lagMs: x.lagMs, whaleSol: x.whaleSol })),
      open
    };
  });
  return {
    now: Date.now(), bootAt: BOOT, version: process.env.APP_VERSION || 'local', errors: ERR.length, eurPerSol: EUR_PER_SOL, goalEur: GOAL_EUR,
    cartera: { start: cart.start, startedAt: cart.startedAt, cash: cart.cash, equity: cartEquity(), fraction: SHARED_FRACTION, maxTrade: MAX_TRADE_SOL, open: Object.values(cart.open), closed: cart.closed.slice(-200), curve: cart.curve.slice(-1500), nClosed: cart.closed.length, wins: cart.closed.filter(x => x.ret > 0).length },
    copiers
  };
}

// ---------- Resumen en consola y Discord ----------
function summary(toDiscord) {
  const s = status();
  const lines = s.copiers.map(c => `${c.label}: x${(c.equity / c.startSol).toFixed(2)} | ${c.closed.length} cerrados, ${c.closed.filter(x => x.ret > 0).length} ganadores`);
  const eq = s.cartera.equity;
  const txt = `📊 RESUMEN ${new Date().toLocaleString('es-ES', { timeZone: 'Europe/Madrid' })} (PAPER)\nCartera única: ${eq.toFixed(4)} SOL (${(eq * EUR_PER_SOL).toFixed(2)} €, ${((eq / s.cartera.start - 1) * 100).toFixed(1)} %) | ${s.cartera.nClosed} trades, ${s.cartera.wins} ganadores\n${lines.join('\n')}`;
  console.log('\n' + txt + '\n');
  if (toDiscord) discord(txt);
}
setTimeout(() => summary(false), 30000);
setInterval(() => summary(false), 10 * 60 * 1000);
setInterval(() => summary(true), 6 * 60 * 60 * 1000);

// ---------- Panel web ----------
const PAGE = () => fs.readFileSync(path.join(DIR, 'dashboard.html'), 'utf8');
const PORT = Number(process.env.SERVER_PORT || process.env.PORT || 3000);
http.createServer((req, res) => {
  const u = new URL(req.url, 'http://x');
  if (process.env.DASH_TOKEN && u.searchParams.get('t') !== process.env.DASH_TOKEN) { res.writeHead(401, { 'Content-Type': 'text/plain; charset=utf-8' }); return res.end('Falta ?t=<token>'); }
  if (u.pathname === '/api/logs') {
    const n = Math.min(800, Number(u.searchParams.get('n') || 200));
    res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8', 'Cache-Control': 'no-store' });
    return res.end((u.searchParams.get('err') ? ERR : LOG).slice(-n).join('\n'));
  }
  if (u.pathname === '/api/status') { res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' }); return res.end(JSON.stringify(status())); }
  res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' }); res.end(PAGE());
}).listen(PORT, '0.0.0.0', () => console.log(`Panel web en el puerto ${PORT} (abre http://<ip-del-servidor>:${PORT})`));

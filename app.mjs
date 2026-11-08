// Servidor del copiador PAPER (0 € reales): lanza un copiador por ballena, lleva una CARTERA ÚNICA que copia a todas
// con el mismo dinero (lo que harías de verdad con tu saldo), sirve el panel web y, si hay webhook, avisa por Discord.
import { fork } from 'node:child_process';
import fs from 'node:fs';
import zlib from 'node:zlib';
import path from 'node:path';
import http from 'node:http';
import { fileURLToPath } from 'node:url';

const DIR = path.dirname(fileURLToPath(import.meta.url)); // código (se actualiza solo desde GitHub)
const ROOT = process.cwd();                                  // carpeta del servidor: .env y data/ (no se tocan al actualizar)
fs.mkdirSync(path.join(ROOT, 'data'), { recursive: true });

// ---------- Registro en memoria (para /api/logs) ----------
const LOG = [], ERR = [];
// Registro persistente en disco: sobrevive a reinicios/actualizaciones (data/logs.txt, rota a ~2 MB)
const LOGF = path.join(ROOT, 'data', 'logs.txt');
const NL = String.fromCharCode(10);
try { for (const l of fs.readFileSync(LOGF, 'utf8').split(NL).filter(Boolean).slice(-800)) LOG.push(l); LOG.push(`${new Date().toISOString().replace('T', ' ').slice(0, 19)} ===== reinicio (versión ${process.env.APP_VERSION || 'local'}) =====`); } catch {}
const persist = line => { try { fs.appendFileSync(LOGF, line + NL); if (fs.statSync(LOGF).size > 2e6) { const kept = fs.readFileSync(LOGF, 'utf8').split(NL).slice(-4000).join(NL); fs.writeFileSync(LOGF, kept); } } catch {} };
const keep = (arr, line, max) => { arr.push(line); if (arr.length > max) arr.splice(0, arr.length - max); };
const stamp = () => new Date().toISOString().replace('T', ' ').slice(0, 19);
const _log = console.log.bind(console), _err = console.error.bind(console);
console.log = (...a) => { const l = `${stamp()} ${a.join(' ')}`; keep(LOG, l, 800); persist(l); _log(...a); };
console.error = (...a) => { const l = a.map(x => x?.stack || String(x)).join(' '); keep(ERR, `${stamp()} ${l}`, 300); keep(LOG, `${stamp()} ERROR ${l}`, 800); persist(`${stamp()} ERROR ${l}`); _err(...a); };
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
if (!process.env.HELIUS_API_KEY) console.log('Sin HELIUS_API_KEY: se usan solo RPCs públicos gratuitos (PublicNode y el oficial de Solana).');

// Las entradas con "off": true quedan guardadas en wallets.json pero no se arrancan
const COPIERS = JSON.parse(fs.readFileSync(path.join(DIR, 'wallets.json'), 'utf8')).filter(c => !c.off);
const BOOT = Date.now();
let EUR_PER_SOL = Number(process.env.EUR_PER_SOL || 82), EUR_AT = 0, EUR_SRC = 'fijo';
// Precio real de SOL en euros cada minuto (CoinGecko gratis; Binance de respaldo). Si ambos fallan se mantiene el último.
async function eurTick() {
  try { const r = await fetch('https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=eur', { signal: AbortSignal.timeout(8000) }); const v = (await r.json())?.solana?.eur; if (v > 1) { EUR_PER_SOL = v; EUR_AT = Date.now(); EUR_SRC = 'CoinGecko'; return; } } catch {}
  try { const r = await fetch('https://api.binance.com/api/v3/ticker/price?symbol=SOLEUR', { signal: AbortSignal.timeout(8000) }); const v = Number((await r.json())?.price); if (v > 1) { EUR_PER_SOL = v; EUR_AT = Date.now(); EUR_SRC = 'Binance'; } } catch {}
}
eurTick(); setInterval(eurTick, 60000);
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
// reset.json (en el repo) puede fijar los parámetros de la cartera sin tocar el servidor: start, fraction, maxOpen, minTrade, dailyLoss
const RJ = (() => { try { return JSON.parse(fs.readFileSync(path.join(DIR, 'reset.json'), 'utf8')); } catch { return {}; } })();
const START = Number(RJ.start ?? process.env.START_SOL ?? 0.09);
const SHARED_FRACTION = Number(RJ.fraction ?? process.env.SHARED_FRACTION ?? 0.25);
const MAX_TRADE_SOL = Number(process.env.MAX_TRADE_SOL || 1);
const MAX_OPEN = Number(RJ.maxOpen ?? 99), MIN_TRADE = Number(RJ.minTrade ?? 0.005), DAILY_LOSS = Number(RJ.dailyLoss ?? 1);
// RESET_ID (en reset.json del repo): si cambia, la cartera vuelve a START_SOL. Así se puede reiniciar desde git sin tocar el servidor.
const RESET_ID = RJ.cartera || '';
const dayKey = () => new Date().toLocaleDateString('es-ES', { timeZone: 'Europe/Madrid' });
const freshCart = () => ({ startedAt: Date.now(), start: START, cash: START, open: {}, closed: [], curve: [[Date.now(), START]], resetId: RESET_ID });
let cart = fs.existsSync(CF) ? JSON.parse(fs.readFileSync(CF, 'utf8')) : freshCart();
if (RESET_ID && cart.resetId !== RESET_ID) {
  // el historial anterior se guarda aparte (no se borra nada)
  try { if (fs.existsSync(CF)) fs.copyFileSync(CF, path.join(ROOT, 'data', `cartera_${(cart.resetId || 'antigua').replace(/[^\w-]/g, '')}.json`)); } catch {}
  console.log(`[cartera] REINICIO de la cartera a ${START} SOL (reset ${RESET_ID}); historial anterior guardado aparte`); cart = freshCart(); fs.writeFileSync(CF, JSON.stringify(cart));
}
const saveCart = () => fs.writeFileSync(CF, JSON.stringify(cart));
const cartEquity = () => cart.cash + Object.values(cart.open).reduce((a, o) => a + o.size, 0);
function onEvent(name, m) {
  // solo estrategias validadas ABREN en la cartera única; las ventas de posiciones ya abiertas siempre se aplican
  if (COPIERS.find(c => c.name === name)?.cartera === false && !(m.type === 'sell' && cart.open[`${name}:${m.mint}`])) return;
  const key = `${name}:${m.mint}`;
  if (m.type === 'buy') {
    // Tope de pérdida diaria (hora de Madrid): si la cartera cae DAILY_LOSS desde el inicio del día, no abre más hasta mañana
    if (cart.day !== dayKey()) { cart.day = dayKey(); cart.dayStart = cartEquity(); cart.dayStopped = false; }
    if (!cart.dayStopped && cartEquity() < cart.dayStart * (1 - DAILY_LOSS)) { cart.dayStopped = true; console.log(`[cartera] TOPE DIARIO: −${Math.round(DAILY_LOSS * 100)} % hoy, no se abre nada más hasta mañana`); saveCart(); }
    if (cart.dayStopped) return;
    if (Object.keys(cart.open).length >= MAX_OPEN) { console.log(`[cartera] ya hay ${MAX_OPEN} posiciones abiertas, no copio ${name} ${m.mint.slice(0, 6)}`); return; }
    const size = Math.min(cart.cash * SHARED_FRACTION, MAX_TRADE_SOL, cart.cash - 0.003);
    if (size < MIN_TRADE) { console.log(`[cartera] operación de ${size.toFixed(4)} SOL por debajo del mínimo (${MIN_TRADE}), no copio ${name} ${m.mint.slice(0, 6)}`); return; }
    cart.cash -= size;
    cart.open[key] = { name, mint: m.mint, symbol: m.symbol, size, at: Date.now(), whaleSol: m.whaleSol, route: m.route };
    console.log(`[cartera] COMPRA ${name} ${m.mint.slice(0, 6)} con ${size.toFixed(4)} SOL`);
    discord(`🟢 **${name}** compró ${m.whaleSol?.toFixed(2)} SOL de \`${m.mint.slice(0, 6)}\` → copiamos ${size.toFixed(4)} SOL (retraso ${(m.lagMs / 1000).toFixed(1)} s)`);
  } else if (m.type === 'sell') {
    const o = cart.open[key]; if (!o) return;
    delete cart.open[key];
    const back = Math.max(0, o.size * (1 + m.ret));
    cart.cash += back;
    cart.closed.push({ name, mint: m.mint, symbol: o.symbol || m.symbol, size: o.size, ret: m.ret, pnl: back - o.size, at: Date.now(), holdS: m.holdS });
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
  const env = { ...process.env, WALLET: c.wallet, STATE: `data/livecopy_${c.name}.json`, RULE_MIN: String(c.ruleMin ?? 0), RULE_MAX: String(c.ruleMax ?? 1e9), MAX_RISE: String(c.maxRise ?? 1), TRAIL: String(c.trail ?? 0), TRAIL_ARM: String(c.trailArm ?? 0.4), FRACTION: String(c.fraction ?? 0.25), START_SOL: String(c.startSol ?? 0.09) };
  if (c.type === 'grad') Object.assign(env, { MAX_PREMIUM: String(c.maxPremium ?? 0.05), MIN_PREMIUM: String(c.minPremium ?? -0.35), SL_NET: String(c.slNet ?? 0.15), TP_NET: String(c.tpNet ?? 0.12), HOLD_S: String(c.holdS ?? 180) });
  env.HELIUS_FALLBACK = process.env.HELIUS_FALLBACK_KIDS === '1' ? '1' : '0'; // por defecto ningún bot gasta créditos de Helius
  if (c.env) for (const [k, v] of Object.entries(c.env)) env[k] = String(v);
  if (c.type === 'dev' || c.type === 'rec') env.FEED = 'ipc';
  if (!c.type && c.wallet) env.WS_MODE = 'ipc'; // las copias reciben los avisos del WebSocket compartido
  const ch = fork(path.join(DIR, c.script || (c.type === 'grad' ? 'gradbot.mjs' : c.type === 'dev' ? 'devbot.mjs' : c.type === 'rec' ? 'recorder.mjs' : 'livecopy.mjs')), [], { env, cwd: ROOT, stdio: ['ignore', 'pipe', 'pipe', 'ipc'] });
  ch.stdout.on('data', d => { for (const l of String(d).split('\n').filter(Boolean)) console.log(`[${c.name}] ${l}`); });
  ch.stderr.on('data', d => { for (const l of String(d).split('\n').filter(Boolean)) console.error(`[${c.name}] ${l}`); });
  KIDS.add(ch);
  ch.on('message', m => { try { onEvent(c.name, m); } catch (e) { console.error('evento', e); } });
  if (c.type === 'dev' || c.type === 'rec') FEED_KIDS.add(ch);
  if (!c.type && c.wallet) { COPY_KIDS.set(c.wallet, ch); copySub(c.wallet); }
  ch.on('exit', code => { KIDS.delete(ch); FEED_KIDS.delete(ch); console.log(`[${c.name}] se ha parado (código ${code}); relanzando en 10 s`); setTimeout(() => start(c), 10000); });
}
// ---------- WebSocket COMPARTIDO para las copias ----------
// Una sola conexión con todas las carteras suscritas (logsSubscribe 'processed'); cada aviso se reenvía a su copia.
// Usa el WebSocket oficial gratuito. Helius solo con HELIUS_COPY_WS=1 (medido: ~3.600 créditos/día con 12 carteras),
// apagado por defecto para no arriesgar la cuota mensual. La clave nunca se escribe en los registros.
const COPY_KIDS = new Map();
let cws = null, cwsReady = false, cwsId = 1000;
const cwsReq = new Map(), cwsSub = new Map(), cwsWallet = new Map();
function copySub(w) { if (!cwsReady) return; const id = ++cwsId; cwsReq.set(id, w); try { cws.send(JSON.stringify({ jsonrpc: '2.0', id, method: 'logsSubscribe', params: [{ mentions: [w] }, { commitment: 'processed' }] })); } catch {} }
function copyWs() {
  // Helius solo si se pide expresamente (HELIUS_COPY_WS=1): si se agota la cuota gratuita, se pierde todo el mes
  const helius = !!process.env.HELIUS_API_KEY && process.env.HELIUS_COPY_WS === '1';
  cws = new WebSocket(helius ? `wss://mainnet.helius-rpc.com/?api-key=${process.env.HELIUS_API_KEY}` : 'wss://api.mainnet-beta.solana.com');
  cwsReady = false; cwsSub.clear(); cwsReq.clear(); cwsWallet.clear();
  cws.onopen = () => { cwsReady = true; console.log(`[copias] WebSocket compartido conectado (${helius ? 'Helius' : 'oficial'}), ${COPY_KIDS.size} carteras`); for (const w of COPY_KIDS.keys()) copySub(w); };
  cws.onmessage = e => {
    let j; try { j = JSON.parse(String(e.data)); } catch { return; }
    if (j.id && cwsReq.has(j.id)) { const w = cwsReq.get(j.id); cwsReq.delete(j.id); if (j.result != null) { cwsSub.set(j.result, w); cwsWallet.set(w, j.result); } else console.log(`[copias] suscripción rechazada para ${w.slice(0, 6)}`); return; }
    const w = cwsSub.get(j?.params?.subscription); if (!w) return;
    const ch = COPY_KIDS.get(w); try { ch?.send({ type: 'logs', value: j.params.result.value, slot: j.params.result.context?.slot || 0, at: Date.now() }); } catch {}
  };
  cws.onclose = () => { cwsReady = false; console.log('[copias] WebSocket compartido cerrado, reconectando'); setTimeout(copyWs, 2000); };
  cws.onerror = () => { try { cws.close(); } catch {} };
}
setInterval(() => { try { if (cwsReady) cws.send('{"jsonrpc":"2.0","id":7,"method":"getHealth"}'); } catch {} }, 30000);
// Una sola conexión a PumpPortal (lanzamientos) repartida a todos los bots que la necesitan
const FEED_KIDS = new Set();
function feed() {
  const p = new WebSocket('wss://pumpportal.fun/api/data');
  p.onopen = () => { p.send(JSON.stringify({ method: 'subscribeNewToken' })); console.log('[feed] lanzamientos de pump.fun conectados'); };
  p.onmessage = e => { let m; try { m = JSON.parse(String(e.data)); } catch { return; } if (m.txType !== 'create') return; for (const k of FEED_KIDS) { try { k.send({ type: 'create', m }); } catch {} } };
  p.onclose = () => setTimeout(feed, 3000);
  p.onerror = () => { try { p.close(); } catch {} };
}
if (COPIERS.some(c => c.type === 'dev' || c.type === 'rec')) feed();
for (const c of COPIERS) start(c);
if (COPIERS.some(c => !c.type && c.wallet)) copyWs();

// ---------- Estado ----------
function readState(c) { try { return JSON.parse(fs.readFileSync(path.join(ROOT, 'data', `livecopy_${c.name}.json`), 'utf8')); } catch { return null; } }
function status() {
  const copiers = COPIERS.map(c => {
    const s = readState(c);
    const startSol = c.startSol ?? 0.09;
    if (!s) return { name: c.name, label: c.label || c.name, env: c.env || null, wallet: c.wallet, rule: [c.ruleMin, c.ruleMax], maxRise: c.maxRise ?? null, trail: c.trail ?? 0, trailArm: c.trailArm ?? 0.4, inCartera: c.cartera !== false, type: c.type, maxPremium: c.maxPremium, minPremium: c.minPremium, tpNet: c.tpNet, slNet: c.slNet, holdS: c.holdS, expected: c.expected ?? null, startSol, equity: startSol, closed: [], open: [], credits: 0 };
    const open = Object.entries(s.open).map(([mint, o]) => ({ mint, symbol: o.symbol, size: o.size, whaleSol: o.whaleSol, route: o.route || o.pool?.kind, openedAt: o.openedAt, lagMs: o.lagMs, markSol: o.markSol ?? null, markAt: o.markAt ?? null, outSoFar: o.outSoFar || 0 }));
    return {
      name: c.name, label: c.label || c.name, env: c.env || null, wallet: c.wallet, rule: [c.ruleMin, c.ruleMax], maxRise: c.maxRise ?? null, trail: c.trail ?? 0, trailArm: c.trailArm ?? 0.4, inCartera: c.cartera !== false, type: c.type, maxPremium: c.maxPremium, minPremium: c.minPremium, tpNet: c.tpNet, slNet: c.slNet, holdS: c.holdS, expected: c.expected ?? null, startSol, startedAt: s.startedAt, credits: s.credits || 0,
      equity: s.balance + open.reduce((a, o) => a + o.size, 0),
      equityLive: s.balance + open.reduce((a, o) => a + (o.markSol != null ? o.markSol : o.size), 0),
      recent: (s.recent || []).slice(-60), closed: s.closed.map(x => ({ mint: x.mint, symbol: x.symbol, devPct: x.devPct, txs: x.txs, twitter: x.twitter, telegram: x.telegram, website: x.website, ret: x.ret, size: x.size, out: x.out, at: x.at, holdS: x.holdS, lagMs: x.lagMs, whaleSol: x.whaleSol, reason: x.reason, rise: x.rise, peak: x.peak, route: x.route })),
      open
    };
  });
  return {
    now: Date.now(), bootAt: BOOT, version: process.env.APP_VERSION || 'local', errors: ERR.length, eurPerSol: EUR_PER_SOL, eurAt: EUR_AT, eurSrc: EUR_SRC, goalEur: GOAL_EUR,
    cartera: { start: cart.start, startedAt: cart.startedAt, cash: cart.cash, equity: cartEquity(), fraction: SHARED_FRACTION, maxTrade: MAX_TRADE_SOL, maxOpen: MAX_OPEN, minTrade: MIN_TRADE, dailyLoss: DAILY_LOSS, dayStopped: !!cart.dayStopped, open: Object.values(cart.open), closed: cart.closed.slice(-200), curve: cart.curve.slice(-1500), nClosed: cart.closed.length, wins: cart.closed.filter(x => x.ret > 0).length },
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
// Contraseña: PANEL_PASSWORD en .env. Sin ella el panel queda abierto.
// /api/monitor es SOLO LECTURA y pública (estado + registros) para poder revisar el bot a distancia; no permite cambiar nada.
import crypto from 'node:crypto';
const PAGE = () => fs.readFileSync(path.join(DIR, 'dashboard.html'), 'utf8');
const LOGIN = () => fs.readFileSync(path.join(DIR, 'login.html'), 'utf8');
const PORT = Number(process.env.SERVER_PORT || process.env.PORT || 3000);
const PASS = process.env.PANEL_PASSWORD || '';
const sessionValue = () => crypto.createHmac('sha256', PASS).update('panel-session-v1').digest('hex');
const cookies = req => Object.fromEntries((req.headers.cookie || '').split(';').map(c => c.trim().split('=')).filter(p => p[0]).map(([k, ...v]) => [k, decodeURIComponent(v.join('='))]));
const loggedIn = req => !PASS || cookies(req).panel === sessionValue();
const failed = new Map(); // intentos fallidos por IP (freno a fuerza bruta)
const json = (res, obj) => { res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' }); res.end(JSON.stringify(obj)); };
const logsText = (u) => { const n = Math.min(800, Number(u.searchParams.get('n') || 200)); return (u.searchParams.get('err') ? ERR : LOG).slice(-n).join('\n'); };

http.createServer((req, res) => {
  const u = new URL(req.url, 'http://x');
  const ip = req.socket.remoteAddress || '?';
  // Solo lectura, pública
  if (u.pathname === '/api/monitor') return json(res, { ...status(), logs: LOG.slice(-Math.min(400, Number(u.searchParams.get('n') || 150))), errorsList: ERR.slice(-50) });
  // Grabaciones de la máquina del tiempo (solo lectura, pública: datos de mercado). Nombre de archivo validado: sin rutas.
  if (u.pathname === '/api/rec') { const d = path.join(ROOT, 'data', 'rec'); let files = []; try { files = fs.readdirSync(d).filter(f => /^\d{4}-\d{2}-\d{2}\.jsonl$/.test(f)).map(f => ({ f, bytes: fs.statSync(path.join(d, f)).size })); } catch {} return json(res, files); }
  const recM = u.pathname.match(/^\/api\/rec\/(\d{4}-\d{2}-\d{2}\.jsonl)$/);
  if (recM) { const f = path.join(ROOT, 'data', 'rec', recM[1]); if (!fs.existsSync(f)) { res.writeHead(404); return res.end(); } res.writeHead(200, { 'Content-Type': 'application/x-ndjson', 'Content-Encoding': 'gzip', 'Cache-Control': 'no-store' }); return fs.createReadStream(f).pipe(zlib.createGzip()).pipe(res); }
  // Decisiones de dev fuerte (máquina del tiempo exacta), solo lectura y pública
  if (u.pathname === '/api/devlog') { const f = path.join(ROOT, 'data', 'dev_decisions.jsonl'); if (!fs.existsSync(f)) { res.writeHead(404); return res.end(); } res.writeHead(200, { 'Content-Type': 'application/x-ndjson', 'Content-Encoding': 'gzip', 'Cache-Control': 'no-store' }); return fs.createReadStream(f).pipe(zlib.createGzip()).pipe(res); }
  // Login
  if (u.pathname === '/login' && req.method === 'POST') {
    const f = failed.get(ip) || { n: 0, until: 0 };
    if (Date.now() < f.until) { res.writeHead(429, { 'Content-Type': 'text/plain; charset=utf-8' }); return res.end('Demasiados intentos. Espera un minuto.'); }
    let body = ''; req.on('data', c => { body += c; if (body.length > 2000) req.destroy(); });
    return req.on('end', () => {
      const pw = new URLSearchParams(body).get('password') || '';
      const ok = PASS && pw.length === PASS.length && crypto.timingSafeEqual(Buffer.from(pw), Buffer.from(PASS));
      if (!ok) { f.n++; if (f.n >= 5) { f.until = Date.now() + 60000; f.n = 0; } failed.set(ip, f); res.writeHead(302, { Location: '/?error=1' }); return res.end(); }
      failed.delete(ip);
      res.writeHead(302, { 'Set-Cookie': `panel=${sessionValue()}; Path=/; HttpOnly; SameSite=Lax; Max-Age=${30 * 86400}`, Location: '/' });
      return res.end();
    });
  }
  if (u.pathname === '/logout') { res.writeHead(302, { 'Set-Cookie': 'panel=; Path=/; Max-Age=0', Location: '/' }); return res.end(); }
  if (!loggedIn(req)) {
    if (u.pathname.startsWith('/api/')) { res.writeHead(401, { 'Content-Type': 'application/json' }); return res.end('{"error":"login"}'); }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' }); return res.end(LOGIN());
  }
  if (u.pathname === '/api/logs') { res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8', 'Cache-Control': 'no-store' }); return res.end(logsText(u)); }
  if (u.pathname === '/api/status') return json(res, { ...status(), auth: !!PASS });
  res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' }); res.end(PAGE());
}).listen(PORT, '0.0.0.0', () => console.log(`Panel web en el puerto ${PORT}${PASS ? ' (con contraseña)' : ' (SIN contraseña: añade PANEL_PASSWORD en .env)'}`));

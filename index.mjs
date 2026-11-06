// LANZADOR con autoactualización (PAPER, 0 € reales).
// Funciona de dos formas:
//  A) Servidor instalado desde Git (opción "GIT REPO ADDRESS" del panel): el repo ES esta carpeta → se actualiza aquí mismo.
//  B) Solo este archivo subido a mano + GIT_REPO en .env → descarga el bot en ./app y lo mantiene al día.
// Cada 3 minutos mira si hay cambios en GitHub; si los hay, los baja y reinicia el bot. .env y data/ nunca se tocan.
import { fork, execFileSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = path.dirname(fileURLToPath(import.meta.url));
process.chdir(ROOT);
const env = {};
if (fs.existsSync('.env')) for (const l of fs.readFileSync('.env', 'utf8').split(/\r?\n/)) { const m = l.match(/^\s*([A-Z0-9_]+)\s*=\s*(.*)\s*$/); if (m) env[m[1]] = m[2].replace(/^["']|["']$/g, ''); }
const REPO = env.GIT_REPO || process.env.GIT_REPO || 'https://github.com/Fertrix/memecoin-copier';
const BRANCH = env.GIT_BRANCH || 'main';
const IN_PLACE = fs.existsSync(path.join(ROOT, '.git')) && fs.existsSync(path.join(ROOT, 'app.mjs'));
const CODE = IN_PLACE ? ROOT : path.join(ROOT, 'app');
const log = m => console.log(`[lanzador ${new Date().toISOString().slice(11, 19)}] ${m}`);
const git = (...a) => execFileSync('git', a, { cwd: ROOT, stdio: ['ignore', 'pipe', 'pipe'], timeout: 60000 }).toString().trim();
const G = IN_PLACE ? [] : ['-C', 'app'];

function sync() {
  try {
    if (!IN_PLACE && !fs.existsSync(path.join(CODE, '.git'))) {
      log(`descargando el bot desde ${REPO}…`);
      fs.rmSync(CODE, { recursive: true, force: true });
      git('clone', '--depth', '1', '--branch', BRANCH, REPO, 'app');
      return true;
    }
    git(...G, 'fetch', '--depth', '1', IN_PLACE ? 'origin' : 'origin', BRANCH);
    const here = git(...G, 'rev-parse', 'HEAD'), there = git(...G, 'rev-parse', 'FETCH_HEAD');
    if (here === there) return false;
    git(...G, 'reset', '--hard', 'FETCH_HEAD');
    log(`actualizado a la versión ${there.slice(0, 7)}`);
    return true;
  } catch (e) { log(`no pude actualizar desde GitHub: ${String(e.stderr || e.message).slice(0, 300)}`); return false; }
}

let child = null, stopping = false;
function version() { try { return git(...G, 'rev-parse', '--short', 'HEAD'); } catch { return 'local'; } }
function startApp() {
  const entry = path.join(CODE, 'app.mjs');
  if (!fs.existsSync(entry)) { log('todavía no hay bot descargado; reintento en 30 s'); return setTimeout(() => { sync(); startApp(); }, 30000); }
  child = fork(entry, [], { cwd: ROOT, env: { ...process.env, APP_VERSION: version() } });
  log(`bot arrancado (versión ${version()}, modo ${IN_PLACE ? 'Git del panel' : 'carpeta app/'})`);
  child.on('exit', code => { child = null; if (stopping) { stopping = false; return startApp(); } log(`el bot se paró (código ${code}); reinicio en 5 s`); setTimeout(startApp, 5000); });
}
function restart() { if (child) { stopping = true; child.kill(); } else startApp(); }

sync();
startApp();
setInterval(() => { if (sync()) restart(); }, Number(env.UPDATE_SECONDS || 180) * 1000);

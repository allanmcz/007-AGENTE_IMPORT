// Rajada v2 — 2 workers concorrentes (gateway Supabase ~2 edge fn concurrent),
// jitter, retry em 429 e 504 com backoff.
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";

const SUPABASE_URL = "https://ohslwbcswmdlxxgotinq.supabase.co";
const ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im9oc2x3YmNzd21kbHh4Z290aW5xIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjA0ODQ1OTAsImV4cCI6MjA3NjA2MDU5MH0.qL68NxcKiIsAhYhYAKoCN6V7YFrk4PwceXFauMAIl4I";
const EMAIL = "allanmcz@gmail.com";
const PASSWORD = "Amma@2804";
const BASE = "/Users/allan/003_ALMIR_FERRAGENS/HOME_CENTER/SPEDs";
const WORKERS = 1;           // 1 worker sequencial — gateway Supabase em cooldown pós-504
const STAGGER_MS = 0;
const GAP_BETWEEN_UPLOADS_MS = 2000; // breather entre uploads
const INITIAL_COOLDOWN_MS = 30_000;  // aguarda gateway liberar da rajada anterior
const RETRY_AFTER_429 = 15_000;
const RETRY_AFTER_504 = 60_000;      // 504 → espera 1 min
const MAX_WAIT_PER_FILE_MS = 600_000; // 10 min total aguardando 429/504 por arquivo

async function login() {
  const r = await fetch(`${SUPABASE_URL}/auth/v1/token?grant_type=password`, {
    method: "POST",
    headers: { "Content-Type": "application/json", apikey: ANON_KEY },
    body: JSON.stringify({ email: EMAIL, password: PASSWORD }),
  });
  if (!r.ok) throw new Error("login failed: " + await r.text());
  return (await r.json()).access_token;
}

async function uploadOne(jwt, file) {
  const buf = readFileSync(file.path);
  const fd = new FormData();
  fd.append("file", new Blob([buf], { type: "text/plain" }), file.name);
  fd.append("original_size", String(buf.length));
  fd.append("compressed_size", String(buf.length));
  const t0 = Date.now();
  try {
    const resp = await fetch(`${SUPABASE_URL}/functions/v1/process-sped-async`, {
      method: "POST",
      headers: { Authorization: `Bearer ${jwt}`, apikey: ANON_KEY },
      body: fd,
    });
    const body = await resp.text();
    return { status: resp.status, body, dt: Date.now() - t0 };
  } catch (err) {
    return { status: 0, body: String(err), dt: Date.now() - t0 };
  }
}

async function pgCount(jwt, query) {
  const r = await fetch(`${SUPABASE_URL}/rest/v1/${query}`, {
    method: "HEAD",
    headers: { Authorization: `Bearer ${jwt}`, apikey: ANON_KEY, Prefer: "count=exact" }
  });
  const range = r.headers.get("content-range");
  return range ? parseInt(range.split("/")[1]) : null;
}

function collectFiles(alreadyImportedYm) {
  const all = [];
  for (const year of ["2021","2022","2023","2024","2025"]) {
    const dir = join(BASE, year);
    for (const f of readdirSync(dir)) {
      if (!f.endsWith(".txt") || !f.includes("SPED-EFD")) continue;
      const m = f.match(/-(\d{8})-\d{8}-/);
      if (!m) continue;
      const ym = m[1].substring(0, 6);
      if (alreadyImportedYm.has(ym)) continue;
      all.push({ path: join(dir, f), name: f, yyyymm: ym });
    }
  }
  all.sort((a,b) => a.yyyymm.localeCompare(b.yyyymm));
  return all;
}

const jwt = await login();

// Query banco para saber o que já foi importado (evita re-upload)
const importedRaw = await fetch(
  `${SUPABASE_URL}/rest/v1/sped_fiscal?user_id=eq.53e9674d-3cbe-4df0-be39-2ec9f2d99657&select=periodo_inicio`,
  { headers: { Authorization: `Bearer ${jwt}`, apikey: ANON_KEY } }
).then(r => r.json());
const alreadyImportedYm = new Set(
  (importedRaw || []).map(r => r.periodo_inicio.replace(/-/g, '').substring(0, 6))
);
console.log(`Já importados no banco: ${alreadyImportedYm.size} meses`);

const files = collectFiles(alreadyImportedYm);
console.log(`A importar: ${files.length} SPEDs`);
console.log(`Primeiros: ${files.slice(0,5).map(f=>f.yyyymm).join(", ")} …`);

const startT = Date.now();
const stats = { uploaded: 0, failed: 0, retried_429: 0, retried_504: 0 };
const queue = [...files];
let workersActive = WORKERS;

async function worker(id) {
  await new Promise(r => setTimeout(r, id * STAGGER_MS));

  while (queue.length > 0) {
    const file = queue.shift();
    if (!file) return;

    const deadline = Date.now() + MAX_WAIT_PER_FILE_MS;
    let attempt = 0;
    let succeeded = false;
    while (Date.now() < deadline) {
      attempt++;
      const res = await uploadOne(jwt, file);

      if (res.status === 200 || res.status === 201) {
        stats.uploaded++;
        succeeded = true;
        try {
          const json = JSON.parse(res.body);
          console.log(`[w${id}][${file.yyyymm}] ✅ chunks=${json.chunks?.created}/${json.chunks?.total} em ${res.dt}ms (tent ${attempt})`);
        } catch {
          console.log(`[w${id}][${file.yyyymm}] ✅ em ${res.dt}ms (tent ${attempt})`);
        }
        break;
      }

      if (res.status === 429) {
        stats.retried_429++;
        console.log(`[w${id}][${file.yyyymm}] 🟡 429 (tent ${attempt}) — aguardando ${RETRY_AFTER_429/1000}s`);
        await new Promise(r => setTimeout(r, RETRY_AFTER_429));
        continue;
      }

      if (res.status === 504 || res.status === 0) {
        stats.retried_504++;
        console.log(`[w${id}][${file.yyyymm}] 🟠 ${res.status || 'net'} (tent ${attempt}) em ${res.dt}ms — aguardando ${RETRY_AFTER_504/1000}s`);
        await new Promise(r => setTimeout(r, RETRY_AFTER_504));
        continue;
      }

      // Outros erros — fail definitivo (sem retry)
      stats.failed++;
      console.log(`[w${id}][${file.yyyymm}] ❌ HTTP ${res.status} — ${res.body.substring(0, 200)}`);
      break;
    }

    // Deadline estourou sem sucesso → recoloca no final da queue
    if (!succeeded && stats.failed === 0) {
      console.log(`[w${id}][${file.yyyymm}] ♻️ deadline ${MAX_WAIT_PER_FILE_MS/1000}s — recolocando no fim da queue`);
      queue.push(file);
    }

    // Breather entre uploads (mesmo sucesso) — poupa o gateway
    if (queue.length > 0) await new Promise(r => setTimeout(r, GAP_BETWEEN_UPLOADS_MS));
  }

  workersActive--;
}

// Monitor
(async () => {
  while (workersActive > 0) {
    await new Promise(r => setTimeout(r, 20_000));
    const active = await pgCount(jwt, "processing_jobs?user_id=eq.53e9674d-3cbe-4df0-be39-2ec9f2d99657&status=in.(pending,processing)");
    const completed = await pgCount(jwt, "processing_jobs?user_id=eq.53e9674d-3cbe-4df0-be39-2ec9f2d99657&status=eq.completed");
    const failed = await pgCount(jwt, "processing_jobs?user_id=eq.53e9674d-3cbe-4df0-be39-2ec9f2d99657&status=eq.failed");
    const chunksPend = await pgCount(jwt, "processing_queue?status=in.(pending,processing)");
    const elapsed = ((Date.now() - startT) / 1000).toFixed(0);
    console.log(`[mon] t+${elapsed}s — queue=${queue.length}  active=${active}  completed=${completed}  failed=${failed}  chunks_pend=${chunksPend}  up_done=${stats.uploaded}/${files.length}  r429=${stats.retried_429}  r504=${stats.retried_504}  fail=${stats.failed}`);
  }
})();

// Cooldown inicial para o gateway esfriar após rajadas recentes com 504
console.log(`Cooldown inicial: ${INITIAL_COOLDOWN_MS/1000}s antes de começar…`);
await new Promise(r => setTimeout(r, INITIAL_COOLDOWN_MS));

// Dispara workers
const workerPromises = [];
for (let i = 0; i < WORKERS; i++) workerPromises.push(worker(i + 1));
await Promise.all(workerPromises);

console.log("\n=== FIM DOS UPLOADS ===");
console.log(stats);

console.log("\nAguardando jobs pendentes concluírem…");
let lastActive = -1, sameCount = 0;
while (true) {
  const pend = await pgCount(jwt, "processing_jobs?user_id=eq.53e9674d-3cbe-4df0-be39-2ec9f2d99657&status=in.(pending,processing)");
  if (!pend || pend === 0) break;
  if (pend === lastActive) sameCount++; else { sameCount = 0; lastActive = pend; }
  if (sameCount > 20) { console.log(`  STALLED em ${pend} jobs há 5+ min, saindo…`); break; }
  console.log(`  ${pend} jobs ainda processando…`);
  await new Promise(r => setTimeout(r, 15_000));
}

console.log("\n=== VALIDAÇÃO FINAL ===");
const totalJobs = await pgCount(jwt, "processing_jobs?user_id=eq.53e9674d-3cbe-4df0-be39-2ec9f2d99657");
const compl = await pgCount(jwt, "processing_jobs?user_id=eq.53e9674d-3cbe-4df0-be39-2ec9f2d99657&status=eq.completed");
const fail = await pgCount(jwt, "processing_jobs?user_id=eq.53e9674d-3cbe-4df0-be39-2ec9f2d99657&status=eq.failed");
console.log(`Total jobs: ${totalJobs}  completed: ${compl}  failed: ${fail}`);
console.log(`Duração total: ${((Date.now() - startT) / 60000).toFixed(1)} min`);

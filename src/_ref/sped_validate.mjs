// Validação final pós-rajada: confirma E100/E110/E200/E210 persistiram
// em todos os SPEDs, zero jobs órfãos, zero chunks stuck.
const SUPABASE_URL = "https://ohslwbcswmdlxxgotinq.supabase.co";
const ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im9oc2x3YmNzd21kbHh4Z290aW5xIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjA0ODQ1OTAsImV4cCI6MjA3NjA2MDU5MH0.qL68NxcKiIsAhYhYAKoCN6V7YFrk4PwceXFauMAIl4I";
const EMAIL = "allanmcz@gmail.com";
const PASSWORD = "Amma@2804";
const USER_ID = "53e9674d-3cbe-4df0-be39-2ec9f2d99657";

async function login() {
  const r = await fetch(`${SUPABASE_URL}/auth/v1/token?grant_type=password`, {
    method: "POST",
    headers: { "Content-Type": "application/json", apikey: ANON_KEY },
    body: JSON.stringify({ email: EMAIL, password: PASSWORD }),
  });
  return (await r.json()).access_token;
}

async function pgCount(jwt, query) {
  const r = await fetch(`${SUPABASE_URL}/rest/v1/${query}`, {
    method: "HEAD",
    headers: { Authorization: `Bearer ${jwt}`, apikey: ANON_KEY, Prefer: "count=exact" }
  });
  const range = r.headers.get("content-range");
  return range ? parseInt(range.split("/")[1]) : null;
}

const jwt = await login();

console.log("=== VALIDAÇÃO FINAL — SPEDs OLIVEIRA ===\n");

// 1. Contagem de jobs
const jobsTotal = await pgCount(jwt, `processing_jobs?user_id=eq.${USER_ID}`);
const jobsCompleted = await pgCount(jwt, `processing_jobs?user_id=eq.${USER_ID}&status=eq.completed`);
const jobsFailed = await pgCount(jwt, `processing_jobs?user_id=eq.${USER_ID}&status=eq.failed`);
const jobsActive = await pgCount(jwt, `processing_jobs?user_id=eq.${USER_ID}&status=in.(pending,processing)`);
const orphanPlaceholders = await pgCount(jwt, `processing_jobs?user_id=eq.${USER_ID}&status=in.(pending,processing)&sped_fiscal_id=is.null`);

console.log(`processing_jobs:  total=${jobsTotal}  completed=${jobsCompleted}  failed=${jobsFailed}  active=${jobsActive}  orphan_placeholders=${orphanPlaceholders}`);

// 2. SPEDs por período
const fiscalRows = await fetch(
  `${SUPABASE_URL}/rest/v1/sped_fiscal?user_id=eq.${USER_ID}&select=id,periodo_inicio,nome_arquivo&order=periodo_inicio.asc`,
  { headers: { Authorization: `Bearer ${jwt}`, apikey: ANON_KEY } }
).then(r => r.json());

console.log(`\nsped_fiscal: ${fiscalRows.length} SPEDs importados`);
console.log(`Períodos: ${fiscalRows[0]?.periodo_inicio} → ${fiscalRows[fiscalRows.length-1]?.periodo_inicio}`);

// 3. Bloco E — por SPED, confirmar que tem pelo menos 1 E100 e 1 E110
const missingE = [];
const spedSummary = [];
for (const sped of fiscalRows) {
  const e100 = await pgCount(jwt, `sped_e100?sped_fiscal_id=eq.${sped.id}`);

  // Buscar e100 ids para E110/E200
  const e100s = await fetch(
    `${SUPABASE_URL}/rest/v1/sped_e100?sped_fiscal_id=eq.${sped.id}&select=id`,
    { headers: { Authorization: `Bearer ${jwt}`, apikey: ANON_KEY } }
  ).then(r => r.json());
  const e100Ids = e100s.map(r => r.id);

  let e110 = 0, e200 = 0, e210 = 0;
  if (e100Ids.length) {
    e110 = await pgCount(jwt, `sped_e110?sped_e100_id=in.(${e100Ids.join(",")})`);
    e200 = await pgCount(jwt, `sped_e200?sped_e100_id=in.(${e100Ids.join(",")})`);
    if (e200 > 0) {
      const e200s = await fetch(
        `${SUPABASE_URL}/rest/v1/sped_e200?sped_e100_id=in.(${e100Ids.join(",")})&select=id`,
        { headers: { Authorization: `Bearer ${jwt}`, apikey: ANON_KEY } }
      ).then(r => r.json());
      if (e200s.length) e210 = await pgCount(jwt, `sped_e210?sped_e200_id=in.(${e200s.map(r=>r.id).join(",")})`);
    }
  }

  const ym = sped.periodo_inicio.substring(0, 7);
  spedSummary.push({ ym, e100, e110, e200, e210 });
  if (e100 === 0 || e110 === 0) missingE.push(ym);
}

console.log(`\nBloco E por SPED:`);
console.log("Período     E100  E110  E200  E210");
for (const s of spedSummary) {
  const mark = (s.e100 === 0 || s.e110 === 0) ? " ⚠️" : "";
  console.log(`${s.ym.padEnd(12)}${String(s.e100).padStart(4)}${String(s.e110).padStart(6)}${String(s.e200).padStart(6)}${String(s.e210).padStart(6)}${mark}`);
}

// 4. Totais
const totalE100 = await pgCount(jwt, `sped_e100?estabelecimento_id=eq.48f4edf1-8d4f-483e-82ad-7c06a601ed91`);
const totalE110 = await pgCount(jwt, `sped_e110?estabelecimento_id=eq.48f4edf1-8d4f-483e-82ad-7c06a601ed91`);
const totalE200 = await pgCount(jwt, `sped_e200?estabelecimento_id=eq.48f4edf1-8d4f-483e-82ad-7c06a601ed91`);
const totalE210 = await pgCount(jwt, `sped_e210?estabelecimento_id=eq.48f4edf1-8d4f-483e-82ad-7c06a601ed91`);

console.log(`\nTotais OLIVEIRA: E100=${totalE100}  E110=${totalE110}  E200=${totalE200}  E210=${totalE210}`);

// 5. Chunks stuck
const chunksPending = await pgCount(jwt, "processing_queue?status=in.(pending,processing)");
const chunksFailed = await pgCount(jwt, "processing_queue?status=eq.failed");
console.log(`\nprocessing_queue: pending/processing=${chunksPending}  failed=${chunksFailed}`);

// 6. Diagnóstico
console.log("\n=== DIAGNÓSTICO ===");
if (missingE.length) console.log(`❌ ${missingE.length} SPEDs sem bloco E: ${missingE.join(", ")}`);
else console.log("✅ Todos os SPEDs têm bloco E populado");

if (orphanPlaceholders > 0) console.log(`❌ ${orphanPlaceholders} placeholders órfãos (sped_fiscal_id=NULL)`);
else console.log("✅ Zero placeholders órfãos");

if (jobsFailed > 0) console.log(`⚠️ ${jobsFailed} jobs com status=failed — inspecionar`);
else console.log("✅ Zero jobs failed");

if (chunksPending > 0) console.log(`⏳ ${chunksPending} chunks ainda processando`);
if (chunksFailed > 0) console.log(`⚠️ ${chunksFailed} chunks failed`);

// 7. Comparar com o esperado (60 arquivos da Oliveira)
const EXPECTED_MONTHS = 60;
if (fiscalRows.length === EXPECTED_MONTHS) console.log(`✅ ${fiscalRows.length}/${EXPECTED_MONTHS} SPEDs importados`);
else console.log(`⚠️ ${fiscalRows.length}/${EXPECTED_MONTHS} SPEDs importados — ${EXPECTED_MONTHS - fiscalRows.length} faltando`);

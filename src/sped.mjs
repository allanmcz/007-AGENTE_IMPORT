import { readFile } from "node:fs/promises";
import { invokeFn, query } from "./supabase.mjs";

export async function runSpedBatch(speds, { jwt, userId, supabaseUrl, anonKey, config }) {
  const RETRY_AFTER_429 = 15_000;
  const RETRY_AFTER_504 = 60_000;
  
  const concurrency = config?.spedConcurrency || 1;
  const gapMs = config?.spedGapMs || 2000;
  const cooldownMs = config?.spedCooldownMs || 30000;
  const deadlineMs = config?.spedDeadlineMs || 600000;

  const stats = { uploaded: 0, failed: 0, retried429: 0, retried504: 0, jobs: [] };

  if (speds.length === 0) return stats;

  const context = { jwt, anonKey, supabaseUrl };

  // 1. Descobrir o que já tem no banco (para economizar banda)
  let importedYm = new Set();
  try {
    const data = await query("sped_fiscal", `user_id=eq.${userId}&select=periodo_inicio`, context);
    if (data && Array.isArray(data)) {
      data.forEach(r => {
         if (r.periodo_inicio) {
           importedYm.add(r.periodo_inicio.replace(/-/g, '').substring(0, 6)); // YYYYMM
         }
      });
    }
  } catch (err) {
    console.warn(`[SPED] Aviso: Não foi possível checar SPEDs já importados no banco. Tentando upload de todos. (${err.message})`);
  }

  // 2. Filtra queue
  const queue = [];
  for (const sped of speds) {
    // periodoInicio do txt (DDMMAAAA) -> converte pra AAAAMM pra match com banco
    const pIni = sped.periodoInicio; // Ex: 01012023
    let yyyymm = null;
    if (pIni && pIni.length === 8) {
      yyyymm = pIni.substring(4, 8) + pIni.substring(2, 4);
    }
    
    if (yyyymm && importedYm.has(yyyymm)) {
      console.log(`[SPED] Pulando ${sped.name} (${yyyymm}) - já existe no banco.`);
      continue;
    }
    queue.push({ ...sped, yyyymm: yyyymm || sped.periodoInicio || sped.name });
  }

  if (queue.length === 0) {
    console.log("[SPED] Nenhum SPED novo para upload.");
    return stats;
  }

  console.log(`[SPED] Iniciando upload de ${queue.length} SPEDs. (Cooldown de ${cooldownMs/1000}s)`);
  if (cooldownMs > 0) {
    await new Promise(r => setTimeout(r, cooldownMs));
  }

  async function worker(id) {
    while (queue.length > 0) {
      const file = queue.shift();
      const deadline = Date.now() + deadlineMs;
      let attempt = 0;
      let succeeded = false;

      while (Date.now() < deadline) {
        attempt++;
        let buf = null;
        try {
          buf = await readFile(file.path);
        } catch (e) {
          console.error(`[SPED][w${id}] Erro ao ler arquivo no disco: ${file.path}`);
          stats.failed++;
          break;
        }

        const fd = new FormData();
        fd.append("file", new Blob([buf], { type: "text/plain" }), file.name);
        fd.append("original_size", String(buf.length));
        fd.append("compressed_size", String(buf.length));

        const t0 = Date.now();
        const res = await invokeFn("process-sped-async", {
          method: "POST",
          body: fd,
          ...context
        });
        const dt = Date.now() - t0;

        if (res.status === 200 || res.status === 201 || res.status === 204) {
          stats.uploaded++;
          succeeded = true;
          let jobInfoStr = "✅";
          if (res.body?.job_id) {
            stats.jobs.push(res.body.job_id);
            jobInfoStr = `✅ job_id=${res.body.job_id} chunks=${res.body.chunks?.created}/${res.body.chunks?.total}`;
          }
          console.log(`[SPED][w${id}][${file.yyyymm}] ${jobInfoStr} em ${dt}ms (tent ${attempt})`);
          break;
        }

        if (res.status === 429) {
          stats.retried429++;
          console.log(`[SPED][w${id}][${file.yyyymm}] 🟡 429 (tent ${attempt}) — aguardando ${RETRY_AFTER_429/1000}s`);
          await new Promise(r => setTimeout(r, RETRY_AFTER_429));
          continue;
        }

        if (res.status === 504 || res.status >= 500 || res.status === 0) {
          stats.retried504++;
          console.log(`[SPED][w${id}][${file.yyyymm}] 🟠 ${res.status || 'net'} (tent ${attempt}) em ${dt}ms — aguardando ${RETRY_AFTER_504/1000}s`);
          await new Promise(r => setTimeout(r, RETRY_AFTER_504));
          continue;
        }

        stats.failed++;
        console.log(`[SPED][w${id}][${file.yyyymm}] ❌ HTTP ${res.status} — ${JSON.stringify(res.body).substring(0, 200)}`);
        break;
      }

      if (!succeeded && stats.failed === 0) {
        console.log(`[SPED][w${id}][${file.yyyymm}] ♻️ deadline alcançado — recolocando no fim da fila`);
        queue.push(file);
      }

      if (queue.length > 0 && gapMs > 0) {
        await new Promise(r => setTimeout(r, gapMs));
      }
    }
  }

  const workers = Array.from({ length: concurrency }, (_, i) => worker(i + 1));
  await Promise.all(workers);

  return stats;
}

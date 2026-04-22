import "dotenv/config";
import { login, listEstabelecimentos } from "./supabase.mjs";
import { discover } from "./discovery.mjs";
import { buildPlan, executePlan } from "./orchestrator.mjs";

async function main() {
  const args = process.argv.slice(2);
  let path = null;
  let dryRun = false;
  let year = null;
  let only = null;

  for (const arg of args) {
    if (arg === "--dry-run") {
      dryRun = true;
    } else if (arg.startsWith("--year=")) {
      year = arg.split("=")[1];
    } else if (arg.startsWith("--only=")) {
      only = arg.split("=")[1];
    } else if (!arg.startsWith("--")) {
      path = arg;
    }
  }

  // Fallback para ROOT_PATH
  if (!path) {
    path = process.env.IMPORT_ROOT_PATH;
  }

  if (!path) {
    console.error("\x1b[31m[ERRO] Caminho não especificado.\x1b[0m");
    console.error("Uso: node src/cli.mjs <caminho> [--dry-run] [--year=2024] [--only=sped|xml]");
    process.exit(1);
  }

  const email = process.env.SUPABASE_EMAIL;
  const password = process.env.SUPABASE_PASSWORD;
  const supabaseUrl = process.env.SUPABASE_URL;
  const anonKey = process.env.SUPABASE_ANON_KEY;

  if (!email || !password || !supabaseUrl || !anonKey) {
    console.error("\x1b[31m[ERRO] Variáveis de ambiente incompletas (SUPABASE_EMAIL, SUPABASE_PASSWORD, SUPABASE_URL, SUPABASE_ANON_KEY).\x1b[0m");
    process.exit(1);
  }

  const config = {
    spedConcurrency: parseInt(process.env.SPED_CONCURRENCY || "1"),
    xmlConcurrency: parseInt(process.env.XML_CONCURRENCY || "3"),
    spedGapMs: parseInt(process.env.SPED_GAP_MS || "2000"),
    spedCooldownMs: parseInt(process.env.SPED_COOLDOWN_MS || "30000"),
    spedDeadlineMs: parseInt(process.env.SPED_DEADLINE_MS || "600000")
  };

  try {
    console.log(`\x1b[36m[CLI] Autenticando com Supabase (${email})...\x1b[0m`);
    const { jwt, userId } = await login({ email, password, supabaseUrl, anonKey });

    const context = { jwt, userId, supabaseUrl, anonKey, config };

    console.log("\x1b[36m[CLI] Carregando estabelecimentos autorizados...\x1b[0m");
    const estabelecimentos = await listEstabelecimentos(context);
    console.log(`[CLI] ${estabelecimentos.length} estabelecimentos carregados.`);

    console.log(`\n\x1b[36m[DISCOVER] Varrrendo sistema de arquivos: ${path}...\x1b[0m`);
    const discovery = await discover(path, estabelecimentos);
    
    console.log(`  - ${discovery.speds.length} SPEDs encontrados.`);
    console.log(`  - ${discovery.xmlZips.length} Arquivos ZIP encontrados.`);
    console.log(`  - ${discovery.xmlFiles.length} XMLs soltos encontrados.`);
    if (discovery.unknown.length > 0) {
      console.log(`  - ${discovery.unknown.length} Arquivos ignorados (Formato inválido ou irrelevante).`);
    }

    console.log(`\n\x1b[36m[PLAN] Montando plano de execução...\x1b[0m`);
    const plan = buildPlan(discovery, { year, only, estabelecimentos });

    if (plan.length === 0) {
      console.log("\x1b[33m[PLAN] Nenhum arquivo atendeu aos filtros fornecidos ou cruzamento falhou.\x1b[0m");
      process.exit(0);
    }

    if (dryRun) {
      console.log(`\n\x1b[33m[DRY-RUN ativado] - PLANO GERADO (${plan.length} PASSOS)\x1b[0m`);
      for (let i = 0; i < plan.length; i++) {
         const p = plan[i];
         const infoArch = p.archType === "batch" || p.archType === "files" ? `(${p.arquivos.length} itens)` : `(${p.arquivo.name})`;
         console.log(`  ${i+1}. [${p.estabelecimento.razao_social}] >> Enviar ${p.tipo.toUpperCase()} ${infoArch}`);
      }
      console.log("\n[DRY-RUN] Execução abortada conforme solicitado. Nenhum envio feito ao banco.");
      process.exit(0);
    }

    await executePlan(plan, context);

  } catch (error) {
    console.error(`\n\x1b[31m[FALHA FATAL]\x1b[0m ${error.message}`);
    process.exit(1);
  }
}

main();

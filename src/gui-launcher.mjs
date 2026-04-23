import { execSync, spawn } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = fileURLToPath(new URL(".", import.meta.url));
const ROOT_DIR = path.resolve(__dirname, "..");

function runAppleScript(script) {
  try {
    return execSync(`osascript -e '${script}'`, { stdio: ["pipe", "pipe", "ignore"] })
      .toString()
      .trim();
  } catch (error) {
    // Caso o usuário aperte em "Cancelar"
    return null;
  }
}

function main() {
  console.log("Abrindo prompt do sistema operacional...");

  // Passo 1: Obter a pasta
  const folderScript = `set folderPath to POSIX path of (choose folder with prompt "Selecione a pasta raiz contendo os arquivos fiscais do cliente:")\nreturn folderPath`;
  const folderPath = runAppleScript(folderScript);

  if (!folderPath) {
    console.log("Operação cancelada pelo usuário (Seleção de Pasta).");
    process.exit(0);
  }

  // Passo 2: Selecionar o modo (Teste ou Produção)
  const modeScript = `choose from list {"1. MODO TESTE (Simular apenas | Dry-Run)", "2. MODO PRODUÇÃO (Importação real para o Banco)"} with prompt "Como deseja iniciar essa carga?" default items {"1. MODO TESTE (Simular apenas | Dry-Run)"} with title "Modo de Execução"`;
  let modeChoice = runAppleScript(modeScript);

  if (!modeChoice || modeChoice === "false") {
    console.log("Operação cancelada pelo usuário (Seleção de Modo).");
    process.exit(0);
  }

  const args = [folderPath];
  let isDryRun = true;

  if (modeChoice.startsWith("1")) {
    args.push("--dry-run");
  } else {
    isDryRun = false;
  }

  // Passo 3: Preparar Log file e Detached child process
  const data = new Date().toISOString().slice(0, 10);
  const logFilePath = path.join(ROOT_DIR, `agente_importacao_${data}.log`);
  
  fs.appendFileSync(logFilePath, `\n\n============================================\n[${new Date().toISOString()}] NOVA SESSÃO INICIADA\nPasta: ${folderPath}\nModo: ${isDryRun ? 'DRY-RUN (Simulação)' : 'PRODUÇÃO'}\n============================================\n\n`);

  const out = fs.openSync(logFilePath, "a");
  const err = fs.openSync(logFilePath, "a");

  const childPath = path.join(ROOT_DIR, "src", "cli.mjs");
  
  const child = spawn("node", [childPath, ...args], {
    detached: true,
    stdio: ["ignore", out, err],
    cwd: ROOT_DIR
  });

  child.unref(); // Permite que o laucher se encerre enquanto o CLI trabalha livremente
  
  // Passo 4: Feedback final ao usuário
  const successScript = `display dialog "O Orquestrador foi iniciado no modo em background (oculto).\\n\\nModo: ${isDryRun ? 'Simulação (Dry-Run)' : 'Produção Real'}\\n\\nPode fechar o seu terminal se quiser. Acompanhe os resultados abindo o arquivo de texto [agente_importacao_${data}.log] no seu Finder ou editor." buttons {"Estou ciente"} default button "Estou ciente" with title "Execução Pronta" with icon note`;
  
  runAppleScript(successScript);
  
  console.log(`Processo engatilhado! PID do agente background: ${child.pid}`);
  console.log(`Consulte ${logFilePath} para ver a atividade.`);
  process.exit(0);
}

main();

import { readdir, stat, readFile } from "node:fs/promises";
import { join, basename, extname } from "node:path";
import { execSync } from "node:child_process";
import { classifyXml } from "./classify-xml.mjs";

/**
 * Faz varredura recursiva da pasta e categoriza todos os arquivos
 * @param {string} rootPath Pasta origem
 * @param {Array<{cnpj: string}>} estabelecimentos Lista do backend para classificar (entrada/saída)
 * @returns {Promise<Object>}
 */
export async function discover(rootPath, estabelecimentos = []) {
  const result = {
    speds: [],
    xmlZips: [],
    xmlFiles: [],
    rarFiles: [],
    unknown: [],
  };

  async function walk(dir) {
    let entries;
    try {
      entries = await readdir(dir);
    } catch {
      return; // directory not readable or doesnt exist
    }

    for (const entry of entries) {
      const fullPath = join(dir, entry);
      let fileStat;
      try {
        fileStat = await stat(fullPath);
      } catch {
        continue;
      }

      if (fileStat.isDirectory()) {
        await walk(fullPath);
        continue;
      }

      const lCname = entry.toLowerCase();
      const ext = extname(lCname);
      const sizeAndName = {
        path: fullPath,
        name: entry,
        size: fileStat.size,
      };

      if (ext === ".rar") {
        result.rarFiles.push(sizeAndName);
        console.warn(`\x1b[33m[WARNING]\x1b[0m Arquivo RAR ignorado: ${fullPath}`);
        continue;
      }

      if (ext === ".txt") {
        // Pule arquivos vazios
        if (fileStat.size === 0) {
           result.unknown.push({ path: fullPath, reason: "TXT vazio" });
           continue;
        }

        // Tenta ler o comecinho para ver se é SPED
        // Como o TXT pode ser grande, podemos ler os primeiros KB apenas, 
        // mas readFile total geralmente é bem ok. Para performance extra em arquivos GIGANTES poderíamos usar streams.
        // Já que CLI admin com Node.js costuma guentar SPEDs (máx 100-200MB de texto) de forma OK.
        // Vamos ler as primeiras linhas
        let head = "";
        try {
          const { stdout } = execSync(`head -n 5 "${fullPath}"`);
          head = stdout.toString();
        } catch (err) {
           result.unknown.push({ path: fullPath, reason: "Falha ao ler primeiras linhas do TXT (head falhou)" });
           continue;
        }

        const lines = head.split(/\r?\n/);
        let isSped = false;
        
        for (const line of lines) {
          if (line.includes("|0000|")) {
            isSped = true;
            const parts = line.split("|");
            // 0 = "", 1 = "0000", ... 4 = dt_ini, 5 = dt_fin, 7 = cnpj
            if (parts.length >= 8) {
              const dtIni = parts[4];
              const dtFin = parts[5];
              const cnpjBruto = parts[7];
              const cnpjLocal = (cnpjBruto || "").replace(/\D/g, "");

              result.speds.push({
                ...sizeAndName,
                cnpj: cnpjLocal,
                periodoInicio: dtIni,
                periodoFim: dtFin
              });
            } else {
              result.unknown.push({ path: fullPath, reason: "TXT tem |0000| mas colunas insuficientes" });
            }
            break;
          }
        }
        
        if (!isSped) {
          result.unknown.push({ path: fullPath, reason: "TXT não é tipo SPED (sem |0000|)" });
        }
        continue;
      }

      if (ext === ".xml") {
        // Tenta ler o conteúdo
        let xmlContent = "";
        try {
          xmlContent = await readFile(fullPath, "utf8");
        } catch {
          result.unknown.push({ path: fullPath, reason: "Falha na leitura do arquivo XML" });
          continue;
        }
        
        const metadata = classifyXml(xmlContent, estabelecimentos);
        if (metadata) {
          result.xmlFiles.push({
            ...sizeAndName,
            tipo: metadata.tipo,
            cnpjEstab: metadata.emitCnpj,
            year: metadata.ano
          });
        } else {
          result.unknown.push({ path: fullPath, reason: "XML não formatado como NF-e/NFC-e" });
        }
        continue;
      }

      if (ext === ".zip") {
        // Puxar primeiro XML dentro do zip
        try {
          // Lista arquivos do zip
          const zipContent = execSync(`unzip -Z1 "${fullPath}"`, { maxBuffer: 1024 * 1024 * 10 }).toString();
          const zipLines = zipContent.split(/\r?\n/);
          let firstXmlName = zipLines.find(n => n.toLowerCase().endsWith(".xml"));
          
          if (!firstXmlName) {
            result.unknown.push({ path: fullPath, reason: "ZIP sem nenhum XML dentro" });
            continue;
          }

          // Lê o primeiro XML
          const xmlRawStr = execSync(`unzip -p "${fullPath}" "${firstXmlName}"`, { maxBuffer: 1024 * 1024 * 50 }).toString();
          
          let metadata = classifyXml(xmlRawStr, estabelecimentos);

          // Tentar deduzir o ano do path, caso fallback
          let yearFallback = null;
          const matchYear = fullPath.match(/\b(20\d{2})\b/);
          if (matchYear) yearFallback = matchYear[1];

          if (metadata) {
            // No caso do ZIP inteiro, muitas vezes é de um mês ou ano. O CNPJ é o emitente geral? Depende.
            result.xmlZips.push({
              ...sizeAndName,
              tipo: metadata.tipo,
              cnpjEstab: metadata.emitCnpj,
              year: metadata.ano || yearFallback
            });
          } else {
            result.unknown.push({ path: fullPath, reason: "Primeiro arquivo do ZIP não tem cara de NF-e válido para classificação." });
          }

        } catch (err) {
          result.unknown.push({ path: fullPath, reason: "Falha ao escanear interior do ZIP (talvez corrompido)" });
        }
        continue;
      }

      // fallback
      result.unknown.push({ path: fullPath, reason: `Extensão não suportada: ${ext}` });
    }
  }

  await walk(rootPath);

  return result;
}

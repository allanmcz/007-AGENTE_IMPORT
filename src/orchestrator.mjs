import { runSpedBatch } from "./sped.mjs";
import { runXmlZip, runXmlFile } from "./xml.mjs";

export function buildPlan(discovery, { year, only, estabelecimentos }) {
  const planItems = [];

  // Cria lookup rápido de estabelecimento por CNPJ
  const estabByCnpj = {};
  for (const e of estabelecimentos) {
    estabByCnpj[e.cnpj] = e;
  }

  const matchesFilter = (fYear, type) => {
    if (year && String(year) !== String(fYear)) return false;
    if (only) {
      if (only === "sped" && type !== "sped") return false;
      if (only === "xml" && type === "sped") return false;
    }
    return true;
  };

  // Separa e agrupa arquivos por estabelecimento
  const agrupado = {};

  const bindToEstab = (cnpj, item, type, fileGroup) => {
    if (!cnpj) return; // ignora se não tem cnpj (muito raro)
    const estab = estabByCnpj[cnpj];
    if (!estab) {
      // Arquivo pertence a um estabelecimento não gerido? 
      // Pode ser um erro ou filial não cadastrada. Ignoramos ou botamos num bucket genérico? 
      // O requisito diz agrupar por (CNPJ). Vamos ignorar por segurança com warning no log.
      return; 
    }
    
    if (!agrupado[estab.id]) {
      agrupado[estab.id] = {
        estabelecimento: estab,
        sped: [],
        nfeEntradaTextos: [],
        nfeSaidaTextos: [],
        nfceTextos: [],
        zips: []
      };
    }
    agrupado[estab.id][fileGroup].push(item);
  };

  if (!only || only === "sped") {
    for (const sped of discovery.speds) {
       // extrair ano aproximado pelo periodoInicio
       let spedYear = null;
       if (sped.periodoInicio && sped.periodoInicio.length >= 8) {
         spedYear = sped.periodoInicio.substring(4, 8);
       }
       if (!matchesFilter(spedYear, "sped")) continue;
       
       bindToEstab(sped.cnpj, sped, "sped", "sped");
    }
  }

  if (!only || only === "xml") {
    // Zips
    for (const zip of discovery.xmlZips) {
      if (!matchesFilter(zip.year, "xml")) continue;
      bindToEstab(zip.cnpjEstab, zip, "xml", "zips");
    }

    // Xmls soltos
    for (const xml of discovery.xmlFiles) {
      if (!matchesFilter(xml.year, "xml")) continue;
      
      let group = "";
      if (xml.tipo === "nfe-entrada") group = "nfeEntradaTextos";
      else if (xml.tipo === "nfe-saida") group = "nfeSaidaTextos";
      else if (xml.tipo === "nfce-saida") group = "nfceTextos";
      
      if (group) bindToEstab(xml.cnpjEstab, xml, "xml", group);
    }
  }

  // Agora vamos achatar na ordem correta
  for (const estabId in agrupado) {
    const data = agrupado[estabId];

    // 1. SPEDs - ordena cronologicamente
    if (data.sped.length > 0) {
      data.sped.sort((a, b) => {
         const d1 = a.periodoInicio || "";
         const d2 = b.periodoInicio || "";
         return d1.localeCompare(d2);
      });
      planItems.push({
        tipo: "sped",
        archType: "batch",
        arquivos: data.sped,
        estabelecimento: data.estabelecimento
      });
    }

    // 2. XMLs - O agrupamento não precisa ser cronológico estrito,
    // mas a ordem de tipologia é: zip (pois mescla), dps nfe entrada, nfe saida, nfce

    if (data.zips.length > 0) {
       // Processa zips primeiro 
       data.zips.forEach(z => {
         planItems.push({
            tipo: z.tipo || "misto", 
            archType: "zip",
            arquivo: z,
            estabelecimento: data.estabelecimento
         });
       });
    }

    if (data.nfeEntradaTextos.length > 0) {
      planItems.push({ tipo: "nfe-entrada", archType: "files", arquivos: data.nfeEntradaTextos, estabelecimento: data.estabelecimento });
    }
    if (data.nfeSaidaTextos.length > 0) {
      planItems.push({ tipo: "nfe-saida", archType: "files", arquivos: data.nfeSaidaTextos, estabelecimento: data.estabelecimento });
    }
    if (data.nfceTextos.length > 0) {
      planItems.push({ tipo: "nfce-saida", archType: "files", arquivos: data.nfceTextos, estabelecimento: data.estabelecimento });
    }
  }

  return planItems;
}

export async function executePlan(plan, context) {
  let totals = {
    spedUploaded: 0,
    spedFailed: 0,
    xmlInserted: 0,
    xmlDuplicates: 0,
    xmlErrors: 0
  };

  const startTime = Date.now();
  console.log(`\n\x1b[36m=== INICIANDO EXECUÇÃO DO PLANO (${plan.length} PASSOS) ===\x1b[0m\n`);

  for (let i = 0; i < plan.length; i++) {
    const item = plan[i];
    const eName = item.estabelecimento.razao_social;
    console.log(`\x1b[35m[PASSO ${i + 1}/${plan.length}]\x1b[0m \x1b[33m${eName}\x1b[0m - Tipo: ${item.tipo}`);

    const runContext = {
       ...context,
       estabelecimentoId: item.estabelecimento.id,
       cnpjEstab: item.estabelecimento.cnpj
    };

    if (item.tipo === "sped" && item.archType === "batch") {
       const res = await runSpedBatch(item.arquivos, runContext);
       totals.spedUploaded += res.uploaded;
       totals.spedFailed += res.failed;
    } 
    else if (item.archType === "zip") {
       console.log(`[ZIP] Extraindo e processando ${item.arquivo.name}...`);
       const res = await runXmlZip(item.arquivo.path, runContext);
       totals.xmlInserted += res.inserted;
       totals.xmlDuplicates += res.duplicates;
       totals.xmlErrors += res.errors + res.parseErrors;
    }
    else if (item.archType === "files") {
       console.log(`[XML] Processando ${item.arquivos.length} arquivos soltos de ${item.tipo}...`);
       for (const f of item.arquivos) {
         const res = await runXmlFile(f.path, runContext);
         totals.xmlInserted += res.inserted;
         totals.xmlDuplicates += res.duplicates;
         totals.xmlErrors += res.errors + res.parseErrors;
       }
    }
  }

  const dur = ((Date.now() - startTime) / 1000).toFixed(1);

  console.log(`\n\x1b[36m=== EXECUÇÃO CONCLUÍDA EM ${dur}s ===\x1b[0m`);
  console.log(`SPEDs:`);
  console.log(`   Upados: ${totals.spedUploaded}`);
  console.log(`   Falhas: ${totals.spedFailed}`);
  console.log(`XMLs NF-e/NFC-e:`);
  console.log(`   Inseridos:  ${totals.xmlInserted}`);
  console.log(`   Duplicados: ${totals.xmlDuplicates}`);
  console.log(`   Erros:      ${totals.xmlErrors}`);
}

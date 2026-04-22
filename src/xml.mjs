import { readFile, rm, mkdir, readdir, stat } from "node:fs/promises";
import { join } from "node:path";
import { execSync } from "node:child_process";
import { XMLParser } from "fast-xml-parser";
import { rpc } from "./supabase.mjs";

const parser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: "@_",
  parseAttributeValue: false,
  parseTagValue: false,
  trimValues: true,
  allowBooleanAttributes: true,
});

const num = (v) => {
  if (v === undefined || v === null || v === "") return null;
  const n = parseFloat(String(v).replace(",", "."));
  return isNaN(n) ? null : n;
};

const int = (v) => {
  if (v === undefined || v === null || v === "") return null;
  const n = parseInt(String(v), 10);
  return isNaN(n) ? null : n;
};

const str = (v) => (v === undefined || v === null ? null : String(v));

function getChaveFromInfNFe(infNFe) {
  const id = infNFe?.["@_Id"];
  if (!id) return null;
  const match = id.match(/(\d{44})/);
  return match ? match[1] : null;
}

function consolidaICMS(icms) {
  if (!icms) return {};
  const inner = Object.values(icms)[0] || {};
  return {
    cst_icms: str(inner.CST || inner.CSOSN),
    bc_icms: num(inner.vBC),
    aliq_icms: num(inner.pICMS),
    valor_icms: num(inner.vICMS),
    origem: str(inner.orig),
    bc_st: num(inner.vBCST),
    aliq_st: num(inner.pICMSST),
    valor_icms_st: num(inner.vICMSST),
    p_mva_st: num(inner.pMVAST),
    bc_st_ret: num(inner.vBCSTRet),
    valor_st_ret: num(inner.vICMSSTRet),
    aliq_st_ret: num(inner.pST),
  };
}

function parseXmlToPayload(xmlContent, userId, estabelecimentoId, cnpjEstab) {
  const parsed = parser.parse(xmlContent);
  const nfe = parsed.nfeProc?.NFe || parsed.NFe;
  if (!nfe) return null;
  const infNFe = nfe.infNFe;
  if (!infNFe) return null;

  const chave = getChaveFromInfNFe(infNFe);
  if (!chave || chave.length !== 44) return null;

  const ide = infNFe.ide || {};
  const emit = infNFe.emit || {};
  const dest = infNFe.dest || {};
  const total = infNFe.total?.ICMSTot || {};

  const emitCnpj = (str(emit.CNPJ) || "").replace(/\D/g, "");
  const destCnpj = (str(dest.CNPJ) || str(dest.CPF) || "").replace(/\D/g, "");

  const tipoOperacao = emitCnpj === String(cnpjEstab) ? "saida" : "entrada";

  const detRaw = infNFe.det;
  const detArr = Array.isArray(detRaw) ? detRaw : detRaw ? [detRaw] : [];
  const itens = detArr.map((det) => {
    const nItem = int(det["@_nItem"]);
    const prod = det.prod || {};
    const imposto = det.imposto || {};
    const icmsConsol = consolidaICMS(imposto.ICMS);
    const ipi = imposto.IPI?.IPITrib || imposto.IPI?.IPINT || {};
    const pis = imposto.PIS?.PISAliq || imposto.PIS?.PISNT || imposto.PIS?.PISOutr || {};
    const cofins = imposto.COFINS?.COFINSAliq || imposto.COFINS?.COFINSNT || imposto.COFINS?.COFINSOutr || {};
    return {
      numero_item: nItem,
      codigo_produto: str(prod.cProd),
      codigo_barras: str(prod.cEAN === "SEM GTIN" ? null : prod.cEAN),
      descricao: str(prod.xProd) || "SEM DESCRIÇÃO",
      ncm: str(prod.NCM),
      cest: str(prod.CEST),
      cfop: str(prod.CFOP),
      unidade: str(prod.uCom),
      quantidade: num(prod.qCom),
      valor_unitario: num(prod.vUnCom),
      valor_total: num(prod.vProd),
      valor_desconto: num(prod.vDesc),
      ...icmsConsol,
      cst_ipi: str(ipi.CST),
      bc_ipi: num(ipi.vBC),
      aliq_ipi: num(ipi.pIPI),
      valor_ipi: num(ipi.vIPI),
      cst_pis: str(pis.CST),
      bc_pis: num(pis.vBC),
      aliq_pis: num(pis.pPIS),
      valor_pis: num(pis.vPIS),
      cst_cofins: str(cofins.CST),
      bc_cofins: num(cofins.vBC),
      aliq_cofins: num(cofins.pCOFINS),
      valor_cofins: num(cofins.vCOFINS),
    };
  });

  return {
    user_id: userId,
    estabelecimento_id: estabelecimentoId,
    chave_acesso: chave,
    numero_nf: int(ide.nNF),
    serie: int(ide.serie),
    data_emissao: str(ide.dhEmi || ide.dEmi),
    valor_total_nf: num(total.vNF),
    valor_produtos: num(total.vProd),
    valor_desconto: num(total.vDesc),
    valor_frete: num(total.vFrete),
    emit_cnpj: emitCnpj,
    emit_nome: str(emit.xNome),
    dest_cnpj: destCnpj,
    dest_nome: str(dest.xNome),
    bc_icms: num(total.vBC),
    valor_icms: num(total.vICMS),
    bc_icms_st: num(total.vBCST),
    valor_icms_st: num(total.vST),
    valor_ipi: num(total.vIPI),
    valor_pis: num(total.vPIS),
    valor_cofins: num(total.vCOFINS),
    bc_st_ret: num(total.vBCSTRet),
    valor_st_ret: num(total.vSTRet),
    valor_fcp: num(total.vFCP),
    valor_fcp_st: num(total.vFCPST),
    valor_fcp_st_ret: num(total.vFCPSTRet),
    tipo_operacao: tipoOperacao,
    origem: "xml_import",
    status: "autorizada",
    _itens: itens,
  };
}

async function processQueue(queue, context, stats, conf) {
  const RETRY_AFTER_429 = 15_000;
  const RETRY_AFTER_5XX = 30_000;
  const MAX_ATTEMPTS = 5;

  while (queue.length > 0) {
    const file = queue.shift();
    if (!file) return;

    let payload;
    try {
      const content = await readFile(file, "utf8");
      payload = parseXmlToPayload(content, context.userId, context.estabelecimentoId, context.cnpjEstab);
      if (!payload) {
        stats.parseErrors++;
        continue;
      }
    } catch (err) {
      stats.parseErrors++;
      continue;
    }

    for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
      const res = await rpc("process_single_xml", { p_nfe: payload }, context);
      
      if (res.status === 200 || res.status === 201) {
         if (res.body?.duplicate) stats.duplicates++;
         else if (res.body?.inserted) stats.inserted++;
         else stats.errors++;
         break;
      }

      if (res.status === 429) {
        stats.retry429++;
        await new Promise(r => setTimeout(r, RETRY_AFTER_429));
        continue;
      }

      if (res.status >= 500 || res.status === 0) {
        stats.retry5xx++;
        await new Promise(r => setTimeout(r, RETRY_AFTER_5XX));
        continue;
      }

      // Erro definitivo (ex: 400 bad request)
      stats.errors++;
      break;
    }
  }
}

/**
 * Processa um único arquivo XML não zipado
 */
export async function runXmlFile(xmlPath, options) {
  const { jwt, anonKey, supabaseUrl, userId, estabelecimentoId, cnpjEstab, config } = options;
  const context = { jwt, anonKey, supabaseUrl, userId, estabelecimentoId, cnpjEstab };
  const stats = { inserted: 0, duplicates: 0, errors: 0, parseErrors: 0, retry429: 0, retry5xx: 0 };
  
  await processQueue([xmlPath], context, stats, config);
  return stats;
}

/**
 * Extrai e processa todos os XMLs de um ZIP
 */
export async function runXmlZip(zipPath, options) {
  const { jwt, anonKey, supabaseUrl, userId, estabelecimentoId, cnpjEstab, config } = options;
  const concurrency = config?.xmlConcurrency || 3;
  
  const tmpDir = `/tmp/xml_import_${Date.now()}_${Math.floor(Math.random() * 10000)}`;
  
  // Extrai
  await mkdir(tmpDir, { recursive: true });
  execSync(`unzip -q -o "${zipPath}" -d "${tmpDir}"`);

  // Varre
  const xmlFiles = [];
  async function walk(target) {
    const entries = await readdir(target);
    for (const e of entries) {
      const full = join(target, e);
      const st = await stat(full);
      if (st.isDirectory()) await walk(full);
      else if (e.toLowerCase().endsWith(".xml")) xmlFiles.push(full);
    }
  }
  await walk(tmpDir);

  const stats = { inserted: 0, duplicates: 0, errors: 0, parseErrors: 0, retry429: 0, retry5xx: 0 };
  const context = { jwt, anonKey, supabaseUrl, userId, estabelecimentoId, cnpjEstab };
  
  const queue = [...xmlFiles];
  const workers = Array.from({ length: concurrency }, () => processQueue(queue, context, stats, config));
  
  await Promise.all(workers);

  // Cleanup
  try {
     await rm(tmpDir, { recursive: true, force: true });
  } catch (err) {
     console.warn(`[XML] Aviso ao limpar pasta temp ${tmpDir}: ${err.message}`);
  }

  return stats;
}
